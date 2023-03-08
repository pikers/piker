# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of pikers)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

'''
Supervisor for docker with included specific-image service helpers.

'''
from collections import ChainMap
from functools import partial
import os
import time
from typing import (
    Optional,
    Callable,
    Any,
)
from contextlib import asynccontextmanager as acm

import trio
from trio_typing import TaskStatus
import tractor
from tractor.msg import NamespacePath
import docker
import json
from docker.models.containers import Container as DockerContainer
from docker.errors import (
    DockerException,
    APIError,
    # ContainerError,
)
import requests
from requests.exceptions import (
    ConnectionError,
    ReadTimeout,
)

from ..log import (
    get_logger,
    get_console_log,
)
from .. import config

log = get_logger(__name__)


class DockerNotStarted(Exception):
    'Prolly you dint start da daemon bruh'


class ApplicationLogError(Exception):
    'App in container reported an error in logs'


@acm
async def open_docker(
    url: Optional[str] = None,
    **kwargs,

) -> docker.DockerClient:

    client: Optional[docker.DockerClient] = None
    try:
        client = docker.DockerClient(
            base_url=url,
            **kwargs
        ) if url else docker.from_env(**kwargs)

        yield client

    except (
        DockerException,
        APIError,
    ) as err:

        def unpack_msg(err: Exception) -> str:
            args = getattr(err, 'args', None)
            if args:
                return args
            else:
                return str(err)

        # could be more specific so let's check if it's just perms.
        if err.args:
            errs = err.args
            for err in errs:
                msg = unpack_msg(err)
                if 'PermissionError' in msg:
                    raise DockerException('You dint run as root yo!')

                elif 'FileNotFoundError' in msg:
                    raise DockerNotStarted('Did you start da service sister?')

        # not perms?
        raise

    # finally:
    #     if client:
    #         client.close()


class Container:
    '''
    Wrapper around a ``docker.models.containers.Container`` to include
    log capture and relay through our native logging system and helper
    method(s) for cancellation/teardown.

    '''
    def __init__(
        self,
        cntr: DockerContainer,
    ) -> None:

        self.cntr = cntr
        # log msg de-duplication
        self.seen_so_far = set()

    async def process_logs_until(
        self,
        log_msg_key: str,

        # this is a predicate func for matching log msgs emitted by the
        # underlying containerized app
        patt_matcher: Callable[[str], bool],

        # XXX WARNING XXX: do not touch this sleep value unless
        # you know what you are doing! the value is critical to
        # making sure the caller code inside the startup context
        # does not timeout BEFORE we receive a match on the
        # ``patt_matcher()`` predicate above.
        checkpoint_period: float = 0.001,

    ) -> bool:
        '''
        Attempt to capture container log messages and relay through our
        native logging system.

        '''
        seen_so_far = self.seen_so_far

        while True:
            logs = self.cntr.logs()
            try:
                logs = self.cntr.logs()
            except (
                docker.errors.NotFound,
                docker.errors.APIError
            ):
                log.exception('Failed to parse logs?')
                return False

            entries = logs.decode().split('\n')
            for entry in entries:

                # ignore null lines
                if not entry:
                    continue

                entry = entry.strip()
                try:
                    record = json.loads(entry)
                    msg = record[log_msg_key]
                    level = record['level']

                except json.JSONDecodeError:
                    msg = entry
                    level = 'error'

                # TODO: do we need a more general mechanism
                # for these kinda of "log record entries"?
                # if 'Error' in entry:
                #     raise RuntimeError(entry)

                if (
                    msg
                    and entry not in seen_so_far
                ):
                    seen_so_far.add(entry)
                    getattr(log, level.lower(), log.error)(f'{msg}')

                    if level == 'fatal':
                        raise ApplicationLogError(msg)

                if await patt_matcher(msg):
                    return True

                # do a checkpoint so we don't block if cancelled B)
                await trio.sleep(checkpoint_period)

        return False

    @property
    def cuid(self) -> str:
        fqcn: str = self.cntr.attrs['Config']['Image']
        return f'{fqcn}[{self.cntr.short_id}]'

    def try_signal(
        self,
        signal: str = 'SIGINT',

    ) -> bool:
        try:
            # XXX: market store doesn't seem to shutdown nicely all the
            # time with this (maybe because there are still open grpc
            # connections?) noticably after client connections have been
            # made or are in use/teardown. It works just fine if you
            # just start and stop the container tho?..
            log.cancel(f'SENDING {signal} to {self.cntr.id}')
            self.cntr.kill(signal)
            return True

        except docker.errors.APIError as err:
            if 'is not running' in err.explanation:
                return False

    def hard_kill(self, start: float) -> None:
        delay = time.time() - start
        # get out the big guns, bc apparently marketstore
        # doesn't actually know how to terminate gracefully
        # :eyeroll:...
        log.error(
            f'SIGKILL-ing: {self.cntr.id} after {delay}s\n'
        )
        self.try_signal('SIGKILL')
        self.cntr.wait(
            timeout=3,
            condition='not-running',
        )

    async def cancel(
        self,
        log_msg_key: str,
        stop_predicate: Callable[[str], bool],

        hard_kill: bool = False,

    ) -> None:
        '''
        Attempt to cancel this container gracefully, fail over to
        a hard kill on timeout.

        '''
        cid = self.cntr.id

        # first try a graceful cancel
        log.cancel(
            f'SIGINT cancelling container: {self.cuid}\n'
            'waiting on stop predicate...'
        )
        self.try_signal('SIGINT')

        start = time.time()
        for _ in range(6):

            with trio.move_on_after(0.5) as cs:
                log.cancel('polling for CNTR logs...')

                try:
                    await self.process_logs_until(
                        log_msg_key,
                        stop_predicate,
                    )
                except ApplicationLogError:
                    hard_kill = True
                else:
                    # if we aren't cancelled on above checkpoint then we
                    # assume we read the expected stop msg and
                    # terminated.
                    break

            if cs.cancelled_caught:
                # on timeout just try a hard kill after
                # a quick container sync-wait.
                hard_kill = True

            try:
                log.info(f'Polling for container shutdown:\n{cid}')

                if self.cntr.status not in {'exited', 'not-running'}:
                    self.cntr.wait(
                        timeout=0.1,
                        condition='not-running',
                    )

                # graceful exit if we didn't time out
                break

            except (
                ReadTimeout,
            ):
                log.info(f'Still waiting on container:\n{cid}')
                continue

            except (
                docker.errors.APIError,
                ConnectionError,
                requests.exceptions.ConnectionError,
                trio.Cancelled,
            ):
                log.exception('Docker connection failure')
                self.hard_kill(start)
                raise

            except trio.Cancelled:
                log.exception('trio cancelled...')
                self.hard_kill(start)
        else:
            hard_kill = True

        if hard_kill:
            self.hard_kill(start)
        else:
            log.cancel(f'Container stopped: {cid}')


@tractor.context
async def open_ahabd(
    ctx: tractor.Context,
    endpoint: str,  # ns-pointer str-msg-type
    loglevel: str | None = 'cancel',

    **kwargs,

) -> None:

    log = get_console_log(
        loglevel,
        name=__name__,
    )

    async with open_docker() as client:

        # TODO: eventually offer a config-oriented API to do the mounts,
        # params, etc. passing to ``Containter.run()``?
        # call into endpoint for container config/init
        ep_func = NamespacePath(endpoint).load_ref()
        (
            dcntr,
            cntr_config,
            start_lambda,
            stop_lambda,
        ) = ep_func(client)
        cntr = Container(dcntr)

        conf: ChainMap[str, Any] = ChainMap(

            # container specific
            cntr_config,

            # defaults
            {
                # startup time limit which is the max the supervisor
                # will wait for the container to be registered in
                # ``client.containers.list()``
                'startup_timeout': 1.0,

                # how fast to poll for the starup predicate by sleeping
                # this amount incrementally thus yielding to the
                # ``trio`` scheduler on during sync polling execution.
                'startup_query_period': 0.001,

                # str-key value expected to contain log message body-contents
                # when read using:
                # ``json.loads(entry for entry in DockerContainer.logs())``
                'log_msg_key': 'msg',
            },
        )

        with trio.move_on_after(conf['startup_timeout']) as cs:
            async with trio.open_nursery() as tn:
                tn.start_soon(
                    partial(
                        cntr.process_logs_until,
                        log_msg_key=conf['log_msg_key'],
                        patt_matcher=start_lambda,
                        checkpoint_period=conf['startup_query_period'],
                    )
                )

                # poll for container startup or timeout
                while not cs.cancel_called:
                    if dcntr in client.containers.list():
                        break

                    await trio.sleep(conf['startup_query_period'])

                # sync with remote caller actor-task but allow log
                # processing to continue running in bg.
                await ctx.started((
                    cntr.cntr.id,
                    os.getpid(),
                    cntr_config,
                ))

        try:
            # XXX: if we timeout on finding the "startup msg" we expect then
            # we want to FOR SURE raise an error upwards!
            if cs.cancelled_caught:
                # if dcntr not in client.containers.list():
                for entry in cntr.seen_so_far:
                    log.info(entry)

                raise DockerNotStarted(
                    f'Failed to start container: {cntr.cuid}\n'
                    f'due to startup_timeout={conf["startup_timeout"]}s\n\n'
                    "prolly you should check your container's logs for deats.."
                )

            # TODO: we might eventually want a proxy-style msg-prot here
            # to allow remote control of containers without needing
            # callers to have root perms?
            await trio.sleep_forever()

        finally:
            # TODO: ensure loglevel can be set and teardown logs are
            # reported if possible on error or cancel..
            with trio.CancelScope(shield=True):
                await cntr.cancel(
                    log_msg_key=conf['log_msg_key'],
                    stop_predicate=stop_lambda,
                )


async def start_ahab(
    service_name: str,
    endpoint: Callable[docker.DockerClient, DockerContainer],
    loglevel: str | None = 'cancel',

    task_status: TaskStatus[
        tuple[
            trio.Event,
            dict[str, Any],
        ],
    ] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Start a ``docker`` container supervisor with given service name.

    Currently the actor calling this task should normally be started
    with root permissions (until we decide to use something that doesn't
    require this, like docker's rootless mode or some wrapper project) but
    te root perms are de-escalated after the docker supervisor sub-actor
    is started.

    '''
    cn_ready = trio.Event()
    try:
        async with tractor.open_nursery() as an:

            portal = await an.start_actor(
                service_name,
                enable_modules=[__name__],
                loglevel=loglevel,
            )

            # TODO: we have issues with this on teardown
            # where ``tractor`` tries to issue ``os.kill()``
            # and hits perms errors since the root process
            # doesn't any longer have root perms..

            # de-escalate root perms to the original user
            # after the docker supervisor actor is spawned.
            if config._parent_user:
                import pwd
                os.setuid(
                    pwd.getpwnam(
                        config._parent_user
                    )[2]  # named user's uid
                )

            async with portal.open_context(
                open_ahabd,
                endpoint=str(NamespacePath.from_ref(endpoint)),
                loglevel='cancel',
            ) as (ctx, first):

                cid, pid, cntr_config = first

                task_status.started((
                    cn_ready,
                    cntr_config,
                    (cid, pid),
                ))

                await trio.sleep_forever()

    # since we demoted root perms in this parent
    # we'll get a perms error on proc cleanup in
    # ``tractor`` nursery exit. just make sure
    # the child is terminated and don't raise the
    # error if so.

    # TODO: we could also consider adding
    # a ``tractor.ZombieDetected`` or something that we could raise
    # if we find the child didn't terminate.
    except PermissionError:
        log.warning('Failed to cancel root permsed container')

    except (
        trio.MultiError,
    ) as err:
        for subexc in err.exceptions:
            if isinstance(subexc, PermissionError):
                log.warning('Failed to cancel root perms-ed container')
                return
        else:
            raise
