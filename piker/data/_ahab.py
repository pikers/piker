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
    ContainerError,
)
from requests.exceptions import ConnectionError, ReadTimeout

from ..log import get_logger, get_console_log
from .. import config

log = get_logger(__name__)


class DockerNotStarted(Exception):
    'Prolly you dint start da daemon bruh'


class ContainerError(RuntimeError):
    'Error reported via app-container logging level'


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
        patt: str,
        bp_on_msg: bool = False,
    ) -> bool:
        '''
        Attempt to capture container log messages and relay through our
        native logging system.

        '''
        seen_so_far = self.seen_so_far

        while True:
            logs = self.cntr.logs()
            entries = logs.decode().split('\n')
            for entry in entries:

                # ignore null lines
                if not entry:
                    continue

                try:
                    record = json.loads(entry.strip())
                except json.JSONDecodeError:
                    if 'Error' in entry:
                        raise RuntimeError(entry)
                    raise

                msg = record['msg']
                level = record['level']
                if msg and entry not in seen_so_far:
                    seen_so_far.add(entry)
                    if bp_on_msg:
                        await tractor.breakpoint()

                    getattr(log, level, log.error)(f'{msg}')

                    # print(f'level: {level}')
                    if level in ('error', 'fatal'):
                        raise ContainerError(msg)

                if patt in msg:
                    return True

                # do a checkpoint so we don't block if cancelled B)
                await trio.sleep(0.01)

        return False

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
        log.error(
            f'Failed to kill container {cid} after {delay}s\n'
            'sending SIGKILL..'
        )
        # get out the big guns, bc apparently marketstore
        # doesn't actually know how to terminate gracefully
        # :eyeroll:...
        self.try_signal('SIGKILL')
        self.cntr.wait(
            timeout=3,
            condition='not-running',
        )

    async def cancel(
        self,
        stop_msg: str,
    ) -> None:

        cid = self.cntr.id
        # first try a graceful cancel
        log.cancel(
            f'SIGINT cancelling container: {cid}\n'
            f'waiting on stop msg: "{stop_msg}"'
        )
        self.try_signal('SIGINT')

        start = time.time()
        for _ in range(30):

            with trio.move_on_after(0.5) as cs:
                cs.shield = True
                await self.process_logs_until(stop_msg)

                # if we aren't cancelled on above checkpoint then we
                # assume we read the expected stop msg and terminated.
                break

            try:
                log.info(f'Polling for container shutdown:\n{cid}')

                if self.cntr.status not in {'exited', 'not-running'}:
                    self.cntr.wait(
                        timeout=0.1,
                        condition='not-running',
                    )

                break

            except (
                ReadTimeout,
            ):
                log.info(f'Still waiting on container:\n{cid}')
                continue

            except (
                docker.errors.APIError,
                ConnectionError,
            ):
                log.exception('Docker connection failure')
                self.hard_kill(start)
        else:
            self.hard_kill(start)

        log.cancel(f'Container stopped: {cid}')


@tractor.context
async def open_ahabd(
    ctx: tractor.Context,
    endpoint: str,  # ns-pointer str-msg-type

    **kwargs,

) -> None:
    get_console_log('info', name=__name__)

    async with open_docker() as client:

        # TODO: eventually offer a config-oriented API to do the mounts,
        # params, etc. passing to ``Containter.run()``?
        # call into endpoint for container config/init
        ep_func = NamespacePath(endpoint).load_ref()
        (
            dcntr,
            cntr_config,
            start_msg,
            stop_msg,
        ) = ep_func(client)
        cntr = Container(dcntr)

        with trio.move_on_after(1):
            found = await cntr.process_logs_until(start_msg)

            if not found and cntr not in client.containers.list():
                raise RuntimeError(
                    'Failed to start `marketstore` check logs deats'
                )

        await ctx.started((
            cntr.cntr.id,
            os.getpid(),
            cntr_config,
        ))

        try:

            # TODO: we might eventually want a proxy-style msg-prot here
            # to allow remote control of containers without needing
            # callers to have root perms?
            await trio.sleep_forever()

        finally:
            with trio.CancelScope(shield=True):
                await cntr.cancel(stop_msg)


async def start_ahab(
    service_name: str,
    endpoint: Callable[docker.DockerClient, DockerContainer],
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
        async with tractor.open_nursery(
            loglevel='runtime',
        ) as tn:

            portal = await tn.start_actor(
                service_name,
                enable_modules=[__name__]
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
