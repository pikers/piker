# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
from typing import (
    Optional,
    # Any,
)
from contextlib import asynccontextmanager as acm

import trio
from trio_typing import TaskStatus
import tractor
import docker
import json
from docker.models.containers import Container as DockerContainer
from docker.errors import DockerException, APIError
from requests.exceptions import ConnectionError, ReadTimeout

from ..log import get_logger, get_console_log
from .. import config

log = get_logger(__name__)


_config = '''
# piker's ``marketstore`` config.

# mount this config using:
# sudo docker run --mount \
# type=bind,source="$HOME/.config/piker/",target="/etc" -i -p \
# 5993:5993 alpacamarkets/marketstore:latest

root_directory: data
listen_port: 5993
grpc_listen_port: 5995
log_level: debug
queryable: true
stop_grace_period: 0
wal_rotate_interval: 5
stale_threshold: 5
enable_add: true
enable_remove: false

triggers:
  - module: ondiskagg.so
    on: "*/1Sec/OHLCV"
    config:
        # filter: "nasdaq"
        destinations:
            - 1Min
            - 5Min
            - 15Min
            - 1H
            - 1D

  - module: stream.so
    on: '*/*/*'
    # config:
    #     filter: "nasdaq"

'''


class DockerNotStarted(Exception):
    'Prolly you dint start da daemon bruh'


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

    finally:
        if client:
            client.close()
            for c in client.containers.list():
                c.kill()


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

    async def cancel(
        self,
    ) -> None:

        cid = self.cntr.id
        self.try_signal('SIGINT')

        with trio.move_on_after(0.5) as cs:
            cs.shield = True
            await self.process_logs_until('initiating graceful shutdown')
            await self.process_logs_until('exiting...',)

        for _ in range(10):
            with trio.move_on_after(0.5) as cs:
                cs.shield = True
                await self.process_logs_until('exiting...',)
                break

            if cs.cancelled_caught:
                # get out the big guns, bc apparently marketstore
                # doesn't actually know how to terminate gracefully
                # :eyeroll:...
                self.try_signal('SIGKILL')

                try:
                    log.info('Waiting on container shutdown: {cid}')
                    self.cntr.wait(
                        timeout=0.1,
                        condition='not-running',
                    )
                    break

                except (
                    ReadTimeout,
                    ConnectionError,
                ):
                    log.error(f'failed to wait on container {cid}')
                    raise

        else:
            raise RuntimeError('Failed to cancel container {cid}')

        log.cancel(f'Container stopped: {cid}')


@tractor.context
async def open_marketstored(
    ctx: tractor.Context,
    **kwargs,

) -> None:
    '''
    Start and supervise a marketstore instance with its config bind-mounted
    in from the piker config directory on the system.

    The equivalent cli cmd to this code is:

        sudo docker run --mount \
        type=bind,source="$HOME/.config/piker/",target="/etc" -i -p \
        5993:5993 alpacamarkets/marketstore:latest

    '''
    get_console_log('info', name=__name__)

    async with open_docker() as client:

        # create a mount from user's local piker config dir into container
        config_dir_mnt = docker.types.Mount(
            target='/etc',
            source=config._config_dir,
            type='bind',
        )

        # create a user config subdir where the marketstore
        # backing filesystem database can be persisted.
        persistent_data_dir = os.path.join(
            config._config_dir, 'data',
        )
        if not os.path.isdir(persistent_data_dir):
            os.mkdir(persistent_data_dir)

        data_dir_mnt = docker.types.Mount(
            target='/data',
            source=persistent_data_dir,
            type='bind',
        )

        dcntr: DockerContainer = client.containers.run(
            'alpacamarkets/marketstore:latest',
            # do we need this for cmds?
            # '-i',

            # '-p 5993:5993',
            ports={
                '5993/tcp': 5993,  # jsonrpc
                '5995/tcp': 5995,  # grpc
            },
            mounts=[config_dir_mnt, data_dir_mnt],
            detach=True,
            # stop_signal='SIGINT',
            init=True,
            # remove=True,
        )
        cntr = Container(dcntr)

        with trio.move_on_after(1):
            found = await cntr.process_logs_until(
                "launching tcp listener for all services...",
            )

            if not found and cntr not in client.containers.list():
                raise RuntimeError(
                    'Failed to start `marketstore` check logs deats'
                )

        await ctx.started((cntr.cntr.id, os.getpid()))

        try:

            # TODO: we might eventually want a proxy-style msg-prot here
            # to allow remote control of containers without needing
            # callers to have root perms?
            await trio.sleep_forever()

        except (
            BaseException,
            # trio.Cancelled,
            # KeyboardInterrupt,
        ):

            with trio.CancelScope(shield=True):
                await cntr.cancel()

            raise


async def start_ahab(
    service_name: str,
    task_status: TaskStatus[trio.Event] = trio.TASK_STATUS_IGNORED,

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

            task_status.started(cn_ready)

            async with portal.open_context(
                open_marketstored,
            ) as (ctx, first):

                cid, pid = first

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
