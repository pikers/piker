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
# import time

import trio
from trio_typing import TaskStatus
import tractor
import docker
import json
from docker.models.containers import Container
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
            # for c in client.containers.list():
            #     c.kill()
            client.close()
            # client.api._custom_adapter.close()


# async def waitfor(
#     cntr: Container,
#     attr_path: tuple[str],
#     expect=None,
#     timeout: float = 0.5,

# ) -> Any:
#     '''
#     Wait for a container's attr value to be set. If ``expect`` is
#     provided wait for the value to be set to that value.

#     This is an async version of the helper from our ``pytest-dockerctl``
#     plugin.

#     '''
#     def get(val, path):
#         for key in path:
#             val = val[key]
#         return val

#     start = time.time()
#     while time.time() - start < timeout:
#         cntr.reload()
#         val = get(cntr.attrs, attr_path)
#         if expect is None and val:
#             return val
#         elif val == expect:
#             return val
#     else:
#         raise TimeoutError("{} failed to be {}, value: \"{}\"".format(
#             attr_path, expect if expect else 'not None', val))


@tractor.context
async def open_marketstore(
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
    log = get_console_log('info', name=__name__)

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

        cntr: Container = client.containers.run(
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
            stop_signal='SIGINT',
            init=True,
            # remove=True,
        )
        try:
            seen_so_far = set()

            async def process_logs_until(
                match: str,
                bp_on_msg: bool = False,
            ):
                logs = cntr.logs(stream=True)
                for entry in logs:
                    entry = entry.decode()

                    try:
                        record = json.loads(entry.strip())
                    except json.JSONDecodeError:
                        if 'Error' in entry:
                            raise RuntimeError(entry)

                    msg = record['msg']
                    if msg and entry not in seen_so_far:
                        seen_so_far.add(entry)
                        if bp_on_msg:
                            await tractor.breakpoint()
                        log.info(f'{msg}')

                    # if "launching tcp listener for all services..." in msg:
                    if match in msg:
                        return True

                    # do a checkpoint so we don't block if cancelled B)
                    await trio.sleep(0)

                return False

            with trio.move_on_after(0.5):
                found = await process_logs_until(
                    "launching tcp listener for all services...",
                )

                if not found and cntr not in client.containers.list():
                    raise RuntimeError(
                        'Failed to start `marketstore` check logs deats'
                    )

            await ctx.started(cntr.id)
            await process_logs_until('exiting...',)

        except (
            trio.Cancelled,
            KeyboardInterrupt,
        ):
            cntr.kill('SIGINT')
            with trio.move_on_after(0.5) as cs:
                cs.shield = True
                await process_logs_until('exiting...',)
            raise

        finally:
            try:
                cntr.wait(
                    timeout=0.5,
                    condition='not-running',
                )
            except (
                ReadTimeout,
                ConnectionError,
            ):
                cntr.kill()


async def start_ahab(
    service_name: str,
    task_status: TaskStatus[trio.Event] = trio.TASK_STATUS_IGNORED,

) -> None:

    cn_ready = trio.Event()
    async with tractor.open_nursery(
        loglevel='runtime',
    ) as tn:

        portal = await tn.start_actor(
            service_name,
            enable_modules=[__name__]
        )

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
            open_marketstore,
        ) as (ctx, first):

            assert str(first)
            # run till cancelled
            await trio.sleep_forever()


async def main():
    await start_ahab()
    await trio.sleep_forever()


if __name__ == '__main__':
    trio.run(main)
