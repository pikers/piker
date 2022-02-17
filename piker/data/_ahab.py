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
from typing import (
    Optional,
    # Any,
)
from contextlib import asynccontextmanager as acm
from requests.exceptions import ConnectionError
# import time

import trio
from trio_typing import TaskStatus
import tractor
import docker
import json
from docker.models.containers import Container

from ..log import get_logger  # , get_console_log
from ..config import _config_dir

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
log_level: info
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

    try:
        client = docker.DockerClient(
            base_url=url,
            **kwargs
        ) if url else docker.from_env(**kwargs)
    except (
        ConnectionError,
        docker.errors.DockerException,
    ):
        raise DockerNotStarted('!?!?')

    try:
        yield client
    finally:
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
    # log = get_console_log('info', name=__name__)

    async with open_docker() as client:
        # create a mount from user's local piker config dir into container
        config_dir_mnt = docker.types.Mount(
            target='/etc',
            source=_config_dir,
            type='bind',
        )

        cntr: Container = client.containers.run(
            'alpacamarkets/marketstore:latest',
            # do we need this for cmds?
            # '-i',

            # '-p 5993:5993',
            ports={'5993/tcp': 5993},
            mounts=[config_dir_mnt],
            detach=True,
            # stop_signal='SIGINT',
            # init=True,
            # remove=True,
        )
        try:
            started: bool = False
            logs = cntr.logs(stream=True)

            with trio.move_on_after(0.5):
                for entry in logs:
                    entry = entry.decode()
                    try:
                        record = json.loads(entry.strip())
                    except json.JSONDecodeError:
                        if 'Error' in entry:
                            raise RuntimeError(entry)
                    msg = record['msg']

                    if "launching tcp listener for all services..." in msg:
                        started = True
                        break

                    await trio.sleep(0)

            if not started and cntr not in client.containers.list():
                raise RuntimeError(
                    'Failed to start `marketstore` check logs output for deats'
                )

            await ctx.started(cntr.id)
            await trio.sleep_forever()

        finally:
            cntr.stop()


async def start_ahab(
    task_status: TaskStatus[trio.Event] = trio.TASK_STATUS_IGNORED,

) -> None:

    cn_ready = trio.Event()
    task_status.started(cn_ready)
    async with tractor.open_nursery(
        loglevel='runtime',
    ) as tn:
        async with (
            (
                await tn.start_actor('ahab', enable_modules=[__name__])
            ).open_context(
                open_marketstore,
            ) as (ctx, first),
        ):
            assert str(first)

            # run till cancelled
            await trio.sleep_forever()


async def main():
    await start_ahab()
    await trio.sleep_forever()


if __name__ == '__main__':
    trio.run(main)
