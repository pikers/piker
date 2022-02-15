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
from typing import Optional
from contextlib import contextmanager as cm
# import time

import trio
import tractor
import docker
import json
# from docker.containers import Container
from requests import ConnectionError

from ..log import get_logger, get_console_log

log = get_logger(__name__)


_config = '''
# mount this config using:
# sudo docker run --mount type=bind,source="$HOME/.config/piker/",target="/etc" -i -p 5993:5993 alpacamarkets/marketstore:latest
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


@cm
def open_docker(
    url: Optional[str] = None,
    **kwargs,

) -> docker.DockerClient:

    # yield docker.Client(
    #     base_url=url,
    #     **kwargs
    # ) if url else
    yield docker.from_env(**kwargs)


@tractor.context
async def open_marketstore_container(
    ctx: tractor.Context,
    **kwargs,

) -> None:
    log = get_console_log('info', name=__name__)
    # this cli should "just work"
    # sudo docker run --mount
    # type=bind,source="$HOME/.config/piker/",target="/etc" -i -p
    # 5993:5993 alpacamarkets/marketstore:latest
    client = docker.from_env(**kwargs)

    # with open_docker() as client:
    ctnr = client.containers.run(
        'alpacamarkets/marketstore:latest',
        [
            '--mount',
            'type=bind,source="$HOME/.config/piker/",target="/etc"',
            '-i',
            '-p 5993:5993',
        ],
        detach=True,
    )
    started: bool = False
    logs = ctnr.logs(stream=True)

    with trio.move_on_after(0.5):
        for entry in logs:
            entry = entry.decode()
            try:
                record = json.loads(entry.strip())
            except json.JSONDecodeError:
                if 'Error' in entry:
                    raise RuntimeError(entry)
                # await tractor.breakpoint()
            msg = record['msg']

            if "launching tcp listener for all services..." in msg:
                started = True
                break

            await trio.sleep(0)

    if not started and ctnr not in client.containers.list():
        raise RuntimeError(
            'Failed to start `marketstore` check logs output for deats'
        )

    await ctx.started()
    await tractor.breakpoint()


async def main():
    async with tractor.open_nursery(
        loglevel='info',
    ) as tn:
        portal = await tn.start_actor('ahab', enable_modules=[__name__])

        async with portal.open_context(
            open_marketstore_container

        ) as (first, ctx):
            await trio.sleep_forever()

if __name__ == '__main__':
    trio.run(main)
