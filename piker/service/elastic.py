# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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

from __future__ import annotations
from contextlib import asynccontextmanager as acm
from typing import (
    Any,
    TYPE_CHECKING,
)

import asks

if TYPE_CHECKING:
    import docker
    from ._ahab import DockerContainer

from ._util import log  # sub-sys logger
from ._util import (
    get_console_log,
)


# container level config
_config = {
    'port': 19200,
    'log_level': 'debug',

    # hardcoded to our image version
    'version': '7.17.4',
}


def start_elasticsearch(
    client: docker.DockerClient,

    **kwargs,

) -> tuple[DockerContainer, dict[str, Any]]:
    '''
    Start and supervise an elasticsearch instance with its config bind-mounted
    in from the piker config directory on the system.

    The equivalent cli cmd to this code is:

        sudo docker run \
            -itd \
            --rm \
            --network=host \
            --mount type=bind,source="$(pwd)"/elastic,\
              target=/usr/share/elasticsearch/data \
            --env "elastic_username=elastic" \
            --env "elastic_password=password" \
            --env "xpack.security.enabled=false" \
            elastic

    '''
    get_console_log('info', name=__name__)

    dcntr: DockerContainer = client.containers.run(
        'piker:elastic',
        name='piker-elastic',
        network='host',
        detach=True,
        remove=True
    )

    async def health_query(msg: str | None = None):
        if (
            msg
            and _config['version'] in msg
        ):
            return True

        try:
            health = (await asks.get(
                'http://localhost:19200/_cat/health',
                params={'format': 'json'}
            )).json()
            log.info(
                'ElasticSearch cntr health:\n'
                f'{health}'
            )

        except OSError:
            log.exception('couldnt reach elastic container')
            return False

        log.info(health)
        return health[0]['status'] == 'green'

    async def chk_for_closed_msg(msg: str):
        return msg == 'closed'

    return (
        dcntr,
        {
            # apparently we're REALLY tolerant of startup latency
            # for CI XD
            'startup_timeout': 240.0,

            # XXX: decrease http poll period bc docker
            # is shite at handling fast poll rates..
            'startup_query_period': 0.1,

            'log_msg_key': 'message',

            # 'started_afunc': health_query,
        },
        # expected startup and stop msgs
        health_query,
        chk_for_closed_msg,
    )


@acm
async def start_ahab_daemon(
    service_mngr: Services,
    user_config: dict | None = None,
    loglevel: str | None = None,

) -> tuple[str, dict]:
    '''
    Task entrypoint to start the estasticsearch docker container using
    the service manager.

    '''
    from ._ahab import start_ahab_service

    # dict-merge any user settings
    conf: dict = _config.copy()
    if user_config:
        conf = conf | user_config

    dname: str = 'esd'
    log.info(f'Spawning `{dname}` supervisor')
    async with start_ahab_service(
        service_mngr,
        dname,

        # NOTE: docker-py client is passed at runtime
        start_elasticsearch,
        ep_kwargs={'user_config': conf},
        loglevel=loglevel,

    ) as (
        ctn_ready,
        config,
        (cid, pid),
    ):
        log.info(
            f'`{dname}` up!\n'
            f'pid: {pid}\n'
            f'container id: {cid[:12]}\n'
            f'config: {pformat(config)}'
        )
        yield dname, conf
