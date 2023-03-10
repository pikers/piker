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
from typing import (
    Any,
    TYPE_CHECKING,
)


if TYPE_CHECKING:
    import docker
    from ._ahab import DockerContainer

from piker.log import (
    get_logger,
    get_console_log
)

import asks


log = get_logger(__name__)


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
            kog.info(
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
