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
from pprint import pformat
from typing import (
    Any,
    TYPE_CHECKING,
)

import pyqtgraph as pg
import numpy as np
import tractor


if TYPE_CHECKING:
    import docker
    from ._ahab import DockerContainer

from piker.log import (
    get_logger,
    get_console_log
)

import asks
import math


log = get_logger(__name__)


# container level config
_config = {
    'port': 19200,
    'log_level': 'debug',
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
            --mount type=bind,source="$(pwd)"/elastic,target=/usr/share/elasticsearch/data \
            --env "elastic_username=elastic" \
            --env "elastic_password=password" \
            --env "xpack.security.enabled=false" \
            elastic

    '''
    import docker
    get_console_log('info', name=__name__)

    dcntr: DockerContainer = client.containers.run(
        'piker:elastic',
        name='piker-elastic',
        network='host',
        detach=True,
        remove=True
    )

    async def start_matcher(msg: str):
        try:
            health = (await asks.get(
                f'http://localhost:19200/_cat/health',
                params={'format': 'json'}
            )).json()

        except OSError:
            log.error('couldnt reach elastic container')
            return False

        log.info(health)
        return health[0]['status'] == 'green'

    async def stop_matcher(msg: str):
        return msg == 'closed'

    return (
        dcntr,
        {},
        # expected startup and stop msgs
        start_matcher,
        stop_matcher,
    )



ES_HOST = 'http://localhost:19200'

shards = 2;
replicas = 0;
refresh = '1s';

compression = 'best_compression';

default_idx_settings = {
    'index': {
        'number_of_shards': shards,
        'refresh_interval': refresh,
        'number_of_replicas': replicas,
        'codec': compression
    }
};

oi_mapping = {
    'order': 0,
    'index_patterns': [
        '*-oi'
    ],
    'settings': default_idx_settings,
    'mappings': {
        'properties': {
            'timestamp': {'type': 'date'},
            'open_interest': {'type': 'double'}
        }
    }
};

trades_mapping = {
    'order': 0,
    'index_patterns': [
        '*-trades'
    ],
    'settings': default_idx_settings,
    'mappings': {
        'properties': {
            'direction': {'type': 'keyword'},
            'amount': {'type': 'double'},
            'price': {'type': 'double'},
            'timestamp': {'type': 'date'}
        }
    }
};

max_pain_mapping = {
    'order': 0,
    'index_patterns': [
        '*-max-pain'
    ],
    'settings': default_idx_settings,
    'mappings': {
        'properties': {
            'max_pain': {'type': 'double'},
            'dollar_value': {'type': 'double'},
            'timestamp': {'type': 'date'}
        }
    }
}


def es_prefix(instrument_name, kind):
    return f'{instrument_name.lower()}-{kind}'