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

'''
``elasticsearch`` integration.

- client management routines
- ticK data ingest routines
- websocket client for subscribing to write triggers
- todo: tick sequence stream-cloning for testing

'''
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from pprint import pformat
from typing import (
    Any,
    # Optional,
    #Union,
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

from elasticsearch import Elasticsearch


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
        network='host',
        detach=True,
        remove=True,
    )

    return (
        dcntr,
        {},
        # expected startup and stop msgs
        lambda start_msg: start_msg == "started",
        lambda stop_msg: stop_msg == "closed",
    )
