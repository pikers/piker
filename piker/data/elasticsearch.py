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
from datetime import datetime
from pprint import pformat
from typing import (
    Any,
    Optional,
    Union,
    TYPE_CHECKING,
)
import time
from math import isnan

from bidict import bidict
from msgspec.msgpack import encode, decode
import pyqtgraph as pg
import numpy as np
import tractor
from trio_websocket import open_websocket_url
import pendulum
import purerpc

if TYPE_CHECKING:
    import docker
    from ._ahab import DockerContainer

from .feed import maybe_open_feed
from ..log import get_logger, get_console_log
from .._profile import Profiler

from elasticsearch import Elasticsearch
from docker.types import LogConfig

log = get_logger(__name__)


# container level config
_config = {
    'port': 9200,
    'log_level': 'debug',
}

_yaml_config = '''
# piker's ``elasticsearch`` config.

# mount this config using:
# sudo docker run \
    # -itd \
    # --rm \
    # --network=host \
    # --mount type=bind,source="$(pwd)"/elastic,target=/usr/share/elasticsearch/data \
    # --env "elastic_username=elastic" \
    # --env "elastic_password=password" \
    # --env "xpack.security.enabled=false" \
    # elastic

root_directory: data
port: {port}
log_level: {log_level}
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

'''.format(**_config)


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
    import os
    import docker
    from .. import config
    get_console_log('info', name=__name__)

    esdir = os.path.join(config._config_dir, 'elasticsearch')

    # create dirs when dne
    if not os.path.isdir(config._config_dir):
        os.mkdir(config._config_dir)

    if not os.path.isdir(esdir):
        os.mkdir(esdir)

    yml_file = os.path.join(esdir, 'es.yml')
    if not os.path.isfile(yml_file):
        log.warning(
            f'No `elasticsearch` config exists?: {yml_file}\n'
            'Generating new file from template:\n'
            f'{_yaml_config}\n'
        )
        with open(yml_file, 'w') as yf:
            yf.write(_yaml_config)

    # create a mount from user's local piker config dir into container
    config_dir_mnt = docker.types.Mount(
        target='/etc',
        source=esdir,
        type='bind',
    )

    # create a user config subdir where the elasticsearch
    # backing filesystem database can be persisted.
    persistent_data_dir = os.path.join(
        esdir, 'data',
    )
    if not os.path.isdir(persistent_data_dir):
        os.mkdir(persistent_data_dir)

    data_dir_mnt = docker.types.Mount(
        target='/data',
        source=persistent_data_dir,
        type='bind',
    )

    dcntr: DockerContainer = client.containers.run(
        'elastic',
        ports={
            '9200':9200,
        },
        mounts=[
            config_dir_mnt,
            data_dir_mnt,
        ],
        # log_config=log_cf,
        detach=True,
        # stop_signal='SIGINT',
        # init=True,
        # remove=True,
    )

    return (
        dcntr,
        _config,

        # expected startup and stop msgs
        "launching listener for all services...",
        "exiting...",
    )


_tick_tbk_ids: tuple[str, str] = ('1Sec', 'TICK')
_tick_tbk: str = '{}/' + '/'.join(_tick_tbk_ids)

_tick_dt = [
    # these two are required for as a "primary key"
    ('Epoch', 'i8'),
    ('Nanoseconds', 'i4'),
    ('IsTrade', 'i1'),
    ('IsBid', 'i1'),
    ('Price', 'f4'),
    ('Size', 'f4')
]

_quote_dt = [
    # these two are required for as a "primary key"
    ('Epoch', 'i8'),
    ('Nanoseconds', 'i4'),

    ('Tick', 'i4'),  # (-1, 0, 1) = (on bid, same, on ask)
    # ('fill_time', 'f4'),
    ('Last', 'f4'),
    ('Bid', 'f4'),
    ('Bsize', 'i8'),
    ('Asize', 'i8'),
    ('Ask', 'f4'),
    ('Size', 'i8'),
    ('Volume', 'i8'),
    # ('brokerd_ts', 'i64'),
    # ('VWAP', 'f4')
]

_quote_tmp = {}.fromkeys(dict(_quote_dt).keys(), np.nan)
_tick_map = {
    'Up': 1,
    'Equal': 0,
    'Down': -1,
    None: np.nan,
}

_ohlcv_dt = [
    # these two are required for as a "primary key"
    ('Epoch', 'i8'),
    # ('Nanoseconds', 'i4'),

    # ohlcv sampling
    ('Open', 'f4'),
    ('High', 'f4'),
    ('Low', 'f4'),
    ('Close', 'f4'),
    ('Volume', 'f4'),
]


ohlc_key_map = bidict({
    'Epoch': 'time',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
})

# # map of seconds ints to "time frame" accepted keys
tf_in_1s = bidict({
    1: '1Sec',
    60: '1Min',
    60*5: '5Min',
    60*15: '15Min',
    60*30: '30Min',
    60*60: '1H',
    60*60*24: '1D',
})
