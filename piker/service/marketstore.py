# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
``marketstore`` integration.

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
    TYPE_CHECKING,
)
import time
from math import isnan
from pathlib import Path

from bidict import bidict
from msgspec.msgpack import (
    encode,
    decode,
)
# import pyqtgraph as pg
import numpy as np
import tractor
from trio_websocket import open_websocket_url
from anyio_marketstore import (  # noqa
    open_marketstore_client,
    MarketstoreClient,
    Params,
)
import pendulum
# TODO: import this for specific error set expected by mkts client
# import purerpc

from ..data.feed import maybe_open_feed
from . import Services
from ._util import (
    log,  # sub-sys logger
    get_console_log,
)

if TYPE_CHECKING:
    import docker
    from ._ahab import DockerContainer



# ahabd-supervisor and container level config
_config = {
    'grpc_listen_port': 5995,
    'ws_listen_port': 5993,
    'log_level': 'debug',
    'startup_timeout': 2,
}

_yaml_config_str: str = '''
# piker's ``marketstore`` config.

# mount this config using:
# sudo docker run --mount \
# type=bind,source="$HOME/.config/piker/",target="/etc" -i -p \
# 5993:5993 alpacamarkets/marketstore:latest

root_directory: data
listen_port: {ws_listen_port}
grpc_listen_port: {grpc_listen_port}
log_level: {log_level}
queryable: true
stop_grace_period: 0
wal_rotate_interval: 5
stale_threshold: 5
enable_add: true
enable_remove: false

# SUPER DUPER CRITICAL to address a super weird issue:
# https://github.com/pikers/piker/issues/443
# seems like "variable compression" is possibly borked
# or snappy compression somehow breaks easily?
disable_variable_compression: true

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


def start_marketstore(
    client: docker.DockerClient,
    user_config: dict,
    **kwargs,

) -> tuple[DockerContainer, dict[str, Any]]:
    '''
    Start and supervise a marketstore instance with its config
    bind-mounted in from the piker config directory on the system.

    The equivalent cli cmd to this code is:

        sudo docker run --mount \
        type=bind,source="$HOME/.config/piker/",target="/etc" -i -p \
        5993:5993 alpacamarkets/marketstore:latest

    '''
    import os
    import docker
    from .. import config
    get_console_log('info', name=__name__)

    mktsdir = os.path.join(config._config_dir, 'marketstore')

    # create dirs when dne
    if not os.path.isdir(config._config_dir):
        Path(config._config_dir).mkdir(parents=True, exist_ok=True)

    if not os.path.isdir(mktsdir):
        os.mkdir(mktsdir)

    yml_file = os.path.join(mktsdir, 'mkts.yml')
    yaml_config = _yaml_config_str.format(**user_config)

    if not os.path.isfile(yml_file):
        log.warning(
            f'No `marketstore` config exists?: {yml_file}\n'
            'Generating new file from template:\n'
            f'{yaml_config}\n'
        )
        with open(yml_file, 'w') as yf:
            yf.write(yaml_config)

    # create a mount from user's local piker config dir into container
    config_dir_mnt = docker.types.Mount(
        target='/etc',
        source=mktsdir,
        type='bind',
    )

    # create a user config subdir where the marketstore
    # backing filesystem database can be persisted.
    persistent_data_dir = os.path.join(
        mktsdir, 'data',
    )
    if not os.path.isdir(persistent_data_dir):
        os.mkdir(persistent_data_dir)

    data_dir_mnt = docker.types.Mount(
        target='/data',
        source=persistent_data_dir,
        type='bind',
    )

    grpc_listen_port = int(user_config['grpc_listen_port'])
    ws_listen_port = int(user_config['ws_listen_port'])

    dcntr: DockerContainer = client.containers.run(
        'alpacamarkets/marketstore:latest',
        # do we need this for cmds?
        # '-i',

        # '-p 5993:5993',
        ports={
            f'{ws_listen_port}/tcp': ws_listen_port,
            f'{grpc_listen_port}/tcp': grpc_listen_port,
        },
        mounts=[
            config_dir_mnt,
            data_dir_mnt,
        ],

        # XXX: this must be set to allow backgrounding/non-blocking
        # usage interaction with the container's process.
        detach=True,

        # stop_signal='SIGINT',
        init=True,
        # remove=True,
    )

    async def start_matcher(msg: str):
        return "launching tcp listener for all services..." in msg

    async def stop_matcher(msg: str):
        return (
            # not sure when this happens, some kinda stop condition
            "exiting..." in msg

            # after we send SIGINT..
            or "initiating graceful shutdown due to 'interrupt' request" in msg
        )

    return (
        dcntr,
        _config,

        # expected startup and stop msgs
        start_matcher,
        stop_matcher,
    )


@acm
async def start_ahab_daemon(
    service_mngr: Services,
    user_config: dict | None = None,
    loglevel: str | None = None,

) -> tuple[str, dict]:
    '''
    Task entrypoint to start the marketstore docker container using the
    service manager.

    '''
    from ._ahab import start_ahab_service

    # dict-merge any user settings
    conf: dict = _config.copy()
    if user_config:
        conf: dict = conf | user_config

    dname: str = 'marketstored'
    log.info(f'Spawning `{dname}` supervisor')
    async with start_ahab_service(
        service_mngr,
        dname,

        # NOTE: docker-py client is passed at runtime
        start_marketstore,
        ep_kwargs={'user_config': conf},
        loglevel=loglevel,
    ) as (
        _,
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


def mk_tbk(keys: tuple[str, str, str]) -> str:
    '''
    Generate a marketstore table key from a tuple.
    Converts,
        ``('SPY', '1Sec', 'TICK')`` -> ``"SPY/1Sec/TICK"```

    '''
    return '/'.join(keys)


def quote_to_marketstore_structarray(
    quote: dict[str, Any],
    last_fill: float | None,

) -> np.array:
    '''
    Return marketstore writeable structarray from quote ``dict``.

    '''
    if last_fill:
        # new fill bby
        now = int(pendulum.parse(last_fill).timestamp)
    else:
        # this should get inserted upstream by the broker-client to
        # subtract from IPC latency
        now = time.time_ns()

    secs, ns = now / 10**9, now % 10**9

    # pack into list[tuple[str, Any]]
    array_input = []

    # insert 'Epoch' entry first and then 'Nanoseconds'.
    array_input.append(int(secs))
    array_input.append(int(ns))

    # append remaining fields
    for name, dt in _quote_dt[2:]:
        if 'f' in dt:
            none = np.nan
        else:
            # for ``np.int`` we use 0 as a null value
            none = 0

        # casefold? see https://github.com/alpacahq/marketstore/issues/324
        val = quote.get(name.casefold(), none)
        array_input.append(val)

    return np.array([tuple(array_input)], dtype=_quote_dt)


@acm
async def get_client(
    host: str | None,
    port: int | None,

) -> MarketstoreClient:
    '''
    Load a ``anyio_marketstore`` grpc client connected
    to an existing ``marketstore`` server.

    '''
    async with open_marketstore_client(
        host or 'localhost',
        port or _config['grpc_listen_port'],
    ) as client:
        yield client


class MarketStoreError(Exception):
    "Generic marketstore client error"


# def err_on_resp(response: dict) -> None:
#     """Raise any errors found in responses from client request.
#     """
#     responses = response['responses']
#     if responses is not None:
#         for r in responses:
#             err = r['error']
#             if err:
#                 raise MarketStoreError(err)


# map of seconds ints to "time frame" accepted keys
tf_in_1s = bidict({
    1: '1Sec',
    60: '1Min',
    60*5: '5Min',
    60*15: '15Min',
    60*30: '30Min',
    60*60: '1H',
    60*60*24: '1D',
})


async def ingest_quote_stream(
    symbols: list[str],
    brokername: str,
    tries: int = 1,
    loglevel: str = None,

) -> None:
    '''
    Ingest a broker quote stream into a ``marketstore`` tsdb.

    '''
    async with (
        maybe_open_feed(brokername, symbols, loglevel=loglevel) as feed,
        get_client() as ms_client,
    ):
        async for quotes in feed.stream:
            log.info(quotes)
            for symbol, quote in quotes.items():
                for tick in quote.get('ticks', ()):
                    ticktype = tick.get('type', 'n/a')

            # techtonic tick write
            array = quote_to_marketstore_structarray({
                'IsTrade': 1 if ticktype == 'trade' else 0,
                'IsBid': 1 if ticktype in ('bid', 'bsize') else 0,
                'Price': tick.get('price'),
                'Size': tick.get('size')
            }, last_fill=quote.get('broker_ts', None))

            await ms_client.write(array, _tick_tbk)

            # LEGACY WRITE LOOP (using old tick dt)
            # quote_cache = {
            #     'size': 0,
            #     'tick': 0
            # }

            # async for quotes in qstream:
            #     log.info(quotes)
            #     for symbol, quote in quotes.items():

            #         # remap tick strs to ints
            #         quote['tick'] = _tick_map[quote.get('tick', 'Equal')]

            #         # check for volume update (i.e. did trades happen
            #         # since last quote)
            #         new_vol = quote.get('volume', None)
            #         if new_vol is None:
            #             log.debug(f"No fills for {symbol}")
            #             if new_vol == quote_cache.get('volume'):
            #                 # should never happen due to field diffing
            #                 # on sender side
            #                 log.error(
            #                     f"{symbol}: got same volume as last quote?")

            #         quote_cache.update(quote)

            #         a = quote_to_marketstore_structarray(
            #             quote,
            #             # TODO: check this closer to the broker query api
            #             last_fill=quote.get('fill_time', '')
            #         )
            #         await ms_client.write(symbol, a)


async def stream_quotes(
    symbols: list[str],
    host: str = 'localhost',
    port: int = 5993,
    diff_cached: bool = True,
    loglevel: str = None,

) -> None:
    '''
    Open a symbol stream from a running instance of marketstore and
    log to console.

    '''
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    tbks: dict[str, str] = {sym: f"{sym}/*/*" for sym in symbols}

    async with open_websocket_url(f'ws://{host}:{port}/ws') as ws:
        # send subs topics to server
        resp = await ws.send_message(

            encode({'streams': list(tbks.values())})
        )
        log.info(resp)

        async def recv() -> dict[str, Any]:
            return decode((await ws.get_message()), encoding='utf-8')

        streams = (await recv())['streams']
        log.info(f"Subscribed to {streams}")

        _cache = {}

        while True:
            msg = await recv()

            # unpack symbol and quote data
            # key is in format ``<SYMBOL>/<TIMEFRAME>/<ID>``
            symbol = msg['key'].split('/')[0]
            data = msg['data']

            # calc time stamp(s)
            s, ns = data.pop('Epoch'), data.pop('Nanoseconds')
            ts = s * 10**9 + ns
            data['broker_fill_time_ns'] = ts

            quote = {}
            for k, v in data.items():
                if isnan(v):
                    continue

                quote[k.lower()] = v

            quote['symbol'] = symbol

            quotes = {}

            if diff_cached:
                last = _cache.setdefault(symbol, {})
                new = set(quote.items()) - set(last.items())
                if new:
                    log.info(f"New quote {quote['symbol']}:\n{new}")

                    # only ship diff updates and other required fields
                    payload = {k: quote[k] for k, v in new}
                    payload['symbol'] = symbol

                    # if there was volume likely the last size of
                    # shares traded is useful info and it's possible
                    # that the set difference from above will disregard
                    # a "size" value since the same # of shares were traded
                    size = quote.get('size')
                    volume = quote.get('volume')
                    if size and volume:
                        new_volume_since_last = max(
                            volume - last.get('volume', 0), 0)
                        log.warning(
                            f"NEW VOLUME {symbol}:{new_volume_since_last}")
                        payload['size'] = size
                        payload['last'] = quote.get('last')

                    # XXX: we append to a list for the options case where the
                    # subscription topic (key) is the same for all
                    # expiries even though this is uncessary for the
                    # stock case (different topic [i.e. symbol] for each
                    # quote).
                    quotes.setdefault(symbol, []).append(payload)

                    # update cache
                    _cache[symbol].update(quote)
            else:
                quotes = {
                    symbol: [{key.lower(): val for key, val in quote.items()}]}

            if quotes:
                yield quotes
