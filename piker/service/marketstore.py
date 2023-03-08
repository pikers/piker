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
``marketstore`` integration.

- client management routines
- ticK data ingest routines
- websocket client for subscribing to write triggers
- todo: tick sequence stream-cloning for testing

'''
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from datetime import datetime
from typing import (
    Any,
    Optional,
    Union,
    TYPE_CHECKING,
)
import time
from math import isnan
from pathlib import Path

from bidict import bidict
from msgspec.msgpack import encode, decode
# import pyqtgraph as pg
import numpy as np
import tractor
from trio_websocket import open_websocket_url
from anyio_marketstore import (
    open_marketstore_client,
    MarketstoreClient,
    Params,
)
import pendulum
import purerpc

if TYPE_CHECKING:
    import docker
    from ._ahab import DockerContainer

from ..data.feed import maybe_open_feed
from ..log import get_logger, get_console_log
from .._profile import Profiler


log = get_logger(__name__)


# ahabd-supervisor and container level config
_config = {
    'grpc_listen_port': 5995,
    'ws_listen_port': 5993,
    'log_level': 'debug',
    'startup_timeout': 2,
}

_yaml_config = '''
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


def start_marketstore(
    client: docker.DockerClient,

    **kwargs,

) -> tuple[DockerContainer, dict[str, Any]]:
    '''
    Start and supervise a marketstore instance with its config bind-mounted
    in from the piker config directory on the system.

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
    if not os.path.isfile(yml_file):
        log.warning(
            f'No `marketstore` config exists?: {yml_file}\n'
            'Generating new file from template:\n'
            f'{_yaml_config}\n'
        )
        with open(yml_file, 'w') as yf:
            yf.write(_yaml_config)

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

    dcntr: DockerContainer = client.containers.run(
        'alpacamarkets/marketstore:latest',
        # do we need this for cmds?
        # '-i',

        # '-p 5993:5993',
        ports={
            '5993/tcp': 5993,  # jsonrpc / ws?
            '5995/tcp': 5995,  # grpc
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
        return "exiting..." in msg

    return (
        dcntr,
        _config,

        # expected startup and stop msgs
        start_matcher,
        stop_matcher,
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


def mk_tbk(keys: tuple[str, str, str]) -> str:
    '''
    Generate a marketstore table key from a tuple.
    Converts,
        ``('SPY', '1Sec', 'TICK')`` -> ``"SPY/1Sec/TICK"```

    '''
    return '/'.join(keys)


def quote_to_marketstore_structarray(
    quote: dict[str, Any],
    last_fill: Optional[float]

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
    host: str = 'localhost',
    port: int = 5995

) -> MarketstoreClient:
    '''
    Load a ``anyio_marketstore`` grpc client connected
    to an existing ``marketstore`` server.

    '''
    async with open_marketstore_client(
        host,
        port
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


class Storage:
    '''
    High level storage api for both real-time and historical ingest.

    '''
    def __init__(
        self,
        client: MarketstoreClient,

    ) -> None:
        # TODO: eventually this should be an api/interface type that
        # ensures we can support multiple tsdb backends.
        self.client = client

        # series' cache from tsdb reads
        self._arrays: dict[str, np.ndarray] = {}

    async def list_keys(self) -> list[str]:
        return await self.client.list_symbols()

    async def search_keys(self, pattern: str) -> list[str]:
        '''
        Search for time series key in the storage backend.

        '''
        ...

    async def write_ticks(self, ticks: list) -> None:
        ...

    async def load(
        self,
        fqsn: str,
        timeframe: int,

    ) -> tuple[
        np.ndarray,  # timeframe sampled array-series
        Optional[datetime],  # first dt
        Optional[datetime],  # last dt
    ]:

        first_tsdb_dt, last_tsdb_dt = None, None
        hist = await self.read_ohlcv(
            fqsn,
            # on first load we don't need to pull the max
            # history per request size worth.
            limit=3000,
            timeframe=timeframe,
        )
        log.info(f'Loaded tsdb history {hist}')

        if len(hist):
            times = hist['Epoch']
            first, last = times[0], times[-1]
            first_tsdb_dt, last_tsdb_dt = map(
                pendulum.from_timestamp, [first, last]
            )

        return (
            hist,  # array-data
            first_tsdb_dt,  # start of query-frame
            last_tsdb_dt,  # most recent
        )

    async def read_ohlcv(
        self,
        fqsn: str,
        timeframe: int | str,
        end: Optional[int] = None,
        limit: int = int(800e3),

    ) -> np.ndarray:

        client = self.client
        syms = await client.list_symbols()

        if fqsn not in syms:
            return {}

        # use the provided timeframe or 1s by default
        tfstr = tf_in_1s.get(timeframe, tf_in_1s[1])

        params = Params(
            symbols=fqsn,
            timeframe=tfstr,
            attrgroup='OHLCV',
            end=end,
            # limit_from_start=True,

            # TODO: figure the max limit here given the
            # ``purepc`` msg size limit of purerpc: 33554432
            limit=limit,
        )

        try:
            result = await client.query(params)
        except purerpc.grpclib.exceptions.UnknownError as err:
            # indicate there is no history for this timeframe
            log.exception(
                f'Unknown mkts QUERY error: {params}\n'
                f'{err.args}'
            )
            return {}

        # TODO: it turns out column access on recarrays is actually slower:
        # https://jakevdp.github.io/PythonDataScienceHandbook/02.09-structured-data-numpy.html#RecordArrays:-Structured-Arrays-with-a-Twist
        # it might make sense to make these structured arrays?
        data_set = result.by_symbols()[fqsn]
        array = data_set.array

        # XXX: ensure sample rate is as expected
        time = data_set.array['Epoch']
        if len(time) > 1:
            time_step = time[-1] - time[-2]
            ts = tf_in_1s.inverse[data_set.timeframe]

            if time_step != ts:
                log.warning(
                    f'MKTS BUG: wrong timeframe loaded: {time_step}'
                    'YOUR DATABASE LIKELY CONTAINS BAD DATA FROM AN OLD BUG'
                    f'WIPING HISTORY FOR {ts}s'
                )
                await self.delete_ts(fqsn, timeframe)

                # try reading again..
                return await self.read_ohlcv(
                    fqsn,
                    timeframe,
                    end,
                    limit,
                )

        return array

    async def delete_ts(
        self,
        key: str,
        timeframe: Optional[Union[int, str]] = None,
        fmt: str = 'OHLCV',

    ) -> bool:

        client = self.client
        syms = await client.list_symbols()
        if key not in syms:
            raise KeyError(f'`{key}` table key not found in\n{syms}?')

        tbk = mk_tbk((
            key,
            tf_in_1s.get(timeframe, tf_in_1s[60]),
            fmt,
        ))
        return await client.destroy(tbk=tbk)

    async def write_ohlcv(
        self,
        fqsn: str,
        ohlcv: np.ndarray,
        timeframe: int,
        append_and_duplicate: bool = True,
        limit: int = int(800e3),

    ) -> None:
        # build mkts schema compat array for writing
        mkts_dt = np.dtype(_ohlcv_dt)
        mkts_array = np.zeros(
            len(ohlcv),
            dtype=mkts_dt,
        )
        # copy from shm array (yes it's this easy):
        # https://numpy.org/doc/stable/user/basics.rec.html#assignment-from-other-structured-arrays
        mkts_array[:] = ohlcv[[
            'time',
            'open',
            'high',
            'low',
            'close',
            'volume',
        ]]

        m, r = divmod(len(mkts_array), limit)

        tfkey = tf_in_1s[timeframe]
        for i in range(m, 1):
            to_push = mkts_array[i-1:i*limit]

            # write to db
            resp = await self.client.write(
                to_push,
                tbk=f'{fqsn}/{tfkey}/OHLCV',

                # NOTE: will will append duplicates
                # for the same timestamp-index.
                # TODO: pre-deduplicate?
                isvariablelength=append_and_duplicate,
            )

            log.info(
                f'Wrote {mkts_array.size} datums to tsdb\n'
            )

            for resp in resp.responses:
                err = resp.error
                if err:
                    raise MarketStoreError(err)

        if r:
            to_push = mkts_array[m*limit:]

            # write to db
            resp = await self.client.write(
                to_push,
                tbk=f'{fqsn}/{tfkey}/OHLCV',

                # NOTE: will will append duplicates
                # for the same timestamp-index.
                # TODO: pre deduplicate?
                isvariablelength=append_and_duplicate,
            )

            log.info(
                f'Wrote {mkts_array.size} datums to tsdb\n'
            )

            for resp in resp.responses:
                err = resp.error
                if err:
                    raise MarketStoreError(err)

    # XXX: currently the only way to do this is through the CLI:

    # sudo ./marketstore connect --dir ~/.config/piker/data
    # >> \show mnq.globex.20220617.ib/1Sec/OHLCV 2022-05-15
    # and this seems to block and use up mem..
    # >> \trim mnq.globex.20220617.ib/1Sec/OHLCV 2022-05-15

    # relevant source code for this is here:
    # https://github.com/alpacahq/marketstore/blob/master/cmd/connect/session/trim.go#L14
    # def delete_range(self, start_dt, end_dt) -> None:
    #     ...


@acm
async def open_storage_client(
    fqsn: str,
    period: Optional[Union[int, str]] = None,  # in seconds

) -> tuple[Storage, dict[str, np.ndarray]]:
    '''
    Load a series by key and deliver in ``numpy`` struct array format.

    '''
    async with (
        # eventually a storage backend endpoint
        get_client() as client,
    ):
        # slap on our wrapper api
        yield Storage(client)


@acm
async def open_tsdb_client(
    fqsn: str,
) -> Storage:

    # TODO: real-time dedicated task for ensuring
    # history consistency between the tsdb, shm and real-time feed..

    # update sequence design notes:

    # - load existing highest frequency data from mkts
    #   * how do we want to offer this to the UI?
    #    - lazy loading?
    #    - try to load it all and expect graphics caching/diffing
    #      to  hide extra bits that aren't in view?

    # - compute the diff between latest data from broker and shm
    #   * use sql api in mkts to determine where the backend should
    #     start querying for data?
    #   * append any diff with new shm length
    #   * determine missing (gapped) history by scanning
    #   * how far back do we look?

    # - begin rt update ingest and aggregation
    #   * could start by always writing ticks to mkts instead of
    #     worrying about a shm queue for now.
    #   * we have a short list of shm queues worth groking:
    #     - https://github.com/pikers/piker/issues/107
    #   * the original data feed arch blurb:
    #     - https://github.com/pikers/piker/issues/98
    #
    profiler = Profiler(
        disabled=True,  # not pg_profile_enabled(),
        delayed=False,
    )

    async with (
        open_storage_client(fqsn) as storage,

        maybe_open_feed(
            [fqsn],
            start_stream=False,

        ) as feed,
    ):
        profiler(f'opened feed for {fqsn}')

        # to_append = feed.hist_shm.array
        # to_prepend = None

        if fqsn:
            flume = feed.flumes[fqsn]
            symbol = flume.symbol
            if symbol:
                fqsn = symbol.fqsn

            # diff db history with shm and only write the missing portions
            # ohlcv = flume.hist_shm.array

            # TODO: use pg profiler
            # for secs in (1, 60):
            #     tsdb_array = await storage.read_ohlcv(
            #         fqsn,
            #         timeframe=timeframe,
            #     )
            #     # hist diffing:
            #     # these aren't currently used but can be referenced from
            #     # within the embedded ipython shell below.
            #     to_append = ohlcv[ohlcv['time'] > ts['Epoch'][-1]]
            #     to_prepend = ohlcv[ohlcv['time'] < ts['Epoch'][0]]

            # profiler('Finished db arrays diffs')

            syms = await storage.client.list_symbols()
            # log.info(f'Existing tsdb symbol set:\n{pformat(syms)}')
            # profiler(f'listed symbols {syms}')
            yield storage

        # for array in [to_append, to_prepend]:
        #     if array is None:
        #         continue

        #     log.info(
        #         f'Writing datums {array.size} -> to tsdb from shm\n'
        #     )
        #     await storage.write_ohlcv(fqsn, array)

        # profiler('Finished db writes')


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
