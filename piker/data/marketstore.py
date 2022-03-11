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
from contextlib import asynccontextmanager as acm
from pprint import pformat
from typing import (
    Any,
    Optional,
    Union,
    # Callable,
    # TYPE_CHECKING,
)
import time
from math import isnan

from bidict import bidict
import msgpack
import numpy as np
import pandas as pd
import tractor
from trio_websocket import open_websocket_url
from anyio_marketstore import (
    open_marketstore_client,
    MarketstoreClient,
    Params,
)
import purerpc

from .feed import maybe_open_feed
from ._source import (
    mk_fqsn,
    # Symbol,
)
from ..log import get_logger, get_console_log

# if TYPE_CHECKING:
#     from ._sharedmem import ShmArray


log = get_logger(__name__)

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
    ('Low', 'i8'),
    ('Close', 'i8'),
    ('Volume', 'f4'),
]


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
        now = timestamp(last_fill)
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


def timestamp(date, **kwargs) -> int:
    '''
    Return marketstore compatible 'Epoch' integer in nanoseconds
    from a date formatted str.

    '''
    return int(pd.Timestamp(date, **kwargs).value)


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

    async def write_ticks(self, ticks: list) -> None:
        ...

    async def write_ohlcv(self, ohlcv: np.ndarray) -> None:
        ...

    async def read_ohlcv(
        self,
        fqsn: str,
        timeframe: Optional[Union[int, str]] = None,

    ) -> tuple[
        MarketstoreClient,
        Union[dict, np.ndarray]
    ]:
        client = self.client
        syms = await client.list_symbols()

        if fqsn not in syms:
            return {}

        if timeframe is None:
            log.info(f'starting {fqsn} tsdb granularity scan..')
            # loop through and try to find highest granularity
            for tfstr in tf_in_1s.values():
                try:
                    log.info(f'querying for {tfstr}@{fqsn}')
                    result = await client.query(Params(fqsn, tfstr, 'OHLCV',))
                    break
                except purerpc.grpclib.exceptions.UnknownError:
                    # XXX: this is already logged by the container and
                    # thus shows up through `marketstored` logs relay.
                    # log.warning(f'{tfstr}@{fqsn} not found')
                    continue
            else:
                return {}

        else:
            tfstr = tf_in_1s[timeframe]
            result = await client.query(Params(fqsn, tfstr, 'OHLCV',))

        # Fill out a `numpy` array-results map
        arrays = {}
        for fqsn, data_set in result.by_symbols().items():
            arrays.setdefault(fqsn, {})[
                tf_in_1s.inverse[data_set.timeframe]
            ] = data_set.array

        return arrays[fqsn][timeframe] if timeframe else arrays


@acm
async def open_storage_client(
    fqsn: str,
    period: Optional[Union[int, str]] = None,  # in seconds

) -> tuple[Storage, dict[str, np.ndarray]]:
    '''
    Load a series by key and deliver in ``numpy`` struct array format.

    '''
    async with get_client() as client:

        storage_client = Storage(client)
        arrays = await storage_client.read_ohlcv(
            fqsn,
            period,
        )

        yield storage_client, arrays


async def backfill_history_diff(
    # symbol: Symbol

) -> list[str]:

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

    broker = 'ib'
    symbol = 'mnq.globex'

    # broker = 'binance'
    # symbol = 'btcusdt'

    fqsn = mk_fqsn(broker, symbol)

    async with (
        get_client() as client,
        maybe_open_feed(
            broker,
            [symbol],
            loglevel='info',
            # backpressure=False,
            start_stream=False,

        ) as (feed, stream),
    ):
        syms = await client.list_symbols()
        log.info(f'Existing symbol set:\n{pformat(syms)}')

        # diff db history with shm and only write the missing portions
        ohlcv = feed.shm.array

        key = (fqsn, '1Sec', 'OHLCV')
        tbk = mk_tbk(key)

        # diff vs. existing array and append new history
        # TODO:

        # TODO: should be no error?
        # assert not resp.responses

        start = time.time()

        qr = await client.query(
            # Params(fqsn, '1Sec`', 'OHLCV',)
            Params(*key),
        )
        # # Dig out `numpy` results map
        arrays: dict[tuple[str, int], np.ndarray] = {}
        for name, data_set in qr.by_symbols().items():
            in_secs = tf_in_1s.inverse[data_set.timeframe]
            arrays[(name, in_secs)] = data_set.array

        s1 = arrays[(fqsn, 1)]
        to_append = ohlcv[ohlcv['time'] > s1['Epoch'][-1]]

        end_diff = time.time()
        diff_ms = round((end_diff - start) * 1e3, ndigits=2)

        log.info(
            f'Appending {to_append.size} datums to tsdb from shm\n'
            f'Total diff time: {diff_ms} ms'
        )

        # build mkts schema compat array for writing
        mkts_dt = np.dtype(_ohlcv_dt)
        mkts_array = np.zeros(
            len(to_append),
            dtype=mkts_dt,
        )
        # copy from shm array (yes it's this easy):
        # https://numpy.org/doc/stable/user/basics.rec.html#assignment-from-other-structured-arrays
        mkts_array[:] = to_append[[
            'time',
            'open',
            'high',
            'low',
            'close',
            'volume',
        ]]

        # write to db
        resp = await client.write(
            mkts_array,
            tbk=tbk,
            # NOTE: will will append duplicates
            # for the same timestamp-index.
            isvariablelength=True,
        )
        end_write = time.time()
        diff_ms = round((end_write - end_diff) * 1e3, ndigits=2)
        log.info(
            f'Wrote {to_append.size} datums to tsdb\n'
            f'Total write time: {diff_ms} ms'
        )
        for resp in resp.responses:
            err = resp.error
            if err:
                raise MarketStoreError(err)

        # TODO: backfiller loop
        from piker.ui._compression import downsample
        x, y = downsample(
            s1['Epoch'],
            s1['Close'],
            bins=10,
        )
        await tractor.breakpoint()


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
            msgpack.dumps({'streams': list(tbks.values())})
        )
        log.info(resp)

        async def recv() -> dict[str, Any]:
            return msgpack.loads((await ws.get_message()), encoding='utf-8')

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
