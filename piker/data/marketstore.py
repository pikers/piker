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
- todo: docker container management automation
'''
from contextlib import asynccontextmanager
from typing import Any, Optional
import time
from math import isnan

import msgpack
import numpy as np
import pandas as pd
import tractor
from trio_websocket import open_websocket_url
from anyio_marketstore import open_marketstore_client, MarketstoreClient

from ..log import get_logger, get_console_log
from ..data import open_feed


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


def mk_tbk(keys: tuple[str, str, str]) -> str:
    '''
    Generate a marketstore table key from a tuple.
    Converts,
        ``('SPY', '1Sec', 'TICK')`` -> ``"SPY/1Sec/TICK"```

    '''
    return '{}/' + '/'.join(keys)


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


@asynccontextmanager
async def get_client(
    host: str = 'localhost',
    port: int = 5995

) -> MarketstoreClient:
    async with open_marketstore_client(host, port) as client:
        yield client


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
        open_feed(brokername, symbols, loglevel=loglevel) as feed,
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


# async def stream_quotes(
#     symbols: list[str],
#     timeframe: str = '1Min',
#     attr_group: str = 'TICK',
#     host: str = 'localhost',
#     port: int = 5993,
#     loglevel: str = None

# ) -> None:
#     '''
#     Open a symbol stream from a running instance of marketstore and
#     log to console.

#     '''
#     tbks: dict[str, str] = {
#         sym: f'{sym}/{timeframe}/{attr_group}' for sym in symbols}


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
