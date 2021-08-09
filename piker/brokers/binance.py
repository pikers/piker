# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for piker0)

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

"""
Binance backend

"""
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Tuple, Union, Optional
import time

import trio
from trio_typing import TaskStatus
import arrow
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor
from pydantic.dataclasses import dataclass
from pydantic import BaseModel
import wsproto

from .._cacheables import open_cached_client
from ._util import resproc, SymbolNotFound
from ..log import get_logger, get_console_log
from ..data import ShmArray
from ..data._web_bs import open_autorecon_ws

log = get_logger(__name__)


_url = 'https://api.binance.com'


# Broker specific ohlc schema (rest)
_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('bar_wap', float),  # will be zeroed by sampler if not filled

    # XXX: some additional fields are defined in the docs:
    # https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

    # ('close_time', int),
    # ('quote_vol', float),
    # ('num_trades', int),
    # ('buy_base_vol', float),
    # ('buy_quote_vol', float),
    # ('ignore', float),
]

# UI components allow this to be declared such that additional
# (historical) fields can be exposed.
ohlc_dtype = np.dtype(_ohlc_dtype)

_show_wap_in_history = False


# https://binance-docs.github.io/apidocs/spot/en/#exchange-information
class Pair(BaseModel):
    symbol: str
    status: str

    baseAsset: str
    baseAssetPrecision: int
    quoteAsset: str
    quotePrecision: int
    quoteAssetPrecision: int

    baseCommissionPrecision: int
    quoteCommissionPrecision: int

    orderTypes: List[str]

    icebergAllowed: bool
    ocoAllowed: bool
    quoteOrderQtyMarketAllowed: bool
    isSpotTradingAllowed: bool
    isMarginTradingAllowed: bool

    filters: List[Dict[str, Union[str, int, float]]]
    permissions: List[str]


@dataclass
class OHLC:
    """Description of the flattened OHLC quote format.

    For schema details see:
    https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams

    """
    time: int

    open: float
    high: float
    low: float
    close: float
    volume: float

    close_time: int

    quote_vol: float
    num_trades: int
    buy_base_vol: float
    buy_quote_vol: float
    ignore: int

    # null the place holder for `bar_wap` until we
    # figure out what to extract for this.
    bar_wap: float = 0.0


# convert arrow timestamp to unixtime in miliseconds
def binance_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url
        self._pairs: dict[str, Any] = {}

    async def _api(
        self,
        method: str,
        params: dict,
    ) -> Dict[str, Any]:
        resp = await self._sesh.get(
            path=f'/api/v3/{method}',
            params=params,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def symbol_info(

        self,
        sym: Optional[str] = None,

    ) -> dict[str, Any]:
        '''Get symbol info for the exchange.

        '''
        # TODO: we can load from our self._pairs cache
        # on repeat calls...

        # will retrieve all symbols by default
        params = {}

        if sym is not None:
            sym = sym.upper()
            params = {'symbol': sym}

        resp = await self._api(
            'exchangeInfo',
            params=params,
        )

        entries = resp['symbols']
        if not entries:
            raise SymbolNotFound(f'{sym} not found')

        syms = {item['symbol']: item for item in entries}

        if sym is not None:
            return syms[sym]
        else:
            return syms

    async def cache_symbols(
        self,
    ) -> dict:
        if not self._pairs:
            self._pairs = await self.symbol_info()

        return self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,
    ) -> Dict[str, Any]:
        if self._pairs is not None:
            data = self._pairs
        else:
            data = await self.symbol_info()

        matches = fuzzy.extractBests(
            pattern,
            data,
            score_cutoff=50,
        )
        # repack in dict form
        return {item[0]['symbol']: item[0]
         for item in matches}

    async def bars(
        self,
        symbol: str,
        start_time: int = None,
        end_time: int = None,
        limit: int = 1000,  # <- max allowed per query
        as_np: bool = True,

    ) -> dict:

        if start_time is None:
            start_time = binance_timestamp(
                arrow.utcnow().floor('minute').shift(minutes=-limit)
            )

        if end_time is None:
            end_time = binance_timestamp(arrow.utcnow())

        # https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        bars = await self._api(
            'klines',
            params={
                'symbol': symbol.upper(),
                'interval': '1m',
                'startTime': start_time,
                'endTime': end_time,
                'limit': limit
            }
        )

        # TODO: pack this bars scheme into a ``pydantic`` validator type:
        # https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

        # TODO: we should port this to ``pydantic`` to avoid doing
        # manual validation ourselves..
        new_bars = []
        for i, bar in enumerate(bars):

            bar = OHLC(*bar)

            row = []
            for j, (name, ftype) in enumerate(_ohlc_dtype[1:]):

                # TODO: maybe we should go nanoseconds on all
                # history time stamps?
                if name == 'time':
                    # convert to epoch seconds: float
                    row.append(bar.time / 1000.0)

                else:
                    row.append(getattr(bar, name))

            new_bars.append((i,) + tuple(row))

        array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
        return array


@asynccontextmanager
async def get_client() -> Client:
    client = Client()
    await client.cache_symbols()
    yield client


# validation type
class AggTrade(BaseModel):
    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    a: int  # Aggregate trade ID
    p: float  # Price
    q: float  # Quantity
    f: int  # First trade ID
    l: int  # Last trade ID
    T: int  # Trade time
    m: bool  # Is the buyer the market maker?
    M: bool  # Ignore


async def stream_messages(ws):

    timeouts = 0
    while True:

        with trio.move_on_after(3) as cs:
            msg = await ws.recv_msg()

        if cs.cancelled_caught:

            timeouts += 1
            if timeouts > 2:
                log.error("binance feed seems down and slow af? rebooting...")
                await ws._connect()

            continue

        # for l1 streams binance doesn't add an event type field so
        # identify those messages by matching keys
        # https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams

        if msg.get('u'):
            sym = msg['s']
            bid = float(msg['b'])
            bsize = float(msg['B'])
            ask = float(msg['a'])
            asize = float(msg['A'])

            yield 'l1', {
                'symbol': sym,
                'ticks': [
                    {'type': 'bid', 'price': bid, 'size': bsize},
                    {'type': 'bsize', 'price': bid, 'size': bsize},
                    {'type': 'ask', 'price': ask, 'size': asize},
                    {'type': 'asize', 'price': ask, 'size': asize}
                ]
            }

        elif msg.get('e') == 'aggTrade':

            # validate
            msg = AggTrade(**msg)

            # TODO: type out and require this quote format
            # from all backends!
            yield 'trade', {
                'symbol': msg.s,
                'last': msg.p,
                'brokerd_ts': time.time(),
                'ticks': [{
                    'type': 'trade',
                    'price': msg.p,
                    'size': msg.q,
                    'broker_ts': msg.T,
                }],
            }


def make_sub(pairs: List[str], sub_name: str, uid: int) -> Dict[str, str]:
    """Create a request subscription packet dict.

    https://binance-docs.github.io/apidocs/spot/en/#live-subscribing-unsubscribing-to-streams
    """
    return {
        'method': 'SUBSCRIBE',
        'params': [
            f'{pair.lower()}@{sub_name}'
            for pair in pairs
        ],
        'id': uid
    }


async def backfill_bars(
    sym: str,
    shm: ShmArray,  # type: ignore # noqa
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Fill historical bars into shared mem / storage afap.
    """
    with trio.CancelScope() as cs:
        async with open_cached_client('binance') as client:
            bars = await client.bars(symbol=sym)
            shm.push(bars)
            task_status.started(cs)


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: List[str],
    shm: ShmArray,
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[Tuple[Dict, Dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    sym_infos = {}
    uid = 0

    async with (
        open_cached_client('binance') as client,
        send_chan as send_chan,
    ):

        # keep client cached for real-time section
        cache = await client.cache_symbols()

        for sym in symbols:
            d = cache[sym.upper()]
            syminfo = Pair(**d)  # validation

            si = sym_infos[sym] = syminfo.dict()

            # XXX: after manually inspecting the response format we
            # just directly pick out the info we need
            si['price_tick_size'] = syminfo.filters[0]['tickSize']
            si['lot_tick_size'] = syminfo.filters[2]['stepSize']

        symbol = symbols[0]

        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            symbol: {
                'symbol_info': sym_infos[sym],
                'shm_write_opts': {'sum_tick_vml': False},
            },
        }

        @asynccontextmanager
        async def subscribe(ws: wsproto.WSConnection):
            # setup subs

            # trade data (aka L1)
            # https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker
            l1_sub = make_sub(symbols, 'bookTicker', uid)
            await ws.send_msg(l1_sub)

            # aggregate (each order clear by taker **not** by maker)
            # trades data:
            # https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
            agg_trades_sub = make_sub(symbols, 'aggTrade', uid)
            await ws.send_msg(agg_trades_sub)

            # ack from ws server
            res = await ws.recv_msg()
            assert res['id'] == uid

            yield

            subs = []
            for sym in symbols:
                subs.append("{sym}@aggTrade")
                subs.append("{sym}@bookTicker")

            # unsub from all pairs on teardown
            await ws.send_msg({
                "method": "UNSUBSCRIBE",
                "params": subs,
                "id": uid,
            })

            # XXX: do we need to ack the unsub?
            # await ws.recv_msg()

        async with open_autorecon_ws(
            'wss://stream.binance.com/ws',
            fixture=subscribe,
        ) as ws:

            # pull a first quote and deliver
            msg_gen = stream_messages(ws)

            typ, quote = await msg_gen.__anext__()

            while typ != 'trade':
                # TODO: use ``anext()`` when it lands in 3.10!
                typ, quote = await msg_gen.__anext__()

            first_quote = {quote['symbol'].lower(): quote}
            task_status.started((init_msgs,  first_quote))

            # signal to caller feed is ready for consumption
            feed_is_live.set()

            # start streaming
            async for typ, msg in msg_gen:

                topic = msg['symbol'].lower()
                await send_chan.send({topic: msg})


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> Client:
    async with open_cached_client('binance') as client:

        # load all symbols locally for fast search
        cache = await client.cache_symbols()
        await ctx.started()

        async with ctx.open_stream() as stream:

            async for pattern in stream:
                # results = await client.symbol_info(sym=pattern.upper())

                matches = fuzzy.extractBests(
                    pattern,
                    cache,
                    score_cutoff=50,
                )
                # repack in dict form
                await stream.send(
                    {item[0]['symbol']: item[0]
                     for item in matches}
                )
