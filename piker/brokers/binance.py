# piker: trading gear for hackers
# Copyright (C)
#   Guillermo Rodriguez
#   Tyler Goodlet
#   (in stewardship for pikers)

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
from contextlib import asynccontextmanager as acm
from datetime import datetime
from functools import lru_cache
from decimal import Decimal
from typing import (
    Any, Union, Optional,
    AsyncGenerator, Callable,
)
import time

from trio_util import trio_async_generator
import trio
from trio_typing import TaskStatus
import pendulum
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor
import wsproto

from ..accounting._mktinfo import (
    Asset,
    MktPair,
    digits_to_dec,
)
from .._cacheables import open_cached_client
from ._util import (
    resproc,
    SymbolNotFound,
    DataUnavailable,
)
from ._util import (
    log,
    get_console_log,
)
from ..data.types import Struct
from ..data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)


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

# TODO: make this frozen again by pre-processing the
# filters list to a dict at init time?
class Pair(Struct, frozen=True):
    symbol: str
    status: str

    baseAsset: str
    baseAssetPrecision: int
    cancelReplaceAllowed: bool
    allowTrailingStop: bool
    quoteAsset: str
    quotePrecision: int
    quoteAssetPrecision: int

    baseCommissionPrecision: int
    quoteCommissionPrecision: int

    orderTypes: list[str]

    icebergAllowed: bool
    ocoAllowed: bool
    quoteOrderQtyMarketAllowed: bool
    isSpotTradingAllowed: bool
    isMarginTradingAllowed: bool

    defaultSelfTradePreventionMode: str
    allowedSelfTradePreventionModes: list[str]

    filters: dict[
        str,
        Union[str, int, float]
    ]
    permissions: list[str]

    @property
    def price_tick(self) -> Decimal:
        # XXX: lul, after manually inspecting the response format we
        # just directly pick out the info we need
        step_size: str = self.filters['PRICE_FILTER']['tickSize'].rstrip('0')
        return Decimal(step_size)

    @property
    def size_tick(self) -> Decimal:
        step_size: str = self.filters['LOT_SIZE']['stepSize'].rstrip('0')
        return Decimal(step_size)


class OHLC(Struct):
    '''
    Description of the flattened OHLC quote format.

    For schema details see:
    https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams

    '''
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


# convert datetime obj timestamp to unixtime in milliseconds
def binance_timestamp(
    when: datetime
) -> int:
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url
        self._pairs: dict[str, Pair] = {}

    async def _api(
        self,
        method: str,
        params: dict,
    ) -> dict[str, Any]:
        resp = await self._sesh.get(
            path=f'/api/v3/{method}',
            params=params,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def exch_info(

        self,
        sym: str | None = None,

    ) -> dict[str, Pair] | Pair:
        '''
        Fresh exchange-pairs info query for symbol ``sym: str``:
        https://binance-docs.github.io/apidocs/spot/en/#exchange-information

        '''
        cached_pair = self._pairs.get(sym)
        if cached_pair:
            return cached_pair

        # retrieve all symbols by default
        params = {}
        if sym is not None:
            sym = sym.lower()
            params = {'symbol': sym}

        resp = await self._api('exchangeInfo', params=params)
        entries = resp['symbols']
        if not entries:
            raise SymbolNotFound(f'{sym} not found:\n{resp}')

        # pre-process .filters field into a table
        pairs = {}
        for item in entries:
            symbol = item['symbol']
            filters = {}
            filters_ls: list = item.pop('filters')
            for entry in filters_ls:
                ftype = entry['filterType']
                filters[ftype] = entry

            pairs[symbol] = Pair(
                filters=filters,
                **item,
            )

        # pairs = {
        #     item['symbol']: Pair(**item) for item in entries
        # }
        self._pairs.update(pairs)

        if sym is not None:
            return pairs[sym]
        else:
            return self._pairs

    symbol_info = exch_info

    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,
    ) -> dict[str, Any]:
        if self._pairs is not None:
            data = self._pairs
        else:
            data = await self.exch_info()

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
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        limit: int = 1000,  # <- max allowed per query
        as_np: bool = True,

    ) -> dict:

        if end_dt is None:
            end_dt = pendulum.now('UTC').add(minutes=1)

        if start_dt is None:
            start_dt = end_dt.start_of(
                'minute').subtract(minutes=limit)

        start_time = binance_timestamp(start_dt)
        end_time = binance_timestamp(end_dt)

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
            bar.typecast()

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


@acm
async def get_client() -> Client:
    client = Client()
    log.info(f'Caching exchange infos..')
    await client.exch_info()
    yield client


# validation type
class AggTrade(Struct):
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


@trio_async_generator
async def stream_messages(
    ws: NoBsWs,
) -> AsyncGenerator[NoBsWs, dict]:

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

            # NOTE: this is purely for a definition, ``msgspec.Struct``
            # does not runtime-validate until you decode/encode.
            # see: https://jcristharif.com/msgspec/structs.html#type-validation
            msg = AggTrade(**msg)

            # TODO: type out and require this quote format
            # from all backends!
            yield 'trade', {
                'symbol': msg.s,
                'last': msg.p,
                'brokerd_ts': time.time(),
                'ticks': [{
                    'type': 'trade',
                    'price': float(msg.p),
                    'size': float(msg.q),
                    'broker_ts': msg.T,
                }],
            }


def make_sub(pairs: list[str], sub_name: str, uid: int) -> dict[str, str]:
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


@acm
async def open_history_client(
    symbol: str,

) -> tuple[Callable, int]:

    # TODO implement history getter for the new storage layer.
    async with open_cached_client('binance') as client:

        async def get_ohlc(
            timeframe: float,
            end_dt: datetime | None = None,
            start_dt: datetime | None = None,

        ) -> tuple[
            np.ndarray,
            datetime,  # start
            datetime,  # end
        ]:
            if timeframe != 60:
                raise DataUnavailable('Only 1m bars are supported')

            array = await client.bars(
                symbol,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            times = array['time']
            if (
                end_dt is None
            ):
                inow = round(time.time())
                if (inow - times[-1]) > 60:
                    await tractor.breakpoint()

            start_dt = pendulum.from_timestamp(times[0])
            end_dt = pendulum.from_timestamp(times[-1])

            return array, start_dt, end_dt

        yield get_ohlc, {'erlangs': 3, 'rate': 3}


@lru_cache
async def get_mkt_info(
    fqme: str,

) -> tuple[MktPair, Pair]:

    async with open_cached_client('binance') as client:

        pair: Pair = await client.exch_info(fqme.upper())
        mkt = MktPair(
            dst=Asset(
                name=pair.baseAsset,
                atype='crypto',
                tx_tick=digits_to_dec(pair.baseAssetPrecision),
            ),
            src=Asset(
                name=pair.quoteAsset,
                atype='crypto',
                tx_tick=digits_to_dec(pair.quoteAssetPrecision),
            ),
            price_tick=pair.price_tick,
            size_tick=pair.size_tick,
            bs_mktid=pair.symbol,
            broker='binance',
        )
        return mkt, pair


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    uid = 0

    async with (
        send_chan as send_chan,
    ):
        mkt_infos: dict[str, MktPair] = {}
        for sym in symbols:
            mkt, pair = await get_mkt_info(sym)
            mkt_infos[sym] = mkt

        symbol = symbols[0]

        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            symbol: {
                'fqsn': sym,

                'mkt_info': mkt_infos[sym],
                'shm_write_opts': {'sum_tick_vml': False},
            },
        }

        @acm
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
            if ws.connected():
                await ws.send_msg({
                    "method": "UNSUBSCRIBE",
                    "params": subs,
                    "id": uid,
                })

                # XXX: do we need to ack the unsub?
                # await ws.recv_msg()

        async with (
            open_autorecon_ws(
                # XXX: see api docs which show diff addr?
                # https://developers.binance.com/docs/binance-trading-api/websocket_api#general-api-information
                # 'wss://ws-api.binance.com:443/ws-api/v3',
                'wss://stream.binance.com/ws',
                fixture=subscribe,
            ) as ws,

            # avoid stream-gen closure from breaking trio..
            stream_messages(ws) as msg_gen,
        ):
            typ, quote = await anext(msg_gen)

            # pull a first quote and deliver
            while typ != 'trade':
                typ, quote = await anext(msg_gen)

            task_status.started((init_msgs, quote))

            # signal to caller feed is ready for consumption
            feed_is_live.set()

            # import time
            # last = time.time()

            # start streaming
            async for typ, msg in msg_gen:

                # period = time.time() - last
                # hz = 1/period if period else float('inf')
                # if hz > 60:
                #     log.info(f'Binance quotez : {hz}')

                topic = msg['symbol'].lower()
                await send_chan.send({topic: msg})
                # last = time.time()


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> Client:
    async with open_cached_client('binance') as client:

        # load all symbols locally for fast search
        cache = await client.exch_info()
        await ctx.started()

        async with ctx.open_stream() as stream:

            async for pattern in stream:
                # results = await client.exch_info(sym=pattern.upper())

                matches = fuzzy.extractBests(
                    pattern,
                    cache,
                    score_cutoff=50,
                )
                # repack in dict form
                await stream.send({
                    item[0].symbol: item[0]
                    for item in matches
                })
