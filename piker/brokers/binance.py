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
from contextlib import (
    asynccontextmanager as acm,
    aclosing,
)
from datetime import datetime
from decimal import Decimal
import itertools
from typing import (
    Any, Union, Optional,
    AsyncGenerator, Callable,
)
import time

import trio
from trio_typing import TaskStatus
import pendulum
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor

from .._cacheables import async_lifo_cache
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
from piker.data.types import Struct
from piker.data.validate import FeedInit
from piker.data import def_iohlcv_fields
from piker.data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)


_url = 'https://api.binance.com'


# Broker specific ohlc schema (rest)
# XXX TODO? some additional fields are defined in the docs:
# https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

# _ohlc_dtype = [
    # ('close_time', int),
    # ('quote_vol', float),
    # ('num_trades', int),
    # ('buy_base_vol', float),
    # ('buy_quote_vol', float),
    # ('ignore', float),
# ]

# UI components allow this to be declared such that additional
# (historical) fields can be exposed.
# ohlc_dtype = np.dtype(_ohlc_dtype)

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


class L1(Struct):
    # https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams

    update_id: int
    sym: str

    bid: float
    bsize: float
    ask: float
    asize: float


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
            for j, (name, ftype) in enumerate(def_iohlcv_fields[1:]):

                # TODO: maybe we should go nanoseconds on all
                # history time stamps?
                if name == 'time':
                    # convert to epoch seconds: float
                    row.append(bar.time / 1000.0)

                else:
                    row.append(getattr(bar, name))

            new_bars.append((i,) + tuple(row))

        array = np.array(
            new_bars,
            dtype=def_iohlcv_fields,
        ) if as_np else bars
        return array


@acm
async def get_client() -> Client:
    client = Client()
    log.info('Caching exchange infos..')
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


async def stream_messages(
    ws: NoBsWs,
) -> AsyncGenerator[NoBsWs, dict]:

    # TODO: match syntax here!
    msg: dict[str, Any]
    async for msg in ws:
        match msg:
            # for l1 streams binance doesn't add an event type field so
            # identify those messages by matching keys
            # https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams
            case {
                # NOTE: this is never an old value it seems, so
                # they are always sending real L1 spread updates.
                'u': upid,  # update id
                's': sym,
                'b': bid,
                'B': bsize,
                'a': ask,
                'A': asize,
            }:
                # TODO: it would be super nice to have a `L1` piker type
                # which "renders" incremental tick updates from a packed
                # msg-struct:
                # - backend msgs after packed into the type such that we
                #   can reduce IPC usage but without each backend having
                #   to do that incremental update logic manually B)
                # - would it maybe be more efficient to use this instead?
                #   https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
                l1 = L1(
                    update_id=upid,
                    sym=sym,
                    bid=bid,
                    bsize=bsize,
                    ask=ask,
                    asize=asize,
                )
                l1.typecast()

                # repack into piker's tick-quote format
                yield 'l1', {
                    'symbol': l1.sym,
                    'ticks': [
                        {
                            'type': 'bid',
                            'price': l1.bid,
                            'size': l1.bsize,
                        },
                        {
                            'type': 'bsize',
                            'price': l1.bid,
                            'size': l1.bsize,
                        },
                        {
                            'type': 'ask',
                            'price': l1.ask,
                            'size': l1.asize,
                        },
                        {
                            'type': 'asize',
                            'price': l1.ask,
                            'size': l1.asize,
                        }
                    ]
                }

            # https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
            case {
                'e': 'aggTrade',
            }:
                # NOTE: this is purely for a definition,
                # ``msgspec.Struct`` does not runtime-validate until you
                # decode/encode, see:
                # https://jcristharif.com/msgspec/structs.html#type-validation
                msg = AggTrade(**msg)
                msg.typecast()
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
    mkt: MktPair,

) -> tuple[Callable, int]:

    symbol: str = mkt.bs_fqme

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


@async_lifo_cache()
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
        both = mkt, pair
        return both


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

    async with (
        send_chan as send_chan,
    ):
        init_msgs: list[FeedInit] = []
        for sym in symbols:
            mkt, pair = await get_mkt_info(sym)

            # build out init msgs according to latest spec
            init_msgs.append(
                FeedInit(mkt_info=mkt)
            )

        iter_subids = itertools.count()

        @acm
        async def subscribe(ws: NoBsWs):
            # setup subs

            subid: int = next(iter_subids)

            # trade data (aka L1)
            # https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker
            l1_sub = make_sub(symbols, 'bookTicker', subid)
            await ws.send_msg(l1_sub)

            # aggregate (each order clear by taker **not** by maker)
            # trades data:
            # https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
            agg_trades_sub = make_sub(symbols, 'aggTrade', subid)
            await ws.send_msg(agg_trades_sub)

            # might get ack from ws server, or maybe some
            # other msg still in transit..
            res = await ws.recv_msg()
            subid: str | None = res.get('id')
            if subid:
                assert res['id'] == subid

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
                    "id": subid,
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
            aclosing(stream_messages(ws)) as msg_gen,
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
