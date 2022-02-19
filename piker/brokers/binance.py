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
import hmac
import time
import decimal
import hashlib
import logging

from enum import Enum
from typing import (
    List, Dict, Any, Tuple, Union, Optional, AsyncIterator
)
from decimal import Decimal
from contextlib import asynccontextmanager
from collections import OrderedDict

import asks
import trio
import arrow
import numpy as np
import tractor
import wsproto
from fuzzywuzzy import process as fuzzy
from trio_typing import TaskStatus
from pydantic.dataclasses import dataclass
from pydantic import BaseModel

from . import config
from .api import open_cached_client
from .config import BrokerConfigurationError

from ._util import resproc, SymbolNotFound
from ..log import get_logger, get_console_log
from ..data import ShmArray
from ..data._web_bs import open_autorecon_ws
from ..data._source import ohlc_fields, ohlc_with_index

from ..clearing._messages import (
    BrokerdOrder, BrokerdOrderAck, #BrokerdStatus,
    #BrokerdPosition, BrokerdCancel,
    #BrokerdFill,
    # BrokerdError,
)

log = get_logger(__name__)


def get_config() -> 'configparser.ConfigParser':
    conf, path = config.load()

    section =  conf.get('binance')

    if not section:
        log.warning(f'No config section found for binance in {path}')
        return dict()

    return section


_url = 'https://api.binance.com'
_fapi_url = 'https://testnet.binancefuture.com'

# XXX: some additional fields are defined in the docs:
# https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
ohlc_dtype = np.dtype(ohlc_with_index)


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


class KLine(BaseModel):
    """Description of the flattened KLine quote format.

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

    def as_row(self):
        row = []
        for (name, _) in ohlc_fields[:-1]:
            # TODO: maybe we should go nanoseconds on all
            # history time stamps?
            if name == 'time':
                # convert to epoch seconds: float
                row.append(self.time / 1000.0)

            else:
                row.append(getattr(self, name))

        return row


# convert arrow timestamp to unixtime in miliseconds
def binance_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url

        self._fapi_sesh = asks.Session(connections=4)
        self._fapi_sesh.base_location = _fapi_url

        self._pairs: dict[str, Any] = {}

        conf = get_config()
        self.api_key = conf.get('api', {}).get('key')
        self.api_secret = conf.get('api', {}).get('secret')

        if self.api_key:
            api_key_header = {'X-MBX-APIKEY': self.api_key}
            self._sesh.headers.update(api_key_header)
            self._fapi_sesh.headers.update(api_key_header)

    def _get_signature(self, data: OrderedDict) -> str:

        # XXX: Info on security and authentification
        # https://binance-docs.github.io/apidocs/#endpoint-security-type

        if not self.api_secret:
            raise BrokerConfigurationError(
                'Attempt to get a signature without setting up credentials'
            )

        query_str = '&'.join([
            f'{_key}={value}'
            for _key, value in data.items()])
        logging.info(query_str)
        msg_auth = hmac.new(
            self.api_secret.encode('utf-8'),
            query_str.encode('utf-8'),
            hashlib.sha256
        )
        return msg_auth.hexdigest()

    async def _api(
        self,
        method: str,
        params: Union[dict, OrderedDict],
        signed: bool = False,
        action: str = 'get'
    ) -> Dict[str, Any]:

        if signed:
            params['signature'] = self._get_signature(params)

        resp = await getattr(self._sesh, action)(
            path=f'/api/v3/{method}',
            params=params,
            timeout=float('inf')
        )

        return resproc(resp, log)

    async def _fapi(
        self,
        method: str,
        params: Union[dict, OrderedDict],
        signed: bool = False,
        action: str = 'get'
    ) -> Dict[str, Any]:

        if signed:
            params['signature'] = self._get_signature(params)

        resp = await getattr(self._fapi_sesh, action)(
            path=f'/fapi/v1/{method}',
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

        new_bars = []
        for i, bar in enumerate(bars):
            bar = KLine(**{
                key: bar[j]
                for j, key in enumerate(
                    KLine.__dict__['__fields__'].keys())
            })
            bar_wap = .0
            new_bars.append((i,) + tuple(bar.as_row()) + (bar_wap,))

        array = np.array(
            new_bars,
            dtype=ohlc_dtype
        ) if as_np else bars
        
        return array

    async def submit_limit(
        self,
        symbol: str,
        side: str,  # SELL / BUY
        quantity: float,
        price: float,
        # time_in_force: str = 'GTC',
        oid: Optional[int] = None,
        # iceberg_quantity: Optional[float] = None,
        # order_resp_type: Optional[str] = None,
        recv_window: int = 60000
    ) -> int:
        symbol = symbol.upper()

        await self.cache_symbols()

        asset_precision = self._pairs[symbol]['baseAssetPrecision']
        quote_precision = self._pairs[symbol]['quoteAssetPrecision']

        quantity = Decimal(quantity).quantize(
            Decimal(1 ** -asset_precision),
            rounding=decimal.ROUND_HALF_EVEN
        )

        price = Decimal(price).quantize(
            Decimal(1 ** -quote_precision),
            rounding=decimal.ROUND_HALF_EVEN
        )

        params = OrderedDict([
            ('symbol', symbol),
            ('side', side.upper()),
            ('type', 'LIMIT'),
            ('timeInForce', 'GTC'),
            ('quantity', quantity),
            ('price', price),
            ('recvWindow', recv_window),
            ('newOrderRespType', 'ACK'),
            ('timestamp', binance_timestamp(arrow.utcnow()))
        ])

        if oid:
            params['newClientOrderId'] = oid
        
        resp = await self._api(
            'order/test',  # TODO: switch to real `order` endpoint
            params=params,
            signed=True,
            action='post'
        )

        # return resp['orderId']
        return oid

    async def submit_cancel(
        self,
        symbol: str,
        oid: str,
        recv_window: int = 60000
    ) -> None:
        symbol = symbol.upper()

        params = OrderedDict([
            ('symbol', symbol),
            ('orderId', oid),
            ('recvWindow', recv_window),
            ('timestamp', binance_timestamp(arrow.utcnow()))
        ])

        await self._api(
            'order',
            params=params,
            signed=True,
            action='delete'
        )

    async def get_listen_key(self) -> str:
        return await self._api(
            'userDataStream',
            params={},
            action='post'
        )['listenKey']

    async def keep_alive_key(self, listen_key: str) -> None:
        await self._fapi(
            'userDataStream',
            params={'listenKey': listen_key},
            action='put'
        )

    async def close_listen_key(self, listen_key: str) -> None:
        await self._fapi(
            'userDataStream',
            params={'listenKey': listen_key},
            action='delete'
        )

    @asynccontextmanager
    async def manage_listen_key(self):

        async def periodic_keep_alive(
            self,
            listen_key: str,
            timeout=60 * 29  # 29 minutes
        ):
            while True:
                await trio.sleep(timeout)
                await self.keep_alive_key(listen_key)

        key = await self.get_listen_key()

        async with trio.open_nursery() as n:
            n.start_soon(periodic_keep_alive, key)
            yield key

        await self.close_listen_key(key)


@asynccontextmanager
async def get_client() -> Client:
    client = Client()
    await client.cache_symbols()
    yield client


# validation types
class BookTicker(BaseModel):
  u: int  # order book updateId
  s: str  # symbol
  b: float  # best bid price
  B: float  # best bid qty
  a: float  # best ask price
  A: float  # best ask qty


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


async def stream_messages(ws: NoBsWs) -> AsyncGenerator[NoBsWs, dict]:

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
            msg = BookTicker(**msg)

            yield 'l1', {
                'symbol': msg.s,
                'ticks': [
                    {'type': 'bid', 'price': msg.b, 'size': msg.B},
                    {'type': 'bsize', 'price': msg.b, 'size': msg.B},
                    {'type': 'ask', 'price': msg.a, 'size': msg.A},
                    {'type': 'asize', 'price': msg.a, 'size': msg.A}
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
            si['asset_type'] = 'crypto'

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


async def handle_order_requests(
    ems_order_stream: tractor.MsgStream,
    symbol: str
) -> None:
    async with open_cached_client('binance') as client:
        async for request_msg in ems_order_stream:
            log.info(f'Received order request {request_msg}')

            action = request_msg['action']

            if action in {'buy', 'sell'}:
                # validate
                order = BrokerdOrder(**request_msg)

                # call our client api to submit the order
                reqid = await client.submit_limit(
                    order.symbol,
                    order.action,
                    order.size,
                    order.price,
                    oid=order.oid
                )

                # deliver ack that order has been submitted to broker routing
                await ems_order_stream.send(
                    BrokerdOrderAck(
                        # ems order request id
                        oid=order.oid,
                        # broker specific request id
                        reqid=reqid,
                        time_ns=time.time_ns(),
                    ).dict()
                )

            elif action == 'cancel':
                # msg = BrokerdCancel(**request_msg)
                # 
                # await client.submit_cancel(symbol, msg.reqid)
                ...

            else:
                log.error(f'Unknown order command: {request_msg}')


@tractor.context
async def trades_dialogue(
    ctx: tractor.Context,
    loglevel: str = None
) -> AsyncIterator[Dict[str, Any]]:
    
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    positions = {}  # TODO: get already open pos

    await ctx.started(positions)

    async with (
        ctx.open_stream() as ems_stream,
        trio.open_nursery() as n,
        open_cached_client('binance') as client,
        client.manage_listen_key() as listen_key,
    ):
        n.start_soon(handle_order_requests, ems_stream)
        await trio.sleep_forever()
        # async with open_autorecon_ws(
        #     f'wss://stream.binance.com:9443/ws/{listen_key}',
        # ) as ws:
        #     ...


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
