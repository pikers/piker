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
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import asdict, field
from types import ModuleType
from typing import List, Dict, Any, Tuple, Union, Optional
import json
import time

import trio_websocket
from trio_typing import TaskStatus
from trio_websocket._impl import (
    ConnectionClosed,
    DisconnectionTimeout,
    ConnectionRejected,
    HandshakeError,
    ConnectionTimeout,
)

import arrow
import asks
import numpy as np
import trio
import tractor
from pydantic.dataclasses import dataclass
from pydantic import BaseModel


from .api import open_cached_client
from ._util import resproc, SymbolNotFound, BrokerError
from ..log import get_logger, get_console_log
from ..data import ShmArray

log = get_logger(__name__)


_url = 'https://api.binance.com'


# Broker specific ohlc schema (websocket)
_websocket_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('close_time', int),
    ('symbol', str),
    ('interval', str),
    ('first_trade_id', int),
    ('last_trade_id', int),
    ('open', float),
    ('close', float),
    ('high', float),
    ('low', float),
    ('volume', float),
    ('num_trades', int),
    ('closed', bool),
    ('quote_asset_volume', float),
    ('taker_buy_base_asset_volume', float),
    ('taker_buy_quote_asset_volume', float),
    ('ignore', int)
]

# Broker specific ohlc schema (rest)
_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('close_time', int),
    ('quote_vol', float),
    ('num_trades', int),
    ('buy_base_vol', float),
    ('buy_quote_vol', float),
    ('ignore', float)
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
    start_time: int
    end_time: int
    symbol: str
    interval: str
    first_id: int
    last_id: int
    open: float
    close: float
    high: float
    low: float
    base_vol: float
    num_trades: int
    closed: bool
    quote_vol: float
    buy_base_vol: float
    buy_quote_vol: float
    ignore: int


# convert arrow timestamp to unixtime in miliseconds
def binance_timestamp(when):
    return int((when.timestamp * 1000) + (when.microsecond / 1000))


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url

    async def _api(
        self,
        method: str,
        data: dict,
    ) -> Dict[str, Any]:
        resp = await self._sesh.get(
            path=f'/api/v3/{method}',
            params=data,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def symbol_info(
        self,
        sym: Optional[str] = None
    ):
        resp = await self._api('exchangeInfo', {})
        if sym is not None:
            for sym_info in resp['symbols']:
                if sym_info['symbol'] == sym:
                    return sym_info
            else:
                raise BrokerError(f'{sym} not found')
        else:
            return resp['symbols']

    async def bars(
        self,
        symbol: str = 'BTCUSDT',
        start_time: int = None,
        end_time: int = None,
        limit: int = 1000,  # <- max allowed per query
        as_np: bool = True,
    ) -> dict:
        if start_time is None:
            start_time = binance_timestamp(
                arrow.utcnow()
                    .floor('minute')
                    .shift(minutes=-limit)
            )
        
        if end_time is None:
            end_time = binance_timestamp(arrow.utcnow())

        bars = await self._api(
            'klines',
            {
                'symbol': symbol,
                'interval': '1m',
                'startTime': start_time,
                'endTime': end_time,
                'limit': limit
            }
        )

        new_bars = [
            (i,) + tuple(
                ftype(bar[j])
                for j, (name, ftype) in enumerate(_ohlc_dtype[1:])
            ) for i, bar in enumerate(bars)
        ] 

        array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
        return array


@asynccontextmanager
async def get_client() -> Client:
    yield Client()


async def stream_messages(ws):

    too_slow_count = last_hb = 0

    while True:

        with trio.move_on_after(5) as cs:
            msg = await ws.recv_msg()

        if msg.get('e') == 'kline':

            yield 'ohlc', OHLC(*msg['k'].values())


def normalize(
    ohlc: OHLC,
) -> dict:
    quote = asdict(ohlc)
    quote['broker_ts'] = quote['start_time']
    quote['brokerd_ts'] = time.time()
    quote['last'] = quote['close']
    quote['time'] = quote['start_time']

    # print(quote)
    return ohlc.symbol, quote


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


class AutoReconWs:
    """Make ``trio_websocketw` sockets stay up no matter the bs.

    """
    recon_errors = (
        ConnectionClosed,
        DisconnectionTimeout,
        ConnectionRejected,
        HandshakeError,
        ConnectionTimeout,
    )

    def __init__(
        self,
        url: str,
        stack: AsyncExitStack,
        serializer: ModuleType = json,
    ):
        self.url = url
        self._stack = stack
        self._ws: 'WebSocketConnection' = None  # noqa

    async def _connect(
        self,
        tries: int = 10000,
    ) -> None:
        try:
            await self._stack.aclose()
        except (DisconnectionTimeout, RuntimeError):
            await trio.sleep(1)

        last_err = None
        for i in range(tries):
            try:
                self._ws = await self._stack.enter_async_context(
                    trio_websocket.open_websocket_url(self.url)
                )
                log.info(f'Connection success: {self.url}')
                return
            except self.recon_errors as err:
                last_err = err
                log.error(
                    f'{self} connection bail with '
                    f'{type(err)}...retry attempt {i}'
                )
                await trio.sleep(1)
                continue
        else:
            log.exception('ws connection fail...')
            raise last_err

    async def send_msg(
        self,
        data: Any,
    ) -> None:
        while True:
            try:
                return await self._ws.send_message(json.dumps(data))
            except self.recon_errors:
                await self._connect()

    async def recv_msg(
        self,
    ) -> Any:
        while True:
            try:
                return json.loads(await self._ws.get_message())
            except self.recon_errors:
                await self._connect()


@asynccontextmanager
async def open_autorecon_ws(url):
    """Apparently we can QoS for all sorts of reasons..so catch em.

    """
    async with AsyncExitStack() as stack:
        ws = AutoReconWs(url, stack)

        await ws._connect()
        try:
            yield ws

        finally:
            await stack.aclose()


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
    """Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol><fiat_symbol>.
    """
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    sym_infos = {}
    uid = 0

    async with open_cached_client('binance') as client, send_chan as send_chan:

        # keep client cached for real-time section
        for sym in symbols:
            d = await client.symbol_info(sym)
            syminfo = Pair(**d)  # validation
            sym_infos[sym] = syminfo.dict()

        symbol = symbols[0]

        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            symbol: {
                'symbol_info': sym_infos[sym],
                'shm_write_opts': {'sum_tick_vml': False},
            },
        }

        async with open_autorecon_ws('wss://stream.binance.com/ws') as ws:

            # XXX: setup subs
            ohlc_sub = make_sub(symbols, 'kline_1m', uid)
            uid += 1

            await ws.send_msg(ohlc_sub)
            res = await ws.recv_msg()

            # trade data (aka L1)
            l1_sub = make_sub(symbols, 'trade', uid)
            uid += 1

            await ws.send_msg(l1_sub)
            res = await ws.recv_msg()

            # pull a first quote and deliver
            msg_gen = stream_messages(ws)

            # TODO: use ``anext()`` when it lands in 3.10!
            typ, ohlc_last = await msg_gen.__anext__()

            topic, quote = normalize(ohlc_last)

            first_quote = {topic: quote}
            task_status.started((init_msgs,  first_quote))

            # lol, only "closes" when they're margin squeezing clients ;P
            feed_is_live.set()

            # keep start of last interval for volume tracking
            last_interval_start = ohlc_last.end_time

            # start streaming
            async for typ, ohlc in msg_gen:
                ...
                # if typ == 'ohlc':

                #     # TODO: can get rid of all this by using
                #     # ``trades`` subscription...

                #     # generate tick values to match time & sales pane:
                #     # https://trade.kraken.com/charts/KRAKEN:BTC-USD?period=1m
                #     volume = ohlc.volume

                #     # new OHLC sample interval
                #     if ohlc.etime > last_interval_start:
                #         last_interval_start = ohlc.etime
                #         tick_volume = volume

                #     else:
                #         # this is the tick volume *within the interval*
                #         tick_volume = volume - ohlc_last.volume

                #     ohlc_last = ohlc
                #     last = ohlc.close

                #     if tick_volume:
                #         ohlc.ticks.append({
                #             'type': 'trade',
                #             'price': last,
                #             'size': tick_volume,
                #         })

                #     topic, quote = normalize(ohlc)

                # elif typ == 'l1':
                #     quote = ohlc
                #     topic = quote['symbol']

                # # XXX: format required by ``tractor.msg.pub``
                # # requires a ``Dict[topic: str, quote: dict]``
                # await send_chan.send({topic: quote})
