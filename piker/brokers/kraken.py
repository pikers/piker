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

"""
Kraken backend.
"""
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import asdict, field
from types import ModuleType
from typing import List, Dict, Any, Tuple, Optional
import json
import time

import trio_websocket
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

from ._util import resproc, SymbolNotFound, BrokerError
from ..log import get_logger, get_console_log
from ..data import (
    _buffer,
    # iterticks,
    attach_shm_array,
    get_shm_token,
    subscribe_ohlc_for_increment,
)

log = get_logger(__name__)


# <uri>/<version>/
_url = 'https://api.kraken.com/0'


# Broker specific ohlc schema which includes a vwap field
_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('count', int),
    ('bar_wap', float),
]

# UI components allow this to be declared such that additional
# (historical) fields can be exposed.
ohlc_dtype = np.dtype(_ohlc_dtype)

_show_wap_in_history = True


_symbol_info_translation: Dict[str, str] = {
    'tick_decimals': 'pair_decimals',
}


# https://www.kraken.com/features/api#get-tradable-pairs
class Pair(BaseModel):
    altname: str  # alternate pair name
    wsname: str  # WebSocket pair name (if available)
    aclass_base: str  # asset class of base component
    base: str  # asset id of base component
    aclass_quote: str  # asset class of quote component
    quote: str  # asset id of quote component
    lot: str  # volume lot size

    pair_decimals: int  # scaling decimal places for pair
    lot_decimals: int  # scaling decimal places for volume

    # amount to multiply lot volume by to get currency volume
    lot_multiplier: float

    # array of leverage amounts available when buying
    leverage_buy: List[int]
    # array of leverage amounts available when selling
    leverage_sell: List[int]

    # fee schedule array in [volume, percent fee] tuples
    fees: List[Tuple[int, float]]

    # maker fee schedule array in [volume, percent fee] tuples (if on
    # maker/taker)
    fees_maker: List[Tuple[int, float]]

    fee_volume_currency: str  # volume discount currency
    margin_call: str  # margin call level
    margin_stop: str  # stop-out/liquidation margin level
    ordermin: float  # minimum order volume for pair


@dataclass
class OHLC:
    """Description of the flattened OHLC quote format.

    For schema details see:
        https://docs.kraken.com/websockets/#message-ohlc
    """
    chan_id: int  # internal kraken id
    chan_name: str  # eg. ohlc-1  (name-interval)
    pair: str  # fx pair
    time: float  # Begin time of interval, in seconds since epoch
    etime: float  # End time of interval, in seconds since epoch
    open: float  # Open price of interval
    high: float  # High price within interval
    low: float  # Low price within interval
    close: float  # Close price of interval
    vwap: float  # Volume weighted average price within interval
    volume: float  # Accumulated volume **within interval**
    count: int  # Number of trades within interval
    # (sampled) generated tick data
    ticks: List[Any] = field(default_factory=list)


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url
        self._sesh.headers.update({
            'User-Agent':
                'krakenex/2.1.0 (+https://github.com/veox/python3-krakenex)'
        })

    async def _public(
        self,
        method: str,
        data: dict,
    ) -> Dict[str, Any]:
        resp = await self._sesh.post(
            path=f'/public/{method}',
            json=data,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def symbol_info(
        self,
        pair: str = 'all',
    ):
        resp = await self._public('AssetPairs', {'pair': pair})
        err = resp['error']
        if err:
            raise BrokerError(err)
        true_pair_key, data = next(iter(resp['result'].items()))
        return data

    async def bars(
        self,
        symbol: str = 'XBTUSD',
        # UTC 2017-07-02 12:53:20
        since: int = None,
        count: int = 720,  # <- max allowed per query
        as_np: bool = True,
    ) -> dict:
        if since is None:
            since = arrow.utcnow().floor('minute').shift(
                minutes=-count).timestamp
        # UTC 2017-07-02 12:53:20 is oldest seconds value
        since = str(max(1499000000, since))
        json = await self._public(
            'OHLC',
            data={
                'pair': symbol,
                'since': since,
            },
        )
        try:
            res = json['result']
            res.pop('last')
            bars = next(iter(res.values()))

            new_bars = []

            first = bars[0]
            last_nz_vwap = first[-3]
            if last_nz_vwap == 0:
                # use close if vwap is zero
                last_nz_vwap = first[-4]

            # convert all fields to native types
            for i, bar in enumerate(bars):
                # normalize weird zero-ed vwap values..cmon kraken..
                # indicates vwap didn't change since last bar
                vwap = float(bar.pop(-3))
                if vwap != 0:
                    last_nz_vwap = vwap
                if vwap == 0:
                    vwap = last_nz_vwap

                # re-insert vwap as the last of the fields
                bar.append(vwap)

                new_bars.append(
                    (i,) + tuple(
                        ftype(bar[j]) for j, (name, ftype) in enumerate(
                            _ohlc_dtype[1:]
                        )
                    )
                )
            array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
            return array
        except KeyError:
            raise SymbolNotFound(json['error'][0] + f': {symbol}')


@asynccontextmanager
async def get_client() -> Client:
    yield Client()


async def stream_messages(ws):

    too_slow_count = last_hb = 0

    while True:

        with trio.move_on_after(5) as cs:
            msg = await ws.recv_msg()

        # trigger reconnection if heartbeat is laggy
        if cs.cancelled_caught:

            too_slow_count += 1

            if too_slow_count > 10:
                log.warning(
                    "Heartbeat is too slow, resetting ws connection")

                await ws._connect()
                too_slow_count = 0
                continue

        if isinstance(msg, dict):
            if msg.get('event') == 'heartbeat':

                now = time.time()
                delay = now - last_hb
                last_hb = now

                # XXX: why tf is this not printing without --tl flag?
                log.debug(f"Heartbeat after {delay}")
                # print(f"Heartbeat after {delay}")

                continue

            err = msg.get('errorMessage')
            if err:
                raise BrokerError(err)
        else:
            chan_id, *payload_array, chan_name, pair = msg

            if 'ohlc' in chan_name:

                yield 'ohlc', OHLC(chan_id, chan_name, pair, *payload_array[0])

            elif 'spread' in chan_name:

                bid, ask, ts, bsize, asize = map(float, payload_array[0])

                # TODO: really makes you think IB has a horrible API...
                quote = {
                    'symbol': pair.replace('/', ''),
                    'ticks': [
                        {'type': 'bid', 'price': bid, 'size': bsize},
                        {'type': 'bsize', 'price': bid, 'size': bsize},

                        {'type': 'ask', 'price': ask, 'size': asize},
                        {'type': 'asize', 'price': ask, 'size': asize},
                    ],
                }
                yield 'l1', quote

            # elif 'book' in msg[-2]:
            #     chan_id, *payload_array, chan_name, pair = msg
            #     print(msg)

            else:
                print(f'UNHANDLED MSG: {msg}')


def normalize(
    ohlc: OHLC,
) -> dict:
    quote = asdict(ohlc)
    quote['broker_ts'] = quote['time']
    quote['brokerd_ts'] = time.time()
    quote['symbol'] = quote['pair'] = quote['pair'].replace('/', '')
    quote['last'] = quote['close']

    # seriously eh? what's with this non-symmetry everywhere
    # in subscription systems...
    topic = quote['pair'].replace('/', '')

    # print(quote)
    return topic, quote


def make_sub(pairs: List[str], data: Dict[str, Any]) -> Dict[str, str]:
    """Create a request subscription packet dict.

    https://docs.kraken.com/websockets/#message-subscribe

    """
    # eg. specific logic for this in kraken's sync client:
    # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
    return {
        'pair': pairs,
        'event': 'subscribe',
        'subscription': data,
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
        # async with trio_websocket.open_websocket_url(url) as ws:
        #     await tractor.breakpoint()

        await ws._connect()
        try:
            yield ws

        finally:
            await stack.aclose()


# @tractor.msg.pub
async def stream_quotes(
    # get_topics: Callable,
    shm_token: Tuple[str, str, List[tuple]],
    symbols: List[str] = ['XBTUSD', 'XMRUSD'],
    # These are the symbols not expected by the ws api
    # they are looked up inside this routine.
    sub_type: str = 'ohlc',
    loglevel: str = None,
    # compat with eventual ``tractor.msg.pub``
    topics: Optional[List[str]] = None,
) -> None:
    """Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol>/<fiat_symbol>.
    """
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    ws_pairs = {}
    sym_infos = {}
    async with get_client() as client:

        # keep client cached for real-time section
        for sym in symbols:
            si = Pair(**await client.symbol_info(sym))  # validation
            syminfo = si.dict()
            syminfo['price_tick_size'] = 1 / 10**si.pair_decimals
            syminfo['lot_tick_size'] = 1 / 10**si.lot_decimals
            sym_infos[sym] = syminfo
            ws_pairs[sym] = si.wsname

        # maybe load historical ohlcv in to shared mem
        # check if shm has already been created by previous
        # feed initialization
        writer_exists = get_shm_token(shm_token['shm_name'])

        symbol = symbols[0]

        if not writer_exists:
            shm = attach_shm_array(
                token=shm_token,
                # we are writer
                readonly=False,
            )
            bars = await client.bars(symbol=symbol)

            shm.push(bars)
            shm_token = shm.token

            times = shm.array['time']
            delay_s = times[-1] - times[times != times[-1]][-1]
            subscribe_ohlc_for_increment(shm, delay_s)

        # yield shm_token, not writer_exists
        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            symbol: {
                'is_shm_writer': not writer_exists,
                'shm_token': shm_token,
                'symbol_info': sym_infos[sym],
            }
            # for sym in symbols
        }
        yield init_msgs

        async with open_autorecon_ws('wss://ws.kraken.com/') as ws:

            # XXX: setup subs
            # https://docs.kraken.com/websockets/#message-subscribe
            # specific logic for this in kraken's shitty sync client:
            # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
            ohlc_sub = make_sub(
                list(ws_pairs.values()),
                {'name': 'ohlc', 'interval': 1}
            )

            # TODO: we want to eventually allow unsubs which should
            # be completely fine to request from a separate task
            # since internally the ws methods appear to be FIFO
            # locked.
            await ws.send_msg(ohlc_sub)

            # trade data (aka L1)
            l1_sub = make_sub(
                list(ws_pairs.values()),
                {'name': 'spread'}  # 'depth': 10}
            )

            await ws.send_msg(l1_sub)

            # pull a first quote and deliver
            msg_gen = stream_messages(ws)

            typ, ohlc_last = await msg_gen.__anext__()

            topic, quote = normalize(ohlc_last)

            # packetize as {topic: quote}
            yield {topic: quote}

            # tell incrementer task it can start
            _buffer.shm_incrementing(shm_token['shm_name']).set()

            # keep start of last interval for volume tracking
            last_interval_start = ohlc_last.etime

            # start streaming
            async for typ, ohlc in msg_gen:

                if typ == 'ohlc':

                    # TODO: can get rid of all this by using
                    # ``trades`` subscription...

                    # generate tick values to match time & sales pane:
                    # https://trade.kraken.com/charts/KRAKEN:BTC-USD?period=1m
                    volume = ohlc.volume

                    # new interval
                    if ohlc.etime > last_interval_start:
                        last_interval_start = ohlc.etime
                        tick_volume = volume
                    else:
                        # this is the tick volume *within the interval*
                        tick_volume = volume - ohlc_last.volume

                    last = ohlc.close
                    if tick_volume:
                        ohlc.ticks.append({
                            'type': 'trade',
                            'price': last,
                            'size': tick_volume,
                        })

                    topic, quote = normalize(ohlc)

                    # if we are the lone tick writer start writing
                    # the buffer with appropriate trade data
                    if not writer_exists:
                        # update last entry
                        # benchmarked in the 4-5 us range
                        o, high, low, v = shm.array[-1][
                            ['open', 'high', 'low', 'volume']
                        ]
                        new_v = tick_volume

                        if v == 0 and new_v:
                            # no trades for this bar yet so the open
                            # is also the close/last trade price
                            o = last

                        # write shm
                        shm.array[
                            ['open',
                             'high',
                             'low',
                             'close',
                             'bar_wap',  # in this case vwap of bar
                             'volume']
                        ][-1] = (
                            o,
                            max(high, last),
                            min(low, last),
                            last,
                            ohlc.vwap,
                            volume,
                        )
                    ohlc_last = ohlc

                elif typ == 'l1':
                    quote = ohlc
                    topic = quote['symbol']

                # XXX: format required by ``tractor.msg.pub``
                # requires a ``Dict[topic: str, quote: dict]``
                yield {topic: quote}
