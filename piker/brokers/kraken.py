# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any, Tuple, Optional
import json
import time

import trio_websocket
from trio_websocket._impl import ConnectionClosed, DisconnectionTimeout
import arrow
import asks
import numpy as np
import trio
import tractor

from ._util import resproc, SymbolNotFound, BrokerError
from ..log import get_logger, get_console_log
from ..data import (
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

    # XXX: ugh, super hideous.. needs built-in converters.
    def __post_init__(self):
        for f, val in self.__dataclass_fields__.items():
            if f == 'ticks':
                continue
            setattr(self, f, val.type(getattr(self, f)))


async def recv_msg(recv):
    too_slow_count = last_hb = 0

    while True:
        with trio.move_on_after(1.5) as cs:
            msg = await recv()

        # trigger reconnection logic if too slow
        if cs.cancelled_caught:
            too_slow_count += 1
            if too_slow_count > 2:
                log.warning(
                    "Heartbeat is to slow, "
                    "resetting ws connection")
                raise trio_websocket._impl.ConnectionClosed(
                    "Reset Connection")

        if isinstance(msg, dict):
            if msg.get('event') == 'heartbeat':

                now = time.time()
                delay = now - last_hb
                last_hb = now
                log.trace(f"Heartbeat after {delay}")

                # TODO: hmm i guess we should use this
                # for determining when to do connection
                # resets eh?
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
    async with get_client() as client:

        # keep client cached for real-time section
        for sym in symbols:
            ws_pairs[sym] = (await client.symbol_info(sym))['wsname']

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

        yield shm_token, not writer_exists

        while True:
            try:
                async with trio_websocket.open_websocket_url(
                    'wss://ws.kraken.com/',
                ) as ws:

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
                    await ws.send_message(json.dumps(ohlc_sub))

                    # trade data (aka L1)
                    l1_sub = make_sub(
                        list(ws_pairs.values()),
                        {'name': 'spread'}  # 'depth': 10}

                    )
                    await ws.send_message(json.dumps(l1_sub))

                    async def recv():
                        return json.loads(await ws.get_message())

                    # pull a first quote and deliver
                    msg_gen = recv_msg(recv)
                    typ, ohlc_last = await msg_gen.__anext__()

                    topic, quote = normalize(ohlc_last)

                    # packetize as {topic: quote}
                    yield {topic: quote}

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

            except (ConnectionClosed, DisconnectionTimeout):
                log.exception("Good job kraken...reconnecting")
