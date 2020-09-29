"""
Kraken backend.
"""
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, field
from itertools import starmap
from typing import List, Dict, Any, Callable
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

log = get_logger(__name__)


# <uri>/<version>/
_url = 'https://api.kraken.com/0'


# conversion to numpy worthy types
_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('vwap', float),
    ('volume', float),
    ('count', int)
]

# UI components allow this to be declared such that additional
# (historical) fields can be exposed.
ohlc_dtype = np.dtype(_ohlc_dtype)


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

            # convert all fields to native types
            new_bars = []
            last_nz_vwap = None
            for i, bar in enumerate(bars):
                # normalize weird zero-ed vwap values..cmon kraken..
                vwap = float(bar[-3])
                if vwap != 0:
                    last_nz_vwap = vwap
                if vwap == 0:
                    bar[-3] = last_nz_vwap

                new_bars.append(
                    (i,) + tuple(
                        ftype(bar[j]) for j, (name, ftype) in enumerate(_ohlc_dtype[1:])
                    )
                )
            return np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
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


async def recv_ohlc(recv):
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
            chan_id, ohlc_array, chan_name, pair = msg
            yield OHLC(chan_id, chan_name, pair, *ohlc_array)


def normalize(
    ohlc: OHLC,
) -> dict:
    quote = asdict(ohlc)
    quote['broker_ts'] = quote['time']
    quote['brokerd_ts'] = time.time()
    quote['pair'] = quote['pair'].replace('/', '')

    # seriously eh? what's with this non-symmetry everywhere
    # in subscription systems...
    topic = quote['pair'].replace('/', '')

    # print(quote)
    return topic, quote


@tractor.msg.pub
async def stream_quotes(
    get_topics: Callable,
    # These are the symbols not expected by the ws api
    # they are looked up inside this routine.
    symbols: List[str] = ['XBTUSD', 'XMRUSD'],
    sub_type: str = 'ohlc',
    loglevel: str = None,
) -> None:
    """Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol>/<fiat_symbol>.
    """
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    ws_pairs = {}
    async with get_client() as client:
        for sym in symbols:
            ws_pairs[sym] = (await client.symbol_info(sym))['wsname']

    while True:
        try:
            async with trio_websocket.open_websocket_url(
                'wss://ws.kraken.com',
            ) as ws:
                # setup subs
                # see: https://docs.kraken.com/websockets/#message-subscribe
                subs = {
                    'pair': list(ws_pairs.values()),
                    'event': 'subscribe',
                    'subscription': {
                        'name': sub_type,
                        'interval': 1,  # 1 min
                        # 'name': 'ticker',
                        # 'name': 'openOrders',
                        # 'depth': '25',
                    },
                }
                # TODO: we want to eventually allow unsubs which should
                # be completely fine to request from a separate task
                # since internally the ws methods appear to be FIFO
                # locked.
                await ws.send_message(json.dumps(subs))

                async def recv():
                    return json.loads(await ws.get_message())

                # pull a first quote and deliver
                ohlc_gen = recv_ohlc(recv)
                ohlc_last = await ohlc_gen.__anext__()

                topic, quote = normalize(ohlc_last)

                # packetize as {topic: quote}
                yield {topic: quote}

                # keep start of last interval for volume tracking
                last_interval_start = ohlc_last.etime

                # start streaming
                async for ohlc in ohlc_gen:

                    # generate tick values to match time & sales pane:
                    # https://trade.kraken.com/charts/KRAKEN:BTC-USD?period=1m
                    volume = ohlc.volume
                    if ohlc.etime > last_interval_start:  # new interval
                        last_interval_start = ohlc.etime
                        tick_volume = volume
                    else:
                        # this is the tick volume *within the interval*
                        tick_volume = volume - ohlc_last.volume

                    if tick_volume:
                        ohlc.ticks.append({
                            'type': 'trade',
                            'price': ohlc.close,
                            'size': tick_volume,
                        })

                    topic, quote = normalize(ohlc)

                    # XXX: format required by ``tractor.msg.pub``
                    # requires a ``Dict[topic: str, quote: dict]``
                    yield {topic: quote}

                    ohlc_last = ohlc

        except (ConnectionClosed, DisconnectionTimeout):
            log.exception("Good job kraken...reconnecting")


if __name__ == '__main__':

    async def stream_ohlc():
        async for msg in stream_quotes():
            print(msg)

    tractor.run(stream_ohlc)
