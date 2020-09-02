"""
Kraken backend.
"""
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, field
from itertools import starmap
from typing import List, Dict, Any
import json

import trio_websocket
import arrow
import asks
import numpy as np
import tractor

from ._util import resproc, SymbolNotFound, BrokerError
from ..log import get_logger, get_console_log

log = get_logger(__name__)


# <uri>/<version>/
_url = 'https://api.kraken.com/0'


# conversion to numpy worthy types
ohlc_dtype = [
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
            bars = list(starmap(
                lambda i, bar:
                    (i,) + tuple(
                        ftype(bar[i]) for i, (name, ftype)
                        in enumerate(ohlc_dtype[1:])
                    ),
                enumerate(bars))
            )
            return np.array(bars, dtype=ohlc_dtype) if as_np else bars
        except KeyError:
            raise SymbolNotFound(json['error'][0] + f': {symbol}')


@asynccontextmanager
async def get_client() -> Client:
    yield Client()


@dataclass
class OHLC:
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


async def stream_quotes(
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
                await ws.send_message(json.dumps(subs))

                async def recv():
                    return json.loads(await ws.get_message())

                import time

                async def recv_ohlc():
                    last_hb = 0
                    while True:
                        msg = await recv()
                        if isinstance(msg, dict):
                            if msg.get('event') == 'heartbeat':
                                log.trace(
                                    f"Heartbeat after {time.time() - last_hb}")
                                last_hb = time.time()
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

                ohlc_gen = recv_ohlc()
                ohlc_last = await ohlc_gen.__anext__()
                yield asdict(ohlc_last)

                # keep start of last interval for volume tracking
                last_interval_start = ohlc_last.etime

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
                    yield asdict(ohlc)
                    ohlc_last = ohlc
        except trio_websocket._impl.ConnectionClosed:
            log.error("Good job kraken...")


if __name__ == '__main__':

    async def stream_ohlc():
        async for msg in stream_quotes():
            print(msg)

    tractor.run(stream_ohlc)
