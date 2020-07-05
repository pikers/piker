"""
Kraken backend.
"""
from dataclasses import dataclass, asdict
from typing import List
import json

import tractor
from trio_websocket import open_websocket_url


async def stream_quotes(
    pairs: List[str] = ['BTC/USD', 'XRP/USD'],
    sub_type: str = 'ohlc',
) -> None:
    """Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol>/<fiat_symbol>.
    """
    async with open_websocket_url(
        'wss://ws.kraken.com',
    ) as ws:
        # setup subs
        # see: https://docs.kraken.com/websockets/#message-subscribe
        subs = {
            'pair': pairs,
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
            volume: int  # Accumulated volume within interval
            count: int  # Number of trades within interval

        while True:
            msg = await recv()
            if isinstance(msg, dict):
                if msg.get('event') == 'heartbeat':
                    continue
            else:
                chan_id, ohlc_array, chan_name, pair = msg
                ohlc = OHLC(chan_id, chan_name, pair, *ohlc_array)
                yield ohlc


if __name__ == '__main__':

    async def stream_ohlc():
        async for msg in stream_quotes():
            print(asdict(msg))

    tractor.run(stream_ohlc)
