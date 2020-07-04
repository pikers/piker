"""
Kraken backend.
"""
from typing import List
import json

import trio
import tractor
from trio_websocket import open_websocket_url


if __name__ == '__main__':

    async def stream_quotes(
        pairs: List[str] = ['BTC/USD'],
    ) -> None:
        """Subscribe ohlc quotes for ``pairs``.

        ``pairs`` must be formatted like `crypto/fiat`.
        """
        async with open_websocket_url(
            'wss://ws.kraken.com',
        ) as ws:
            # setup subs
            subs = {
                'event': 'subscribe',
                'pair': pairs,
                'subscription': {
                    'name': 'ohlc',
                    # 'name': 'ticker',
                    # 'name': 'openOrders',
                    # 'depth': '25',
                },
            }
            await ws.send_message(json.dumps(subs))

            while True:
                msg = json.loads(await ws.get_message())
                if isinstance(msg, dict) and msg.get('event') == 'heartbeat':
                    continue

                print(msg)

    trio.run(stream_quotes)
