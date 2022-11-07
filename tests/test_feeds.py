'''
Data feed layer APIs, performance, msg throttling.

'''
from pprint import pprint

import pytest
import trio
from piker import (
    open_piker_runtime,
    open_feed,
)
from piker.data import ShmArray


@pytest.mark.parametrize(
    'fqsns',
    [
        ['btcusdt.binance']
    ],
    ids=lambda param: f'fqsns={param}',
)
def test_basic_rt_feed(
    fqsns: list[str],
):
    '''
    Start a real-time data feed for provided fqsn and pull
    a few quotes then simply shut down.

    '''
    async def main():
        async with (
            open_piker_runtime('test_basic_rt_feed'),
            open_feed(
                fqsns,
                loglevel='info',

                # TODO: ensure throttle rate is applied
                # limit to at least display's FPS
                # avoiding needless Qt-in-guest-mode context switches
                # tick_throttle=_quote_throttle_rate,

            ) as feed
        ):
            for fqin in fqsns:
                assert feed.symbols[fqin]

            ohlcv: ShmArray = feed.rt_shm
            hist_ohlcv: ShmArray = feed.hist_shm

            count: int = 0
            async for quotes in feed.stream:

                # print quote msg, rt and history
                # buffer values on console.
                pprint(quotes)
                pprint(ohlcv.array[-1])
                pprint(hist_ohlcv.array[-1])

                if count >= 100:
                    break

                count += 1

    trio.run(main)
