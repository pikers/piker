'''
Data feed layer APIs, performance, msg throttling.

'''
from pprint import pprint

import pytest
import tractor
import trio
from piker import (
    open_piker_runtime,
    open_feed,
)
from piker.data import ShmArray


@pytest.mark.parametrize(
    'fqsns',
    [
        ['btcusdt.binance', 'ethusdt.binance']
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
            open_piker_runtime(
                'test_basic_rt_feed',
                # XXX tractor BUG: this doesn't translate through to the
                # ``tractor._state._runtimevars``...
                registry_addr=('127.0.0.1', 6666),
                debug_mode=True,
            ),
            open_feed(
                fqsns,
                loglevel='info',

                # TODO: ensure throttle rate is applied
                # limit to at least display's FPS
                # avoiding needless Qt-in-guest-mode context switches
                # tick_throttle=_quote_throttle_rate,

            ) as feed
        ):
            # verify shm buffers exist
            for fqin in fqsns:
                flume = feed.flumes[fqin]
                ohlcv: ShmArray = flume.rt_shm
                hist_ohlcv: ShmArray = flume.hist_shm

            quote_count: int = 0
            stream = feed.streams['binance']
            async for quotes in stream:
                for fqsn, quote in quotes.items():

                    # await tractor.breakpoint()
                    flume = feed.flumes[fqsn]
                    ohlcv: ShmArray = flume.rt_shm
                    hist_ohlcv: ShmArray = flume.hist_shm

                    # print quote msg, rt and history
                    # buffer values on console.
                    rt_row = ohlcv.array[-1]
                    hist_row = hist_ohlcv.array[-1]
                    # last = quote['last']

                    # assert last == rt_row['close']
                    # assert last == hist_row['close']
                    pprint(
                        f'{fqsn}: {quote}\n'
                        f'rt_ohlc: {rt_row}\n'
                        f'hist_ohlc: {hist_row}\n'
                    )
                    quote_count += 1

                if quote_count >= 100:
                    break

    trio.run(main)
