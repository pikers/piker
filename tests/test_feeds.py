'''
Data feed layer APIs, performance, msg throttling.

'''
from collections import Counter
from pprint import pprint
from typing import AsyncContextManager

import pytest
# import tractor
import trio
from piker.data import (
    ShmArray,
    open_feed,
)
from piker.data.flows import Flume
from piker.accounting._mktinfo import (
    unpack_fqme,
)


@pytest.mark.parametrize(
    'fqmes',
    [
        # binance
        (100, {'btcusdt.binance', 'ethusdt.binance'}, False),

        # kraken
        (20, {'ethusdt.kraken', 'xbtusd.kraken'}, True),

        # binance + kraken
        (100, {'btcusdt.binance', 'xbtusd.kraken'}, False),
    ],
    ids=lambda param: f'quotes={param[0]}@fqmes={param[1]}',
)
def test_multi_fqsn_feed(
    open_test_pikerd: AsyncContextManager,
    fqmes: set[str],
    loglevel: str,
    ci_env: bool
):
    '''
    Start a real-time data feed for provided fqme and pull
    a few quotes then simply shut down.

    '''
    max_quotes, fqmes, run_in_ci = fqmes

    if (
        ci_env
        and not run_in_ci
    ):
        pytest.skip('Skipping CI disabled test due to feed restrictions')

    brokers = set()
    for fqme in fqmes:
        brokername, *_ = unpack_fqme(fqme)
        brokers.add(brokername)

    async def main():
        async with (
            open_test_pikerd(),
            open_feed(
                fqmes,
                loglevel=loglevel,

                # TODO: ensure throttle rate is applied
                # limit to at least display's FPS
                # avoiding needless Qt-in-guest-mode context switches
                # tick_throttle=_quote_throttle_rate,

            ) as feed
        ):
            # verify shm buffers exist
            for fqin in fqmes:
                flume = feed.flumes[fqin]
                ohlcv: ShmArray = flume.rt_shm
                hist_ohlcv: ShmArray = flume.hist_shm

            async with feed.open_multi_stream(brokers) as stream:

                # pull the first startup quotes, one for each fqme, and
                # ensure they match each flume's startup quote value.
                fqsns_copy = fqmes.copy()
                with trio.fail_after(0.5):
                    for _ in range(1):
                        first_quotes = await stream.receive()
                        for fqme, quote in first_quotes.items():

                            # XXX: TODO: WTF apparently this error will get
                            # supressed and only show up in the teardown
                            # excgroup if we don't have the fix from
                            # <tractorbugurl>
                            # assert 0

                            fqsns_copy.remove(fqme)
                            flume: Flume = feed.flumes[fqme]
                            assert quote['last'] == flume.first_quote['last']

                cntr = Counter()
                with trio.fail_after(6):
                    async for quotes in stream:
                        for fqme, quote in quotes.items():
                            cntr[fqme] += 1

                            # await tractor.breakpoint()
                            flume = feed.flumes[fqme]
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
                               f'{fqme}: {quote}\n'
                               f'rt_ohlc: {rt_row}\n'
                               f'hist_ohlc: {hist_row}\n'
                            )

                        if cntr.total() >= max_quotes:
                            break

            assert set(cntr.keys()) == fqmes

    trio.run(main)
