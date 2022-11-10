'''
Data feed layer APIs, performance, msg throttling.

'''
from collections import Counter
from pprint import pprint

import pytest
import tractor
import trio
from piker import (
    open_piker_runtime,
    open_feed,
)
from piker.data import ShmArray
from piker.data._source import (
    unpack_fqsn,
)


@pytest.mark.parametrize(
    'fqsns',
    [
        (100, {'btcusdt.binance', 'ethusdt.binance'}),
        (50, {'xbteur.kraken', 'xbtusd.kraken'}),
    ],
    ids=lambda param: f'quotes={param[0]}@fqsns={param[1]}',
)
def test_basic_rt_feed(
    fqsns: set[str],
):
    '''
    Start a real-time data feed for provided fqsn and pull
    a few quotes then simply shut down.

    '''
    max_quotes, fqsns = fqsns

    brokers = set()
    for fqsn in fqsns:
        brokername, key, suffix = unpack_fqsn(fqsn)
        brokers.add(brokername)

    # NOTE: we only have single broker-backed multi-symbol streams
    # currently.
    assert len(brokers) == 1

    async def main():
        async with (
            open_piker_runtime(
                'test_basic_rt_feed',

                # XXX tractor BUG: this doesn't translate through to the
                # ``tractor._state._runtimevars``...
                registry_addr=('127.0.0.1', 6666),

                # debug_mode=True,
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

            # stream some ticks and ensure we see data from both symbol
            # subscriptions.
            stream = feed.streams[brokername]

            # pull the first startup quotes, one for each fqsn, and
            # ensure they match each flume's startup quote value.
            fqsns_copy = fqsns.copy()
            for _ in range(1):
                first_quotes = await stream.receive()
                for fqsn, quote in first_quotes.items():

                    # XXX: TODO: WTF apparently this error will get
                    # supressed and only show up in the teardown
                    # excgroup if we don't have the fix from
                    # <tractorbugurl>
                    # assert 0

                    fqsns_copy.remove(fqsn)
                    flume = feed.flumes[fqsn]
                    assert quote['last'] == flume.first_quote['last']

            cntr = Counter()
            async for quotes in stream:
                for fqsn, quote in quotes.items():
                    cntr[fqsn] += 1

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

                if cntr.total() >= 100:
                    break

            # await tractor.breakpoint()
            assert set(cntr.keys()) == fqsns

    trio.run(main)
