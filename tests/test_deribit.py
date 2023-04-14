import trio
import pytest
import tractor

from piker import config

from piker.brokers.deribit import api as deribit
from piker.brokers.deribit.api import _testnet_ws_url

from piker._cacheables import open_cached_client


TESTNET_KEY_ID: str | None = None
TESTNET_KEY_SECRET: str | None = None

@pytest.mark.skipif(
    not TESTNET_KEY_ID or not TESTNET_KEY_SECRET,
    reason='configure a deribit testnet key pair before running this test'
)
def test_deribit_get_ticker(open_test_pikerd):

    async def _test_main():
        async with open_test_pikerd() as _:
            async with open_cached_client('deribit') as client:

                symbols = await client.symbol_info()

                syms = list(symbols.keys())
                sym = syms[int(len(syms) / 2)]

                async with deribit.maybe_open_ticker_feed(sym) as tick_stream:
                    async for typ, msg in tick_stream:
                        assert typ == 'ticker'
                        assert 'open_interest' in msg['data']
                        break



    config.write({
        'deribit': {
            'ws_url': _testnet_ws_url,
            'key_id': TESTNET_KEY_ID,
            'key_secret': TESTNET_KEY_SECRET
        }
    })
    trio.run(_test_main)
