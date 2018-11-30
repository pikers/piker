"""
Questrade broker testing
"""
import time

import trio
import tractor
from trio.testing import trio_test
from tractor.testing import tractor_test
from piker.brokers import questrade as qt
import pytest


@pytest.fixture(autouse=True)
def check_qt_conf_section(brokerconf):
    """Skip this module's tests if we have not quetrade API creds.
    """
    if not brokerconf.has_section('questrade'):
        pytest.skip("No questrade API credentials available")


# stock quote
_ex_quotes = {
    'stock': {
        "VWAP": 7.383792,
        "askPrice": 7.56,
        "askSize": 2,
        "bidPrice": 6.1,
        "bidSize": 2,
        "delay": 0,
        "high52w": 9.68,
        "highPrice": 8,
        "key": "EMH.VN",
        "isHalted": 'false',
        "lastTradePrice": 6.96,
        "lastTradePriceTrHrs": 6.97,
        "lastTradeSize": 2000,
        "lastTradeTick": "Down",
        "lastTradeTime": "2018-02-07T15:59:59.259000-05:00",
        "low52w": 1.03,
        "lowPrice": 6.88,
        "openPrice": 7.64,
        "symbol": "EMH.VN",
        "symbolId": 10164524,
        "tier": "",
        "volume": 5357805
    },
    'option': {
        'VWAP': 0,
        'askPrice': None,
        'askSize': 0,
        'bidPrice': None,
        'bidSize': 0,
        'delay': 0,
        'delta': -0.212857,
        'gamma': 0.003524,
        'highPrice': 0,
        'isHalted': False,
        "key": ["WEED.TO", '2018-10-23T00:00:00.000000-04:00'],
        'lastTradePrice': 22,
        'lastTradePriceTrHrs': None,
        'lastTradeSize': 0,
        'lastTradeTick': 'Equal',
        'lastTradeTime': '2018-10-23T00:00:00.000000-04:00',
        'lowPrice': 0,
        'openInterest': 1,
        'openPrice': 0,
        'rho': -0.891868,
        'symbol': 'WEED15Jan21P54.00.MX',
        'symbolId': 22739148,
        'theta': -0.012911,
        'underlying': 'WEED.TO',
        'underlyingId': 16529510,
        'vega': 0.220885,
        'volatility': 75.514171,
        'volume': 0
    }
}


def match_packet(symbols, quotes, feed_type='stock'):
    """Verify target ``symbols`` match keys in ``quotes`` packet.
    """
    assert len(quotes) == len(symbols)
    # for ticker in symbols:
    for quote in quotes.copy():
        assert quote['key'] in symbols
        quotes.remove(quote)

        # verify the quote packet format hasn't changed
        for key in _ex_quotes[feed_type]:
            quote.pop(key)

        # no additional fields either
        assert not quote

    # not more quotes then in target set
    assert not quotes


@trio_test
async def test_batched_stock_quote(us_symbols):
    """Use the client stock quote api and verify quote response format.
    """
    async with qt.get_client() as client:
        quotes = await client.quote(us_symbols)
        assert len(quotes) == len(us_symbols)
        match_packet(us_symbols, quotes)


@trio_test
async def test_stock_quoter_context(us_symbols):
    """Test that a quoter "context" used by the data feed daemon.
    """
    async with qt.get_client() as client:
        quoter = await qt.stock_quoter(client, us_symbols)
        quotes = await quoter(us_symbols)
        match_packet(us_symbols, quotes)


@trio_test
async def test_option_contracts(tmx_symbols):
    """Verify we can retrieve contracts by expiry.
    """
    async with qt.get_client() as client:
        for symbol in tmx_symbols:
            contracts = await client.symbol2contracts(symbol)
            key, byroot = next(iter(contracts.items()))
            assert isinstance(key.id, int)
            assert isinstance(byroot, dict)
            for key in contracts:
                # check that datetime is same as reported in contract
                assert key.expiry.isoformat(
                    timespec='microseconds') == contracts[key]['expiryDate']


@trio_test
async def test_option_chain(tmx_symbols):
    """Verify we can retrieve all option chains for a list of symbols.
    """
    async with qt.get_client() as client:
        # contract lookup - should be cached
        contracts = await client.get_all_contracts([tmx_symbols[0]])
        # chains quote for all symbols
        quotes = await client.option_chains(contracts)
        # verify contents match what we expect
        for quote in quotes:
            assert quote['underlying'] in tmx_symbols
            for key in _ex_quotes['option']:
                quote.pop(key)
            assert not quote


@trio_test
async def test_option_quote_latency(tmx_symbols):
    """Audit option quote latencies.
    """
    async with qt.get_client() as client:
        # all contracts lookup - should be cached
        contracts = await client.get_all_contracts(['WEED.TO'])

        # build single expriry contract
        id, by_expiry = next(iter(contracts.items()))
        dt, by_strike = next(iter(by_expiry.items()))
        single = {id: {dt: None}}

        for expected_latency, contract in [
            # NOTE: request latency is usually 2x faster that these
            (5, contracts), (0.5, single)
        ]:
            for _ in range(3):
                # chains quote for all symbols
                start = time.time()
                await client.option_chains(contract)
                took = time.time() - start
                print(f"Request took {took}")
                assert took <= expected_latency
                await trio.sleep(0.1)


@tractor_test
async def test_option_streaming(tmx_symbols, loglevel):
    """Set up option streaming using the broker daemon.
    """
    async with tractor.find_actor('brokerd') as portal:
        async with tractor.open_nursery() as nursery:
            # only one per host address, spawns an actor if None
            if not portal:
                # no brokerd actor found
                portal = await nursery.start_actor(
                    'data_feed',
                    rpc_module_paths=[
                        'piker.brokers.data',
                        'piker.brokers.core'
                    ],
                )

            symbol = 'APHA.TO'  # your fave greenhouse LP
            async with qt.get_client() as client:
                contracts = await client.get_all_contracts([symbol])

            contractkey = next(iter(contracts))
            subs_keys = list(
                map(lambda item: (item.symbol, item.expiry), contracts))
            sub = subs_keys[0]

            agen = await portal.run(
                'piker.brokers.data',
                'start_quote_stream',
                broker='questrade',
                symbols=[sub],
                feed_type='option',
                diff_cached=False,
            )
            try:
                # wait on the data streamer to actually start
                # delivering
                await agen.__anext__()

                # it'd sure be nice to have an asyncitertools here...
                with trio.fail_after(2.1):
                    loops = 8
                    count = 0
                    async for quotes in agen:
                        # print(f'got quotes for {quotes.keys()}')
                        # we should receive all calls and puts
                        assert len(quotes) == len(contracts[contractkey]) * 2
                        for symbol, quote in quotes.items():
                            assert quote['key'] == sub
                            for key in _ex_quotes['option']:
                                quote.pop(key)
                            assert not quote
                        count += 1
                        if count == loops:
                            break
            finally:
                # unsub
                await portal.run(
                    'piker.brokers.data',
                    'modify_quote_stream',
                    broker='questrade',
                    feed_type='option',
                    tickers=[],
                )
                # stop all spawned subactors
                await nursery.cancel()
