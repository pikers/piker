"""
Questrade broker testing
"""
import time

import trio
from trio.testing import trio_test
from piker.brokers import questrade as qt
import pytest


@pytest.fixture(autouse=True)
def check_qt_conf_section(brokerconf):
    """Skip this module's tests if we have not quetrade API creds.
    """
    if not brokerconf.has_section('questrade'):
        pytest.skip("No questrade API credentials available")


# stock quote
_ex_quote = {
    "VWAP": 7.383792,
    "askPrice": 7.56,
    "askSize": 2,
    "bidPrice": 6.1,
    "bidSize": 2,
    "delay": 0,
    "high52w": 9.68,
    "highPrice": 8,
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
}


# option quote
_ex_contract = {
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


def match_packet(symbols, quotes):
    """Verify target ``symbols`` match keys in ``quotes`` packet.
    """
    assert len(quotes) == len(symbols)
    for ticker in symbols:
        quote = quotes.pop(ticker)

        # verify the quote packet format hasn't changed
        for key in _ex_quote:
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
async def test_quoter_context(us_symbols):
    """Test that a quoter "context" used by the data feed daemon.
    """
    async with qt.get_client() as client:
        quoter = await qt.quoter(client, us_symbols)
        quotes = await quoter(us_symbols)
        match_packet(us_symbols, quotes)


@trio_test
async def test_option_contracts(tmx_symbols):
    """Verify we can retrieve contracts by expiry.
    """
    async with qt.get_client() as client:
        for symbol in tmx_symbols:
            id, contracts = await client.symbol2contracts(symbol)
            assert isinstance(id, int)
            assert isinstance(contracts, dict)
            for dt in contracts:
                assert dt.isoformat(
                    timespec='microseconds') == contracts[dt]['expiryDate']


@trio_test
async def test_option_chain(tmx_symbols):
    """Verify we can retrieve all option chains for a list of symbols.
    """
    async with qt.get_client() as client:
        # contract lookup - should be cached
        contracts = await client.get_all_contracts(tmx_symbols)
        # chains quote for all symbols
        quotes = await client.option_chains(contracts)
        for key in tmx_symbols:
            contracts = quotes.pop(key)
            for key, quote in contracts.items():
                for key in _ex_contract:
                    quote.pop(key)
                assert not quote
        # chains for each symbol were retreived
        assert not quotes


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
            for _ in range(10):
                # chains quote for all symbols
                start = time.time()
                await client.option_chains(contract)
                took = time.time() - start
                print(f"Request took {took}")
                assert took <= expected_latency
                await trio.sleep(0.1)
