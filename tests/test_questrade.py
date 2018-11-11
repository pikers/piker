"""
Questrade broker testing
"""
from trio.testing import trio_test
from piker.brokers import questrade as qt


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
