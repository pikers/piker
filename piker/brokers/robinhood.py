# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Robinhood API backend.

WARNING: robinhood now requires authenticated access to use the quote
endpoints (it didn't originally). We need someone with a valid US
account to test this code.
"""
from functools import partial
from typing import List

from async_generator import asynccontextmanager
import asks

from ._util import (
    resproc,
    BrokerError,
    log,
)
from ..calc import percent_change

_service_ep = 'https://api.robinhood.com'


class _API:
    """Robinhood API endpoints exposed as methods and wrapped with an
    http session.
    """
    def __init__(self, session: asks.Session):
        self._sess = session

    async def _request(self, path: str, params=None) -> dict:
        resp = await self._sess.get(path=f'/{path}', params=params)
        return resproc(resp, log)

    async def quotes(self, symbols: str) -> dict:
        return await self._request('quotes/', params={'symbols': symbols})

    async def fundamentals(self, symbols: str) -> dict:
        return await self._request(
            'fundamentals/', params={'symbols': symbols})


class Client:
    """API client suitable for use as a long running broker daemon or
    single api requests.
    """
    def __init__(self):
        self._sess = asks.Session()
        self._sess.base_location = _service_ep
        self.api = _API(self._sess)

    def _zip_in_order(self, symbols: [str], quotes: List[dict]):
        return {
            quote.get('symbol', sym) if quote else sym: quote
            for sym, quote in zip(symbols, quotes)
        }

    async def quote(self, symbols: [str]):
        """Retrieve quotes for a list of ``symbols``.
        """
        try:
            quotes = (await self.api.quotes(','.join(symbols)))['results']
        except BrokerError:
            quotes = [None] * len(symbols)

        for quote in quotes:
            # insert our subscription key field
            if quote is not None:
                quote['key'] = quote['symbol']

        return list(filter(bool, quotes))

    async def symbol_data(self, symbols: [str]):
        """Retrieve symbol data via the ``fundamentals`` endpoint.
        """
        return self._zip_in_order(
            symbols,
            (await self.api.fundamentals(','.join(symbols)))['results']
        )


@asynccontextmanager
async def get_client() -> Client:
    """Spawn a RH broker client.
    """
    yield Client()


async def quoter(client: Client, tickers: [str]):
    """Quoter context.
    """
    return client.quote


# Robinhood key conversion / column order
_rh_keys = {
    'symbol': 'symbol',  # done manually in qtconvert
    '%': '%',
    'last_trade_price': ('last', partial(round, ndigits=3)),
    'last_extended_hours_trade_price': 'last pre-mkt',
    'ask_price': ('ask', partial(round, ndigits=3)),
    'bid_price': ('bid', partial(round, ndigits=3)),
    # 'lastTradeSize': 'size',  # not available?
    'bid_size': 'bsize',
    'ask_size': 'asize',
    # 'VWAP': ('VWAP', partial(round, ndigits=3)),
    # 'mktcap': ('mktcap', humanize),
    # '$ vol': ('$ vol', humanize),
    # 'volume': ('vol', humanize),
    'previous_close': 'close',
    'adjusted_previous_close': 'adj close',
    # 'trading_halted': 'halted',

    # example fields
    # "adjusted_previous_close": "8.1900",
    # "ask_price": "8.2800",
    # "ask_size": 1200,
    # "bid_price": "8.2500",
    # "bid_size": 1800,
    # "has_traded": true,
    # "last_extended_hours_trade_price": null,
    # "last_trade_price": "8.2350",
    # "last_trade_price_source": "nls",
    # "previous_close": "8.1900",
    # "previous_close_date": "2018-03-20",
    # "symbol": "CRON",
    # "trading_halted": false,
    # "updated_at": "2018-03-21T13:46:05Z"
}

_bidasks = {
    'last': ['bid', 'ask'],
    # 'size': ['bsize', 'asize'],
    # 'VWAP': ['low', 'high'],
    # 'last pre-mkt': ['close', 'adj close'],
}


def format_quote(
    quote: dict, symbol_data: dict,
    keymap: dict = _rh_keys,
) -> (dict, dict):
    """remap a list of quote dicts ``quotes`` using the mapping of old keys
    -> new keys ``keymap`` returning 2 dicts: one with raw data and the other
    for display.

    returns 2 dicts: first is the original values mapped by new keys,
    and the second is the same but with all values converted to a
    "display-friendly" string format.
    """
    last = quote['last_trade_price']
    # symbol = quote['symbol']
    previous = quote['previous_close']
    change = percent_change(float(previous), float(last))
    # share_count = symbol_data[symbol].get('outstandingshares', none)
    # mktcap = share_count * last if share_count else 'na'
    computed = {
        'symbol': quote['symbol'],
        '%': round(change, 3),
        'ask_price': float(quote['ask_price']),
        'bid_price': float(quote['bid_price']),
        'last_trade_price': float(quote['last_trade_price']),
        # 'mktcap': mktcap,
        # '$ vol': round(quote['vwap'] * quote['volume'], 3),
        'close': previous,
    }
    new = {}
    displayable = {}

    for key, new_key in keymap.items():
        display_value = value = computed.get(key) or quote.get(key)

        # api servers can return `None` vals when markets are closed (weekend)
        value = 0 if value is None else value

        # convert values to a displayble format using available formatting func
        if isinstance(new_key, tuple):
            new_key, func = new_key
            display_value = func(value)

        new[new_key] = value
        displayable[new_key] = display_value

    return new, displayable
