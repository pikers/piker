# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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

'''
Kraken backend.

Sub-modules within break into the core functionalities:

- .api: for the core API machinery which generally
        a ``asks``/``trio-websocket`` implemented ``Client``.
- .broker: part for orders / trading endpoints.
- .feed: for real-time and historical data query endpoints.
- .ledger: for transaction processing as it pertains to accounting.
- .symbols: for market (name) search and symbology meta-defs.

'''
from .symbols import (
    Pair,  # for symcache
    open_symbol_search,
    # required by `.accounting`, `.data`
    get_mkt_info,
)
# required by `.brokers`
from .api import (
    get_client,
)
from .feed import (
    # required by `.data`
    stream_quotes,
    open_history_client,
)
from .broker import (
    # required by `.clearing`
    open_trade_dialog,
)
from .ledger import (
    # required by `.accounting`
    norm_trade,
    norm_trade_records,
)


__all__ = [
    'get_client',
    'get_mkt_info',
    'Pair',
    'open_trade_dialog',
    'open_history_client',
    'open_symbol_search',
    'stream_quotes',
    'norm_trade_records',
    'norm_trade',
]


# tractor RPC enable arg
__enable_modules__: list[str] = [
    'api',
    'broker',
    'feed',
    'symbols',
]
