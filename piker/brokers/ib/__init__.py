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

"""
Interactive Brokers API backend.

Sub-modules within break into the core functionalities:

- ``broker.py`` part for orders / trading endpoints
- ``feed.py`` for real-time data feed endpoints
- ``api.py`` for the core API machinery which is ``trio``-ized
  wrapping around ``ib_insync``.

"""
from .api import (
    get_client,
)
from .feed import (
    open_history_client,
    stream_quotes,
    get_mkt_info,
    open_symbol_search,
)
from .broker import (
    open_trade_dialog,
)
from .ledger import (
    norm_trade,
    norm_trade_records,
)
# TODO:
# from .symbols import (
#     get_mkt_info,
#     open_symbol_search,
# )

__all__ = [
    'get_client',
    'get_mkt_info',
    'norm_trade',
    'norm_trade_records',
    'open_trade_dialog',
    'open_history_client',
    'open_symbol_search',
    'stream_quotes',
]

_brokerd_mods: list[str] = [
    'api',
    'broker',
]

_datad_mods: list[str] = [
    'feed',
]


# tractor RPC enable arg
__enable_modules__: list[str] = (
    _brokerd_mods
    +
    _datad_mods
)

# passed to ``tractor.ActorNursery.start_actor()``
_spawn_kwargs = {
    'infect_asyncio': True,
}

# annotation to let backend agnostic code
# know if ``brokerd`` should be spawned with
# ``tractor``'s aio mode.
_infect_asyncio: bool = True

# XXX NOTE: for now we disable symcache with this backend since
# there is no clearly simple nor practical way to download "all
# symbology info" for all supported venues..
_no_symcache: bool = True
