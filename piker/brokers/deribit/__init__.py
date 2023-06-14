# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for piker0)

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
Deribit backend.

'''

from piker.log import get_logger

from .api import (
    get_client,
)
from .feed import (
    open_history_client,
    open_symbol_search,
    stream_quotes,
    # backfill_bars,
)
# from .broker import (
    # open_trade_dialog,
    # norm_trade_records,
# )

log = get_logger(__name__)

__all__ = [
    'get_client',
#    'trades_dialogue',
    'open_history_client',
    'open_symbol_search',
    'stream_quotes',
#    'norm_trade_records',
]


# tractor RPC enable arg
__enable_modules__: list[str] = [
    'api',
    'feed',
#   'broker',
]

# passed to ``tractor.ActorNursery.start_actor()``
_spawn_kwargs = {
    'infect_asyncio': True,
}

# annotation to let backend agnostic code
# know if ``brokerd`` should be spawned with
# ``tractor``'s aio mode.
_infect_asyncio: bool = True
