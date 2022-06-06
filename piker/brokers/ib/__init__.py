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
- ``data.py`` for real-time data feed endpoints

- ``client.py`` for the core API machinery which is ``trio``-ized
  wrapping around ``ib_insync``.

- ``report.py`` for the hackery to build manual pp calcs
  to avoid ib's absolute bullshit FIFO style position
  tracking..

"""
from .client import (
    get_client,
)
from .feed import (
    open_history_client,
    open_symbol_search,
    stream_quotes,
)
from .broker import trades_dialogue

__all__ = [
    'get_client',
    'trades_dialogue',
    'open_history_client',
    'open_symbol_search',
    'stream_quotes',
]


# tractor RPC enable arg
__enable_modules__: list[str] = [
    'client',
    'feed',
    'broker',
]

# passed to ``tractor.ActorNursery.start_actor()``
_spawn_kwargs = {
    'infect_asyncio': True,
}

# annotation to let backend agnostic code
# know if ``brokerd`` should be spawned with
# ``tractor``'s aio mode.
_infect_asyncio: bool = True
