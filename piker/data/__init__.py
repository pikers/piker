# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
Data infra.

We provide tsdb integrations for retrieving
and storing data from your brokers as well as
sharing live streams over a network.

"""
from .ticktools import iterticks
from ._sharedmem import (
    maybe_open_shm_array,
    attach_shm_array,
    open_shm_array,
    get_shm_token,
    ShmArray,
)
from ._source import (
    def_iohlcv_fields,
    def_ohlcv_fields,
)
from .feed import (
    Feed,
    open_feed,
)
from .flows import Flume
from ._symcache import (
    SymbologyCache,
    open_symcache,
    get_symcache,
)
from .types import Struct


__all__: list[str] = [
    'Flume',
    'Feed',
    'open_feed',
    'ShmArray',
    'iterticks',
    'maybe_open_shm_array',
    'attach_shm_array',
    'open_shm_array',
    'get_shm_token',
    'def_iohlcv_fields',
    'def_ohlcv_fields',
    'open_symcache',
    'get_symcache',
    'SymbologyCache',
    'Struct',
]
