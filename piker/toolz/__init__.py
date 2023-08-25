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
Toolz for debug, profile and trace of the distributed runtime :surfer:

'''
from .debug import (
    open_crash_handler,
)
from .profile import (
    Profiler,
    pg_profile_enabled,
    ms_slower_then,
    timeit,
)

# TODO: other mods to include?
# - DROP .trionics, already moved into tractor
# - move in `piker.calc`

__all__: list[str] = [
    'open_crash_handler',
    'pg_profile_enabled',
    'ms_slower_then',
    'Profiler',
    'timeit',
]
