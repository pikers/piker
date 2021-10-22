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
Profiling wrappers for internal libs.

"""
import time
from functools import wraps

_pg_profile: bool = True


def pg_profile_enabled() -> bool:
    global _pg_profile
    return _pg_profile


def timeit(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        t = time.time()
        res = fn(*args, **kwargs)
        print(
            '%s.%s: %.4f sec'
            % (fn.__module__, fn.__qualname__, time.time() - t)
        )
        return res

    return wrapper
