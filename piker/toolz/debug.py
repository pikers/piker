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
Debugger wrappers for `pdbp` as used by `tractor`.

'''
from contextlib import contextmanager as cm

import pdbp


# TODO: better naming and what additionals?
# - optional runtime plugging?
# - detection for sync vs. async code?
# - specialized REPL entry when in distributed mode?
@cm
def open_crash_handler():
    '''
    Super basic crash handler using `pdbp` debugger.

    '''
    try:
        yield
    except BaseException:
        pdbp.xpm()
        raise
