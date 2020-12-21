# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet

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
Stream format enforcement.
"""

from typing import AsyncIterator, Optional, Tuple

import numpy as np


def iterticks(
    quote: dict,
    types: Tuple[str] = ('trade', 'utrade'),
) -> AsyncIterator:
    """Iterate through ticks delivered per quote cycle.
    """
    # print(f"{quote}\n\n")
    ticks = quote.get('ticks', ())
    if ticks:
        for tick in ticks:
            # print(f"{quote['symbol']}: {tick}")
            if tick.get('type') in types:
                yield tick
