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

from typing import AsyncIterator

import numpy as np

from ..data._normalize import iterticks


async def _tina_vwap(
    source,  #: AsyncStream[np.ndarray],
    ohlcv: np.ndarray,  # price time-frame "aware"
) -> AsyncIterator[np.ndarray]:  # maybe something like like FspStream?
    """Streaming volume weighted moving average.


    Calling this "tina" for now since we're using OHL3 instead of tick.

    """
    # TODO: anchor to session start

    a = ohlcv.array
    ohl3 = (a['open'] + a['high'] + a['low']) / 3

    v = a['volume']
    cum_v = np.cumsum(v)
    cum_weights = np.cumsum(ohl3 * v)

    vwap = cum_weights / cum_v

    # deliver historical output as "first yield"
    yield vwap

    weights_tot = cum_weights[-1]
    v_tot = cum_v[-1]

    async for quote in source:

        for tick in iterticks(quote, types=['trade']):

            o, h, l, v = ohlcv.array[-1][
                ['open', 'high', 'low', 'volume']
            ]
            v_tot += v

            yield ((((o + h + l) / 3) * v) + weights_tot) / v_tot
