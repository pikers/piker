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

from typing import AsyncIterator, Optional

import numpy as np

from ..data._normalize import iterticks


def wap(
    signal: np.ndarray,
    weights: np.ndarray,
) -> np.ndarray:
    """Weighted average price from signal and weights.

    """
    cum_weights = np.cumsum(weights)
    cum_weighted_input = np.cumsum(signal * weights)
    return cum_weighted_input / cum_weights, cum_weighted_input, cum_weights


async def _tina_vwap(
    source,  #: AsyncStream[np.ndarray],
    ohlcv: np.ndarray,  # price time-frame "aware"
    anchors: Optional[np.ndarray] = None,
) -> AsyncIterator[np.ndarray]:  # maybe something like like FspStream?
    """Streaming volume weighted moving average.


    Calling this "tina" for now since we're using HLC3 instead of tick.

    """
    if anchors is None:
        # TODO:
        # anchor to session start of data if possible
        pass

    a = ohlcv.array
    chl3 = (a['close'] + a['high'] + a['low']) / 3
    v = a['volume']

    h_vwap, cum_wp, cum_v = wap(chl3, v)

    # deliver historical output as "first yield"
    yield h_vwap

    w_tot = cum_wp[-1]
    v_tot = cum_v[-1]
    # vwap_tot = h_vwap[-1]

    async for quote in source:

        for tick in iterticks(quote, types=['trade']):

            # c, h, l, v = ohlcv.array[-1][
            #     ['closes', 'high', 'low', 'volume']
            # ]

            # this computes tick-by-tick weightings from here forward
            size = tick['size']
            price = tick['price']

            v_tot += size
            w_tot += price * size

            # yield ((((o + h + l) / 3) * v) weights_tot) / v_tot
            yield w_tot / v_tot
