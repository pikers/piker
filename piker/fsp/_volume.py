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

from typing import AsyncIterator, Optional, Union

import numpy as np
from tractor._broadcast import AsyncReceiver

from ..data._normalize import iterticks
from ..data._sharedmem import ShmArray


def wap(

    signal: np.ndarray,
    weights: np.ndarray,

) -> np.ndarray:
    """Weighted average price from signal and weights.

    """
    cum_weights = np.cumsum(weights)
    cum_weighted_input = np.cumsum(signal * weights)

    # cum_weighted_input / cum_weights
    # but, avoid divide by zero errors
    avg = np.divide(
        cum_weighted_input,
        cum_weights,
        where=cum_weights != 0
    )

    return (
        avg,
        cum_weighted_input,
        cum_weights,
    )


async def _tina_vwap(

    source: AsyncReceiver[dict],
    ohlcv: ShmArray,  # OHLC sampled history

    # TODO: anchor logic (eg. to session start)
    anchors: Optional[np.ndarray] = None,

) -> Union[
    AsyncIterator[np.ndarray],
    float
]:
    '''Streaming volume weighted moving average.

    Calling this "tina" for now since we're using HLC3 instead of tick.

    '''
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


async def dolla_vlm(
    source: AsyncReceiver[dict],
    ohlcv: ShmArray,  # OHLC sampled history

) -> Union[
    AsyncIterator[np.ndarray],
    float
]:
    a = ohlcv.array
    chl3 = (a['close'] + a['high'] + a['low']) / 3
    v = a['volume']

    # history
    yield chl3 * v

    i = ohlcv.index
    lvlm = 0

    async for quote in source:
        for tick in iterticks(quote):

            # this computes tick-by-tick weightings from here forward
            size = tick['size']
            price = tick['price']

            li = ohlcv.index
            if li > i:
                i = li
                lvlm = 0

            c, h, l, v = ohlcv.last()[
                ['close', 'high', 'low', 'volume']
            ]

            lvlm += price * size
            tina_lvlm = c+h+l/3 * v
            # print(f' tinal vlm: {tina_lvlm}')

            yield lvlm
