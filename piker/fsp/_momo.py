# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship of pikers)

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
Momentum bby.

"""
from typing import AsyncIterator, Optional

import numpy as np
from numba import jit, float64, optional, int64

from ._api import fsp
from ..data import iterticks
from ..data._sharedmem import ShmArray


@jit(
    float64[:](
        float64[:],
        optional(float64),
        optional(float64)
    ),
    nopython=True,
    nogil=True
)
def ema(

    y: 'np.ndarray[float64]',
    alpha: optional(float64) = None,
    ylast: optional(float64) = None,

) -> 'np.ndarray[float64]':
    r'''
    Exponential weighted moving average owka 'Exponential smoothing'.

    - https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
    - https://en.wikipedia.org/wiki/Exponential_smoothing

    Fun facts:
        A geometric progression is the discrete version of an
        exponential function, that is where the name for this
        smoothing method originated according to statistics lore. In
        signal processing parlance, an EMA is a first order IIR filter.

    .. math::

        .tex
        {S_{t}={\begin{cases}Y_{1},&t=1
         \\\alpha Y_{t}+(1-\alpha )\cdot S_{t-1},&t>1\end{cases}}}

        .nerd
        (2) s = {
                s[0] = y[0]; t = 0
                s[t] = a*y[t] + (1-a)*s[t-1], t > 0.
            }

    More discussion here:
    https://stackoverflow.com/questions/42869495/numpy-version-of-exponential-weighted-moving-average-equivalent-to-pandas-ewm

    '''
    n = y.shape[0]

    if alpha is None:
        # https://en.wikipedia.org/wiki/Moving_average#Relationship_between_SMA_and_EMA
        # use the "center of mass" convention making an ema compare
        # directly to the com of a SMA or WMA:
        alpha = 2 / float(n + 1)

    s = np.empty(n, dtype=float64)

    if n == 1:
        s[0] = y[0] * alpha + ylast * (1 - alpha)

    else:
        if ylast is None:
            s[0] = y[0]
        else:
            s[0] = ylast

        for i in range(1, n):
            s[i] = y[i] * alpha + s[i-1] * (1 - alpha)

    return s


# @jit(
#     float64[:](
#         float64[:],
#         int64,
#         float64,
#         float64,
#     ),
#     nopython=True,
#     nogil=True
# )
def _rsi(

    # TODO: use https://github.com/ramonhagenaars/nptyping
    signal: 'np.ndarray[float64]',
    period: int64 = 14,
    up_ema_last: float64 = None,
    down_ema_last: float64 = None,

) -> 'np.ndarray[float64]':
    '''
    relative strengggth.

    '''
    alpha = 1/period

    df = np.diff(signal, prepend=0)

    up = np.where(df > 0, df, 0)
    up_ema = ema(up, alpha, up_ema_last)

    down = np.where(df < 0, -df, 0)
    down_ema = ema(down, alpha, down_ema_last)

    # avoid dbz errors, this leaves the first
    # index == 0 right?
    rs = np.divide(
        up_ema,
        down_ema,
        out=np.zeros_like(signal),
        where=down_ema != 0
    )

    # map rs through sigmoid (with range [0, 100])
    rsi = 100 - 100 / (1 + rs)
    # rsi = 100 * (up_ema / (up_ema + down_ema))

    # also return the last ema state for next iteration
    return rsi, up_ema[-1], down_ema[-1]


def _wma(

    signal: np.ndarray,
    length: int,
    weights: Optional[np.ndarray] = None,

) -> np.ndarray:
    '''
    Compute a windowed moving average of ``signal`` with window
    ``length`` and optional ``weights`` (must be same size as
    ``signal``).

    '''
    if weights is None:
        # default is a standard arithmetic mean
        seq = np.full((length,), 1)
        weights = seq / seq.sum()

    assert length == len(weights)

    # lol, for long sequences this is nutso slow and expensive..
    return np.convolve(signal, weights, 'valid')


@fsp
async def wma(

    source,  #: AsyncStream[np.ndarray],
    length: int,
    ohlcv: np.ndarray,  # price time-frame "aware"

) -> AsyncIterator[np.ndarray]:  # maybe something like like FspStream?
    '''
    Streaming weighted moving average.

    ``weights`` is a sequence of already scaled values. As an example
    for the WMA often found in "techincal analysis":
    ``weights = np.arange(1, N) * N*(N-1)/2``.

    '''
    # deliver historical output as "first yield"
    yield _wma(ohlcv.array['close'], length)

    # begin real-time section

    async for quote in source:
        for tick in iterticks(quote, type='trade'):
            yield _wma(ohlcv.last(length))


@fsp
async def rsi(

    source: 'QuoteStream[Dict[str, Any]]',  # noqa
    ohlcv: ShmArray,
    period: int = 14,

) -> AsyncIterator[np.ndarray]:
    '''
    Multi-timeframe streaming RSI.

    https://en.wikipedia.org/wiki/Relative_strength_index

    '''
    sig = ohlcv.array['close']

    # wilder says to seed the RSI EMAs with the SMA for the "period"
    seed = _wma(ohlcv.last(period)['close'], period)[0]

    # TODO: the emas here should be seeded with a period SMA as per
    # wilder's original formula..
    rsi_h, last_up_ema_close, last_down_ema_close = _rsi(
        sig, period, seed, seed)
    up_ema_last = last_up_ema_close
    down_ema_last = last_down_ema_close

    # deliver history
    yield rsi_h

    index = ohlcv.index
    async for quote in source:
        # tick based updates
        for tick in iterticks(quote):
            # though incorrect below is interesting
            # sig = ohlcv.last(period)['close']

            # get only the last 2 "datums" which will be diffed to
            # calculate the real-time RSI output datum
            sig = ohlcv.last(2)['close']

            # the ema needs to be computed from the "last bar"
            # TODO: how to make this cleaner
            if ohlcv.index > index:
                last_up_ema_close = up_ema_last
                last_down_ema_close = down_ema_last
                index = ohlcv.index

            rsi_out, up_ema_last, down_ema_last = _rsi(
                sig,
                period=period,
                up_ema_last=last_up_ema_close,
                down_ema_last=last_down_ema_close,
            )
            yield rsi_out[-1:]
