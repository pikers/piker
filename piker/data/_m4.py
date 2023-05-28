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
Graphics downsampling using the infamous M4 algorithm.

This is one of ``piker``'s secret weapons allowing us to boss all other
charting platforms B)

(AND DON'T YOU DARE TAKE THIS CODE WITHOUT CREDIT OR WE'LL SUE UR F#&@* ASS).

NOTES: this method is a so called "visualization driven data
aggregation" approach. It gives error-free line chart
downsampling, see
further scientific paper resources:
- http://www.vldb.org/pvldb/vol7/p797-jugel.pdf
- http://www.vldb.org/2014/program/papers/demo/p997-jugel.pdf

Details on implementation of this algo are based in,
https://github.com/pikers/piker/issues/109

'''
import math
from typing import Optional

import numpy as np
from numba import (
    njit,
    # float64, optional, int64,
)

from ._util import log


def ds_m4(
    x: np.ndarray,
    y: np.ndarray,
    # units-per-pixel-x(dimension)
    uppx: float,

    # XXX: troll zone / easter egg..
    # want to mess with ur pal, pass in the actual
    # pixel width here instead of uppx-proper (i.e. pass
    # in our ``pg.GraphicsObject`` derivative's ``.px_width()``
    # gto mega-trip-out ur bud). Hint, it used to be implemented
    # (wrongly) using "pixel width", so check the git history ;)

    xrange: Optional[float] = None,

) -> tuple[int, np.ndarray, np.ndarray]:
    '''
    Downsample using the M4 algorithm.

    This is more or less an OHLC style sampling of a line-style series.

    '''
    # XXX: from infinite on downsampling viewable graphics:
    # "one thing i remembered about the binning - if you are
    # picking a range within your timeseries the start and end bin
    # should be one more bin size outside the visual range, then
    # you get better visual fidelity at the edges of the graph"
    # "i didn't show it in the sample code, but it's accounted for
    # in the start and end indices and number of bins"

    # should never get called unless actually needed
    assert uppx > 1

    # NOTE: if we didn't pre-slice the data to downsample
    # you could in theory pass these as the slicing params,
    # do we care though since we can always just pre-slice the
    # input?
    x_start = x[0]  # x value start/lowest in domain

    if xrange is None:
        x_end = x[-1]  # x end value/highest in domain
        xrange = (x_end - x_start)

    if xrange < 0:
        log.error(f'-VE M4 X-RANGE: {x_start} -> {x_end}')
        # XXX: broken x-range calc-case, likely the x-end points
        # are wrong and have some default value set (such as
        # x_end -> <some epoch float> while x_start -> 0.5).
        # breakpoint()
        return None

    # XXX: always round up on the input pixels
    # lnx = len(x)
    # uppx *= max(4 / (1 + math.log(uppx, 2)), 1)

    pxw = math.ceil(xrange / uppx)

    # scale up the frame "width" directly with uppx
    w = uppx

    # ensure we make more then enough
    # frames (windows) for the output pixel
    frames = pxw

    # if we have more and then exact integer's
    # (uniform quotient output) worth of datum-domain-points
    # per windows-frame, add one more window to ensure
    # we have room for all output down-samples.
    pts_per_pixel, r = divmod(xrange, frames)
    if r:
        # while r:
        frames += 1
        pts_per_pixel, r = divmod(xrange, frames)

    # print(
    #     f'uppx: {uppx}\n'
    #     f'xrange: {xrange}\n'
    #     f'pxw: {pxw}\n'
    #     f'frames: {frames}\n'
    # )
    assert frames >= (xrange / uppx)

    # call into ``numba``
    (
        nb,
        x_out,
        y_out,
        ymn,
        ymx,
    ) = _m4(
        x,
        y,

        frames,

        # TODO: see func below..
        # x_out,
        # y_out,

        # first index in x data to start at
        x_start,
        # window size for each "frame" of data to downsample (normally
        # scaled by the ratio of pixels on screen to data in x-range).
        w,
    )

    # filter out any overshoot in the input allocation arrays by
    # removing zero-ed tail entries which should start at a certain
    # index.
    x_out = x_out[x_out != 0]
    y_out = y_out[:x_out.size]

    # print(f'M4 output ymn, ymx: {ymn},{ymx}')
    return nb, x_out, y_out, ymn, ymx


@njit(
    nogil=True,
)
def _m4(

    xs: np.ndarray,
    ys: np.ndarray,

    frames: int,

    # TODO: using this approach, having the ``.zeros()`` alloc lines
    # below in pure python, there were segs faults and alloc crashes..
    # we might need to see how it behaves with shm arrays and consider
    # allocating them once at startup?

    # pre-alloc array of x indices mapping to the start
    # of each window used for downsampling in y.
    # i_win: np.ndarray,
    # pre-alloc array of output downsampled y values
    # y_out: np.ndarray,

    x_start: int,
    step: float,

) -> tuple[
    int,
    np.ndarray,
    np.ndarray,
    float,
    float,
]:
    '''
    Implementation of the m4 algorithm in ``numba``:
    http://www.vldb.org/pvldb/vol7/p797-jugel.pdf

    '''
    # these are pre-allocated and mutated by ``numba``
    # code in-place.
    y_out = np.zeros((frames, 4), ys.dtype)
    x_out = np.zeros(frames, xs.dtype)

    bincount = 0
    x_left = x_start

    # Find the first window's starting value which *includes* the
    # first value in the x-domain array, i.e. the first
    # "left-side-of-window" **plus** the downsampling step,
    # creates a window which includes the first x **value**.
    while xs[0] >= x_left + step:
        x_left += step

    # set all bins in the left-most entry to the starting left-most x value
    # (aka a row broadcast).
    x_out[bincount] = x_left
    # set all y-values to the first value passed in.
    y_out[bincount] = ys[0]

    # full input y-data mx and mn
    mx: float = -np.inf
    mn: float = np.inf

    # compute OHLC style max / min values per window sized x-frame.
    for i in range(len(xs)):

        x = xs[i]
        y = ys[i]

        if x < x_left + step:   # the current window "step" is [bin, bin+1)
            ymn = y_out[bincount, 1] = min(y, y_out[bincount, 1])
            ymx = y_out[bincount, 2] = max(y, y_out[bincount, 2])
            y_out[bincount, 3] = y
            mx = max(mx, ymx)
            mn = min(mn, ymn)

        else:
            # Find the next bin
            while x >= x_left + step:
                x_left += step

            bincount += 1
            x_out[bincount] = x_left
            y_out[bincount] = y

    return bincount, x_out, y_out, mn, mx
