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
Graphics related downsampling routines for compressing to pixel
limits on the display device.

'''
from typing import Optional

import numpy as np
# from numpy.lib.recfunctions import structured_to_unstructured
from numba import (
    jit,
    # float64, optional, int64,
)

from ..log import get_logger


log = get_logger(__name__)


def hl2mxmn(
    ohlc: np.ndarray,
    # downsample_by: int = 0,

) -> np.ndarray:
    '''
    Convert a OHLC struct-array containing 'high'/'low' columns
    to a "joined" max/min 1-d array.

    '''
    index = ohlc['index']
    hls = ohlc[[
        'low',
        'high',
    ]]

    # XXX: don't really need this any more since we implemented
    # the "tracer" routine, `numba`-style..
    # create a "max and min" sequence from ohlc datums
    # hl2d = structured_to_unstructured(hls)
    # hl1d = hl2d.flatten()

    mxmn = np.empty(2*hls.size, dtype=np.float64)
    x = np.empty(2*hls.size, dtype=np.float64)
    trace_hl(hls, mxmn, x, index[0])
    x = x + index[0]

    return mxmn, x

    # if downsample_by < 2:
    #     return mxmn, x

    # dsx, dsy = downsample(
    #     y=mxmn,
    #     x=x,
    #     bins=downsample_by,
    # )
    # log.info(f'downsampling by {downsample_by}')
    # print(f'downsampling by {downsample_by}')
    # return dsy, dsx


@jit(
    # TODO: the type annots..
    # float64[:](float64[:],),
    nopython=True,
)
def trace_hl(
    hl: 'np.ndarray',
    out: np.ndarray,
    x: np.ndarray,
    start: int,

    # the "offset" values in the x-domain which
    # place the 2 output points around each ``int``
    # master index.
    margin: float = 0.43,

) -> None:
    '''
    "Trace" the outline of the high-low values of an ohlc sequence
    as a line such that the maximum deviation (aka disperaion) between
    bars if preserved.

    This routine is expected to modify input arrays in-place.

    '''
    last_l = hl['low'][0]
    last_h = hl['high'][0]

    for i in range(hl.size):
        row = hl[i]
        l, h = row['low'], row['high']

        up_diff = h - last_l
        down_diff = last_h - l

        if up_diff > down_diff:
            out[2*i + 1] = h
            out[2*i] = last_l
        else:
            out[2*i + 1] = l
            out[2*i] = last_h

        last_l = l
        last_h = h

        x[2*i] = int(i) - margin
        x[2*i + 1] = int(i) + margin

    return out


def downsample(
    x: np.ndarray,
    y: np.ndarray,
    bins: int = 2,

    method: str = 'peak',

    **kwargs,

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Downsample x/y data for lesser curve graphics gen.

    The "peak" method is originally copied verbatim from
    ``pyqtgraph.PlotDataItem.getDisplayDataset()``.

    '''
    # py3.10 syntax
    match method:
        case 'peak':
            if bins < 2:
                log.warning('No downsampling taking place?')

            ds = bins
            n = len(x) // ds
            x1 = np.empty((n, 2))

            # start of x-values; try to select a somewhat centered point
            stx = ds // 2
            x1[:] = x[stx:stx+n*ds:ds, np.newaxis]
            x = x1.reshape(n*2)

            y1 = np.empty((n, 2))
            y2 = y[:n*ds].reshape((n, ds))

            y1[:, 0] = y2.max(axis=1)
            y1[:, 1] = y2.min(axis=1)
            y = y1.reshape(n*2)

            return x, y

        case 'm4':
            return ds_m4(x, y, kwargs['px_width'])


def ds_m4(
    x: np.ndarray,
    y: np.ndarray,

    # this is the width of the data in view
    # in display-device-local pixel units.
    px_width: int,

    factor: Optional[int] = None,

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Downsample using the M4 algorithm.

    '''

    # NOTE: this method is a so called "visualization driven data
    # aggregation" approach. It gives error-free line chart
    # downsampling, see
    # further scientific paper resources:
    # - http://www.vldb.org/pvldb/vol7/p797-jugel.pdf
    # - http://www.vldb.org/2014/program/papers/demo/p997-jugel.pdf

    # Details on implementation of this algo are based in,
    # https://github.com/pikers/piker/issues/109

    # XXX: from infinite on downsampling viewable graphics:
    # "one thing i remembered about the binning - if you are
    # picking a range within your timeseries the start and end bin
    # should be one more bin size outside the visual range, then
    # you get better visual fidelity at the edges of the graph"
    # "i didn't show it in the sample code, but it's accounted for
    # in the start and end indices and number of bins"

    assert px_width > 1  # width of screen in pxs?

    # NOTE: if we didn't pre-slice the data to downsample
    # you could in theory pass these as the slicing params,
    # do we care though since we can always just pre-slice the
    # input?
    x_start = 0  # x index start
    x_end = len(x)  # x index end

    # uppx: units-per-pixel
    pts_per_pixel = len(x) / px_width
    print(f'UPPX: {pts_per_pixel}')

    # ratio of indexed x-value to width of raster in pixels.
    if factor is None:
        w = (x_end-x_start) / float(px_width)
        print(f' pts/pxs = {w}')
    else:
        w = factor

    # these are pre-allocated and mutated by ``numba``
    # code in-place.
    ds = np.zeros((px_width, 4), y.dtype)
    i_win = np.zeros(px_width, x.dtype)

    # call into ``numba``
    nb = _m4(
        x,
        y,

        i_win,
        ds,

        # first index in x data to start at
        x_start,
        # window size for each "frame" of data to downsample (normally
        # scaled by the ratio of pixels on screen to data in x-range).
        w,
    )
    print(f'downsampled to {nb} bins')

    return i_win, ds.flatten()


@jit(
    nopython=True,
)
def _m4(

    xs: np.ndarray,
    ys: np.ndarray,

    # pre-alloc array of x indices mapping to the start
    # of each window used for downsampling in y.
    i_win: np.ndarray,

    # pre-alloc array of output downsampled y values
    ds: np.ndarray,

    x_start: int,
    step: float,

) -> int:
    # nbins = len(i_win)
    # count = len(xs)

    bincount = 0
    x_left = x_start

    # Find the first window's starting index which *includes* the
    # first value in the x-domain array.
    # (this allows passing in an array which is indexed (and thus smaller then)
    # the ``x_start`` value normally passed in - say if you normally
    # want to start 0-indexed.
    first = xs[0]
    while first >= x_left + step:
        x_left += step

    # set all bins in the left-most entry to the starting left-most x value
    # (aka a row broadcast).
    i_win[bincount] = x_left
    # set all y-values to the first value passed in.
    ds[bincount] = ys[0]

    for i in range(len(xs)):
        x = xs[i]
        y = ys[i]
        if x < x_left + step:   # the current window "step" is [bin, bin+1)
            ds[bincount, 1] = min(y, ds[bincount, 1])
            ds[bincount, 2] = max(y, ds[bincount, 2])
            ds[bincount, 3] = y
        else:
            # Find the next bin
            while x >= x_left + step:
                x_left += step

            bincount += 1
            i_win[bincount] = x_left
            ds[bincount] = y

    return bincount
