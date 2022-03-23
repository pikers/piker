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
import math
from typing import Optional

import numpy as np
from numpy.lib import recfunctions as rfn
from numba import (
    jit,
    # float64, optional, int64,
)

from ..log import get_logger


log = get_logger(__name__)


def hl2mxmn(ohlc: np.ndarray) -> np.ndarray:
    '''
    Convert a OHLC struct-array containing 'high'/'low' columns
    to a "joined" max/min 1-d array.

    '''
    index = ohlc['index']
    hls = ohlc[[
        'low',
        'high',
    ]]

    mxmn = np.empty(2*hls.size, dtype=np.float64)
    x = np.empty(2*hls.size, dtype=np.float64)
    trace_hl(hls, mxmn, x, index[0])
    x = x + index[0]

    return mxmn, x


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
    ``pyqtgraph.PlotDataItem.getDisplayDataset()`` which gets
    all credit, though we will likely drop this in favor of the M4
    algo below.

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


def ohlc_flatten(
    ohlc: np.ndarray,
    use_mxmn: bool = False,

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Convert an OHLCV struct-array into a flat ready-for-line-plotting
    1-d array that is 4 times the size with x-domain values distributed
    evenly (by 0.5 steps) over each index.

    '''
    index = ohlc['index']

    if use_mxmn:
        flat, x = hl2mxmn(ohlc)
    else:
        flat = rfn.structured_to_unstructured(
            ohlc[['open', 'high', 'low', 'close']]
        ).flatten()

        x = np.linspace(
            start=index[0] - 0.5,
            stop=index[-1] + 0.5,
            num=len(flat),
        )
    return x, flat


def ohlc_to_m4_line(
    ohlc: np.ndarray,
    px_width: int,

    downsample: bool = False,
    uppx: Optional[float] = None,
    pretrace: bool = False,

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Convert an OHLC struct-array to a m4 downsampled 1-d array.

    '''
    xpts, flat = ohlc_flatten(
        ohlc,
        use_mxmn=pretrace,
    )

    if uppx:
        # optionally log-scale down the "supposed pxs on screen"
        # as the units-per-px (uppx) get's large.
        scaler = round(
            max(
                # NOTE: found that a 16x px width brought greater
                # detail, likely due to dpi scaling?
                # px_width=px_width * 16,
                32 / (1 + math.log(uppx, 2)),
                1
            )
        )
        px_width *= scaler

    if downsample:
        bins, x, y = ds_m4(
            xpts,
            flat,
            px_width=px_width,
        )
        x = np.broadcast_to(x[:, None], y.shape)
        x = (x + np.array([-0.43, 0, 0, 0.43])).flatten()
        y = y.flatten()

        return x, y
    else:
        return xpts, flat


def ds_m4(
    x: np.ndarray,
    y: np.ndarray,

    # this is the width of the data in view
    # in display-device-local pixel units.
    px_width: int,

) -> tuple[int, np.ndarray, np.ndarray]:
    '''
    Downsample using the M4 algorithm.

    This is more or less an OHLC style sampling of a line-style series.

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
    x_start = x[0]  # x value start/lowest in domain
    x_end = x[-1]  # x end value/highest in domain

    # XXX: always round up on the input pixels
    px_width = math.ceil(px_width)

    x_range = x_end - x_start

    # ratio of indexed x-value to width of raster in pixels.
    # this is more or less, uppx: units-per-pixel.
    w = x_range / float(px_width)

    # ensure we make more then enough
    # frames (windows) for the output pixel
    frames = px_width

    # if we have more and then exact integer's
    # (uniform quotient output) worth of datum-domain-points
    # per windows-frame, add one more window to ensure
    # we have room for all output down-samples.
    pts_per_pixel, r = divmod(len(x), frames)
    if r:
        frames += 1

    # these are pre-allocated and mutated by ``numba``
    # code in-place.
    y_out = np.zeros((frames, 4), y.dtype)
    i_win = np.zeros(frames, x.dtype)

    # call into ``numba``
    nb = _m4(
        x,
        y,

        i_win,
        y_out,

        # first index in x data to start at
        x_start,
        # window size for each "frame" of data to downsample (normally
        # scaled by the ratio of pixels on screen to data in x-range).
        w,
    )

    # filter out any overshoot in the input allocation arrays by
    # removing zero-ed tail entries which should start at a certain
    # index.
    i_win = i_win[i_win != 0]
    y_out = y_out[:i_win.size]

    return nb, i_win, y_out


@jit(
    nopython=True,
    nogil=True,
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

    # Find the first window's starting value which *includes* the
    # first value in the x-domain array, i.e. the first
    # "left-side-of-window" **plus** the downsampling step,
    # creates a window which includes the first x **value**.
    while xs[0] >= x_left + step:
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
