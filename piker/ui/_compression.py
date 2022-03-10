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
# from typing import Optional

import numpy as np
# from numpy.lib.recfunctions import structured_to_unstructured
from numba import (
    jit,
    float64, optional, int64,
)

from ..log import get_logger


log = get_logger(__name__)


def hl2mxmn(
    ohlc: np.ndarray,
    downsample_by: int = 0,

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
    x = x + index[0] - 1

    if not downsample_by > 2:
        return mxmn, x

    dsx, dsy = downsample(
        y=mxmn,
        x=x,
        bins=downsample_by,
    )
    log.info(f'downsampling by {downsample_by}')
    return dsy, dsx


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

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Downsample x/y data for lesser curve graphics gen.

    The "peak" method is originally copied verbatim from
    ``pyqtgraph.PlotDataItem.getDisplayDataset()``.

    '''
    # py3.10 syntax
    match method:
        case 'peak':
            # breakpoint()
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

        # TODO: this algo from infinite, see
        # https://github.com/pikers/piker/issues/109
        case 'infinite_4px':

            # Ex. from infinite on downsampling viewable graphics.
            # "one thing i remembered about the binning - if you are
            # picking a range within your timeseries the start and end bin
            # should be one more bin size outside the visual range, then
            # you get better visual fidelity at the edges of the graph"
            # "i didn't show it in the sample code, but it's accounted for
            # in the start and end indices and number of bins"

            def build_subchart(
                self,
                subchart,
                width,  # width of screen in pxs?
                chart_type,
                lower,  # x start?
                upper,  # x end?
                xvals,
                yvals
            ):
                pts_per_pixel = len(xvals) / width
                if pts_per_pixel > 1:

                    # this is mutated in-place
                    data = np.zeros((width, 4), yvals.dtype)
                    bins = np.zeros(width, xvals.dtype)

                    nb = subset_by_x(
                        xvals,
                        yvals,
                        bins,
                        data,
                        lower,
                        # this is scaling the x-range by
                        # the width of the screen?
                        (upper-lower)/float(width),
                    )
                    print(f'downsampled to {nb} bins')

            return x, y


@jit(nopython=True)
def subset_by_x(

    xs: np.ndarray,
    ys: np.ndarray,
    bins: np.ndarray,
    data: np.ndarray,
    x_start: int,
    step: float,

) -> int:
    # nbins = len(bins)
    count = len(xs)
    bincount = 0
    x_left = x_start

    # Find the first bin
    first = xs[0]
    while first >= x_left + step:
        x_left += step

    bins[bincount] = x_left
    data[bincount] = ys[0]

    for i in range(count):
        x = xs[i]
        y = ys[i]
        if x < x_left + step:   # Interval is [bin, bin+1)
            data[bincount, 1] = min(y, data[bincount, 1])
            data[bincount, 2] = max(y, data[bincount, 2])
            data[bincount, 3] = y
        else:
            # Find the next bin
            while x >= x_left + step:
                x_left += step
            bincount += 1
            bins[bincount] = x_left
            data[bincount] = y

    return bincount
