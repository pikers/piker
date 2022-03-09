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
import numpy as np
from numba import (
    jit, float64, optional, int64,
)


def downsample(
    x: np.ndarray,
    y: np.ndarray,
    bins: int,
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
            ds = bins
            n = len(x) // ds
            x1 = np.empty((n, 2))
            # start of x-values; try to select a somewhat centered point
            stx = ds//2
            x1[:] = x[stx:stx+n*ds:ds, np.newaxis]
            x = x1.reshape(n*2)
            y1 = np.empty((n, 2))
            y2 = y[:n*ds].reshape((n, ds))
            y1[:, 0] = y2.max(axis=1)
            y1[:, 1] = y2.min(axis=1)
            y = y1.reshape(n*2)

        case '4px':

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
                width,  # width of screen?
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
    count = len(xs)
    # nbins = len(bins)
    bincount = 0
    x_left = start
    # Find the first bin
    while xs[0] >= x_left + step:
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
