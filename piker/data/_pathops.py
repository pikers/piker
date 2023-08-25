# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of pikers)

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
Super fast ``QPainterPath`` generation related operator routines.

"""
import numpy as np
from numpy.lib import recfunctions as rfn
from numba import (
    # types,
    njit,
    float64,
    int64,
    # optional,
)

# TODO: for ``numba`` typing..
# from ._source import numba_ohlc_dtype
from ._m4 import ds_m4


def xy_downsample(
    x,
    y,
    uppx,

    x_spacer: float = 0.5,

) -> tuple[
    np.ndarray,
    np.ndarray,
    float,
    float,
]:
    '''
    Downsample 1D (flat ``numpy.ndarray``) arrays using M4 given an input
    ``uppx`` (units-per-pixel) and add space between discreet datums.

    '''
    # downsample whenever more then 1 pixels per datum can be shown.
    # always refresh data bounds until we get diffing
    # working properly, see above..
    m4_out = ds_m4(
        x,
        y,
        uppx,
    )

    if m4_out is not None:
        bins, x, y, ymn, ymx = m4_out
        # flatten output to 1d arrays suitable for path-graphics generation.
        x = np.broadcast_to(x[:, None], y.shape)
        x = (x + np.array(
            [-x_spacer, 0, 0, x_spacer]
        )).flatten()
        y = y.flatten()

        return x, y, ymn, ymx

    # XXX: we accept a None output for the case where the input range
    # to ``ds_m4()`` is bad (-ve) and we want to catch and debug
    # that (seemingly super rare) circumstance..
    return None


@njit(
    # NOTE: need to construct this manually for readonly
    # arrays, see https://github.com/numba/numba/issues/4511
    # (
    #     types.Array(
    #         numba_ohlc_dtype,
    #         1,
    #         'C',
    #         readonly=True,
    #     ),
    #     int64,
    #     types.unicode_type,
    #     optional(float64),
    # ),
    nogil=True
)
def path_arrays_from_ohlc(
    data: np.ndarray,
    start: int64,
    bar_w: float64,
    bar_gap: float64 = 0.16,
    use_time_index: bool = True,

    # XXX: ``numba`` issue: https://github.com/numba/numba/issues/8622
    # index_field: str,

) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    '''
    Generate an array of lines objects from input ohlc data.

    '''
    size = int(data.shape[0] * 6)

    # XXX: see this for why the dtype might have to be defined outside
    # the routine.
    # https://github.com/numba/numba/issues/4098#issuecomment-493914533
    x = np.zeros(
        shape=size,
        dtype=float64,
    )
    y, c = x.copy(), x.copy()

    half_w: float = bar_w/2

    # TODO: report bug for assert @
    # ../piker/env/lib/python3.8/site-packages/numba/core/typing/builtins.py:991
    for i, q in enumerate(data[start:], start):

        open = q['open']
        high = q['high']
        low = q['low']
        close = q['close']

        if use_time_index:
            index = float64(q['time'])
        else:
            index = float64(q['index'])

        # XXX: ``numba`` issue: https://github.com/numba/numba/issues/8622
        # index = float64(q[index_field])
        # AND this (probably)
        # open, high, low, close, index = q[
        #     ['open', 'high', 'low', 'close', 'index']]

        istart = i * 6
        istop = istart + 6

        # x,y detail the 6 points which connect all vertexes of a ohlc bar
        mid: float = index + half_w
        x[istart:istop] = (
            index + bar_gap,
            mid,
            mid,
            mid,
            mid,
            index + bar_w - bar_gap,
        )
        y[istart:istop] = (
            open,
            open,
            low,
            high,
            close,
            close,
        )

        # specifies that the first edge is never connected to the
        # prior bars last edge thus providing a small "gap"/"space"
        # between bars determined by ``bar_gap``.
        c[istart:istop] = (1, 1, 1, 1, 1, 0)

    return x, y, c


def hl2mxmn(
    ohlc: np.ndarray,
    index_field: str = 'index',

) -> np.ndarray:
    '''
    Convert a OHLC struct-array containing 'high'/'low' columns
    to a "joined" max/min 1-d array.

    '''
    index = ohlc[index_field]
    hls = ohlc[[
        'low',
        'high',
    ]]

    mxmn = np.empty(2*hls.size, dtype=np.float64)
    x = np.empty(2*hls.size, dtype=np.float64)
    trace_hl(hls, mxmn, x, index[0])
    x = x + index[0]

    return mxmn, x


@njit(
    # TODO: the type annots..
    # float64[:](float64[:],),
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
        lo, hi = row['low'], row['high']

        up_diff = hi - last_l
        down_diff = last_h - lo

        if up_diff > down_diff:
            out[2*i + 1] = hi
            out[2*i] = last_l
        else:
            out[2*i + 1] = lo
            out[2*i] = last_h

        last_l = lo
        last_h = hi

        x[2*i] = int(i) - margin
        x[2*i + 1] = int(i) + margin

    return out


def ohlc_flatten(
    ohlc: np.ndarray,
    use_mxmn: bool = True,
    index_field: str = 'index',

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Convert an OHLCV struct-array into a flat ready-for-line-plotting
    1-d array that is 4 times the size with x-domain values distributed
    evenly (by 0.5 steps) over each index.

    '''
    index = ohlc[index_field]

    if use_mxmn:
        # traces a line optimally over highs to lows
        # using numba. NOTE: pretty sure this is faster
        # and looks about the same as the below output.
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
