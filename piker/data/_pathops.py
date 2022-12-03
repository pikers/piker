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
from .._profile import (
    Profiler,
    pg_profile_enabled,
    ms_slower_then,
)


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
    bins, x, y, ymn, ymx = ds_m4(
        x,
        y,
        uppx,
    )

    # flatten output to 1d arrays suitable for path-graphics generation.
    x = np.broadcast_to(x[:, None], y.shape)
    x = (x + np.array(
        [-x_spacer, 0, 0, x_spacer]
    )).flatten()
    y = y.flatten()

    return x, y, ymn, ymx


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
    bar_gap: float64 = 0.43,

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

    # TODO: report bug for assert @
    # /home/goodboy/repos/piker/env/lib/python3.8/site-packages/numba/core/typing/builtins.py:991
    for i, q in enumerate(data[start:], start):

        open = q['open']
        high = q['high']
        low = q['low']
        close = q['close']
        index = float64(q['index'])

        # XXX: ``numba`` issue: https://github.com/numba/numba/issues/8622
        # index = float64(q[index_field])
        # AND this (probably)
        # open, high, low, close, index = q[
        #     ['open', 'high', 'low', 'close', 'index']]

        istart = i * 6
        istop = istart + 6

        # x,y detail the 6 points which connect all vertexes of a ohlc bar
        x[istart:istop] = (
            index - bar_gap,
            index,
            index,
            index,
            index,
            index + bar_gap,
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


@njit
def _slice_from_time(
    arr: np.ndarray,
    start_t: float,
    stop_t: float,

) -> tuple[
    tuple[int, int],
    tuple[int, int],
    np.ndarray | None,
]:
    '''
    Slice an input struct array to a time range and return the absolute
    and "readable" slices for that array as well as the indexing mask
    for the caller to use to slice the input array if needed.

    '''
    times = arr['time']
    index = arr['index']

    if (
        start_t < 0
        or start_t >= stop_t
    ):
        return (
            (
                index[0],
                index[-1],
            ),
            (
                0,
                len(arr),
            ),
        )

    # TODO: if we can ensure each time field has a uniform
    # step we can instead do some arithmetic to determine
    # the equivalent index like we used to?
    # return array[
    #     lbar - ifirst:
    #     (rbar - ifirst) + 1
    # ]

    read_i_0: int = 0
    read_i_last: int = 0

    for i in range(times.shape[0]):
        time = times[i]
        if time >= start_t:
            read_i_0 = i
            break

    for i in range(read_i_0, times.shape[0]):
        time = times[i]
        if time > stop_t:
            read_i_last = time
            break

    abs_i_0 = int(index[0]) + read_i_0
    abs_i_last = int(index[0]) + read_i_last

    if read_i_last == 0:
        read_i_last = times.shape[0]

    abs_slc = (
        int(abs_i_0),
        int(abs_i_last),
    )

    read_slc = (
        int(read_i_0),
        int(read_i_last),
    )

    # also return the readable data from the timerange
    return (
        abs_slc,
        read_slc,
    )


def slice_from_time(
    arr: np.ndarray,
    start_t: float,
    stop_t: float,

) -> tuple[
    slice,
    slice,
    np.ndarray | None,
]:
    profiler = Profiler(
        msg='slice_from_time()',
        disabled=not pg_profile_enabled(),
        ms_threshold=ms_slower_then,
    )

    (
        abs_slc_tuple,
        read_slc_tuple,
    ) = _slice_from_time(
        arr,
        start_t,
        stop_t,
    )

    abs_slc = slice(*abs_slc_tuple)
    read_slc = slice(*read_slc_tuple)
    profiler(
        'slicing complete'
        f'{start_t} -> {abs_slc.start} | {read_slc.start}\n'
        f'{stop_t} -> {abs_slc.stop} | {read_slc.stop}\n'
    )
    return (
        abs_slc,
        read_slc,
    )
