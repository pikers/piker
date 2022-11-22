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
from __future__ import annotations
from typing import (
    Optional,
    Callable,
    TYPE_CHECKING,
)

import msgspec
import numpy as np
from numpy.lib import recfunctions as rfn
from numba import njit, float64, int64  # , optional
# import pyqtgraph as pg
from PyQt5 import QtGui
# from PyQt5.QtCore import QLineF, QPointF

from ..data._sharedmem import (
    ShmArray,
)
# from .._profile import pg_profile_enabled, ms_slower_then
from ._compression import (
    ds_m4,
)

if TYPE_CHECKING:
    from ._flows import Renderer


def by_index_and_key(
    renderer: Renderer,
    array: np.ndarray,
    array_key: str,
    vr: tuple[int, int],

) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    return array['index'], array[array_key], 'all'


class IncrementalFormatter(msgspec.Struct):

    shm: ShmArray

    # optional pre-graphics xy formatted data which
    # is incrementally updated in sync with the source data.
    allocate_xy_nd: Optional[Callable[
        [int, slice],
        tuple[np.ndarray, np.nd.array]
    ]] = None

    incr_update_xy_nd: Optional[Callable[
        [int, slice], None]
    ] = None

    # default just returns index, and named array from data
    format_xy_nd_to_1d: Callable[
        [np.ndarray, str],
        tuple[np.ndarray]
    ] = by_index_and_key

    x_nd: Optional[np.ndarray] = None
    y_nd: Optional[np.ndarray] = None

    x_1d: Optional[np.ndarray] = None
    y_1d: Optional[np.ndarray] = None

    # indexes which slice into the above arrays (which are allocated
    # based on source data shm input size) and allow retrieving
    # incrementally updated data.
    # _xy_first: int = 0
    # _xy_last: int = 0
    xy_nd_start: int = 0
    xy_nd_end: int = 0


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
    # TODO: for now need to construct this manually for readonly arrays, see
    # https://github.com/numba/numba/issues/4511
    # ntypes.tuple((float64[:], float64[:], float64[:]))(
    #     numba_ohlc_dtype[::1],  # contiguous
    #     int64,
    #     optional(float64),
    # ),
    nogil=True
)
def path_arrays_from_ohlc(
    data: np.ndarray,
    start: int64,
    bar_gap: float64 = 0.43,

) -> np.ndarray:
    '''
    Generate an array of lines objects from input ohlc data.

    '''
    size = int(data.shape[0] * 6)

    x = np.zeros(
        # data,
        shape=size,
        dtype=float64,
    )
    y, c = x.copy(), x.copy()

    # TODO: report bug for assert @
    # /home/goodboy/repos/piker/env/lib/python3.8/site-packages/numba/core/typing/builtins.py:991
    for i, q in enumerate(data[start:], start):

        # TODO: ask numba why this doesn't work..
        # open, high, low, close, index = q[
        #     ['open', 'high', 'low', 'close', 'index']]

        open = q['open']
        high = q['high']
        low = q['low']
        close = q['close']
        index = float64(q['index'])

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


def gen_ohlc_qpath(
    r: Renderer,
    data: np.ndarray,
    array_key: str,  # we ignore this
    vr: tuple[int, int],

    start: int = 0,  # XXX: do we need this?
    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.43,

) -> QtGui.QPainterPath:
    '''
    More or less direct proxy to ``path_arrays_from_ohlc()``
    but with closed in kwargs for line spacing.

    '''
    x, y, c = path_arrays_from_ohlc(
        data,
        start,
        bar_gap=w,
    )
    return x, y, c


def ohlc_to_line(
    ohlc_shm: ShmArray,
    data_field: str,
    fields: list[str] = ['open', 'high', 'low', 'close']

) -> tuple[
    np.ndarray,
    np.ndarray,
]:
    '''
    Convert an input struct-array holding OHLC samples into a pair of
    flattened x, y arrays with the same size (datums wise) as the source
    data.

    '''
    y_out = ohlc_shm.ustruct(fields)
    first = ohlc_shm._first.value
    last = ohlc_shm._last.value

    # write pushed data to flattened copy
    y_out[first:last] = rfn.structured_to_unstructured(
        ohlc_shm.array[fields]
    )

    # generate an flat-interpolated x-domain
    x_out = (
        np.broadcast_to(
            ohlc_shm._array['index'][:, None],
            (
                ohlc_shm._array.size,
                # 4,  # only ohlc
                y_out.shape[1],
            ),
        ) + np.array([-0.5, 0, 0, 0.5])
    )
    assert y_out.any()

    return (
        x_out,
        y_out,
    )


def update_ohlc_to_line(
    src_shm: ShmArray,
    array_key: str,
    src_update: np.ndarray,
    slc: slice,
    ln: int,
    first: int,
    last: int,
    is_append: bool,

) -> np.ndarray:

    fields = ['open', 'high', 'low', 'close']
    return (
        rfn.structured_to_unstructured(src_update[fields]),
        slc,
    )


def ohlc_flat_to_xy(
    r: Renderer,
    array: np.ndarray,
    array_key: str,
    vr: tuple[int, int],

) -> tuple[
    np.ndarray,
    np.nd.array,
    str,
]:
    # TODO: in the case of an existing ``.update_xy()``
    # should we be passing in array as an xy arrays tuple?

    # 2 more datum-indexes to capture zero at end
    x_flat = r.x_data[r._xy_first:r._xy_last]
    y_flat = r.y_data[r._xy_first:r._xy_last]

    # slice to view
    ivl, ivr = vr
    x_iv_flat = x_flat[ivl:ivr]
    y_iv_flat = y_flat[ivl:ivr]

    # reshape to 1d for graphics rendering
    y_iv = y_iv_flat.reshape(-1)
    x_iv = x_iv_flat.reshape(-1)

    return x_iv, y_iv, 'all'


def to_step_format(
    shm: ShmArray,
    data_field: str,
    index_field: str = 'index',

) -> tuple[int, np.ndarray, np.ndarray]:
    '''
    Convert an input 1d shm array to a "step array" format
    for use by path graphics generation.

    '''
    i = shm._array['index'].copy()
    out = shm._array[data_field].copy()

    x_out = np.broadcast_to(
        i[:, None],
        (i.size, 2),
    ) + np.array([-0.5, 0.5])

    y_out = np.empty((len(out), 2), dtype=out.dtype)
    y_out[:] = out[:, np.newaxis]

    # start y at origin level
    y_out[0, 0] = 0
    return x_out, y_out


def update_step_xy(
    src_shm: ShmArray,
    array_key: str,
    y_update: np.ndarray,
    slc: slice,
    ln: int,
    first: int,
    last: int,
    is_append: bool,

) -> np.ndarray:

    # for a step curve we slice from one datum prior
    # to the current "update slice" to get the previous
    # "level".
    if is_append:
        start = max(last - 1, 0)
        end = src_shm._last.value
        new_y = src_shm._array[start:end][array_key]
        slc = slice(start, end)

    else:
        new_y = y_update

    return (
        np.broadcast_to(
            new_y[:, None], (new_y.size, 2),
        ),
        slc,
    )


def step_to_xy(
    r: Renderer,
    array: np.ndarray,
    array_key: str,
    vr: tuple[int, int],

) -> tuple[
    np.ndarray,
    np.nd.array,
    str,
]:

    # 2 more datum-indexes to capture zero at end
    x_step = r.x_data[r._xy_first:r._xy_last+2]
    y_step = r.y_data[r._xy_first:r._xy_last+2]

    lasts = array[['index', array_key]]
    last = lasts[array_key][-1]
    y_step[-1] = last

    # slice out in-view data
    ivl, ivr = vr
    ys_iv = y_step[ivl:ivr+1]
    xs_iv = x_step[ivl:ivr+1]

    # flatten to 1d
    y_iv = ys_iv.reshape(ys_iv.size)
    x_iv = xs_iv.reshape(xs_iv.size)

    # print(
    #     f'ys_iv : {ys_iv[-s:]}\n'
    #     f'y_iv: {y_iv[-s:]}\n'
    #     f'xs_iv: {xs_iv[-s:]}\n'
    #     f'x_iv: {x_iv[-s:]}\n'
    # )

    return x_iv, y_iv, 'all'
