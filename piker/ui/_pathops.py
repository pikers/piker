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
    # Optional,
    TYPE_CHECKING,
)

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


def xy_downsample(
    x,
    y,
    uppx,

    x_spacer: float = 0.5,

) -> tuple[np.ndarray, np.ndarray]:

    # downsample whenever more then 1 pixels per datum can be shown.
    # always refresh data bounds until we get diffing
    # working properly, see above..
    bins, x, y = ds_m4(
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

    return x, y


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
    fields: list[str] = ['open', 'high', 'low', 'close']

) -> tuple[
    int,  # flattened first index
    int,  # flattened last index
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
        first,
        last,
        x_out,
        y_out,
    )


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
