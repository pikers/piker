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
from typing import (
    Optional,
)

import numpy as np
from numba import njit, float64, int64  # , optional
import pyqtgraph as pg
from PyQt5 import QtGui
# from PyQt5.QtCore import QLineF, QPointF

from .._profile import pg_profile_enabled, ms_slower_then
from ._compression import (
    # ohlc_flatten,
    ds_m4,
)


def xy_downsample(
    x,
    y,
    px_width,
    uppx,

    x_spacer: float = 0.5,

) -> tuple[np.ndarray, np.ndarray]:

    # downsample whenever more then 1 pixels per datum can be shown.
    # always refresh data bounds until we get diffing
    # working properly, see above..
    bins, x, y = ds_m4(
        x,
        y,
        px_width=px_width,
        uppx=uppx,
        # log_scale=bool(uppx)
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
    data: np.ndarray,
    start: int = 0,  # XXX: do we need this?
    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.43,
    path: Optional[QtGui.QPainterPath] = None,

) -> QtGui.QPainterPath:

    path_was_none = path is None

    profiler = pg.debug.Profiler(
        msg='gen_qpath ohlc',
        disabled=not pg_profile_enabled(),
        ms_threshold=ms_slower_then,
    )

    x, y, c = path_arrays_from_ohlc(
        data,
        start,
        bar_gap=w,
    )
    profiler("generate stream with numba")

    # TODO: numba the internals of this!
    path = pg.functions.arrayToQPath(
        x,
        y,
        connect=c,
        path=path,
    )

    # avoid mem allocs if possible
    if path_was_none:
        path.reserve(path.capacity())

    profiler("generate path with arrayToQPath")

    return path
