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
# from numba import njit, float64, int64  # , optional
# import pyqtgraph as pg
# from PyQt5 import QtCore, QtGui, QtWidgets
# from PyQt5.QtCore import QLineF, QPointF

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
