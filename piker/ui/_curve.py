# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
Fast, smooth, sexy curves.

"""
from typing import Tuple

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtGui, QtWidgets
from PyQt5.QtCore import (
    QLineF,
    QSizeF,
    QRectF,
    QPointF,
)

from .._profile import pg_profile_enabled
from ._style import hcolor


def step_path_arrays_from_1d(
    x: np.ndarray,
    y: np.ndarray,

) -> (np.ndarray, np.ndarray):
    '''Generate a "step mode" curve aligned with OHLC style bars
    such that each segment spans each bar (aka "centered" style).

    '''
    y_out = y.copy()
    x_out = x.copy()
    x2 = np.empty(
        # the data + 2 endpoints on either end for
        # "termination of the path".
        (len(x) + 1, 2),
        # we want to align with OHLC or other sampling style
        # bars likely so we need fractinal values
        dtype=float,
    )
    x2[0] = x[0] - 0.5
    x2[1] = x[0] + 0.5
    x2[1:] = x[:, np.newaxis] + 0.5

    # flatten to 1-d
    x_out = x2.reshape(x2.size)

    # we create a 1d with 2 extra indexes to
    # hold the start and (current) end value for the steps
    # on either end
    y_out = np.empty(
        2*len(y) + 2,
        dtype=y.dtype
    )
    y2 = np.empty((len(y), 2), dtype=y.dtype)
    y2[:] = y[:, np.newaxis]

    # flatten and set 0 endpoints
    y_out[1:-1] = y2.reshape(y2.size)
    y_out[0] = 0
    y_out[-1] = 0

    return x_out, y_out


def step_lines_from_point(
    index: float,
    level: float,

) -> Tuple[QLineF]:

    # TODO: maybe consider using `QGraphicsLineItem` ??
    # gives us a ``.boundingRect()`` on the objects which may make
    # computing the composite bounding rect of the last bars + the
    # history path faster since it's done in C++:
    # https://doc.qt.io/qt-5/qgraphicslineitem.html

    # index = x[0]
    # level = y[0]

    # (x0 - 0.5, 0) -> (x0 - 0.5, y0)
    left = QLineF(index - 0.5, 0, index - 0.5, level)

    # (x0 - 0.5, y0) -> (x1 + 0.5, y1)
    top = QLineF(index - 0.5, level, index + 0.5, level)

    # (x1 + 0.5, y1 -> (x1 + 0.5, 0)
    right = QLineF(index + 0.5, level, index + 0.5, 0)

    return [left, top, right]


# TODO: got a feeling that dropping this inheritance gets us even more speedups
class FastAppendCurve(pg.PlotCurveItem):

    def __init__(
        self,
        *args,
        step_mode: bool = False,
        **kwargs
    ) -> None:

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)

        self._last_line: QLineF = None
        self._xrange: Tuple[int, int] = self.dataBounds(ax=0)
        self._step_mode: bool = step_mode
        self._fill = False

        color = hcolor('davies')
        self.setBrush(color)
        self.setPen(color)

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

    def update_from_array(
        self,
        x: np.ndarray,
        y: np.ndarray,

    ) -> QtGui.QPainterPath:

        profiler = pg.debug.Profiler(disabled=not pg_profile_enabled())
        flip_cache = False

        # print(f"xrange: {self._xrange}")
        istart, istop = self._xrange

        prepend_length = istart - x[0]
        append_length = x[-1] - istop

        # step mode: draw flat top discrete "step"
        # over the index space for each datum.
        if self._step_mode:
            x_out, y_out = step_path_arrays_from_1d(x[:-1], y[:-1])

        else:
            # by default we only pull data up to the last (current) index
            x_out, y_out = x[:-1], y[:-1]

        if self.path is None or prepend_length:
            self.path = pg.functions.arrayToQPath(
                x_out,
                y_out,
                connect='all',
                finiteCheck=False,
            )
            profiler('generate fresh path')

            if self._step_mode:
                self.path.closeSubpath()

        # TODO: get this working - right now it's giving heck on vwap...
        # if prepend_length:
        #     breakpoint()

        #     prepend_path = pg.functions.arrayToQPath(
        #         x[0:prepend_length],
        #         y[0:prepend_length],
        #         connect='all'
        #     )

        #     # swap prepend path in "front"
        #     old_path = self.path
        #     self.path = prepend_path
        #     # self.path.moveTo(new_x[0], new_y[0])
        #     self.path.connectPath(old_path)

        if append_length:
            if self._step_mode:
                new_x = x[-append_length - 2:-1]
                new_y = y[-append_length - 2:-1]
                new_x, new_y = step_path_arrays_from_1d(new_x, new_y)
                # new_x = new_x[3:]
                # new_y = new_y[3:]

            else:
                # print(f"append_length: {append_length}")
                new_x = x[-append_length - 2:-1]
                new_y = y[-append_length - 2:-1]
                # print((new_x, new_y))

            append_path = pg.functions.arrayToQPath(
                new_x,
                new_y,
                connect='all',
                finiteCheck=False,
            )

            if self._step_mode:
                if self._fill:
                    path = self.path
                    # self.path = self.path.united(append_path).simplified()
                    path.addPath(append_path.simplified())
                    # path.connectPath(append_path.simplified())
                    path.closeSubpath()
                    # path.simplified()
                else:
                    self.path.connectPath(append_path.simplified())
            else:
                # print(f"append_path br: {append_path.boundingRect()}")
                # self.path.moveTo(new_x[0], new_y[0])
                # self.path.connectPath(append_path)
                self.path.connectPath(append_path.simplified())

            # if self._step_mode:
            #     self.path.closeSubpath()
            # self.fill_path.connectPath(

            # XXX: pretty annoying but, without this there's little
            # artefacts on the append updates to the curve...
            self.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
            self.prepareGeometryChange()
            flip_cache = True

            # print(f"update br: {self.path.boundingRect()}")

        # XXX: lol brutal, the internals of `CurvePoint` (inherited by
        # our `LineDot`) required ``.getData()`` to work..
        self.xData = x
        self.yData = y

        self._xrange = x[0], x[-1]
        if self._step_mode:
            # TODO: use a ``QRectF`` and ``QPainterPath.addRect()``
            self._last_step_lines = step_lines_from_point(x[-1], y[-1])
        else:
            self._last_line = QLineF(x[-2], y[-2], x[-1], y[-1])

        # trigger redraw of path
        # do update before reverting to cache mode
        self.prepareGeometryChange()
        self.update()

        if flip_cache:
            self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

    def boundingRect(self):
        if self.path is None:
            return QtGui.QPainterPath().boundingRect()
        else:
            # dynamically override this method after initial
            # path is created to avoid requiring the above None check
            self.boundingRect = self._br
            return self._br()

    def _br(self):
        """Post init ``.boundingRect()```.

        """
        hb = self.path.controlPointRect()
        hb_size = hb.size()
        # print(f'hb_size: {hb_size}')

        w = hb_size.width() + 1
        h = hb_size.height() + 1

        br = QRectF(

            # top left
            QPointF(hb.topLeft()),

            # total size
            QSizeF(w, h)
        )
        # print(f'bounding rect: {br}')
        return br

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget
    ) -> None:

        profiler = pg.debug.Profiler(disabled=not pg_profile_enabled())
        # p.setRenderHint(p.Antialiasing, True)

        p.setPen(self.opts['pen'])

        if self._step_mode:

            p.drawLines(*tuple(filter(bool, self._last_step_lines)))

            p.drawPath(self.path)

            # fill_path = QtGui.QPainterPath(self.path)
            # if self._fill:
            # self.path.closeSubpath()
            if self._fill:
                print('FILLED')
                p.fillPath(self.path, self.opts['brush'])

            profiler('.drawPath()')

        else:
            p.drawLine(self._last_line)
            profiler('.drawLine()')

            p.drawPath(self.path)
            profiler('.drawPath()')
