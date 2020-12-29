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
Fast, smooth, sexy curves.
"""
from typing import Tuple

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui, QtWidgets

from ..._profile import pg_profile_enabled


class FastAppendCurve(pg.PlotCurveItem):

    def __init__(self, *args, **kwargs):

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)

        self._last_line: QtCore.QLineF = None
        self._xrange: Tuple[int, int] = self.dataBounds(ax=0)

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        self.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

    def update_from_array(
        self,
        x,
        y,
    ) -> QtGui.QPainterPath:

        profiler = pg.debug.Profiler(disabled=not pg_profile_enabled())
        flip_cache = False

        # print(f"xrange: {self._xrange}")
        istart, istop = self._xrange

        prepend_length = istart - x[0]
        append_length = x[-1] - istop

        if self.path is None or prepend_length:
            self.path = pg.functions.arrayToQPath(
                x[:-1],
                y[:-1],
                connect='all'
            )
            profiler('generate fresh path')

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
            # print(f"append_length: {append_length}")
            new_x = x[-append_length - 2:-1]
            new_y = y[-append_length - 2:-1]
            # print((new_x, new_y))

            append_path = pg.functions.arrayToQPath(
                new_x,
                new_y,
                connect='all'
            )
            # print(f"append_path br: {append_path.boundingRect()}")
            # self.path.moveTo(new_x[0], new_y[0])
            # self.path.connectPath(append_path)
            self.path.connectPath(append_path)

            # XXX: pretty annoying but, without this there's little
            # artefacts on the append updates to the curve...
            self.setCacheMode(QtGui.QGraphicsItem.NoCache)
            self.prepareGeometryChange()
            flip_cache = True

            # print(f"update br: {self.path.boundingRect()}")

        # XXX: lol brutal, the internals of `CurvePoint` (inherited by
        # our `LineDot`) required ``.getData()`` to work..
        self.xData = x
        self.yData = y

        self._xrange = x[0], x[-1]
        self._last_line = QtCore.QLineF(x[-2], y[-2], x[-1], y[-1])

        # trigger redraw of path
        # do update before reverting to cache mode
        self.prepareGeometryChange()
        self.update()

        if flip_cache:
            self.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

    def boundingRect(self):
        hb = self.path.controlPointRect()
        hb_size = hb.size()
        # print(f'hb_size: {hb_size}')

        w = hb_size.width() + 1
        h = hb_size.height() + 1
        br = QtCore.QRectF(

            # top left
            QtCore.QPointF(hb.topLeft()),

            # total size
            QtCore.QSizeF(w, h)
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
        p.drawLine(self._last_line)
        profiler('.drawLine()')

        p.drawPath(self.path)
        profiler('.drawPath()')
