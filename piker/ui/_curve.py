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

"""
Fast, smooth, sexy curves.

"""
from typing import Optional

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtGui, QtWidgets
from PyQt5.QtWidgets import QGraphicsItem
from PyQt5.QtCore import (
    Qt,
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
    include_endpoints: bool = False,

) -> (np.ndarray, np.ndarray):
    '''
    Generate a "step mode" curve aligned with OHLC style bars
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
    y2 = np.empty((len(y), 2), dtype=y.dtype)
    y2[:] = y[:, np.newaxis]

    y_out = np.empty(
        2*len(y) + 2,
        dtype=y.dtype
    )

    # flatten and set 0 endpoints
    y_out[1:-1] = y2.reshape(y2.size)
    y_out[0] = 0
    y_out[-1] = 0

    if not include_endpoints:
        return x_out[:-1], y_out[:-1]

    else:
        return x_out, y_out


_line_styles: dict[str, int] = {
    'solid': Qt.PenStyle.SolidLine,
    'dash': Qt.PenStyle.DashLine,
    'dot': Qt.PenStyle.DotLine,
    'dashdot': Qt.PenStyle.DashDotLine,
}


# TODO: got a feeling that dropping this inheritance gets us even more speedups
class FastAppendCurve(pg.PlotCurveItem):
    '''
    A faster, append friendly version of ``pyqtgraph.PlotCurveItem``
    built for real-time data updates.

    The main difference is avoiding regeneration of the entire
    historical path where possible and instead only updating the "new"
    segment(s) via a ``numpy`` array diff calc. Further the "last"
    graphic segment is drawn independently such that near-term (high
    frequency) discrete-time-sampled style updates don't trigger a full
    path redraw.

    '''
    def __init__(
        self,
        *args,
        step_mode: bool = False,
        color: str = 'default_lightest',
        fill_color: Optional[str] = None,
        style: str = 'solid',
        name: Optional[str] = None,
        use_polyline: bool = False,

        **kwargs

    ) -> None:

        self._name = name
        self.path: Optional[QtGui.QPainterPath] = None

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)
        self._xrange: tuple[int, int] = self.dataBounds(ax=0)

        # self._last_draw = time.time()
        self._use_poly = use_polyline
        self.poly = None
        self._redraw: bool = False

        # all history of curve is drawn in single px thickness
        pen = pg.mkPen(hcolor(color))
        pen.setStyle(_line_styles[style])

        if 'dash' in style:
            pen.setDashPattern([8, 3])

        self.setPen(pen)

        # last segment is drawn in 2px thickness for emphasis
        # self.last_step_pen = pg.mkPen(hcolor(color), width=2)
        self.last_step_pen = pg.mkPen(pen, width=2)

        self._last_line: Optional[QLineF] = None
        self._last_step_rect: Optional[QRectF] = None

        # flat-top style histogram-like discrete curve
        self._step_mode: bool = step_mode

        # self._fill = True
        self.setBrush(hcolor(fill_color or color))

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        if step_mode or self._use_poly:
            # don't enable caching by default for the case where the
            # only thing drawn is the "last" line segment which can
            # have a weird artifact where it won't be fully drawn to its
            # endpoint (something we saw on trade rate curves)
            self.setCacheMode(
                QGraphicsItem.DeviceCoordinateCache
            )

    def update_from_array(
        self,
        x: np.ndarray,
        y: np.ndarray,

    ) -> QtGui.QPainterPath:
        '''
        Update curve from input 2-d data.

        Compare with a cached "x-range" state and (pre/a)ppend based on
        a length diff.

        '''
        profiler = pg.debug.Profiler(disabled=not pg_profile_enabled())
        flip_cache = False

        istart, istop = self._xrange
        # print(f"xrange: {self._xrange}")

        # compute the length diffs between the first/last index entry in
        # the input data and the last indexes we have on record from the
        # last time we updated the curve index.
        prepend_length = int(istart - x[0])
        append_length = int(x[-1] - istop)

        # step mode: draw flat top discrete "step"
        # over the index space for each datum.
        if self._step_mode:
            x_out, y_out = step_path_arrays_from_1d(x[:-1], y[:-1])

        else:
            # by default we only pull data up to the last (current) index
            x_out, y_out = x[:-1], y[:-1]

        if (
            self.path is None
            or prepend_length > 0
            or self._redraw
        ):
            if self._use_poly:
                self.poly = pg.functions.arrayToQPolygonF(
                    x_out,
                    y_out,
                )

            else:
                self.path = pg.functions.arrayToQPath(
                    x_out,
                    y_out,
                    connect='all',
                    finiteCheck=False,
                    path=self.path,
                )
                # reserve mem allocs see:
                # - https://doc.qt.io/qt-5/qpainterpath.html#reserve
                # - https://doc.qt.io/qt-5/qpainterpath.html#capacity
                # - https://doc.qt.io/qt-5/qpainterpath.html#clear
                # XXX: right now this is based on had hoc checks on a
                # hidpi 3840x2160 4k monitor but we should optimize for
                # the target display(s) on the sys.
                self.path.reserve(int(500e3))

            profiler('generate fresh path')
            self._redraw = False

            # if self._step_mode:
            #     self.path.closeSubpath()

        # TODO: get this piecewise prepend working - right now it's
        # giving heck on vwap...
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

        elif append_length > 0:
            if self._step_mode:
                new_x, new_y = step_path_arrays_from_1d(
                    x[-append_length - 2:-1],
                    y[-append_length - 2:-1],
                )
                # [1:] since we don't need the vertical line normally at
                # the beginning of the step curve taking the first (x,
                # y) poing down to the x-axis **because** this is an
                # appended path graphic.
                new_x = new_x[1:]
                new_y = new_y[1:]

            else:
                # print(f"append_length: {append_length}")
                new_x = x[-append_length - 2:-1]
                new_y = y[-append_length - 2:-1]
                # print((new_x, new_y))

            if self._use_poly:
                union_poly = pg.functions.arrayToQPolygonF(
                    new_x,
                    new_y,
                )

            else:
                append_path = pg.functions.arrayToQPath(
                    new_x,
                    new_y,
                    connect='all',
                    finiteCheck=False,
                )

            # other merging ideas:
            # https://stackoverflow.com/questions/8936225/how-to-merge-qpainterpaths
            if self._step_mode:
                assert not self._use_poly, 'Dunno howw this worx yet'

                # path.addPath(append_path)
                self.path.connectPath(append_path)

                # TODO: try out new work from `pyqtgraph` main which
                # should repair horrid perf:
                # https://github.com/pyqtgraph/pyqtgraph/pull/2032
                # ok, nope still horrible XD
                # if self._fill:
                #     # XXX: super slow set "union" op
                #     self.path = self.path.united(append_path).simplified()

                #     # path.addPath(append_path)
                #     # path.closeSubpath()

            else:
                if self._use_poly:
                    self.poly = self.poly.united(union_poly)
                else:
                    # print(f"append_path br: {append_path.boundingRect()}")
                    # self.path.moveTo(new_x[0], new_y[0])
                    self.path.connectPath(append_path)
                    # path.connectPath(append_path)

                    # XXX: lol this causes a hang..
                    # self.path = self.path.simplified()

            self.disable_cache()
            flip_cache = True

        # XXX: do we need this any more?
        # if (
        #     self._step_mode
        # ):
        #     self.disable_cache()
        #     flip_cache = True

            # print(f"update br: {self.path.boundingRect()}")

        # XXX: lol brutal, the internals of `CurvePoint` (inherited by
        # our `LineDot`) required ``.getData()`` to work..
        self.xData = x
        self.yData = y

        x0, x_last = self._xrange = x[0], x[-1]
        y_last = y[-1]

        # draw the "current" step graphic segment so it lines up with
        # the "middle" of the current (OHLC) sample.
        if self._step_mode:
            self._last_line = QLineF(
                x_last - 0.5, 0,
                x_last + 0.5, 0,
            )
            self._last_step_rect = QRectF(
                x_last - 0.5, 0,
                x_last + 0.5, y_last
            )
        else:
            # print((x[-1], y_last))
            self._last_line = QLineF(
                x[-2], y[-2],
                x[-1], y_last
            )

        # trigger redraw of path
        # do update before reverting to cache mode
        self.prepareGeometryChange()
        self.update()

        if flip_cache:
            # XXX: seems to be needed to avoid artifacts (see above).
            self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

    def clear(self):
        '''
        Clear internal graphics making object ready for full re-draw.

        '''
        # NOTE: original code from ``pg.PlotCurveItem``
        self.xData = None  ## raw values
        self.yData = None
        self._renderSegmentList = None
        self.fillPath = None
        self._fillPathList = None
        self._mouseShape = None
        self._mouseBounds = None
        self._boundsCache = [None, None]
        #del self.xData, self.yData, self.xDisp, self.yDisp, self.path

        # path reservation aware non-mem de-alloc cleaning
        if self.path:
            self.path.clear()
            self._redraw = True

            # XXX: if not trying to leverage `.reserve()` allocs
            # then you might as well create a new one..
            # self.path = None

    def disable_cache(self) -> None:
        '''
        Disable the use of the pixel coordinate cache and trigger a geo event.

        '''
        # XXX: pretty annoying but, without this there's little
        # artefacts on the append updates to the curve...
        self.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
        self.prepareGeometryChange()

    def boundingRect(self):
        '''
        Compute and then cache our rect.
        '''
        if self._use_poly:
            if self.poly is None:
                return QtGui.QPolygonF().boundingRect()
            else:
                br = self.boundingRect = self.poly.boundingRect
                return br()
        else:
            if self.path is None:
                return QtGui.QPainterPath().boundingRect()
            else:
                # dynamically override this method after initial
                # path is created to avoid requiring the above None check
                self.boundingRect = self._path_br
                return self._path_br()

    def _path_br(self):
        '''
        Post init ``.boundingRect()```.

        '''
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

        profiler = pg.debug.Profiler(
            # disabled=False, #not pg_profile_enabled(),
        )
        # p.setRenderHint(p.Antialiasing, True)

        if (
            self._step_mode
            and self._last_step_rect
        ):
            brush = self.opts['brush']
            # p.drawLines(*tuple(filter(bool, self._last_step_lines)))
            # p.drawRect(self._last_step_rect)
            p.fillRect(self._last_step_rect, brush)

            # p.drawPath(self.path)
            # profiler('.drawPath()')

        if self._last_line:
            p.setPen(self.last_step_pen)
            p.drawLine(self._last_line)
            profiler('.drawLine()')
            p.setPen(self.opts['pen'])

        # else:
        if self._use_poly:
            assert self.poly
            p.drawPolyline(self.poly)
            profiler('.drawPolyline()')
        else:
            p.drawPath(self.path)
            profiler('.drawPath()')

        # TODO: try out new work from `pyqtgraph` main which should
        # repair horrid perf (pretty sure i did and it was still
        # horrible?):
        # https://github.com/pyqtgraph/pyqtgraph/pull/2032
        # if self._fill:
        #     brush = self.opts['brush']
        #     p.fillPath(self.path, brush)

        # now = time.time()
        # print(f'DRAW RATE {1/(now - self._last_draw)}')
        # self._last_draw = now


# import time
# _last_draw: float = time.time()
