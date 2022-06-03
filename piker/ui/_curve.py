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
from contextlib import contextmanager as cm
from typing import Optional, Callable

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QGraphicsItem
from PyQt5.QtCore import (
    Qt,
    QLineF,
    QSizeF,
    QRectF,
    # QRect,
    QPointF,
)
from PyQt5.QtGui import (
    QPainter,
    QPainterPath,
)
from .._profile import pg_profile_enabled, ms_slower_then
from ._style import hcolor
# from ._compression import (
#     # ohlc_to_m4_line,
#     ds_m4,
# )
from ..log import get_logger


log = get_logger(__name__)


_line_styles: dict[str, int] = {
    'solid': Qt.PenStyle.SolidLine,
    'dash': Qt.PenStyle.DashLine,
    'dot': Qt.PenStyle.DotLine,
    'dashdot': Qt.PenStyle.DashDotLine,
}


class Curve(pg.GraphicsObject):
    '''
    A faster, simpler, append friendly version of
    ``pyqtgraph.PlotCurveItem`` built for highly customizable real-time
    updates.

    This type is a much stripped down version of a ``pyqtgraph`` style
    "graphics object" in the sense that the internal lower level
    graphics which are drawn in the ``.paint()`` method are actually
    rendered outside of this class entirely and instead are assigned as
    state (instance vars) here and then drawn during a Qt graphics
    cycle.

    The main motivation for this more modular, composed design is that
    lower level graphics data can be rendered in different threads and
    then read and drawn in this main thread without having to worry
    about dealing with Qt's concurrency primitives. See
    ``piker.ui._flows.Renderer`` for details and logic related to lower
    level path generation and incremental update. The main differences in
    the path generation code include:

    - avoiding regeneration of the entire historical path where possible
      and instead only updating the "new" segment(s) via a ``numpy``
      array diff calc.
    - here, the "last" graphics datum-segment is drawn independently
      such that near-term (high frequency) discrete-time-sampled style
      updates don't trigger a full path redraw.

    '''

    # sub-type customization methods
    sub_br: Optional[Callable] = None
    sub_paint: Optional[Callable] = None
    declare_paintables: Optional[Callable] = None

    def __init__(
        self,
        *args,

        step_mode: bool = False,
        color: str = 'default_lightest',
        fill_color: Optional[str] = None,
        style: str = 'solid',
        name: Optional[str] = None,
        use_fpath: bool = True,

        **kwargs

    ) -> None:

        self._name = name

        # brutaaalll, see comments within..
        self.yData = None
        self.xData = None

        # self._last_cap: int = 0
        self.path: Optional[QPainterPath] = None

        # additional path used for appends which tries to avoid
        # triggering an update/redraw of the presumably larger
        # historical ``.path`` above.
        self.use_fpath = use_fpath
        self.fast_path: Optional[QPainterPath] = None

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)

        # all history of curve is drawn in single px thickness
        pen = pg.mkPen(hcolor(color))
        pen.setStyle(_line_styles[style])

        if 'dash' in style:
            pen.setDashPattern([8, 3])

        self._pen = pen

        # last segment is drawn in 2px thickness for emphasis
        # self.last_step_pen = pg.mkPen(hcolor(color), width=2)
        self.last_step_pen = pg.mkPen(pen, width=2)

        # self._last_line: Optional[QLineF] = None
        self._last_line = QLineF()
        self._last_w: float = 1

        # flat-top style histogram-like discrete curve
        # self._step_mode: bool = step_mode

        # self._fill = True
        self._brush = pg.functions.mkBrush(hcolor(fill_color or color))

        # NOTE: this setting seems to mostly prevent redraws on mouse
        # interaction which is a huge boon for avg interaction latency.

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        # don't enable caching by default for the case where the
        # only thing drawn is the "last" line segment which can
        # have a weird artifact where it won't be fully drawn to its
        # endpoint (something we saw on trade rate curves)
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

        # XXX: see explanation for different caching modes:
        # https://stackoverflow.com/a/39410081
        # seems to only be useful if we don't re-generate the entire
        # QPainterPath every time
        # curve.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # don't ever use this - it's a colossal nightmare of artefacts
        # and is disastrous for performance.
        # curve.setCacheMode(QtWidgets.QGraphicsItem.ItemCoordinateCache)

        # allow sub-type customization
        declare = self.declare_paintables
        if declare:
            declare()

    # TODO: probably stick this in a new parent
    # type which will contain our own version of
    # what ``PlotCurveItem`` had in terms of base
    # functionality? A `FlowGraphic` maybe?
    def x_uppx(self) -> int:

        px_vecs = self.pixelVectors()[0]
        if px_vecs:
            xs_in_px = px_vecs.x()
            return round(xs_in_px)
        else:
            return 0

    def px_width(self) -> float:

        vb = self.getViewBox()
        if not vb:
            return 0

        vr = self.viewRect()
        l, r = int(vr.left()), int(vr.right())

        start, stop = self._xrange
        lbar = max(l, start)
        rbar = min(r, stop)

        return vb.mapViewToDevice(
            QLineF(lbar, 0, rbar, 0)
        ).length()

    # XXX: lol brutal, the internals of `CurvePoint` (inherited by
    # our `LineDot`) required ``.getData()`` to work..
    def getData(self):
        return self.xData, self.yData

    def clear(self):
        '''
        Clear internal graphics making object ready for full re-draw.

        '''
        # NOTE: original code from ``pg.PlotCurveItem``
        self.xData = None
        self.yData = None

        # XXX: previously, if not trying to leverage `.reserve()` allocs
        # then you might as well create a new one..
        # self.path = None

        # path reservation aware non-mem de-alloc cleaning
        if self.path:
            self.path.clear()

            if self.fast_path:
                # self.fast_path.clear()
                self.fast_path = None

    @cm
    def reset_cache(self) -> None:
        self.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
        yield
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

    def boundingRect(self):
        '''
        Compute and then cache our rect.
        '''
        if self.path is None:
            return QPainterPath().boundingRect()
        else:
            # dynamically override this method after initial
            # path is created to avoid requiring the above None check
            self.boundingRect = self._path_br
            return self._path_br()

    def _path_br(self):
        '''
        Post init ``.boundingRect()```.

        '''
        # hb = self.path.boundingRect()
        hb = self.path.controlPointRect()
        hb_size = hb.size()

        fp = self.fast_path
        if fp:
            fhb = fp.controlPointRect()
            hb_size = fhb.size() + hb_size

        # print(f'hb_size: {hb_size}')

        # if self._last_step_rect:
        #     hb_size += self._last_step_rect.size()

        # if self._line:
        #     br = self._last_step_rect.bottomRight()

        # tl = QPointF(
        #     # self._vr[0],
        #     # hb.topLeft().y(),
        #     # 0,
        #     # hb_size.height() + 1
        # )

        #     br = self._last_step_rect.bottomRight()

        w = hb_size.width()
        h = hb_size.height()

        sbr = self.sub_br
        if sbr:
            w, h = self.sub_br(w, h)
        else:
            # assume plain line graphic and use
            # default unit step in each direction.

            # only on a plane line do we include
            # and extra index step's worth of width
            # since in the step case the end of the curve
            # actually terminates earlier so we don't need
            # this for the last step.
            w += self._last_w
            # ll = self._last_line
            h += 1  # ll.y2() - ll.y1()

        # br = QPointF(
        #     self._vr[-1],
        #     # tl.x() + w,
        #     tl.y() + h,
        # )

        br = QRectF(

            # top left
            # hb.topLeft()
            # tl,
            QPointF(hb.topLeft()),

            # br,
            # total size
            # QSizeF(hb_size)
            # hb_size,
            QSizeF(w, h)
        )
        # print(f'bounding rect: {br}')
        return br

    def paint(
        self,
        p: QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget

    ) -> None:

        profiler = pg.debug.Profiler(
            msg=f'Curve.paint(): `{self._name}`',
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,
        )

        sub_paint = self.sub_paint
        if sub_paint:
            sub_paint(p, profiler)

        p.setPen(self.last_step_pen)
        p.drawLine(self._last_line)
        profiler('.drawLine()')
        p.setPen(self._pen)

        path = self.path
        # cap = path.capacity()
        # if cap != self._last_cap:
        #     print(f'NEW CAPACITY: {self._last_cap} -> {cap}')
        #     self._last_cap = cap

        if path:
            p.drawPath(path)
            profiler(f'.drawPath(path): {path.capacity()}')

        fp = self.fast_path
        if fp:
            p.drawPath(fp)
            profiler('.drawPath(fast_path)')

        # TODO: try out new work from `pyqtgraph` main which should
        # repair horrid perf (pretty sure i did and it was still
        # horrible?):
        # https://github.com/pyqtgraph/pyqtgraph/pull/2032
        # if self._fill:
        #     brush = self.opts['brush']
        #     p.fillPath(self.path, brush)

    def draw_last_datum(
        self,
        path: QPainterPath,
        src_data: np.ndarray,
        render_data: np.ndarray,
        reset: bool,
        array_key: str,

    ) -> None:
        # default line draw last call
        with self.reset_cache():
            x = render_data['index']
            y = render_data[array_key]

            x_last = x[-1]
            y_last = y[-1]

            # draw the "current" step graphic segment so it
            # lines up with the "middle" of the current
            # (OHLC) sample.
            self._last_line = QLineF(
                x[-2], y[-2],
                x_last, y_last
            )


# TODO: this should probably be a "downsampled" curve type
# that draws a bar-style (but for the px column) last graphics
# element such that the current datum in view can be shown
# (via it's max / min) even when highly zoomed out.
class FlattenedOHLC(Curve):

    def draw_last_datum(
        self,
        path: QPainterPath,
        src_data: np.ndarray,
        render_data: np.ndarray,
        reset: bool,
        array_key: str,

    ) -> None:
        lasts = src_data[-2:]
        x = lasts['index']
        y = lasts['close']

        # draw the "current" step graphic segment so it
        # lines up with the "middle" of the current
        # (OHLC) sample.
        self._last_line = QLineF(
            x[-2], y[-2],
            x[-1], y[-1]
        )


class StepCurve(Curve):

    def declare_paintables(
        self,
    ) -> None:
        self._last_step_rect = QRectF()

    def draw_last_datum(
        self,
        path: QPainterPath,
        src_data: np.ndarray,
        render_data: np.ndarray,
        reset: bool,
        array_key: str,

        w: float = 0.5,

    ) -> None:

        # TODO: remove this and instead place all step curve
        # updating into pre-path data render callbacks.
        # full input data
        x = src_data['index']
        y = src_data[array_key]

        x_last = x[-1]
        y_last = y[-1]

        # lol, commenting this makes step curves
        # all "black" for me :eyeroll:..
        self._last_line = QLineF(
            x_last - w, 0,
            x_last + w, 0,
        )
        self._last_step_rect = QRectF(
            x_last - w, 0,
            x_last + w, y_last,
        )

    def sub_paint(
        self,
        p: QPainter,
        profiler: pg.debug.Profiler,

    ) -> None:
        # p.drawLines(*tuple(filter(bool, self._last_step_lines)))
        # p.drawRect(self._last_step_rect)
        p.fillRect(self._last_step_rect, self._brush)
        profiler('.fillRect()')

    def sub_br(
        self,
        path_w: float,
        path_h: float,

    ) -> (float, float):
        # passthrough
        return path_w, path_h
