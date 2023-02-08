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
    QRectF,
)
from PyQt5.QtGui import (
    QPainter,
    QPainterPath,
)
from .._profile import pg_profile_enabled, ms_slower_then
from ._style import hcolor
from ..log import get_logger
from .._profile import Profiler


log = get_logger(__name__)


_line_styles: dict[str, int] = {
    'solid': Qt.PenStyle.SolidLine,
    'dash': Qt.PenStyle.DashLine,
    'dot': Qt.PenStyle.DotLine,
    'dashdot': Qt.PenStyle.DashDotLine,
}


class FlowGraphic(pg.GraphicsObject):
    '''
    Base class with minimal interface for `QPainterPath` implemented,
    real-time updated "data flow" graphics.

    See subtypes below.

    '''
    # sub-type customization methods
    declare_paintables: Callable | None = None
    sub_paint: Callable | None = None

    # XXX-NOTE-XXX: graphics caching B)
    # see explanation for different caching modes:
    # https://stackoverflow.com/a/39410081
    cache_mode: int = QGraphicsItem.DeviceCoordinateCache
    # XXX: WARNING item caching seems to only be useful
    # if we don't re-generate the entire QPainterPath every time
    # don't ever use this - it's a colossal nightmare of artefacts
    # and is disastrous for performance.
    # QGraphicsItem.ItemCoordinateCache
    # TODO: still questions todo with coord-cacheing that we should
    # probably talk to a core dev about:
    # - if this makes trasform interactions slower (such as zooming)
    #   and if so maybe if/when we implement a "history" mode for the
    #   view we disable this in that mode?

    def __init__(
        self,
        *args,
        name: str | None = None,

        # line styling
        color: str = 'bracket',
        last_step_color: str | None = None,
        fill_color: Optional[str] = None,
        style: str = 'solid',

        **kwargs

    ) -> None:

        self._name = name

        # primary graphics item used for history
        self.path: QPainterPath = QPainterPath()

        # additional path that can be optionally used for appends which
        # tries to avoid triggering an update/redraw of the presumably
        # larger historical ``.path`` above. the flag to enable
        # this behaviour is found in `Renderer.render()`.
        self.fast_path: QPainterPath | None = None

        # TODO: evaluating the path capacity stuff and see
        # if it really makes much diff pre-allocating it.
        # self._last_cap: int = 0
        # cap = path.capacity()
        # if cap != self._last_cap:
        #     print(f'NEW CAPACITY: {self._last_cap} -> {cap}')
        #     self._last_cap = cap

        # all history of curve is drawn in single px thickness
        self._color: str = color
        pen = pg.mkPen(hcolor(color), width=1)
        pen.setStyle(_line_styles[style])

        if 'dash' in style:
            pen.setDashPattern([8, 3])

        self._pen = pen
        self._brush = pg.functions.mkBrush(
            hcolor(fill_color or color)
        )

        # last segment is drawn in 2px thickness for emphasis
        if last_step_color:
            self.last_step_pen = pg.mkPen(
                hcolor(last_step_color),
                width=2,
            )
        else:
            self.last_step_pen = pg.mkPen(
                self._pen,
                width=2,
            )

        self._last_line: QLineF = QLineF()

        super().__init__(*args, **kwargs)

        # apply cache mode
        self.setCacheMode(self.cache_mode)

    def x_uppx(self) -> int:

        px_vecs = self.pixelVectors()[0]
        if px_vecs:
            return px_vecs.x()
        else:
            return 0

    def x_last(self) -> float | None:
        '''
        Return the last most x value of the last line segment or if not
        drawn yet, ``None``.

        '''
        return self._last_line.x1() if self._last_line else None


class Curve(FlowGraphic):
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
    ``piker.ui._render.Renderer`` for details and logic related to lower
    level path generation and incremental update. The main differences in
    the path generation code include:

    - avoiding regeneration of the entire historical path where possible
      and instead only updating the "new" segment(s) via a ``numpy``
      array diff calc.
    - here, the "last" graphics datum-segment is drawn independently
      such that near-term (high frequency) discrete-time-sampled style
      updates don't trigger a full path redraw.

    '''
    # TODO: can we remove this?
    # sub_br: Optional[Callable] = None

    def __init__(
        self,
        *args,

        # color: str = 'default_lightest',
        # fill_color: Optional[str] = None,
        # style: str = 'solid',

        **kwargs

    ) -> None:

        # brutaaalll, see comments within..
        self.yData = None
        self.xData = None

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)

        self._last_line: QLineF = QLineF()

        # self._fill = True

        # allow sub-type customization
        declare = self.declare_paintables
        if declare:
            declare()

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
                self.fast_path.clear()
                # self.fast_path = None

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

    # Qt docs: https://doc.qt.io/qt-5/qgraphicsitem.html#boundingRect
    def _path_br(self):
        '''
        Post init ``.boundingRect()```.

        '''
        # profiler = Profiler(
        #     msg=f'Curve.boundingRect(): `{self._name}`',
        #     disabled=not pg_profile_enabled(),
        #     ms_threshold=ms_slower_then,
        # )
        pr = self.path.controlPointRect()
        hb_tl, hb_br = (
            pr.topLeft(),
            pr.bottomRight(),
        )
        mn_y = hb_tl.y()
        mx_y = hb_br.y()
        most_left = hb_tl.x()
        most_right = hb_br.x()
        # profiler('calc path vertices')

        # TODO: if/when we get fast path appends working in the
        # `Renderer`, then we might need to actually use this..
        # fp = self.fast_path
        # if fp:
        #     fhb = fp.controlPointRect()
        #     # hb_size = fhb.size() + hb_size
        #     br = pr.united(fhb)

        # XXX: *was* a way to allow sub-types to extend the
        # boundingrect calc, but in the one use case for a step curve
        # doesn't seem like we need it as long as the last line segment
        # is drawn as it is?

        # sbr = self.sub_br
        # if sbr:
        #     # w, h = self.sub_br(w, h)
        #     sub_br = sbr()
        #     br = br.united(sub_br)

        # assume plain line graphic and use
        # default unit step in each direction.
        ll = self._last_line
        y1, y2 = ll.y1(), ll.y2()
        x1, x2 = ll.x1(), ll.x2()

        ymn = min(y1, y2, mn_y)
        ymx = max(y1, y2, mx_y)
        most_left = min(x1, x2, most_left)
        most_right = max(x1, x2, most_right)
        # profiler('calc last line vertices')

        return QRectF(
            most_left,
            ymn,
            most_right - most_left + 1,
            ymx,
        )

    def paint(
        self,
        p: QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget

    ) -> None:

        profiler = Profiler(
            msg=f'Curve.paint(): `{self._name}`',
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,
        )

        sub_paint = self.sub_paint
        if sub_paint:
            sub_paint(p)

        p.setPen(self.last_step_pen)
        p.drawLine(self._last_line)
        profiler('last datum `.drawLine()`')

        p.setPen(self._pen)
        path = self.path

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
        reset: bool,
        array_key: str,
        index_field: str,

    ) -> None:
        # default line draw last call
        # with self.reset_cache():
        x = src_data[index_field]
        y = src_data[array_key]

        x_last = x[-1]
        x_2last = x[-2]

        # draw the "current" step graphic segment so it
        # lines up with the "middle" of the current
        # (OHLC) sample.
        self._last_line = QLineF(

            # NOTE: currently we draw in x-domain
            # from last datum to current such that
            # the end of line touches the "beginning"
            # of the current datum step span.
            x_2last, y[-2],
            x_last, y[-1],
        )

        return x, y


# TODO: this should probably be a "downsampled" curve type
# that draws a bar-style (but for the px column) last graphics
# element such that the current datum in view can be shown
# (via it's max / min) even when highly zoomed out.
class FlattenedOHLC(Curve):

    def draw_last_datum(
        self,
        path: QPainterPath,
        src_data: np.ndarray,
        reset: bool,
        array_key: str,
        index_field: str,

    ) -> None:
        lasts = src_data[-2:]
        x = lasts[index_field]
        y = lasts['close']

        # draw the "current" step graphic segment so it
        # lines up with the "middle" of the current
        # (OHLC) sample.
        self._last_line = QLineF(
            x[-2], y[-2],
            x[-1], y[-1]
        )
        return x, y


class StepCurve(Curve):

    # avoids strange dragging/smearing artifacts when panning
    # as well as mouse over artefacts when the vlm chart series
    # is "shorter" then some overlay..
    cache_mode: int = QGraphicsItem.NoCache

    def declare_paintables(
        self,
    ) -> None:
        self._last_step_rect = QRectF()

    def draw_last_datum(
        self,
        path: QPainterPath,
        src_data: np.ndarray,
        reset: bool,
        array_key: str,
        index_field: str,

        w: float = 0.5,

    ) -> None:

        # TODO: remove this and instead place all step curve
        # updating into pre-path data render callbacks.
        # full input data
        x = src_data[index_field]
        y = src_data[array_key]

        x_last = x[-1]
        x_2last = x[-2]
        y_last = y[-1]
        step_size = x_last - x_2last

        # lol, commenting this makes step curves
        # all "black" for me :eyeroll:..
        self._last_line = QLineF(
            x_2last, 0,
            x_last, 0,
        )
        self._last_step_rect = QRectF(
            x_last, 0,
            step_size, y_last,
        )
        return x, y

    def sub_paint(
        self,
        p: QPainter,

    ) -> None:
        # p.drawLines(*tuple(filter(bool, self._last_step_lines)))
        # p.drawRect(self._last_step_rect)
        p.fillRect(self._last_step_rect, self._brush)
