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

from .._profile import pg_profile_enabled, ms_slower_then
from ._style import hcolor
from ._compression import (
    # ohlc_to_m4_line,
    ds_m4,
)
from ..log import get_logger


log = get_logger(__name__)


# TODO: numba this instead..
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


class FastAppendCurve(pg.GraphicsObject):
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

        x: np.ndarray = None,
        y: np.ndarray = None,
        *args,

        step_mode: bool = False,
        color: str = 'default_lightest',
        fill_color: Optional[str] = None,
        style: str = 'solid',
        name: Optional[str] = None,
        use_fpath: bool = True,

        **kwargs

    ) -> None:

        # brutaaalll, see comments within..
        self._y = self.yData = y
        self._x = self.xData = x

        self._name = name
        self.path: Optional[QtGui.QPainterPath] = None

        self.use_fpath = use_fpath
        self.fast_path: Optional[QtGui.QPainterPath] = None

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)

        # self._xrange: tuple[int, int] = self.dataBounds(ax=0)
        self._xrange: Optional[tuple[int, int]] = None

        # self._last_draw = time.time()
        self._in_ds: bool = False
        self._last_uppx: float = 0

        # all history of curve is drawn in single px thickness
        pen = pg.mkPen(hcolor(color))
        pen.setStyle(_line_styles[style])

        if 'dash' in style:
            pen.setDashPattern([8, 3])

        self._pen = pen

        # last segment is drawn in 2px thickness for emphasis
        # self.last_step_pen = pg.mkPen(hcolor(color), width=2)
        self.last_step_pen = pg.mkPen(pen, width=2)

        self._last_line: Optional[QLineF] = None
        self._last_step_rect: Optional[QRectF] = None

        # flat-top style histogram-like discrete curve
        self._step_mode: bool = step_mode

        # self._fill = True
        self._brush = pg.functions.mkBrush(hcolor(fill_color or color))

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        if step_mode:
            # don't enable caching by default for the case where the
            # only thing drawn is the "last" line segment which can
            # have a weird artifact where it won't be fully drawn to its
            # endpoint (something we saw on trade rate curves)
            self.setCacheMode(
                QGraphicsItem.DeviceCoordinateCache
            )

        self.update()

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

        if not self._xrange:
            return 0

        start, stop = self._xrange
        lbar = max(l, start)
        rbar = min(r, stop)

        return vb.mapViewToDevice(
            QLineF(lbar, 0, rbar, 0)
        ).length()

    def downsample(
        self,
        x,
        y,
        px_width,
        uppx,

    ) -> tuple[np.ndarray, np.ndarray]:

        # downsample whenever more then 1 pixels per datum can be shown.
        # always refresh data bounds until we get diffing
        # working properly, see above..
        bins, x, y = ds_m4(
            x,
            y,
            px_width=px_width,
            uppx=uppx,
            log_scale=bool(uppx)
        )
        x = np.broadcast_to(x[:, None], y.shape)
        # x = (x + np.array([-0.43, 0, 0, 0.43])).flatten()
        x = (x + np.array([-0.5, 0, 0, 0.5])).flatten()
        y = y.flatten()

        # presumably?
        self._in_ds = True
        return x, y

    def update_from_array(
        self,

        # full array input history
        x: np.ndarray,
        y: np.ndarray,

        # pre-sliced array data that's "in view"
        x_iv: np.ndarray,
        y_iv: np.ndarray,

        view_range: Optional[tuple[int, int]] = None,
        profiler: Optional[pg.debug.Profiler] = None,

    ) -> QtGui.QPainterPath:
        '''
        Update curve from input 2-d data.

        Compare with a cached "x-range" state and (pre/a)ppend based on
        a length diff.

        '''
        profiler = profiler or pg.debug.Profiler(
            msg=f'FastAppendCurve.update_from_array(): `{self._name}`',
            disabled=not pg_profile_enabled(),
            gt=ms_slower_then,
        )
        # flip_cache = False

        if self._xrange:
            istart, istop = self._xrange
        else:
            self._xrange = istart, istop = x[0], x[-1]
        # print(f"xrange: {self._xrange}")

        # XXX: lol brutal, the internals of `CurvePoint` (inherited by
        # our `LineDot`) required ``.getData()`` to work..
        # self.xData = x
        # self.yData = y
        # self._x, self._y = x, y

        if view_range:
            profiler(f'view range slice {view_range}')

        # downsampling incremental state checking
        uppx = self.x_uppx()
        px_width = self.px_width()
        uppx_diff = (uppx - self._last_uppx)

        should_ds = False
        should_redraw = False

        # if a view range is passed, plan to draw the
        # source ouput that's "in view" of the chart.
        if view_range and not self._in_ds:
            # print(f'{self._name} vr: {view_range}')

            # by default we only pull data up to the last (current) index
            x_out, y_out = x_iv[:-1], y_iv[:-1]

            # step mode: draw flat top discrete "step"
            # over the index space for each datum.
            if self._step_mode:
                # TODO: numba this bish
                x_out, y_out = step_path_arrays_from_1d(
                    x_out,
                    y_out
                )
                profiler('generated step arrays')

            should_redraw = True
            profiler('sliced in-view array history')

            # x_last = x_iv[-1]
            # y_last = y_iv[-1]
            # self._last_vr = view_range

            # self.disable_cache()
            # flip_cache = True

        else:
            self._xrange = x[0], x[-1]

        x_last = x[-1]
        y_last = y[-1]

        # check for downsampling conditions
        if (
            # std m4 downsample conditions
            px_width
            and uppx_diff >= 4
            or uppx_diff <= -3
            or self._step_mode and abs(uppx_diff) >= 4

        ):
            log.info(
                f'{self._name} sampler change: {self._last_uppx} -> {uppx}'
            )
            self._last_uppx = uppx
            should_ds = True

        elif (
            uppx <= 2
            and self._in_ds
        ):
            # we should de-downsample back to our original
            # source data so we clear our path data in prep
            # to generate a new one from original source data.
            should_redraw = True
            should_ds = False

        # compute the length diffs between the first/last index entry in
        # the input data and the last indexes we have on record from the
        # last time we updated the curve index.
        prepend_length = int(istart - x[0])
        append_length = int(x[-1] - istop)

        # no_path_yet = self.path is None
        if (
            self.path is None
            or should_redraw
            or should_ds
            or prepend_length > 0
        ):
            if (
                not view_range
                or self._in_ds
            ):
                # by default we only pull data up to the last (current) index
                x_out, y_out = x[:-1], y[:-1]

                # step mode: draw flat top discrete "step"
                # over the index space for each datum.
                if self._step_mode:
                    x_out, y_out = step_path_arrays_from_1d(
                        x_out,
                        y_out,
                    )
                    # TODO: numba this bish
                    profiler('generated step arrays')

            if should_redraw:
                profiler('path reversion to non-ds')
                if self.path:
                    self.path.clear()

                if self.fast_path:
                    self.fast_path.clear()

            if should_redraw and not should_ds:
                if self._in_ds:
                    log.info(f'DEDOWN -> {self._name}')

                self._in_ds = False

            elif should_ds and px_width:
                x_out, y_out = self.downsample(
                    x_out,
                    y_out,
                    px_width,
                    uppx,
                )
                profiler(f'FULL PATH downsample redraw={should_ds}')
                self._in_ds = True

            self.path = pg.functions.arrayToQPath(
                x_out,
                y_out,
                connect='all',
                finiteCheck=False,
                path=self.path,
            )
            profiler('generated fresh path')
            # profiler(f'DRAW PATH IN VIEW -> {self._name}')

            # reserve mem allocs see:
            # - https://doc.qt.io/qt-5/qpainterpath.html#reserve
            # - https://doc.qt.io/qt-5/qpainterpath.html#capacity
            # - https://doc.qt.io/qt-5/qpainterpath.html#clear
            # XXX: right now this is based on had hoc checks on a
            # hidpi 3840x2160 4k monitor but we should optimize for
            # the target display(s) on the sys.
            # if no_path_yet:
            #     self.path.reserve(int(500e3))

        # TODO: get this piecewise prepend working - right now it's
        # giving heck on vwap...
        # elif prepend_length:
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

        elif (
            append_length > 0
            and not view_range
        ):
            new_x = x[-append_length - 2:-1]
            new_y = y[-append_length - 2:-1]
            profiler('sliced append path')

            if self._step_mode:
                new_x, new_y = step_path_arrays_from_1d(
                    new_x,
                    new_y,
                )
                # [1:] since we don't need the vertical line normally at
                # the beginning of the step curve taking the first (x,
                # y) poing down to the x-axis **because** this is an
                # appended path graphic.
                new_x = new_x[1:]
                new_y = new_y[1:]

                profiler('generated step data')

            else:
                profiler(
                    f'diffed array input, append_length={append_length}'
                )

            if should_ds:
                new_x, new_y = self.downsample(
                    new_x,
                    new_y,
                    **should_ds,
                )
                profiler(f'fast path downsample redraw={should_ds}')

            append_path = pg.functions.arrayToQPath(
                new_x,
                new_y,
                connect='all',
                finiteCheck=False,
                path=self.fast_path,
            )
            profiler(f'generated append qpath')

            if self.use_fpath:
                # an attempt at trying to make append-updates faster..
                if self.fast_path is None:
                    self.fast_path = append_path
                    self.fast_path.reserve(int(6e3))
                else:
                    self.fast_path.connectPath(append_path)
                    size = self.fast_path.capacity()
                    profiler(f'connected fast path w size: {size}')

                    # print(f"append_path br: {append_path.boundingRect()}")
                    # self.path.moveTo(new_x[0], new_y[0])
                    # path.connectPath(append_path)

                    # XXX: lol this causes a hang..
                    # self.path = self.path.simplified()
            else:
                size = self.path.capacity()
                profiler(f'connected history path w size: {size}')
                self.path.connectPath(append_path)

            # other merging ideas:
            # https://stackoverflow.com/questions/8936225/how-to-merge-qpainterpaths
            # path.addPath(append_path)
            # path.closeSubpath()

            # TODO: try out new work from `pyqtgraph` main which
            # should repair horrid perf:
            # https://github.com/pyqtgraph/pyqtgraph/pull/2032
            # ok, nope still horrible XD
            # if self._fill:
            #     # XXX: super slow set "union" op
            #     self.path = self.path.united(append_path).simplified()

            # self.disable_cache()
            # flip_cache = True

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
            # print(
            #     f"path br: {self.path.boundingRect()}",
            #     f"fast path br: {self.fast_path.boundingRect()}",
            #     f"last rect br: {self._last_step_rect}",
            # )
        else:
            self._last_line = QLineF(
                x[-2], y[-2],
                x[-1], y_last
            )

        profiler('draw last segment')

        # trigger redraw of path
        # do update before reverting to cache mode
        # self.prepareGeometryChange()
        self.update()
        profiler('.update()')

        # if flip_cache:
        #     # XXX: seems to be needed to avoid artifacts (see above).
        #     self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

    # XXX: lol brutal, the internals of `CurvePoint` (inherited by
    # our `LineDot`) required ``.getData()`` to work..
    def getData(self):
        return self._x, self._y

    # TODO: drop the above after ``Cursor`` re-work
    def get_arrays(self) -> tuple[np.ndarray, np.ndarray]:
        return self._x, self._y

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

        # self.disable_cache()
        # self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

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

        fp = self.fast_path
        if fp:
            fhb = fp.controlPointRect()
            hb_size = fhb.size() + hb_size
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
            msg=f'FastAppendCurve.paint(): `{self._name}`',
            disabled=not pg_profile_enabled(),
            gt=ms_slower_then,
        )

        if (
            self._step_mode
            and self._last_step_rect
        ):
            brush = self._brush

            # p.drawLines(*tuple(filter(bool, self._last_step_lines)))
            # p.drawRect(self._last_step_rect)
            p.fillRect(self._last_step_rect, brush)
            profiler('.fillRect()')

        if self._last_line:
            p.setPen(self.last_step_pen)
            p.drawLine(self._last_line)
            profiler('.drawLine()')
            p.setPen(self._pen)

        path = self.path

        if path:
            p.drawPath(path)
            profiler('.drawPath(path)')

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
