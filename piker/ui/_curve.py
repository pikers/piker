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
# def step_path_arrays_from_1d(
#     x: np.ndarray,
#     y: np.ndarray,
#     include_endpoints: bool = True,

# ) -> (np.ndarray, np.ndarray):
#     '''
#     Generate a "step mode" curve aligned with OHLC style bars
#     such that each segment spans each bar (aka "centered" style).

#     '''
#     # y_out = y.copy()
#     # x_out = x.copy()

#     # x2 = np.empty(
#     #     # the data + 2 endpoints on either end for
#     #     # "termination of the path".
#     #     (len(x) + 1, 2),
#     #     # we want to align with OHLC or other sampling style
#     #     # bars likely so we need fractinal values
#     #     dtype=float,
#     # )

#     x2 = np.broadcast_to(
#         x[:, None],
#         (
#             x.size + 1,
#             # 4,  # only ohlc
#             2,
#         ),
#     ) + np.array([-0.5, 0.5])

#     # x2[0] = x[0] - 0.5
#     # x2[1] = x[0] + 0.5
#     # x2[0, 0] = x[0] - 0.5
#     # x2[0, 1] = x[0] + 0.5
#     # x2[1:] = x[:, np.newaxis] + 0.5
#     # import pdbpp
#     # pdbpp.set_trace()

#     # flatten to 1-d
#     # x_out = x2.reshape(x2.size)
#     # x_out = x2

#     # we create a 1d with 2 extra indexes to
#     # hold the start and (current) end value for the steps
#     # on either end
#     y2 = np.empty(
#         (len(y) + 1, 2),
#         dtype=y.dtype,
#     )
#     y2[:] = y[:, np.newaxis]
#     # y2[-1] = 0

#     # y_out = y2

# #     y_out = np.empty(
# #         2*len(y) + 2,
# #         dtype=y.dtype
# #     )

#     # flatten and set 0 endpoints
#     # y_out[1:-1] = y2.reshape(y2.size)
#     # y_out[0] = 0
#     # y_out[-1] = 0

#     if not include_endpoints:
#         return x2[:-1], y2[:-1]

#     else:
#         return x2, y2


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
        self._vr: Optional[tuple] = None
        self._avr: Optional[tuple] = None
        self._br = None

        self._name = name
        self.path: Optional[QtGui.QPainterPath] = None

        self.use_fpath = use_fpath
        self.fast_path: Optional[QtGui.QPainterPath] = None

        # TODO: we can probably just dispense with the parent since
        # we're basically only using the pen setting now...
        super().__init__(*args, **kwargs)

        # self._xrange: tuple[int, int] = self.dataBounds(ax=0)
        self._xrange: Optional[tuple[int, int]] = None
        # self._x_iv_range = None

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
        # if step_mode:
        # don't enable caching by default for the case where the
        # only thing drawn is the "last" line segment which can
        # have a weird artifact where it won't be fully drawn to its
        # endpoint (something we saw on trade rate curves)
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

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
            # log_scale=bool(uppx)
        )
        x = np.broadcast_to(x[:, None], y.shape)
        # x = (x + np.array([-0.43, 0, 0, 0.43])).flatten()
        x = (x + np.array([-0.5, 0, 0, 0.5])).flatten()
        y = y.flatten()

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
        draw_last: bool = True,
        slice_to_head: int = -1,
        do_append: bool = True,
        should_redraw: bool = False,

    ) -> QtGui.QPainterPath:
        '''
        Update curve from input 2-d data.

        Compare with a cached "x-range" state and (pre/a)ppend based on
        a length diff.

        '''
        profiler = profiler or pg.debug.Profiler(
            msg=f'FastAppendCurve.update_from_array(): `{self._name}`',
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,
        )
        flip_cache = False

        if self._xrange:
            istart, istop = self._xrange
        else:
            self._xrange = istart, istop = x[0], x[-1]

        # compute the length diffs between the first/last index entry in
        # the input data and the last indexes we have on record from the
        # last time we updated the curve index.
        prepend_length = int(istart - x[0])
        append_length = int(x[-1] - istop)

        # this is the diff-mode, "data"-rendered index
        # tracking var..
        self._xrange = x[0], x[-1]

        # print(f"xrange: {self._xrange}")

        # XXX: lol brutal, the internals of `CurvePoint` (inherited by
        # our `LineDot`) required ``.getData()`` to work..
        # self.xData = x
        # self.yData = y
        # self._x, self._y = x, y

        # downsampling incremental state checking
        uppx = self.x_uppx()
        px_width = self.px_width()
        uppx_diff = (uppx - self._last_uppx)

        new_sample_rate = False
        should_ds = self._in_ds
        showing_src_data = self._in_ds
        # should_redraw = False

        # by default we only pull data up to the last (current) index
        x_out_full = x_out = x[:slice_to_head]
        y_out_full = y_out = y[:slice_to_head]

        # if a view range is passed, plan to draw the
        # source ouput that's "in view" of the chart.
        if (
            view_range
            # and not self._in_ds
            # and not prepend_length > 0
        ):
            # print(f'{self._name} vr: {view_range}')

            # by default we only pull data up to the last (current) index
            x_out, y_out = x_iv[:slice_to_head], y_iv[:slice_to_head]
            profiler(f'view range slice {view_range}')

            vl, vr = view_range

            # last_ivr = self._x_iv_range
           # ix_iv, iy_iv = self._x_iv_range = (x_iv[0], x_iv[-1])

            zoom_or_append = False
            last_vr = self._vr
            last_ivr = self._avr

            if last_vr:
                # relative slice indices
                lvl, lvr = last_vr
                # abs slice indices
                al, ar = last_ivr

                # append_length = int(x[-1] - istop)
                # append_length = int(x_iv[-1] - ar)

                # left_change = abs(x_iv[0] - al) >= 1
                # right_change = abs(x_iv[-1] - ar) >= 1

                if (
                    # likely a zoom view change
                    (vr - lvr) > 2 or vl < lvl
                    # append / prepend update
                    # we had an append update where the view range
                    # didn't change but the data-viewed (shifted)
                    # underneath, so we need to redraw.
                    # or left_change and right_change and last_vr == view_range

                        # not (left_change and right_change) and ivr
                    # (
                    # or abs(x_iv[ivr] - livr) > 1
                ):
                    zoom_or_append = True

            # if last_ivr:
            #     liivl, liivr = last_ivr

            if (
                view_range != last_vr
                and (
                    append_length > 1
                    or zoom_or_append
                )
            ):
                should_redraw = True
                # print("REDRAWING BRUH")

            self._vr = view_range
            self._avr = x_iv[0], x_iv[slice_to_head]

            # x_last = x_iv[-1]
            # y_last = y_iv[-1]
            # self._last_vr = view_range

            # self.disable_cache()
            # flip_cache = True

        if prepend_length > 0:
            should_redraw = True

        # check for downsampling conditions
        if (
            # std m4 downsample conditions
            px_width
            and abs(uppx_diff) >= 1
        ):
            log.info(
                f'{self._name} sampler change: {self._last_uppx} -> {uppx}'
            )
            self._last_uppx = uppx
            new_sample_rate = True
            showing_src_data = False
            should_redraw = True
            should_ds = True

        elif (
            uppx <= 2
            and self._in_ds
        ):
            # we should de-downsample back to our original
            # source data so we clear our path data in prep
            # to generate a new one from original source data.
            should_redraw = True
            new_sample_rate = True
            should_ds = False
            showing_src_data = True

        # no_path_yet = self.path is None
        if (
            self.path is None
            or should_redraw
            or new_sample_rate
            or prepend_length > 0
        ):
            if should_redraw:
                if self.path:
                    self.path.clear()
                    profiler('cleared paths due to `should_redraw=True`')

                if self.fast_path:
                    self.fast_path.clear()

                profiler('cleared paths due to `should_redraw` set')

            if new_sample_rate and showing_src_data:
                # if self._in_ds:
                log.info(f'DEDOWN -> {self._name}')

                self._in_ds = False

            elif should_ds and uppx and px_width > 1:
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
            self.prepareGeometryChange()
            profiler(
                f'generated fresh path. (should_redraw: {should_redraw} should_ds: {should_ds} new_sample_rate: {new_sample_rate})'
            )
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
            and do_append
            and not should_redraw
            # and not view_range
        ):
            print(f'{self._name} append len: {append_length}')
            new_x = x[-append_length - 2:slice_to_head]
            new_y = y[-append_length - 2:slice_to_head]
            profiler('sliced append path')

            profiler(
                f'diffed array input, append_length={append_length}'
            )

            # if should_ds:
            #     new_x, new_y = self.downsample(
            #         new_x,
            #         new_y,
            #         px_width,
            #         uppx,
            #     )
            #     profiler(f'fast path downsample redraw={should_ds}')

            append_path = pg.functions.arrayToQPath(
                new_x,
                new_y,
                connect='all',
                finiteCheck=False,
                path=self.fast_path,
            )
            profiler('generated append qpath')

            if self.use_fpath:
                # an attempt at trying to make append-updates faster..
                if self.fast_path is None:
                    self.fast_path = append_path
                    # self.fast_path.reserve(int(6e3))
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

        if draw_last:
            self.draw_last(x, y)
            profiler('draw last segment')


        # if flip_cache:
        # #     # XXX: seems to be needed to avoid artifacts (see above).
        #     self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

        # trigger redraw of path
        # do update before reverting to cache mode
        self.update()
        profiler('.update()')

    def draw_last(
        self,
        x: np.ndarray,
        y: np.ndarray,

    ) -> None:
        x_last = x[-1]
        y_last = y[-1]

        # draw the "current" step graphic segment so it lines up with
        # the "middle" of the current (OHLC) sample.
        if self._step_mode:
            self._last_line = QLineF(
                x_last - 0.5, 0,
                x_last + 0.5, 0,
                # x_last, 0,
                # x_last, 0,
            )
            self._last_step_rect = QRectF(
                x_last - 0.5, 0,
                x_last + 0.5, y_last
                # x_last, 0,
                # x_last, y_last
            )
            # print(
            #     f"path br: {self.path.boundingRect()}",
            #     f"fast path br: {self.fast_path.boundingRect()}",
            #     f"last rect br: {self._last_step_rect}",
            # )
        else:
            self._last_line = QLineF(
                x[-2], y[-2],
                x_last, y_last
            )

        self.update()

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

    def reset_cache(self) -> None:
        self.disable_cache()
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

    def disable_cache(self) -> None:
        '''
        Disable the use of the pixel coordinate cache and trigger a geo event.

        '''
        # XXX: pretty annoying but, without this there's little
        # artefacts on the append updates to the curve...
        self.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
        # self.prepareGeometryChange()

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
        # hb = self.path.boundingRect()
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

        # if self._last_step_rect:
        #     br = self._last_step_rect.bottomRight()

        # else:
        # hb_size += QSizeF(1, 1)
        w = hb_size.width() + 1
        h = hb_size.height() + 1

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
        self._br = br
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
            ms_threshold=ms_slower_then,
        )
        self.prepareGeometryChange()

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
