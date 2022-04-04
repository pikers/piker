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
Super fast OHLC sampling graphics types.

"""
from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
)

import numpy as np
import pyqtgraph as pg
from numba import njit, float64, int64  # , optional
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QLineF, QPointF
# from numba import types as ntypes
# from ..data._source import numba_ohlc_dtype

from .._profile import pg_profile_enabled, ms_slower_then
from ._style import hcolor
from ..log import get_logger
from ._curve import FastAppendCurve
from ._compression import ohlc_flatten

if TYPE_CHECKING:
    from ._chart import LinkedSplits


log = get_logger(__name__)


def bar_from_ohlc_row(
    row: np.ndarray,
    w: float

) -> tuple[QLineF]:
    '''
    Generate the minimal ``QLineF`` lines to construct a single
    OHLC "bar" for use in the "last datum" of a series.

    '''
    open, high, low, close, index = row[
        ['open', 'high', 'low', 'close', 'index']]

    # TODO: maybe consider using `QGraphicsLineItem` ??
    # gives us a ``.boundingRect()`` on the objects which may make
    # computing the composite bounding rect of the last bars + the
    # history path faster since it's done in C++:
    # https://doc.qt.io/qt-5/qgraphicslineitem.html

    # high -> low vertical (body) line
    if low != high:
        hl = QLineF(index, low, index, high)
    else:
        # XXX: if we don't do it renders a weird rectangle?
        # see below for filtering this later...
        hl = None

    # NOTE: place the x-coord start as "middle" of the drawing range such
    # that the open arm line-graphic is at the left-most-side of
    # the index's range according to the view mapping coordinates.

    # open line
    o = QLineF(index - w, open, index, open)

    # close line
    c = QLineF(index, close, index + w, close)

    return [hl, o, c]


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


def gen_qpath(
    data: np.ndarray,
    start: int,  # XXX: do we need this?
    w: float,
    path: Optional[QtGui.QPainterPath] = None,

) -> QtGui.QPainterPath:

    path_was_none = path is None

    profiler = pg.debug.Profiler(
        msg='gen_qpath ohlc',
        disabled=not pg_profile_enabled(),
        gt=ms_slower_then,
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


class BarItems(pg.GraphicsObject):
    '''
    "Price range" bars graphics rendered from a OHLC sampled sequence.

    '''
    sigPlotChanged = QtCore.pyqtSignal(object)

    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.43

    def __init__(
        self,
        linked: LinkedSplits,
        plotitem: 'pg.PlotItem',  # noqa
        pen_color: str = 'bracket',
        last_bar_color: str = 'bracket',

        name: Optional[str] = None,

    ) -> None:
        super().__init__()
        self.linked = linked
        # XXX: for the mega-lulz increasing width here increases draw
        # latency...  so probably don't do it until we figure that out.
        self._color = pen_color
        self.bars_pen = pg.mkPen(hcolor(pen_color), width=1)
        self.last_bar_pen = pg.mkPen(hcolor(last_bar_color), width=2)
        self._name = name

        self._ds_line_xy: Optional[
            tuple[np.ndarray, np.ndarray]
        ] = None

        # NOTE: this prevents redraws on mouse interaction which is
        # a huge boon for avg interaction latency.

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        self._pi = plotitem
        self.path = QtGui.QPainterPath()
        self.fast_path = QtGui.QPainterPath()

        self._xrange: tuple[int, int]
        self._yrange: tuple[float, float]
        self._vrange = None

        # TODO: don't render the full backing array each time
        # self._path_data = None
        self._last_bar_lines: Optional[tuple[QLineF, ...]] = None

        # track the current length of drawable lines within the larger array
        self.start_index: int = 0
        self.stop_index: int = 0

        # downsampler-line state
        self._in_ds: bool = False
        self._ds_line: Optional[FastAppendCurve] = None
        self._dsi: tuple[int, int] = 0, 0
        self._xs_in_px: float = 0

    def draw_from_data(
        self,
        ohlc: np.ndarray,
        start: int = 0,

    ) -> QtGui.QPainterPath:
        '''
        Draw OHLC datum graphics from a ``np.ndarray``.

        This routine is usually only called to draw the initial history.

        '''
        hist, last = ohlc[:-1], ohlc[-1]
        self.path = gen_qpath(hist, start, self.w)

        # save graphics for later reference and keep track
        # of current internal "last index"
        # self.start_index = len(ohlc)
        index = ohlc['index']
        self._xrange = (index[0], index[-1])
        self._yrange = (
            np.nanmax(ohlc['high']),
            np.nanmin(ohlc['low']),
        )

        # up to last to avoid double draw of last bar
        self._last_bar_lines = bar_from_ohlc_row(last, self.w)

        x, y = self._ds_line_xy = ohlc_flatten(ohlc)

        # TODO: figuring out the most optimial size for the ideal
        # curve-path by,
        # - calcing the display's max px width `.screen()`
        # - drawing a curve and figuring out it's capacity:
        #   https://doc.qt.io/qt-5/qpainterpath.html#capacity
        # - reserving that cap for each curve-mapped-to-shm with

        # - leveraging clearing when needed to redraw the entire
        #   curve that does not release mem allocs:
        #   https://doc.qt.io/qt-5/qpainterpath.html#clear
        curve = FastAppendCurve(
            y=y,
            x=x,
            name='OHLC',
            color=self._color,
        )
        curve.hide()
        self._pi.addItem(curve)
        self._ds_line = curve

        self._ds_xrange = (index[0], index[-1])

        # trigger render
        # https://doc.qt.io/qt-5/qgraphicsitem.html#update
        self.update()

        return self.path

    def update_from_array(
        self,

        # full array input history
        ohlc: np.ndarray,

        # pre-sliced array data that's "in view"
        ohlc_iv: np.ndarray,

        view_range: Optional[tuple[int, int]] = None,

    ) -> None:
        '''
        Update the last datum's bar graphic from input data array.

        This routine should be interface compatible with
        ``pg.PlotCurveItem.setData()``. Normally this method in
        ``pyqtgraph`` seems to update all the data passed to the
        graphics object, and then update/rerender, but here we're
        assuming the prior graphics havent changed (OHLC history rarely
        does) so this "should" be simpler and faster.

        This routine should be made (transitively) as fast as possible.

        '''
        profiler = pg.debug.Profiler(
            disabled=not pg_profile_enabled(),
            gt=ms_slower_then,
        )

        # index = self.start_index
        istart, istop = self._xrange
        ds_istart, ds_istop = self._ds_xrange

        index = ohlc['index']
        first_index, last_index = index[0], index[-1]

        # length = len(ohlc)
        # prepend_length = istart - first_index
        # append_length = last_index - istop

        # ds_prepend_length = ds_istart - first_index
        # ds_append_length = last_index - ds_istop

        flip_cache = False

        x_gt = 2
        if self._ds_line:
            uppx = self._ds_line.x_uppx()
        else:
            uppx = 0

        should_line = self._in_ds
        if (
            self._in_ds
            and uppx < x_gt
        ):
            should_line = False

        elif (
            not self._in_ds
            and uppx >= x_gt
        ):
            should_line = True

        if (
            should_line
        ):
            # update the line graphic
            # x, y = self._ds_line_xy = ohlc_flatten(ohlc_iv)
            x, y = self._ds_line_xy = ohlc_flatten(ohlc)
            x_iv, y_iv = self._ds_line_xy = ohlc_flatten(ohlc_iv)
            profiler('flattening bars to line')

            # TODO: we should be diffing the amount of new data which
            # needs to be downsampled. Ideally we actually are just
            # doing all the ds-ing in sibling actors so that the data
            # can just be read and rendered to graphics on events of our
            # choice.
            # diff = do_diff(ohlc, new_bit)
            curve = self._ds_line
            curve.update_from_array(
                x=x,
                y=y,
                x_iv=x_iv,
                y_iv=y_iv,
                view_range=view_range,  # hack
            )
            profiler('udated ds line')

            if not self._in_ds:
                # hide bars and show line
                self.hide()
                # XXX: is this actually any faster?
                # self._pi.removeItem(self)

                # TODO: a `.ui()` log level?
                log.info(
                    f'downsampling to line graphic {self._name}'
                )

                # self._pi.addItem(curve)
                curve.show()
                curve.update()
                self._in_ds = True

            # stop here since we don't need to update bars path any more
            # as we delegate to the downsample line with updates.
            return

        elif (
            not should_line
            and self._in_ds
        ):
            # flip back to bars graphics and hide the downsample line.
            log.info(f'showing bars graphic {self._name}')

            curve = self._ds_line
            curve.hide()
            # self._pi.removeItem(curve)

            # XXX: is this actually any faster?
            # self._pi.addItem(self)
            self.show()
            self._in_ds = False

        # generate in_view path
        self.path = gen_qpath(
            ohlc_iv,
            0,
            self.w,
            # path=self.path,
        )

        # TODO: to make the downsampling faster
        # - allow mapping only a range of lines thus only drawing as
        #   many bars as exactly specified.
        # - move ohlc "flattening" to a shmarr
        # - maybe move all this embedded logic to a higher
        #   level type?

        # ohlc = in_view

        # if prepend_length:
        #     # new history was added and we need to render a new path
        #     prepend_bars = ohlc[:prepend_length]

        # if ds_prepend_length:
        #     ds_prepend_bars = ohlc[:ds_prepend_length]
        #     pre_x, pre_y = ohlc_flatten(ds_prepend_bars)
        #     fx = np.concatenate((pre_x, fx))
        #     fy = np.concatenate((pre_y, fy))
        #     profiler('ds line prepend diff complete')

        # if append_length:
        #     # generate new graphics to match provided array
        #     # path appending logic:
        #     # we need to get the previous "current bar(s)" for the time step
        #     # and convert it to a sub-path to append to the historical set
        #     # new_bars = ohlc[istop - 1:istop + append_length - 1]
        #     append_bars = ohlc[-append_length - 1:-1]
        #     # print(f'ohlc bars to append size: {append_bars.size}\n')

        # if ds_append_length:
        #     ds_append_bars = ohlc[-ds_append_length - 1:-1]
        #     post_x, post_y = ohlc_flatten(ds_append_bars)
        #     print(
        #         f'ds curve to append sizes: {(post_x.size, post_y.size)}'
        #     )
        #     fx = np.concatenate((fx, post_x))
        #     fy = np.concatenate((fy, post_y))

        #     profiler('ds line append diff complete')

        profiler('array diffs complete')

        # does this work?
        last = ohlc[-1]
        # fy[-1] = last['close']

        # # incremental update and cache line datums
        # self._ds_line_xy = fx, fy

        # maybe downsample to line
        # ds = self.maybe_downsample()
        # if ds:
        #     # if we downsample to a line don't bother with
        #     # any more path generation / updates
        #     self._ds_xrange = first_index, last_index
        #     profiler('downsampled to line')
        #     return

        # print(in_view.size)

        # if self.path:
        #     self.path = path
        #     self.path.reserve(path.capacity())
        #     self.path.swap(path)

        # path updates
        # if prepend_length:
        #     # XXX: SOMETHING IS MAYBE FISHY HERE what with the old_path
        #     # y value not matching the first value from
        #     # ohlc[prepend_length + 1] ???
        #     prepend_path = gen_qpath(prepend_bars, 0, self.w)
        #     old_path = self.path
        #     self.path = prepend_path
        #     self.path.addPath(old_path)
        #     profiler('path PREPEND')

        # if append_length:
        #     append_path = gen_qpath(append_bars, 0, self.w)

        #     self.path.moveTo(
        #         float(istop - self.w),
        #         float(append_bars[0]['open'])
        #     )
        #     self.path.addPath(append_path)

        #     profiler('path APPEND')
        #     fp = self.fast_path
        #     if fp is None:
        #         self.fast_path = append_path

        #     else:
        #         fp.moveTo(
        #             float(istop - self.w), float(new_bars[0]['open'])
        #         )
        #         fp.addPath(append_path)

        #     self.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
        #     flip_cache = True

        self._xrange = first_index, last_index

        # trigger redraw despite caching
        self.prepareGeometryChange()

        # generate new lines objects for updatable "current bar"
        self._last_bar_lines = bar_from_ohlc_row(last, self.w)

        # last bar update
        i, o, h, l, last, v = last[
            ['index', 'open', 'high', 'low', 'close', 'volume']
        ]
        # assert i == self.start_index - 1
        # assert i == last_index
        body, larm, rarm = self._last_bar_lines

        # XXX: is there a faster way to modify this?
        rarm.setLine(rarm.x1(), last, rarm.x2(), last)

        # writer is responsible for changing open on "first" volume of bar
        larm.setLine(larm.x1(), o, larm.x2(), o)

        if l != h:  # noqa

            if body is None:
                body = self._last_bar_lines[0] = QLineF(i, l, i, h)
            else:
                # update body
                body.setLine(i, l, i, h)

            # XXX: pretty sure this is causing an issue where the bar has
            # a large upward move right before the next sample and the body
            # is getting set to None since the next bar is flat but the shm
            # array index update wasn't read by the time this code runs. Iow
            # we're doing this removal of the body for a bar index that is
            # now out of date / from some previous sample. It's weird
            # though because i've seen it do this to bars i - 3 back?

        profiler('last bar set')

        self.update()
        profiler('.update()')

        if flip_cache:
            self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

    def boundingRect(self):
        # Qt docs: https://doc.qt.io/qt-5/qgraphicsitem.html#boundingRect

        # TODO: Can we do rect caching to make this faster
        # like `pg.PlotCurveItem` does? In theory it's just
        # computing max/min stuff again like we do in the udpate loop
        # anyway. Not really sure it's necessary since profiling already
        # shows this method is faf.

        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bounding rect for us).

        # apparently this a lot faster says the docs?
        # https://doc.qt.io/qt-5/qpainterpath.html#controlPointRect
        hb = self.path.controlPointRect()
        hb_tl, hb_br = (
            hb.topLeft(),
            hb.bottomRight(),
        )

        # fp = self.fast_path
        # if fp:
        #     fhb = fp.controlPointRect()
        #     print((hb_tl, hb_br))
        #     print(fhb)
        #     hb_tl, hb_br = (
        #         fhb.topLeft() + hb.topLeft(),
        #         fhb.bottomRight() + hb.bottomRight(),
        #     )

        # need to include last bar height or BR will be off
        mx_y = hb_br.y()
        mn_y = hb_tl.y()

        last_lines = self._last_bar_lines
        if last_lines:
            body_line = self._last_bar_lines[0]
            if body_line:
                mx_y = max(mx_y, max(body_line.y1(), body_line.y2()))
                mn_y = min(mn_y, min(body_line.y1(), body_line.y2()))

        return QtCore.QRectF(

            # top left
            QPointF(
                hb_tl.x(),
                mn_y,
            ),

            # bottom right
            QPointF(
                hb_br.x() + 1,
                mx_y,
            )

        )

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget

    ) -> None:

        if self._in_ds:
            return

        profiler = pg.debug.Profiler(
            disabled=not pg_profile_enabled(),
            gt=ms_slower_then,
        )

        # p.setCompositionMode(0)

        # TODO: one thing we could try here is pictures being drawn of
        # a fixed count of bars such that based on the viewbox indices we
        # only draw the "rounded up" number of "pictures worth" of bars
        # as is necesarry for what's in "view". Not sure if this will
        # lead to any perf gains other then when zoomed in to less bars
        # in view.
        p.setPen(self.last_bar_pen)
        p.drawLines(*tuple(filter(bool, self._last_bar_lines)))
        profiler('draw last bar')

        p.setPen(self.bars_pen)
        p.drawPath(self.path)
        profiler(f'draw history path: {self.path.capacity()}')

        # if self.fast_path:
        #     p.drawPath(self.fast_path)
        #     profiler('draw fast path')
