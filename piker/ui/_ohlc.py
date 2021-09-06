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
from typing import List, Optional, Tuple

import numpy as np
import pyqtgraph as pg
from numba import njit, float64, int64  # , optional
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QLineF, QPointF
# from numba import types as ntypes
# from ..data._source import numba_ohlc_dtype

from .._profile import pg_profile_enabled
from ._style import hcolor


def _mk_lines_array(
    data: List,
    size: int,
    elements_step: int = 6,
) -> np.ndarray:
    """Create an ndarray to hold lines graphics info.

    """
    return np.zeros_like(
        data,
        shape=(int(size), elements_step),
        dtype=object,
    )


def lines_from_ohlc(
    row: np.ndarray,
    w: float
) -> Tuple[QLineF]:

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
    # ntypes.Tuple((float64[:], float64[:], float64[:]))(
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
    """Generate an array of lines objects from input ohlc data.

    """
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
        c[istart:istop] = (0, 1, 1, 1, 1, 1)

    return x, y, c


def gen_qpath(
    data,
    start,  # XXX: do we need this?
    w,
) -> QtGui.QPainterPath:

    profiler = pg.debug.Profiler(disabled=not pg_profile_enabled())

    x, y, c = path_arrays_from_ohlc(data, start, bar_gap=w)
    profiler("generate stream with numba")

    # TODO: numba the internals of this!
    path = pg.functions.arrayToQPath(x, y, connect=c)
    profiler("generate path with arrayToQPath")

    return path


class BarItems(pg.GraphicsObject):
    """Price range bars graphics rendered from a OHLC sequence.
    """
    sigPlotChanged = QtCore.pyqtSignal(object)

    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.43

    def __init__(
        self,
        # scene: 'QGraphicsScene',  # noqa
        plotitem: 'pg.PlotItem',  # noqa
        pen_color: str = 'bracket',
    ) -> None:
        super().__init__()

        # XXX: for the mega-lulz increasing width here increases draw latency...
        # so probably don't do it until we figure that out.
        self.bars_pen = pg.mkPen(hcolor(pen_color), width=1)

        # NOTE: this prevents redraws on mouse interaction which is
        # a huge boon for avg interaction latency.

        # TODO: one question still remaining is if this makes trasform
        # interactions slower (such as zooming) and if so maybe if/when
        # we implement a "history" mode for the view we disable this in
        # that mode?
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # not sure if this is actually impoving anything but figured it
        # was worth a shot:
        # self.path.reserve(int(100e3 * 6))

        self.path = QtGui.QPainterPath()

        self._pi = plotitem

        self._xrange: Tuple[int, int]
        self._yrange: Tuple[float, float]

        # TODO: don't render the full backing array each time
        # self._path_data = None
        self._last_bar_lines: Optional[Tuple[QLineF, ...]] = None

        # track the current length of drawable lines within the larger array
        self.start_index: int = 0
        self.stop_index: int = 0

    def draw_from_data(
        self,
        data: np.ndarray,
        start: int = 0,
    ) -> QtGui.QPainterPath:
        """Draw OHLC datum graphics from a ``np.ndarray``.

        This routine is usually only called to draw the initial history.
        """
        hist, last = data[:-1], data[-1]

        self.path = gen_qpath(hist, start, self.w)

        # save graphics for later reference and keep track
        # of current internal "last index"
        # self.start_index = len(data)
        index = data['index']
        self._xrange = (index[0], index[-1])
        self._yrange = (
            np.nanmax(data['high']),
            np.nanmin(data['low']),
        )

        # up to last to avoid double draw of last bar
        self._last_bar_lines = lines_from_ohlc(last, self.w)

        # trigger render
        # https://doc.qt.io/qt-5/qgraphicsitem.html#update
        self.update()

        return self.path

    def update_from_array(
        self,
        array: np.ndarray,
        just_history=False,
    ) -> None:
        """Update the last datum's bar graphic from input data array.

        This routine should be interface compatible with
        ``pg.PlotCurveItem.setData()``. Normally this method in
        ``pyqtgraph`` seems to update all the data passed to the
        graphics object, and then update/rerender, but here we're
        assuming the prior graphics havent changed (OHLC history rarely
        does) so this "should" be simpler and faster.

        This routine should be made (transitively) as fast as possible.
        """
        # index = self.start_index
        istart, istop = self._xrange

        index = array['index']
        first_index, last_index = index[0], index[-1]

        # length = len(array)
        prepend_length = istart - first_index
        append_length = last_index - istop

        flip_cache = False

        # TODO: allow mapping only a range of lines thus
        # only drawing as many bars as exactly specified.

        if prepend_length:

            # new history was added and we need to render a new path
            new_bars = array[:prepend_length]
            prepend_path = gen_qpath(new_bars, 0, self.w)

            # XXX: SOMETHING IS MAYBE FISHY HERE what with the old_path
            # y value not matching the first value from
            # array[prepend_length + 1] ???

            # update path
            old_path = self.path
            self.path = prepend_path
            self.path.addPath(old_path)

            # trigger redraw despite caching
            self.prepareGeometryChange()

        if append_length:
            # generate new lines objects for updatable "current bar"
            self._last_bar_lines = lines_from_ohlc(array[-1], self.w)

            # generate new graphics to match provided array
            # path appending logic:
            # we need to get the previous "current bar(s)" for the time step
            # and convert it to a sub-path to append to the historical set
            # new_bars = array[istop - 1:istop + append_length - 1]
            new_bars = array[-append_length - 1:-1]
            append_path = gen_qpath(new_bars, 0, self.w)
            self.path.moveTo(float(istop - self.w), float(new_bars[0]['open']))
            self.path.addPath(append_path)

            # trigger redraw despite caching
            self.prepareGeometryChange()
            self.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
            flip_cache = True

        self._xrange = first_index, last_index

        # last bar update
        i, o, h, l, last, v = array[-1][
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

        self.update()

        if flip_cache:
            self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget
    ) -> None:

        profiler = pg.debug.Profiler(disabled=not pg_profile_enabled())

        # p.setCompositionMode(0)
        p.setPen(self.bars_pen)

        # TODO: one thing we could try here is pictures being drawn of
        # a fixed count of bars such that based on the viewbox indices we
        # only draw the "rounded up" number of "pictures worth" of bars
        # as is necesarry for what's in "view". Not sure if this will
        # lead to any perf gains other then when zoomed in to less bars
        # in view.
        p.drawLines(*tuple(filter(bool, self._last_bar_lines)))
        profiler('draw last bar')

        p.drawPath(self.path)
        profiler('draw history path')

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
        hb_tl, hb_br = hb.topLeft(), hb.bottomRight()

        # need to include last bar height or BR will be off
        mx_y = hb_br.y()
        mn_y = hb_tl.y()

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
