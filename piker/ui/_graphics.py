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
Chart graphics for displaying a slew of different data types.
"""

from typing import List, Optional, Tuple

import numpy as np
import pyqtgraph as pg
from numba import jit, float64, int64  # , optional
# from numba import types as ntypes
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QLineF, QPointF

from .._profile import timeit
# from ..data._source import numba_ohlc_dtype
from ._style import (
    _xaxis_at,
    hcolor,
    _font,
)
from ._axes import YAxisLabel, XAxisLabel, YSticky


# XXX: these settings seem to result in really decent mouse scroll
# latency (in terms of perceived lag in cross hair) so really be sure
# there's an improvement if you want to change it.
_mouse_rate_limit = 60  # calc current screen refresh rate?
_debounce_delay = 1 / 2e3
_ch_label_opac = 1


# TODO: we need to handle the case where index is outside
# the underlying datums range
class LineDot(pg.CurvePoint):

    def __init__(
        self,
        curve: pg.PlotCurveItem,
        index: int,
        pos=None,
        size: int = 2,  # in pxs
        color: str = 'default_light',
    ) -> None:
        pg.CurvePoint.__init__(
            self,
            curve,
            index=index,
            pos=pos,
            rotate=False,
        )

        # TODO: get pen from curve if not defined?
        cdefault = hcolor(color)
        pen = pg.mkPen(cdefault)
        brush = pg.mkBrush(cdefault)

        # presuming this is fast since it's built in?
        dot = self.dot = QtGui.QGraphicsEllipseItem(
            QtCore.QRectF(-size / 2, -size / 2, size, size)
        )
        # if we needed transformable dot?
        # dot.translate(-size*0.5, -size*0.5)
        dot.setPen(pen)
        dot.setBrush(brush)
        dot.setParentItem(self)

        # keep a static size
        self.setFlag(self.ItemIgnoresTransformations)


_corner_anchors = {
    'top': 0,
    'left': 0,
    'bottom': 1,
    'right': 1,
}
# XXX: fyi naming here is confusing / opposite to coords
_corner_margins = {
    ('top', 'left'): (-4, -5),
    ('top', 'right'): (4, -5),
    ('bottom', 'left'): (-4, 5),
    ('bottom', 'right'): (4, 5),
}


class ContentsLabel(pg.LabelItem):
    """Label anchored to a ``ViewBox`` typically for displaying
    datum-wise points from the "viewed" contents.

    """
    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa
        anchor_at: str = ('top', 'right'),
        justify_text: str = 'left',
        font_size: Optional[int] = None,
    ) -> None:
        font_size = font_size or _font.font.pixelSize()
        super().__init__(justify=justify_text, size=f'{str(font_size)}px')

        # anchor to viewbox
        self.setParentItem(chart._vb)
        chart.scene().addItem(self)
        self.chart = chart

        v, h = anchor_at
        index = (_corner_anchors[h], _corner_anchors[v])
        margins = _corner_margins[(v, h)]

        self.anchor(itemPos=index, parentPos=index, offset=margins)

    def update_from_ohlc(
        self,
        name: str,
        index: int,
        array: np.ndarray,
    ) -> None:
        # this being "html" is the dumbest shit :eyeroll:
        self.setText(
            "<b>i</b>:{index}<br/>"
            "<b>O</b>:{}<br/>"
            "<b>H</b>:{}<br/>"
            "<b>L</b>:{}<br/>"
            "<b>C</b>:{}<br/>"
            "<b>V</b>:{}".format(
                # *self._array[index].item()[2:8],
                *array[index].item()[2:8],
                name=name,
                index=index,
            )
        )

    def update_from_value(
        self,
        name: str,
        index: int,
        array: np.ndarray,
    ) -> None:
        if index < len(array):
            data = array[index][name]
            self.setText(f"{name}: {data:.2f}")


class CrossHair(pg.GraphicsObject):

    def __init__(
        self,
        linkedsplitcharts: 'LinkedSplitCharts',  # noqa
        digits: int = 0
    ) -> None:
        super().__init__()
        # XXX: not sure why these are instance variables?
        # It's not like we can change them on the fly..?
        self.pen = pg.mkPen(
            color=hcolor('default'),
            style=QtCore.Qt.DashLine,
        )
        self.lines_pen = pg.mkPen(
            color='#a9a9a9',  # gray?
            style=QtCore.Qt.DashLine,
        )
        self.lsc = linkedsplitcharts
        self.graphics = {}
        self.plots = []
        self.active_plot = None
        self.digits = digits
        self._lastx = None

    def add_plot(
        self,
        plot: 'ChartPlotWidget',  # noqa
        digits: int = 0,
    ) -> None:
        # add ``pg.graphicsItems.InfiniteLine``s
        # vertical and horizonal lines and a y-axis label
        vl = plot.addLine(x=0, pen=self.lines_pen, movable=False)

        hl = plot.addLine(y=0, pen=self.lines_pen, movable=False)
        hl.hide()

        yl = YAxisLabel(
            parent=plot.getAxis('right'),
            digits=digits or self.digits,
            opacity=_ch_label_opac,
            bg_color='default',
        )
        yl.hide()  # on startup if mouse is off screen

        # TODO: checkout what ``.sigDelayed`` can be used for
        # (emitted once a sufficient delay occurs in mouse movement)
        px_moved = pg.SignalProxy(
            plot.scene().sigMouseMoved,
            rateLimit=_mouse_rate_limit,
            slot=self.mouseMoved,
            delay=_debounce_delay,
        )
        px_enter = pg.SignalProxy(
            plot.sig_mouse_enter,
            rateLimit=_mouse_rate_limit,
            slot=lambda: self.mouseAction('Enter', plot),
            delay=_debounce_delay,
        )
        px_leave = pg.SignalProxy(
            plot.sig_mouse_leave,
            rateLimit=_mouse_rate_limit,
            slot=lambda: self.mouseAction('Leave', plot),
            delay=_debounce_delay,
        )
        self.graphics[plot] = {
            'vl': vl,
            'hl': hl,
            'yl': yl,
            'px': (px_moved, px_enter, px_leave),
        }
        self.plots.append(plot)

        # Determine where to place x-axis label.
        # Place below the last plot by default, ow
        # keep x-axis right below main chart
        plot_index = -1 if _xaxis_at == 'bottom' else 0

        self.xaxis_label = XAxisLabel(
            parent=self.plots[plot_index].getAxis('bottom'),
            opacity=_ch_label_opac,
            bg_color='default',
        )
        # place label off-screen during startup
        self.xaxis_label.setPos(self.plots[0].mapFromView(QPointF(0, 0)))

    def add_curve_cursor(
        self,
        plot: 'ChartPlotWidget',  # noqa
        curve: 'PlotCurveItem',  # noqa
    ) -> LineDot:
        # if this plot contains curves add line dot "cursors" to denote
        # the current sample under the mouse
        cursor = LineDot(curve, index=len(plot._ohlc))
        plot.addItem(cursor)
        self.graphics[plot].setdefault('cursors', []).append(cursor)
        return cursor

    def mouseAction(self, action, plot):  # noqa
        if action == 'Enter':
            self.active_plot = plot

            # show horiz line and y-label
            self.graphics[plot]['hl'].show()
            self.graphics[plot]['yl'].show()

        else:  # Leave
            self.active_plot = None

            # hide horiz line and y-label
            self.graphics[plot]['hl'].hide()
            self.graphics[plot]['yl'].hide()

    def mouseMoved(
        self,
        evt: 'Tuple[QMouseEvent]',  # noqa
    ) -> None:  # noqa
        """Update horizonal and vertical lines when mouse moves inside
        either the main chart or any indicator subplot.
        """
        pos = evt[0]

        # find position inside active plot
        try:
            # map to view coordinate system
            mouse_point = self.active_plot.mapToView(pos)
        except AttributeError:
            # mouse was not on active plot
            return

        x, y = mouse_point.x(), mouse_point.y()
        plot = self.active_plot

        # update y-range items
        self.graphics[plot]['hl'].setY(y)

        self.graphics[self.active_plot]['yl'].update_label(
            abs_pos=pos, value=y
        )

        # Update x if cursor changed after discretization calc
        # (this saves draw cycles on small mouse moves)
        lastx = self._lastx
        ix = round(x)  # since bars are centered around index

        if ix != lastx:
            for plot, opts in self.graphics.items():

                # move the vertical line to the current "center of bar"
                opts['vl'].setX(ix)

                # update the chart's "contents" label
                plot.update_contents_labels(ix)

                # update all subscribed curve dots
                for cursor in opts.get('cursors', ()):
                    cursor.setIndex(ix)

            # update the label on the bottom of the crosshair
            self.xaxis_label.update_label(

                # XXX: requires:
                # https://github.com/pyqtgraph/pyqtgraph/pull/1418
                # otherwise gobbles tons of CPU..

                # map back to abs (label-local) coordinates
                abs_pos=plot.mapFromView(QPointF(ix, y)),
                value=x,
            )

        self._lastx = ix

    def boundingRect(self):
        try:
            return self.active_plot.boundingRect()
        except AttributeError:
            return self.plots[0].boundingRect()


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


def lines_from_ohlc(row: np.ndarray, w: float) -> Tuple[QLineF]:
    open, high, low, close, index = row[
        ['open', 'high', 'low', 'close', 'index']]

    # high -> low vertical (body) line
    if low != high:
        hl = QLineF(index, low, index, high)
    else:
        # XXX: if we don't do it renders a weird rectangle?
        # see below for filtering this later...
        hl = None

    # NOTE: place the x-coord start as "middle" of the drawing range such
    # that the open arm line-graphic is at the left-most-side of
    # the index's range according to the view mapping.

    # open line
    o = QLineF(index - w, open, index, open)
    # close line
    c = QLineF(index, close, index + w, close)

    return [hl, o, c]


# @timeit
@jit(
    # TODO: for now need to construct this manually for readonly arrays, see
    # https://github.com/numba/numba/issues/4511
    # ntypes.Tuple((float64[:], float64[:], float64[:]))(
    #     numba_ohlc_dtype[::1],  # contiguous
    #     int64,
    #     optional(float64),
    # ),
    nopython=True,
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


@timeit
def gen_qpath(
    data,
    start,
    w,
) -> QtGui.QPainterPath:

    x, y, c = path_arrays_from_ohlc(data, start, bar_gap=w)
    return pg.functions.arrayToQPath(x, y, connect=c)


class BarItems(pg.GraphicsObject):
    """Price range bars graphics rendered from a OHLC sequence.
    """
    sigPlotChanged = QtCore.Signal(object)

    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.43

    # XXX: for the mega-lulz increasing width here increases draw latency...
    # so probably don't do it until we figure that out.
    bars_pen = pg.mkPen(hcolor('bracket'))

    def __init__(
        self,
        # scene: 'QGraphicsScene',  # noqa
        plotitem: 'pg.PlotItem',  # noqa
    ) -> None:
        super().__init__()

        self.last_bar = QtGui.QPicture()
        self.history = QtGui.QPicture()

        self.path = QtGui.QPainterPath()
        self._h_path = QtGui.QGraphicsPathItem(self.path)

        self._pi = plotitem

        # XXX: not sure this actually needs to be an array other
        # then for the old tina mode calcs for up/down bars below?
        # lines container
        # self.lines = _mk_lines_array([], 50e3, 6)

        # TODO: don't render the full backing array each time
        # self._path_data = None
        self._last_bar_lines: Optional[Tuple[QLineF, ...]] = None

        # track the current length of drawable lines within the larger array
        self.index: int = 0

    # @timeit
    def draw_from_data(
        self,
        data: np.ndarray,
        start: int = 0,
    ):
        """Draw OHLC datum graphics from a ``np.ndarray``.

        This routine is usually only called to draw the initial history.
        """
        self.path = gen_qpath(data, start, self.w)

        # save graphics for later reference and keep track
        # of current internal "last index"
        self.index = len(data)

        # up to last to avoid double draw of last bar
        self._last_bar_lines = lines_from_ohlc(data[-1], self.w)

        # create pics
        self.draw_history()
        self.draw_last_bar()

        # trigger render
        # https://doc.qt.io/qt-5/qgraphicsitem.html#update
        self.update()

    def draw_last_bar(self) -> None:
        """Currently this draws lines to a cached ``QPicture`` which
        is supposed to speed things up on ``.paint()`` calls (which
        is a call to ``QPainter.drawPicture()`` but I'm not so sure.

        """
        p = QtGui.QPainter(self.last_bar)
        p.setPen(self.bars_pen)
        p.drawLines(*tuple(filter(bool, self._last_bar_lines)))
        p.end()

    @timeit
    def draw_history(self) -> None:
        # TODO: avoid having to use a ```QPicture` to calc the
        # ``.boundingRect()``, use ``QGraphicsPathItem`` instead?
        # https://doc.qt.io/qt-5/qgraphicspathitem.html
        # self._h_path.setPath(self.path)

        p = QtGui.QPainter(self.history)
        p.setPen(self.bars_pen)
        p.drawPath(self.path)
        p.end()

    @timeit
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
        index = self.index
        length = len(array)
        extra = length - index

        # TODO: allow mapping only a range of lines thus
        # only drawing as many bars as exactly specified.
        if extra > 0:

            # generate new lines objects for updatable "current bar"
            self._last_bar_lines = lines_from_ohlc(array[-1], self.w)
            self.draw_last_bar()

            # generate new graphics to match provided array
            # path appending logic:
            # we need to get the previous "current bar(s)" for the time step
            # and convert it to a sub-path to append to the historical set
            new_history_istart = length - 2
            to_history = array[new_history_istart:new_history_istart + extra]
            new_history_qpath = gen_qpath(to_history, 0, self.w)

            # move to position of placement for the next bar in history
            # and append new sub-path
            new_bars = array[index:index + extra]
            self.path.moveTo(float(index - self.w), float(new_bars[0]['open']))
            self.path.addPath(new_history_qpath)

            self.index += extra

            self.draw_history()

            if just_history:
                self.update()
                return

        # last bar update
        i, o, h, l, last, v = array[-1][
            ['index', 'open', 'high', 'low', 'close', 'volume']
        ]
        assert i == self.index - 1
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

        # else:
        #     # XXX: h == l -> remove any HL line to avoid render bug
        #     if body is not None:
        #         body = self.lines[index - 1][0] = None

        self.draw_last_bar()
        self.update()

    @timeit
    def paint(self, p, opt, widget):

        # profiler = pg.debug.Profiler(disabled=False, delayed=False)

        # TODO: use to avoid drawing artefacts?
        # self.prepareGeometryChange()

        # p.setCompositionMode(0)

        # TODO: one thing we could try here is pictures being drawn of
        # a fixed count of bars such that based on the viewbox indices we
        # only draw the "rounded up" number of "pictures worth" of bars
        # as is necesarry for what's in "view". Not sure if this will
        # lead to any perf gains other then when zoomed in to less bars
        # in view.
        # p.drawPicture(0, 0, self.history)
        p.drawPicture(0, 0, self.last_bar)

        p.setPen(self.bars_pen)

        # TODO: does it matter which we use?
        # p.drawPath(self._h_path.path())
        p.drawPath(self.path)

    # @timeit
    def boundingRect(self):
        # TODO: can we do rect caching to make this faster
        # like `pg.PlotCurveItem` does? In theory it's just
        # computing max/min stuff again like we do in the udpate loop
        # anyway.

        # Qt docs: https://doc.qt.io/qt-5/qgraphicsitem.html#boundingRect
        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bounding rect for us).

        # compute aggregate bounding rectangle
        lb = self.last_bar.boundingRect()
        hb = self.history.boundingRect()
        # hb = self._h_path.boundingRect()

        return QtCore.QRectF(
            # top left
            QtCore.QPointF(hb.topLeft()),
            # total size
            # QtCore.QSizeF(QtCore.QSizeF(lb.size()) + hb.size())
            QtCore.QSizeF(lb.size() + hb.size())
        )


# XXX: when we get back to enabling tina mode for xb
# class CandlestickItems(BarItems):

#     w2 = 0.7
#     line_pen = pg.mkPen('#000000')
#     bull_brush = pg.mkBrush('#00ff00')
#     bear_brush = pg.mkBrush('#ff0000')

#     def _generate(self, p):
#         rects = np.array(
#             [
#                 QtCore.QRectF(
#                   q.id - self.w,
#                   q.open,
#                   self.w2,
#                   q.close - q.open
#               )
#                 for q in Quotes
#             ]
#         )

#         p.setPen(self.line_pen)
#         p.drawLines(
#             [QtCore.QLineF(q.id, q.low, q.id, q.high)
#              for q in Quotes]
#         )

#         p.setBrush(self.bull_brush)
#         p.drawRects(*rects[Quotes.close > Quotes.open])

#         p.setBrush(self.bear_brush)
#         p.drawRects(*rects[Quotes.close < Quotes.open])


class LevelLabel(YSticky):

    line_pen = pg.mkPen(hcolor('bracket'))

    _w_margin = 4
    _h_margin = 3
    level: float = 0

    def __init__(
        self,
        chart,
        *args,
        orient_v: str = 'bottom',
        orient_h: str = 'left',
        **kwargs
    ) -> None:
        super().__init__(chart, *args, **kwargs)

        # orientation around axis options
        self._orient_v = orient_v
        self._orient_h = orient_h
        self._v_shift = {
            'top': 1.,
            'bottom': 0,
            'middle': 1 / 2.
        }[orient_v]

        self._h_shift = {
            'left': -1., 'right': 0
        }[orient_h]

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        level: float,  # data for text
        offset: int = 1  # if have margins, k?
    ) -> None:

        # write contents, type specific
        self.set_label_str(level)

        br = self.boundingRect()
        h, w = br.height(), br.width()

        # this triggers ``.pain()`` implicitly?
        self.setPos(QPointF(
            self._h_shift * w - offset,
            abs_pos.y() - (self._v_shift * h) - offset
        ))
        self.update()

        self.level = level

    def set_label_str(self, level: float):
        # this is read inside ``.paint()``
        # self.label_str = '{size} x {level:.{digits}f}'.format(
        self.label_str = '{level:.{digits}f}'.format(
            # size=self._size,
            digits=self.digits,
            level=level
        ).replace(',', ' ')

    def size_hint(self) -> Tuple[None, None]:
        return None, None

    def draw(
        self,
        p: QtGui.QPainter,
        rect: QtCore.QRectF
    ) -> None:
        p.setPen(self.line_pen)

        if self._orient_v == 'bottom':
            lp, rp = rect.topLeft(), rect.topRight()
            # p.drawLine(rect.topLeft(), rect.topRight())
        elif self._orient_v == 'top':
            lp, rp = rect.bottomLeft(), rect.bottomRight()

        p.drawLine(lp.x(), lp.y(), rp.x(), rp.y())


class L1Label(LevelLabel):

    size: float = 0
    size_digits: float = 3

    text_flags = (
        QtCore.Qt.TextDontClip
        | QtCore.Qt.AlignLeft
    )

    def set_label_str(self, level: float) -> None:
        """Reimplement the label string write to include the level's order-queue's
        size in the text, eg. 100 x 323.3.

        """
        self.label_str = '{size:.{size_digits}f} x {level:,.{digits}f}'.format(
            size_digits=self.size_digits,
            size=self.size or '?',
            digits=self.digits,
            level=level
        ).replace(',', ' ')


class L1Labels:
    """Level 1 bid ask labels for dynamic update on price-axis.

    """
    max_value: float = '100.0 x 100 000.00'

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa
        digits: int = 2,
        size_digits: int = 0,
        font_size_inches: float = 4 / 53.,
    ) -> None:

        self.chart = chart

        self.bid_label = L1Label(
            chart=chart,
            parent=chart.getAxis('right'),
            # TODO: pass this from symbol data
            digits=digits,
            opacity=1,
            font_size_inches=font_size_inches,
            bg_color='papas_special',
            fg_color='bracket',
            orient_v='bottom',
        )
        self.bid_label.size_digits = size_digits
        self.bid_label._size_br_from_str(self.max_value)

        self.ask_label = L1Label(
            chart=chart,
            parent=chart.getAxis('right'),
            # TODO: pass this from symbol data
            digits=digits,
            opacity=1,
            font_size_inches=font_size_inches,
            bg_color='papas_special',
            fg_color='bracket',
            orient_v='top',
        )
        self.ask_label.size_digits = size_digits
        self.ask_label._size_br_from_str(self.max_value)


class LevelLine(pg.InfiniteLine):
    def __init__(
        self,
        label: LevelLabel,
        **kwargs,
    ) -> None:
        self.label = label
        super().__init__(**kwargs)
        self.sigPositionChanged.connect(self.set_level)

    def set_level(self, value: float) -> None:
        self.label.update_from_data(0, self.value())


def level_line(
    chart: 'ChartPlogWidget',  # noqa
    level: float,
    digits: int = 1,

    # size 4 font on 4k screen scaled down, so small-ish.
    font_size_inches: float = 4 / 53.,

    **linelabelkwargs
) -> LevelLine:
    """Convenience routine to add a styled horizontal line to a plot.

    """
    label = LevelLabel(
        chart=chart,
        parent=chart.getAxis('right'),
        # TODO: pass this from symbol data
        digits=digits,
        opacity=1,
        font_size_inches=font_size_inches,
        # TODO: make this take the view's bg pen
        bg_color='papas_special',
        fg_color='default',
        **linelabelkwargs
    )
    label.update_from_data(0, level)
    # TODO: can we somehow figure out a max value from the parent axis?
    label._size_br_from_str(label.label_str)

    line = LevelLine(
        label,
        movable=True,
        angle=0,
    )
    line.setValue(level)
    line.setPen(pg.mkPen(hcolor('default')))
    # activate/draw label
    line.setValue(level)

    chart.plotItem.addItem(line)

    return line
