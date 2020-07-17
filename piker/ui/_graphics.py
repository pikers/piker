"""
Chart graphics for displaying a slew of different data types.
"""
from typing import List
from enum import Enum
from itertools import chain

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QLineF

from .quantdom.utils import timeit
# from .quantdom.base import Quotes

from ._style import _xaxis_at  # , _tina_mode
from ._axes import YAxisLabel, XAxisLabel


# TODO:
# - checkout pyqtgraph.PlotCurveItem.setCompositionMode

_mouse_rate_limit = 50


class CrossHair(pg.GraphicsObject):

    def __init__(self, parent, digits: int = 0):
        super().__init__()
        # self.pen = pg.mkPen('#000000')
        self.pen = pg.mkPen('#a9a9a9')
        self.parent = parent
        self.graphics = {}
        self.plots = []
        self.active_plot = None
        self.digits = digits

    def add_plot(
        self,
        plot: 'ChartPlotWidget',  # noqa
        digits: int = 0,
    ) -> None:
        # add ``pg.graphicsItems.InfiniteLine``s
        # vertical and horizonal lines and a y-axis label
        vl = plot.addLine(x=0, pen=self.pen, movable=False)
        hl = plot.addLine(y=0, pen=self.pen, movable=False)
        yl = YAxisLabel(
            parent=plot.getAxis('right'),
            digits=digits or self.digits,
            opacity=1
        )

        # TODO: checkout what ``.sigDelayed`` can be used for
        # (emitted once a sufficient delay occurs in mouse movement)
        px_moved = pg.SignalProxy(
            plot.scene().sigMouseMoved,
            rateLimit=_mouse_rate_limit,
            slot=self.mouseMoved
        )
        px_enter = pg.SignalProxy(
            plot.sig_mouse_enter,
            rateLimit=_mouse_rate_limit,
            slot=lambda: self.mouseAction('Enter', plot),
        )
        px_leave = pg.SignalProxy(
            plot.sig_mouse_leave,
            rateLimit=_mouse_rate_limit,
            slot=lambda: self.mouseAction('Leave', plot),
        )
        self.graphics[plot] = {
            'vl': vl,
            'hl': hl,
            'yl': yl,
            'px': (px_moved, px_enter, px_leave),
        }
        self.plots.append(plot)

        # determine where to place x-axis label
        if _xaxis_at == 'bottom':
            # place below the last plot
            self.xaxis_label = XAxisLabel(
                parent=self.plots[-1].getAxis('bottom'),
                opacity=1
            )
        else:
            # keep x-axis right below main chart
            first = self.plots[0]
            xaxis = first.getAxis('bottom')
            self.xaxis_label = XAxisLabel(parent=xaxis, opacity=1)

    def mouseAction(self, action, plot):  # noqa
        # TODO: why do we no handle all plots the same?
        # -> main plot has special path? would simplify code.
        if action == 'Enter':
            # show horiz line and y-label
            self.graphics[plot]['hl'].show()
            self.graphics[plot]['yl'].show()
            self.active_plot = plot
        else:  # Leave
            # hide horiz line and y-label
            self.graphics[plot]['hl'].hide()
            self.graphics[plot]['yl'].hide()
            self.active_plot = None

    def mouseMoved(self, evt):  # noqa
        """Update horizonal and vertical lines when mouse moves inside
        either the main chart or any indicator subplot.
        """
        pos = evt[0]

        # find position in main chart
        mouse_point = self.plots[0].mapToView(pos)

        # move the vertical line to the current x coordinate in all charts
        for opts in self.graphics.values():
            opts['vl'].setX(mouse_point.x())

        # update the label on the bottom of the crosshair
        self.xaxis_label.update_label(evt_post=pos, point_view=mouse_point)

        # vertical position of the mouse is inside an indicator
        mouse_point_ind = self.active_plot.mapToView(pos)

        self.graphics[self.active_plot]['hl'].setY(
            mouse_point_ind.y()
        )
        self.graphics[self.active_plot]['yl'].update_label(
            evt_post=pos, point_view=mouse_point_ind
        )

    # def paint(self, p, *args):
    #     pass

    def boundingRect(self):
        return self.plots[0].boundingRect()


def _mk_lines_array(data: List, size: int) -> np.ndarray:
    """Create an ndarray to hold lines graphics objects.
    """
    # TODO: might want to just make this a 2d array to be faster at
    # flattening using .ravel()?
    return np.zeros_like(
        data,
        shape=(int(size),),
        dtype=[
            ('index', int),
            ('body', object),
            ('rarm', object),
            ('larm', object)
        ],
    )


def bars_from_ohlc(
    data: np.ndarray,
    start: int = 0,
    w: float = 0.43,
) -> np.ndarray:
    """Generate an array of lines objects from input ohlc data.
    """
    lines = _mk_lines_array(data, data.shape[0])

    for i, q in enumerate(data[start:], start=start):
        open, high, low, close, index = q[
            ['open', 'high', 'low', 'close', 'index']]

        # high - low line
        if low != high:
            hl = QLineF(index, low, index, high)
        else:
            # XXX: if we don't do it renders a weird rectangle?
            # see below too for handling this later...
            hl = QLineF(low, low, low, low)
            hl._flat = True

        # open line
        o = QLineF(index - w, open, index, open)
        # close line
        c = QLineF(index + w, close, index, close)

        # indexing here is as per the below comments
        # lines[3*i:3*i+3] = (hl, o, c)
        lines[i] = (index, hl, o, c)

        # if not _tina_mode:  # piker mode
        # else _tina_mode:
        #     self.lines = lines = np.concatenate(
        #       [high_to_low, open_sticks, close_sticks])
        #     use traditional up/down green/red coloring
        #     long_bars = np.resize(Quotes.close > Quotes.open, len(lines))
        #     short_bars = np.resize(
        #       Quotes.close < Quotes.open, len(lines))

        #     ups = lines[long_bars]
        #     downs = lines[short_bars]

        #     # draw "up" bars
        #     p.setPen(self.bull_brush)
        #     p.drawLines(*ups)

        #     # draw "down" bars
        #     p.setPen(self.bear_brush)
        #     p.drawLines(*downs)

    return lines


class BarItems(pg.GraphicsObject):
    """Price range bars graphics rendered from a OHLC sequence.
    """
    sigPlotChanged = QtCore.Signal(object)

    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.4
    bull_pen = pg.mkPen('#808080')

    # XXX: tina mode, see below
    # bull_brush = pg.mkPen('#00cc00')
    # bear_brush = pg.mkPen('#fa0000')

    def __init__(self):
        super().__init__()
        self.picture = QtGui.QPicture()

        # XXX: not sure this actually needs to be an array other
        # then for the old tina mode calcs for up/down bars below?
        # lines container
        self.lines = _mk_lines_array([], 50e3)

        # track the current length of drawable lines within the larger array
        self.index: int = 0

    @timeit
    def draw_from_data(
        self,
        data: np.recarray,
        start: int = 0,
    ):
        """Draw OHLC datum graphics from a ``np.recarray``.
        """
        lines = bars_from_ohlc(data, start=start)

        # save graphics for later reference and keep track
        # of current internal "last index"
        index = len(lines)
        self.lines[:index] = lines
        self.index = index
        self.draw_lines()

    def draw_lines(self):
        """Draw the current line set using the painter.
        """
        to_draw = self.lines[
            ['body', 'larm', 'rarm']][:self.index]

        # pre-computing a QPicture object allows paint() to run much
        # more quickly, rather than re-drawing the shapes every time.
        p = QtGui.QPainter(self.picture)
        p.setPen(self.bull_pen)

        # TODO: might be better to use 2d array?
        # try our fsp.rec2array() and a np.ravel() for speedup
        # otherwise we might just have to go 2d ndarray of objects.
        # see conlusion on speed here: # https://stackoverflow.com/a/60089929
        p.drawLines(*chain.from_iterable(to_draw))
        p.end()

    def update_from_array(
        self,
        array: np.ndarray,
    ) -> None:
        """Update the last datum's bar graphic from input data array.

        This routine should be interface compatible with
        ``pg.PlotCurveItem.setData()``. Normally this method in
        ``pyqtgraph`` seems to update all the data passed to the
        graphics object, and then update/rerender, but here we're
        assuming the prior graphics havent changed (OHLC history rarely
        does) so this "should" be simpler and faster.
        """
        index = self.index
        length = len(array)
        extra = length - index
        if extra > 0:
            # generate new graphics to match provided array
            new = array[index:index + extra]
            lines = bars_from_ohlc(new)
            bars_added = len(lines)
            self.lines[index:index + bars_added] = lines
            self.index += bars_added

        # else:  # current bar update
        # do we really need to verify the entire past data set?
        # index, time, open, high, low, close, volume
        i, time, open, _, _, close, _ = array[-1]
        last = close
        i, body, larm, rarm = self.lines[index-1]
        if not rarm:
            breakpoint()

        # XXX: is there a faster way to modify this?
        # update right arm
        rarm.setLine(rarm.x1(), last, rarm.x2(), last)

        # update body
        high = body.y2()
        low = body.y1()
        if last < low:
            low = last

        if last > high:
            high = last

        if getattr(body, '_flat', None) and low != high:
            # if the bar was flat it likely does not have
            # the index set correctly due to a rendering bug
            # see above
            body.setLine(i, low, i, high)
            body._flat = False
        else:
            body.setLine(body.x1(), low, body.x2(), high)

        # draw the pic
        self.draw_lines()

        # trigger re-render
        self.update()

    # be compat with ``pg.PlotCurveItem``
    setData = update_from_array

    # XXX: From the customGraphicsItem.py example:
    # The only required methods are paint() and boundingRect()
    def paint(self, p, opt, widget):
        p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bounding rect for us)
        return QtCore.QRectF(self.picture.boundingRect())


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


class ChartType(Enum):
    """Bar type to graphics class map.
    """
    BAR = BarItems
    # CANDLESTICK = CandlestickItems
    LINE = pg.PlotDataItem
