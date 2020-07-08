"""
Chart graphics for displaying a slew of different data types.
"""
# from typing import Dict, Any
from enum import Enum
from contextlib import contextmanager

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QLineF

from .quantdom.utils import timeit
# from .quantdom.base import Quotes

from ._style import _xaxis_at  # , _tina_mode
from ._axes import YAxisLabel, XAxisLabel

# TODO: checkout pyqtgraph.PlotCurveItem.setCompositionMode

_mouse_rate_limit = 50


class CrossHairItem(pg.GraphicsObject):

    def __init__(self, parent, indicators=None, digits=0):
        super().__init__()
        # self.pen = pg.mkPen('#000000')
        self.pen = pg.mkPen('#a9a9a9')
        self.parent = parent
        self.indicators = {}
        self.activeIndicator = None
        self.xaxis = self.parent.getAxis('bottom')
        self.yaxis = self.parent.getAxis('right')

        self.vline = self.parent.addLine(x=0, pen=self.pen, movable=False)
        self.hline = self.parent.addLine(y=0, pen=self.pen, movable=False)

        self.proxy_moved = pg.SignalProxy(
            self.parent.scene().sigMouseMoved,
            rateLimit=_mouse_rate_limit,
            slot=self.mouseMoved,
        )

        self.yaxis_label = YAxisLabel(
            parent=self.yaxis, digits=digits, opacity=1
        )

        indicators = indicators or []

        if indicators:
            # when there are indicators present in sub-plot rows
            # take the last one (nearest to the bottom) and place the
            # crosshair label on it's x-axis.
            last_ind = indicators[-1]

            self.proxy_enter = pg.SignalProxy(
                self.parent.sig_mouse_enter,
                rateLimit=_mouse_rate_limit,
                slot=lambda: self.mouseAction('Enter', False),
            )
            self.proxy_leave = pg.SignalProxy(
                self.parent.sig_mouse_leave,
                rateLimit=_mouse_rate_limit,
                slot=lambda: self.mouseAction('Leave', False),
            )

        # determine where to place x-axis label
        if _xaxis_at == 'bottom':
            # place below is last indicator subplot
            self.xaxis_label = XAxisLabel(
                parent=last_ind.getAxis('bottom'), opacity=1
            )
        else:
            # keep x-axis right below main chart
            self.xaxis_label = XAxisLabel(parent=self.xaxis, opacity=1)

        for i in indicators:
            # add vertial and horizonal lines and a y-axis label
            vl = i.addLine(x=0, pen=self.pen, movable=False)
            hl = i.addLine(y=0, pen=self.pen, movable=False)
            yl = YAxisLabel(parent=i.getAxis('right'), opacity=1)

            px_moved = pg.SignalProxy(
                i.scene().sigMouseMoved,
                rateLimit=_mouse_rate_limit,
                slot=self.mouseMoved
            )
            px_enter = pg.SignalProxy(
                i.sig_mouse_enter,
                rateLimit=_mouse_rate_limit,
                slot=lambda: self.mouseAction('Enter', i),
            )
            px_leave = pg.SignalProxy(
                i.sig_mouse_leave,
                rateLimit=_mouse_rate_limit,
                slot=lambda: self.mouseAction('Leave', i),
            )
            self.indicators[i] = {
                'vl': vl,
                'hl': hl,
                'yl': yl,
                'px': (px_moved, px_enter, px_leave),
            }

    def mouseAction(self, action, ind=False):  # noqa
        if action == 'Enter':
            # show horiz line and y-label
            if ind:
                self.indicators[ind]['hl'].show()
                self.indicators[ind]['yl'].show()
                self.activeIndicator = ind
            else:
                self.yaxis_label.show()
                self.hline.show()
        # Leave
        else:
            # hide horiz line and y-label
            if ind:
                self.indicators[ind]['hl'].hide()
                self.indicators[ind]['yl'].hide()
                self.activeIndicator = None
            else:
                self.yaxis_label.hide()
                self.hline.hide()

    def mouseMoved(self, evt):  # noqa
        """Update horizonal and vertical lines when mouse moves inside
        either the main chart or any indicator subplot.
        """

        pos = evt[0]

        # if the mouse is within the parent ``ChartPlotWidget``
        if self.parent.sceneBoundingRect().contains(pos):
            # mouse_point = self.vb.mapSceneToView(pos)
            mouse_point = self.parent.mapToView(pos)

            # move the vertial line to the current x coordinate
            self.vline.setX(mouse_point.x())

            # update the label on the bottom of the crosshair
            self.xaxis_label.update_label(evt_post=pos, point_view=mouse_point)

            # update the vertical line in any indicators subplots
            for opts in self.indicators.values():
                opts['vl'].setX(mouse_point.x())

            if self.activeIndicator:
                # vertial position of the mouse is inside an indicator
                mouse_point_ind = self.activeIndicator.mapToView(pos)
                self.indicators[self.activeIndicator]['hl'].setY(
                    mouse_point_ind.y()
                )
                self.indicators[self.activeIndicator]['yl'].update_label(
                    evt_post=pos, point_view=mouse_point_ind
                )
            else:
                # vertial position of the mouse is inside the main chart
                self.hline.setY(mouse_point.y())
                self.yaxis_label.update_label(
                    evt_post=pos, point_view=mouse_point
                )

    def paint(self, p, *args):
        pass

    def boundingRect(self):
        return self.parent.boundingRect()


def bars_from_ohlc(
    data: np.ndarray,
    start: int = 0,
    w: float = 0.4,
) -> np.ndarray:
    lines = np.empty_like(data, shape=(data.shape[0]*3,), dtype=object)

    for i, q in enumerate(data[start:], start=start):
        low = q['low']
        high = q['high']
        index = q['index']

        # high - low line
        if low != high:
            hl = QLineF(index, low, index, high)
        else:
            # XXX: if we don't do it renders a weird rectangle?
            # see below too for handling this later...
            hl = QLineF(low, low, low, low)
            hl._flat = True
        # open line
        o = QLineF(index - w, q['open'], index, q['open'])
        # close line
        c = QLineF(index + w, q['close'], index, q['close'])

        # indexing here is as per the below comments
        lines[3*i:3*i+3] = (hl, o, c)

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
        self.lines: np.ndarray = np.empty_like(
            [], shape=(int(50e3),), dtype=object)
        self._index = 0

    def _set_index(self, val):
        # breakpoint()
        self._index = val

    def _get_index(self):
        return self._index

    index = property(_get_index, _set_index)

    # TODO: can we be faster just dropping this?
    @contextmanager
    def painter(self):
        # pre-computing a QPicture object allows paint() to run much
        # more quickly, rather than re-drawing the shapes every time.
        p = QtGui.QPainter(self.picture)
        yield p
        p.end()

    @timeit
    def draw_from_data(
        self,
        data: np.recarray,
        start: int = 0,
    ):
        """Draw OHLC datum graphics from a ``np.recarray``.
        """
        lines = bars_from_ohlc(data, start=start, w=self.w)

        # save graphics for later reference and keep track
        # of current internal "last index"
        index = len(lines)
        self.lines[:index] = lines
        self.index = index

        with self.painter() as p:
            p.setPen(self.bull_pen)
            p.drawLines(*self.lines[:index])

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
        idata = int(index/3) - 1
        extra = length - idata
        if extra > 0:
            # generate new graphics to match provided array
            new = array[-extra:]
            lines = bars_from_ohlc(new, w=self.w)

            added = len(new) * 3
            assert len(lines) == added

            self.lines[index:index + len(lines)] = lines

            self.index += len(lines)

        else:  # current bar update
            # do we really need to verify the entire past data set?
            # index, time, open, high, low, close, volume
            i, time, _, _, _, close, _ = array[-1]
            last = close
            body, larm, rarm = self.lines[index-3:index]
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
        with self.painter() as p:
            p.setPen(self.bull_pen)
            p.drawLines(*self.lines[:index])

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
