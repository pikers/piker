"""
Chart graphics for displaying a slew of different data types.
"""
from enum import Enum
from contextlib import contextmanager

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QLineF

from .quantdom.utils import timeit
from .quantdom.base import Quotes

from ._style import _xaxis_at, _tina_mode
from ._axes import YAxisLabel, XAxisLabel


_mouse_rate_limit = 60


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


class BarItems(pg.GraphicsObject):
    """Price range bars graphics rendered from a OHLC sequence.
    """
    w: float = 0.5

    bull_brush = bear_brush = pg.mkPen('#808080')
    # bull_brush = pg.mkPen('#00cc00')
    # bear_brush = pg.mkPen('#fa0000')

    def __init__(self):
        super().__init__()
        self.picture = QtGui.QPicture()
        self.lines = None
        # self.generatePicture()

    # TODO: this is the routine to be retriggered for redraw
    @contextmanager
    def painter(self):
        # pre-computing a QPicture object allows paint() to run much
        # more quickly, rather than re-drawing the shapes every time.
        p = QtGui.QPainter(self.picture)
        yield p
        p.end()

    @timeit
    def draw_from_data(self, data):
        # XXX: overloaded method to allow drawing other candle types

        high_to_low = np.empty_like(data, dtype=object)
        open_sticks = np.empty_like(data, dtype=object)
        close_sticks = np.empty_like(data, dtype=object)
        with self.painter() as p:
            import time
            start = time.time()
            for i, q in enumerate(data):
                high_to_low[i] = QLineF(q['id'], q['low'], q['id'], q['high'])
                open_sticks[i] = QLineF(
                        q['id'] - self.w, q['open'], q['id'], q['open'])
                close_sticks[i] = QtCore.QLineF(
                        q['id'] + self.w, q['close'], q['id'], q['close'])

            # high_to_low = np.array(
            #     [QtCore.QLineF(q.id, q.low, q.id, q.high) for q in Quotes]
            # )
            # open_sticks = np.array(
            #     [QtCore.QLineF(q.id - self.w, q.open, q.id, q.open)
            #      for q in Quotes]
            # )
            # close_sticks = np.array(
            #     [
            #         QtCore.QLineF(q.id + self.w, q.close, q.id, q.close)
            #         for q in Quotes
            #     ]
            # )
            print(f"took {time.time() - start}")
            self.lines = lines = np.concatenate([high_to_low, open_sticks, close_sticks])

            if _tina_mode:
                long_bars = np.resize(Quotes.close > Quotes.open, len(lines))
                short_bars = np.resize(Quotes.close < Quotes.open, len(lines))
                ups = lines[long_bars]
                downs = lines[short_bars]

                # draw "up" bars
                p.setPen(self.bull_brush)
                p.drawLines(*ups)

                # draw "down" bars
                p.setPen(self.bear_brush)
                p.drawLines(*downs)

            else:  # piker mode
                p.setPen(self.bull_brush)
                p.drawLines(*lines)

    # XXX: From the customGraphicsItem.py example:
    # The only required methods are paint() and boundingRect()
    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bouning rect for us)
        return QtCore.QRectF(self.picture.boundingRect())


class CandlestickItems(BarItems):

    w2 = 0.7
    line_pen = pg.mkPen('#000000')
    bull_brush = pg.mkBrush('#00ff00')
    bear_brush = pg.mkBrush('#ff0000')

    def _generate(self, p):
        rects = np.array(
            [
                QtCore.QRectF(q.id - self.w, q.open, self.w2, q.close - q.open)
                for q in Quotes
            ]
        )

        p.setPen(self.line_pen)
        p.drawLines(
            [QtCore.QLineF(q.id, q.low, q.id, q.high)
             for q in Quotes]
        )

        p.setBrush(self.bull_brush)
        p.drawRects(*rects[Quotes.close > Quotes.open])

        p.setBrush(self.bear_brush)
        p.drawRects(*rects[Quotes.close < Quotes.open])


class ChartType(Enum):
    """Bar type to graphics class map.
    """
    BAR = BarItems
    CANDLESTICK = CandlestickItems
    LINE = pg.PlotDataItem
