"""
Chart graphics for displaying a slew of different data types.
"""
# import time
from typing import List

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QLineF

# from .quantdom.utils import timeit
from ._style import _xaxis_at, hcolor, _font
from ._axes import YAxisLabel, XAxisLabel

# TODO:
# - checkout pyqtgraph.PlotCurveItem.setCompositionMode

_mouse_rate_limit = 60  # calc current screen refresh rate?
_debounce_delay = 1/2e3
_ch_label_opac = 1


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
            QtCore.QRectF(-size/2, -size/2, size, size)
        )
        # dot.translate(-size*0.5, -size*0.5)
        dot.setPen(pen)
        dot.setBrush(brush)
        dot.setParentItem(self)

        # keep a static size
        self.setFlag(self.ItemIgnoresTransformations)

    def paint(self, p, opt, widget):
        p.drawPicture(0, 0, self._pic)


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
        yl = YAxisLabel(
            parent=plot.getAxis('right'),
            digits=digits or self.digits,
            opacity=_ch_label_opac,
            color=self.pen,
        )

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
            color=self.pen,
        )

    def add_curve_cursor(
        self,
        plot: 'ChartPlotWidget',  # noqa
        curve: 'PlotCurveItem',  # noqa
    ) -> LineDot:
        # if this plot contains curves add line dot "cursors" to denote
        # the current sample under the mouse
        cursor = LineDot(curve, index=len(plot._array))
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
            abs_pos=pos, data=y
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
                plot._update_contents_label(ix)

            # update the label on the bottom of the crosshair
            self.xaxis_label.update_label(
                abs_pos=pos,
                data=x
            )

            # update all subscribed curve dots
            for cursor in opts.get('cursors', ()):
                cursor.setIndex(ix)

    def boundingRect(self):
        try:
            return self.active_plot.boundingRect()
        except AttributeError:
            return self.plots[0].boundingRect()


def _mk_lines_array(data: List, size: int) -> np.ndarray:
    """Create an ndarray to hold lines graphics objects.
    """
    return np.zeros_like(
        data,
        shape=(int(size), 3),
        dtype=object,
    )


def bars_from_ohlc(
    data: np.ndarray,
    w: float,
    start: int = 0,
) -> np.ndarray:
    """Generate an array of lines objects from input ohlc data.
    """
    lines = _mk_lines_array(data, data.shape[0])

    for i, q in enumerate(data[start:], start=start):
        open, high, low, close, index = q[
            ['open', 'high', 'low', 'close', 'index']]

        # place the x-coord start as "middle" of the drawing range such
        # that the open arm line-graphic is at the left-most-side of
        # the indexe's range according to the view mapping.
        index_start = index

        # high - low line
        if low != high:
            # hl = QLineF(index, low, index, high)
            hl = QLineF(index_start, low, index_start, high)
        else:
            # XXX: if we don't do it renders a weird rectangle?
            # see below too for handling this later...
            hl = QLineF(low, low, low, low)
            hl._flat = True

        # open line
        o = QLineF(index_start - w, open, index_start, open)
        # close line
        c = QLineF(index_start, close, index_start + w, close)

        # indexing here is as per the below comments
        lines[i] = (hl, o, c)

        # XXX: in theory we could get a further speedup by using a flat
        # array and avoiding the call to `np.ravel()` below?
        # lines[3*i:3*i+3] = (hl, o, c)

        # if not _tina_mode:
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
    w: float = 0.43
    bars_pen = pg.mkPen(hcolor('pikers'))

    # XXX: tina mode, see below
    # bull_brush = pg.mkPen('#00cc00')
    # bear_brush = pg.mkPen('#fa0000')

    def __init__(
        self,
        # scene: 'QGraphicsScene',  # noqa
        plotitem: 'pg.PlotItem',  # noqa
    ) -> None:
        super().__init__()
        self.last = QtGui.QPicture()
        self.history = QtGui.QPicture()
        # TODO: implement updateable pixmap solution
        self._pi = plotitem
        # self._scene = plotitem.vb.scene()
        # self.picture = QtGui.QPixmap(1000, 300)
        # plotitem.addItem(self.picture)
        # self._pmi = None
        # self._pmi = self._scene.addPixmap(self.picture)

        # XXX: not sure this actually needs to be an array other
        # then for the old tina mode calcs for up/down bars below?
        # lines container
        self.lines = _mk_lines_array([], 50e3)

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
        lines = bars_from_ohlc(data, self.w, start=start)

        # save graphics for later reference and keep track
        # of current internal "last index"
        index = len(lines)
        self.lines[:index] = lines
        self.index = index

        # up to last to avoid double draw of last bar
        self.draw_lines(just_history=True, iend=self.index - 1)
        self.draw_lines(iend=self.index)

    # @timeit
    def draw_lines(
        self,
        istart=0,
        iend=None,
        just_history=False,
        # TODO: could get even fancier and only update the single close line?
        lines=None,
    ) -> None:
        """Draw the current line set using the painter.
        """
        if just_history:
            # draw bars for the "history" picture
            iend = iend or self.index - 1
            pic = self.history
        else:
            # draw the last bar
            istart = self.index - 1
            iend = iend or self.index
            pic = self.last

        # use 2d array of lines objects, see conlusion on speed:
        # https://stackoverflow.com/a/60089929
        to_draw = np.ravel(self.lines[istart:iend])

        # pre-computing a QPicture object allows paint() to run much
        # more quickly, rather than re-drawing the shapes every time.
        p = QtGui.QPainter(pic)
        p.setPen(self.bars_pen)

        # TODO: is there any way to not have to pass all the lines every
        # iteration? It seems they won't draw unless it's done this way..
        p.drawLines(*to_draw)
        p.end()

        # XXX: if we ever try using `QPixmap` again...
        # if self._pmi is None:
        #     self._pmi = self.scene().addPixmap(self.picture)
        # else:
        #     self._pmi.setPixmap(self.picture)

        # trigger re-render
        # https://doc.qt.io/qt-5/qgraphicsitem.html#update
        self.update()

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
        """
        index = self.index
        length = len(array)
        extra = length - index

        # start_bar_to_update = index - 100

        if extra > 0:
            # generate new graphics to match provided array
            new = array[index:index + extra]
            lines = bars_from_ohlc(new, self.w)
            bars_added = len(lines)
            self.lines[index:index + bars_added] = lines
            self.index += bars_added

            # start_bar_to_update = index - bars_added
            self.draw_lines(just_history=True)
            if just_history:
                return

        # current bar update
        i, open, last, = array[-1][['index', 'open', 'close']]
        body, larm, rarm = self.lines[index-1]

        # XXX: is there a faster way to modify this?
        # update close line / right arm
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

        self.draw_lines(just_history=False)

    # XXX: From the customGraphicsItem.py example:
    # The only required methods are paint() and boundingRect()
    # @timeit
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
        p.drawPicture(0, 0, self.history)
        p.drawPicture(0, 0, self.last)

        # TODO: if we can ever make pixmaps work...
        # p.drawPixmap(0, 0, self.picture)
        # self._pmi.setPixmap(self.picture)
        # print(self.scene())

        # profiler('bars redraw:')

    def boundingRect(self):
        # TODO: can we do rect caching to make this faster?

        # Qt docs: https://doc.qt.io/qt-5/qgraphicsitem.html#boundingRect
        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bounding rect for us).

        # compute aggregate bounding rectangle
        lb = self.last.boundingRect()
        hb = self.history.boundingRect()
        return QtCore.QRectF(
            # top left
            QtCore.QPointF(hb.topLeft()),
            # total size
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


def h_line(level: float) -> pg.InfiniteLine:
    """Convenience routine to add a styled horizontal line to a plot.

    """
    line = pg.InfiniteLine(
        movable=True,
        angle=0,
        pos=level,
        # label='{value:0.2f}',
        # labelOpts={
        #     'position': 0.01,
        #     'movable': True
        #     'color': (200,200,100),
        #     'fill': (200,200,200,50),
        # }
    )
    default_pen = pg.mkPen(hcolor('default'))
    line.setPen(default_pen)

    if getattr(line, 'label', None):
        line.label.setFont(_font)

    return line
