"""
Real-time quotes charting components
"""
from typing import Callable, List, Tuple

import numpy as np
import pyqtgraph as pg
from pyqtgraph import functions as fn
from PyQt5 import QtCore, QtGui

from .base import Quotes
from .const import ChartType
from .portfolio import Order, Portfolio
from .utils import fromtimestamp, timeit

__all__ = ('QuotesChart')


# white background for tinas like xb
# pg.setConfigOption('background', 'w')

# margins
CHART_MARGINS = (0, 0, 10, 3)

# chart-wide font
_font = QtGui.QFont("Hack", 4)
_i3_rgba = QtGui.QColor.fromRgbF(*[0.14]*3 + [1])


class SampleLegendItem(pg.graphicsItems.LegendItem.ItemSample):

    def paint(self, p, *args):
        p.setRenderHint(p.Antialiasing)
        if isinstance(self.item, tuple):
            positive = self.item[0].opts
            negative = self.item[1].opts
            p.setPen(pg.mkPen(positive['pen']))
            p.setBrush(pg.mkBrush(positive['brush']))
            p.drawPolygon(
                QtGui.QPolygonF(
                    [
                        QtCore.QPointF(0, 0),
                        QtCore.QPointF(18, 0),
                        QtCore.QPointF(18, 18),
                    ]
                )
            )
            p.setPen(pg.mkPen(negative['pen']))
            p.setBrush(pg.mkBrush(negative['brush']))
            p.drawPolygon(
                QtGui.QPolygonF(
                    [
                        QtCore.QPointF(0, 0),
                        QtCore.QPointF(0, 18),
                        QtCore.QPointF(18, 18),
                    ]
                )
            )
        else:
            opts = self.item.opts
            p.setPen(pg.mkPen(opts['pen']))
            p.drawRect(0, 10, 18, 0.5)


class PriceAxis(pg.AxisItem):

    def __init__(self):
        super().__init__(orientation='right')
        # self.setStyle(**{
        #     'textFillLimits': [(0, 0.8)],
        #     # 'tickTextWidth': 5,
        #     # 'tickTextHeight': 5,
        #     # 'autoExpandTextSpace': True,
        #     # 'maxTickLength': -20,
        # })
        # self.setLabel(**{'font-size':'10pt'})
        self.setTickFont(_font)

    # XXX: drop for now since it just eats up h space

    # def tickStrings(self, vals, scale, spacing):
    #     digts = max(0, np.ceil(-np.log10(spacing * scale)))
    #     return [
    #         ('{:<8,.%df}' % digts).format(v).replace(',', ' ') for v in vals
    #     ]


class FromTimeFieldDateAxis(pg.AxisItem):
    tick_tpl = {'D1': '%Y-%b-%d'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setTickFont(_font)
        self.quotes_count = len(Quotes) - 1

        # default styling
        self.setStyle(
            tickTextOffset=7,
            textFillLimits=[(0, 0.90)],
            # TODO: doesn't seem to work -> bug in pyqtgraph?
            # tickTextHeight=2,
        )

    def tickStrings(self, values, scale, spacing):
        # if len(values) > 1 or not values:
        #     values = Quotes.time

        # strings = super().tickStrings(values, scale, spacing)
        s_period = 'D1'
        strings = []
        for ibar in values:
            if ibar > self.quotes_count:
                return strings
            dt_tick = fromtimestamp(Quotes[int(ibar)].time)
            strings.append(
                dt_tick.strftime(self.tick_tpl[s_period])
            )
        return strings


class CenteredTextItem(QtGui.QGraphicsTextItem):
    def __init__(
        self,
        text='',
        parent=None,
        pos=(0, 0),
        pen=None,
        brush=None,
        valign=None,
        opacity=0.1,
    ):
        super().__init__(text, parent)

        self.pen = pen
        self.brush = brush
        self.opacity = opacity
        self.valign = valign
        self.text_flags = QtCore.Qt.AlignCenter
        self.setPos(*pos)
        self.setFlag(self.ItemIgnoresTransformations)

    def boundingRect(self):  # noqa
        r = super().boundingRect()
        if self.valign == QtCore.Qt.AlignTop:
            return QtCore.QRectF(-r.width() / 2, -37, r.width(), r.height())
        elif self.valign == QtCore.Qt.AlignBottom:
            return QtCore.QRectF(-r.width() / 2, 15, r.width(), r.height())

    def paint(self, p, option, widget):
        p.setRenderHint(p.Antialiasing, False)
        p.setRenderHint(p.TextAntialiasing, True)
        p.setPen(self.pen)
        if self.brush.style() != QtCore.Qt.NoBrush:
            p.setOpacity(self.opacity)
            p.fillRect(option.rect, self.brush)
            p.setOpacity(1)
        p.drawText(option.rect, self.text_flags, self.toPlainText())


class AxisLabel(pg.GraphicsObject):

    # bg_color = pg.mkColor('#a9a9a9')
    bg_color = pg.mkColor('#808080')
    fg_color = pg.mkColor('#000000')

    def __init__(self, parent=None, digits=0, color=None, opacity=1, **kwargs):
        super().__init__(parent)
        self.parent = parent
        self.opacity = opacity
        self.label_str = ''
        self.digits = digits
        # self.quotes_count = len(Quotes) - 1

        if isinstance(color, QtGui.QPen):
            self.bg_color = color.color()
            self.fg_color = pg.mkColor('#ffffff')
        elif isinstance(color, list):
            self.bg_color = {'>0': color[0].color(), '<0': color[1].color()}
            self.fg_color = pg.mkColor('#ffffff')

        self.setFlag(self.ItemIgnoresTransformations)

    def tick_to_string(self, tick_pos):
        raise NotImplementedError()

    def boundingRect(self):  # noqa
        raise NotImplementedError()

    def update_label(self, evt_post, point_view):
        raise NotImplementedError()

    def update_label_test(self, ypos=0, ydata=0):
        self.label_str = self.tick_to_string(ydata)
        height = self.boundingRect().height()
        offset = 0  # if have margins
        new_pos = QtCore.QPointF(0, ypos - height / 2 - offset)
        self.setPos(new_pos)

    def paint(self, p, option, widget):
        p.setRenderHint(p.TextAntialiasing, True)
        p.setPen(self.fg_color)
        if self.label_str:
            if not isinstance(self.bg_color, dict):
                bg_color = self.bg_color
            else:
                if int(self.label_str.replace(' ', '')) > 0:
                    bg_color = self.bg_color['>0']
                else:
                    bg_color = self.bg_color['<0']
            p.setOpacity(self.opacity)
            p.fillRect(option.rect, bg_color)
            p.setOpacity(1)
            p.setFont(_font)

        p.drawText(option.rect, self.text_flags, self.label_str)


class XAxisLabel(AxisLabel):

    text_flags = (
        QtCore.Qt.TextDontClip | QtCore.Qt.AlignCenter | QtCore.Qt.AlignTop
    )

    def tick_to_string(self, tick_pos):
        # TODO: change to actual period
        tpl = self.parent.tick_tpl['D1']
        return fromtimestamp(Quotes[round(tick_pos)].time).strftime(tpl)

    def boundingRect(self):  # noqa
        return QtCore.QRectF(0, 0, 145, 50)

    def update_label(self, evt_post, point_view):
        ibar = point_view.x()
        # if ibar > self.quotes_count:
        #     return
        self.label_str = self.tick_to_string(ibar)
        width = self.boundingRect().width()
        offset = 0  # if have margins
        new_pos = QtCore.QPointF(evt_post.x() - width / 2 - offset, 0)
        self.setPos(new_pos)


class YAxisLabel(AxisLabel):

    text_flags = (
        QtCore.Qt.TextDontClip | QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter
    )

    def tick_to_string(self, tick_pos):
        return ('{: ,.%df}' % self.digits).format(tick_pos).replace(',', ' ')

    def boundingRect(self):  # noqa
        return QtCore.QRectF(0, 0, 80, 40)

    def update_label(self, evt_post, point_view):
        self.label_str = self.tick_to_string(point_view.y())
        height = self.boundingRect().height()
        offset = 0  # if have margins
        new_pos = QtCore.QPointF(0, evt_post.y() - height / 2 - offset)
        self.setPos(new_pos)


class ScrollFromRightView(pg.ViewBox):

    def wheelEvent(self, ev, axis=None):
        if axis in (0, 1):
            mask = [False, False]
            mask[axis] = self.state['mouseEnabled'][axis]
        else:
            mask = self.state['mouseEnabled'][:]

        # actual scaling factor
        s = 1.02 ** (ev.delta() * self.state['wheelScaleFactor'])
        s = [(None if m is False else s) for m in mask]

        # XXX: scroll "around" the right most element in the view
        # center = pg.Point(
        #     fn.invertQTransform(self.childGroup.transform()).map(ev.pos())
        # )
        furthest_right_coord = self.boundingRect().topRight()
        center = pg.Point(
           fn.invertQTransform(
               self.childGroup.transform()
            ).map(furthest_right_coord)
        )

        self._resetTarget()
        self.scaleBy(s, center)
        ev.accept()
        self.sigRangeChangedManually.emit(mask)


# TODO: This is a sub-class of ``GracphicView`` which can
# take a ``background`` color setting.
class ChartPlotWidget(pg.PlotWidget):
    """``GraphicsView`` subtype containing a single ``PlotItem``.

    Overrides a ``pyqtgraph.PlotWidget`` (a ``GraphicsView`` containing
    a single ``PlotItem``) to intercept and and re-emit mouse enter/exit
    events.

    (Could be replaced with a ``pg.GraphicsLayoutWidget`` if we
    eventually want multiple plots managed together).
    """

    sig_mouse_leave = QtCore.Signal(object)
    sig_mouse_enter = QtCore.Signal(object)

    def enterEvent(self, ev):  # noqa
        pg.PlotWidget.enterEvent(self, ev)
        self.sig_mouse_enter.emit(self)

    def leaveEvent(self, ev):  # noqa
        pg.PlotWidget.leaveEvent(self, ev)
        self.sig_mouse_leave.emit(self)
        self.scene().leaveEvent(ev)


_mouse_rate_limit = 60
_xaxis_at = 'bottom'


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
                # vertial position of the mouse is inside and main chart
                self.hline.setY(mouse_point.y())
                self.yaxis_label.update_label(
                    evt_post=pos, point_view=mouse_point
                )

    def paint(self, p, *args):
        pass

    def boundingRect(self):
        return self.parent.boundingRect()


class BarItem(pg.GraphicsObject):
    # XXX: From the customGraphicsItem.py example:
    # The only required methods are paint() and boundingRect()

    w = 0.5

    bull_brush = bear_brush = pg.mkPen('#808080')
    # bull_brush = pg.mkPen('#00cc00')
    # bear_brush = pg.mkPen('#fa0000')

    def __init__(self):
        super().__init__()
        self.generatePicture()

    # TODO: this is the routine to be retriggered for redraw
    @timeit
    def generatePicture(self):
        # pre-computing a QPicture object allows paint() to run much
        # more quickly, rather than re-drawing the shapes every time.
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)
        self._generate(p)
        p.end()

    def _generate(self, p):
        # XXX: overloaded method to allow drawing other candle types

        high_to_low = np.array(
            [QtCore.QLineF(q.id, q.low, q.id, q.high) for q in Quotes]
        )
        open_stick = np.array(
            [QtCore.QLineF(q.id - self.w, q.open, q.id, q.open)
             for q in Quotes]
        )
        close_stick = np.array(
            [
                QtCore.QLineF(q.id + self.w, q.close, q.id, q.close)
                for q in Quotes
            ]
        )
        lines = np.concatenate([high_to_low, open_stick, close_stick])
        long_bars = np.resize(Quotes.close > Quotes.open, len(lines))
        short_bars = np.resize(Quotes.close < Quotes.open, len(lines))

        p.setPen(self.bull_brush)
        p.drawLines(*lines[long_bars])

        p.setPen(self.bear_brush)
        p.drawLines(*lines[short_bars])

    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bouning rect for us)
        return QtCore.QRectF(self.picture.boundingRect())


class CandlestickItem(BarItem):

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


def _configure_chart(
    chart: ChartPlotWidget,
) -> None:
    """Configure a chart with common settings.
    """

    # show only right side axes
    chart.hideAxis('left')
    chart.showAxis('right')

    # set panning limits
    chart.setLimits(
        xMin=Quotes[0].id,
        xMax=Quotes[-1].id,
        minXRange=60,
        yMin=Quotes.low.min() * 0.98,
        yMax=Quotes.high.max() * 1.02,
    )
    # show background grid
    chart.showGrid(x=True, y=True, alpha=0.4)

    # use cross-hair for cursor
    chart.setCursor(QtCore.Qt.CrossCursor)


def _configure_quotes_chart(
    chart: ChartPlotWidget,
    style: ChartType,
    update_yrange_limits: Callable,
) -> None:
    """Update and format a chart with quotes data.
    """
    _configure_chart(chart)

    # adds all bar/candle graphics objects for each
    # data point in the np array buffer to
    # be drawn on next render cycle
    chart.addItem(_get_chart_points(style))

    # assign callback for rescaling y-axis automatically
    # based on y-range contents

    # TODO: this can likely be ported to built-in: .enableAutoRange()
    # but needs testing
    chart.sigXRangeChanged.connect(update_yrange_limits)


def _configure_ind_charts(
    indicators: List[Tuple[ChartPlotWidget, np.ndarray]],
    xlink_to_chart: ChartPlotWidget,
) -> None:
    for ind_chart, d in indicators:
        # default config
        _configure_chart(ind_chart)

        curve = pg.PlotDataItem(d, pen='b', antialias=True)
        ind_chart.addItem(curve)

        # XXX: never do this lol
        # ind.setAspectLocked(1)

        # link chart x-axis to main quotes chart
        ind_chart.setXLink(xlink_to_chart)


class QuotesChart(QtGui.QWidget):

    long_pen = pg.mkPen('#006000')
    long_brush = pg.mkBrush('#00ff00')
    short_pen = pg.mkPen('#600000')
    short_brush = pg.mkBrush('#ff0000')

    zoomIsDisabled = QtCore.pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.signals_visible = False
        self.style = ChartType.BAR
        self.indicators = []

        self.xaxis = FromTimeFieldDateAxis(orientation='bottom')
        # self.xaxis = pg.DateAxisItem()

        self.xaxis_ind = FromTimeFieldDateAxis(orientation='bottom')

        if _xaxis_at == 'bottom':
            self.xaxis.setStyle(showValues=False)
        else:
            self.xaxis_ind.setStyle(showValues=False)

        self.layout = QtGui.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)

        self.splitter = QtGui.QSplitter(QtCore.Qt.Vertical)
        self.splitter.setHandleWidth(5)

        self.layout.addWidget(self.splitter)

    def _show_text_signals(self, lbar, rbar):
        signals = [
            sig
            for sig in self.signals_text_items[lbar:rbar]
            if isinstance(sig, CenteredTextItem)
        ]
        if len(signals) <= 50:
            for sig in signals:
                sig.show()
        else:
            for sig in signals:
                sig.hide()

    def _remove_signals(self):
        self.chart.removeItem(self.signals_group_arrow)
        self.chart.removeItem(self.signals_group_text)
        del self.signals_text_items
        del self.signals_group_arrow
        del self.signals_group_text
        self.signals_visible = False

    def _update_sizes(self):
        min_h_ind = int(self.height() * 0.3 / len(self.indicators))
        sizes = [int(self.height() * 0.7)]
        sizes.extend([min_h_ind] * len(self.indicators))
        self.splitter.setSizes(sizes)  # , int(self.height()*0.2)

    def _update_yrange_limits(self):
        """Callback for each y-range update.
        """
        vr = self.chart.viewRect()
        lbar, rbar = int(vr.left()), int(vr.right())

        if self.signals_visible:
            self._show_text_signals(lbar, rbar)

        bars = Quotes[lbar:rbar]
        ylow = bars.low.min() * 0.98
        yhigh = bars.high.max() * 1.02

        std = np.std(bars.close)
        self.chart.setLimits(yMin=ylow, yMax=yhigh, minYRange=std)
        self.chart.setYRange(ylow, yhigh)
        for i, d in self.indicators:
            # ydata = i.plotItem.items[0].getData()[1]
            ydata = d[lbar:rbar]
            ylow = ydata.min() * 0.98
            yhigh = ydata.max() * 1.02
            std = np.std(ydata)
            i.setLimits(yMin=ylow, yMax=yhigh, minYRange=std)
            i.setYRange(ylow, yhigh)

    def plot(self, symbol):
        self.digits = symbol.digits
        self.chart = ChartPlotWidget(
            parent=self.splitter,
            axisItems={'bottom': self.xaxis, 'right': PriceAxis()},
            viewBox=ScrollFromRightView,
            # enableMenu=False,
        )
        self.chart.getPlotItem().setContentsMargins(*CHART_MARGINS)
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)

        # TODO: this is where we would load an indicator chain
        inds = [Quotes.open]

        for d in inds:
            ind = ChartPlotWidget(
                parent=self.splitter,
                axisItems={'bottom': self.xaxis_ind, 'right': PriceAxis()},
                # axisItems={'top': self.xaxis_ind, 'right': PriceAxis()},
                # enableMenu=False,
            )
            ind.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)
            ind.getPlotItem().setContentsMargins(*CHART_MARGINS)
            # self.splitter.addWidget(ind)
            self.indicators.append((ind, d))

        _configure_quotes_chart(
            self.chart,
            self.style,
            self._update_yrange_limits
        )
        _configure_ind_charts(
            self.indicators,
            xlink_to_chart=self.chart
        )
        self._update_sizes()

        ch = CrossHairItem(
            self.chart, [_ind for _ind, d in self.indicators], self.digits
        )
        self.chart.addItem(ch)

    def add_signals(self):
        self.signals_group_text = QtGui.QGraphicsItemGroup()
        self.signals_group_arrow = QtGui.QGraphicsItemGroup()
        self.signals_text_items = np.empty(len(Quotes), dtype=object)

        for p in Portfolio.positions:
            x, price = p.id_bar_open, p.open_price
            if p.type == Order.BUY:
                y = Quotes[x].low * 0.99
                pg.ArrowItem(
                    parent=self.signals_group_arrow,
                    pos=(x, y),
                    pen=self.long_pen,
                    brush=self.long_brush,
                    angle=90,
                    headLen=12,
                    tipAngle=50,
                )
                text_sig = CenteredTextItem(
                    parent=self.signals_group_text,
                    pos=(x, y),
                    pen=self.long_pen,
                    brush=self.long_brush,
                    text=('Buy at {:.%df}' % self.digits).format(price),
                    valign=QtCore.Qt.AlignBottom,
                )
                text_sig.hide()
            else:
                y = Quotes[x].high * 1.01
                pg.ArrowItem(
                    parent=self.signals_group_arrow,
                    pos=(x, y),
                    pen=self.short_pen,
                    brush=self.short_brush,
                    angle=-90,
                    headLen=12,
                    tipAngle=50,
                )
                text_sig = CenteredTextItem(
                    parent=self.signals_group_text,
                    pos=(x, y),
                    pen=self.short_pen,
                    brush=self.short_brush,
                    text=('Sell at {:.%df}' % self.digits).format(price),
                    valign=QtCore.Qt.AlignTop,
                )
                text_sig.hide()

            self.signals_text_items[x] = text_sig

        self.chart.addItem(self.signals_group_arrow)
        self.chart.addItem(self.signals_group_text)
        self.signals_visible = True


# this function is borderline ridiculous.
# The creation of these chart types mutates all the input data
# inside each type's constructor (mind blown)
def _get_chart_points(style):
    if style == ChartType.CANDLESTICK:
        return CandlestickItem()
    elif style == ChartType.BAR:
        return BarItem()
    return pg.PlotDataItem(Quotes.close, pen='b')
