"""
High level Qt chart widgets.
"""
import trio
import numpy as np
import pyqtgraph as pg
from pyqtgraph import functions as fn
from PyQt5 import QtCore, QtGui

from ._axes import (
    FromTimeFieldDateAxis,
    PriceAxis,
)
from ._graphics import CrossHairItem, ChartType
from ._style import _xaxis_at
from ._source import Symbol, ohlc_zeros


# margins
CHART_MARGINS = (0, 0, 10, 3)


class ChartSpace(QtGui.QWidget):
    """High level widget which contains layouts for organizing
    lower level charts as well as other widgets used to control
    or modify them.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.v_layout = QtGui.QVBoxLayout(self)
        self.v_layout.setContentsMargins(0, 0, 0, 0)
        self.toolbar_layout = QtGui.QHBoxLayout()
        self.toolbar_layout.setContentsMargins(10, 10, 15, 0)
        self.h_layout = QtGui.QHBoxLayout()

        # self.init_timeframes_ui()
        # self.init_strategy_ui()

        self.v_layout.addLayout(self.toolbar_layout)
        self.v_layout.addLayout(self.h_layout)
        self._plot_cache = {}

    def init_timeframes_ui(self):
        self.tf_layout = QtGui.QHBoxLayout()
        self.tf_layout.setSpacing(0)
        self.tf_layout.setContentsMargins(0, 12, 0, 0)
        time_frames = ('1M', '5M', '15M', '30M', '1H', '1D', '1W', 'MN')
        btn_prefix = 'TF'
        for tf in time_frames:
            btn_name = ''.join([btn_prefix, tf])
            btn = QtGui.QPushButton(tf)
            # TODO:
            btn.setEnabled(False)
            setattr(self, btn_name, btn)
            self.tf_layout.addWidget(btn)
        self.toolbar_layout.addLayout(self.tf_layout)

    # XXX: strat loader/saver that we don't need yet.
    # def init_strategy_ui(self):
    #     self.strategy_box = StrategyBoxWidget(self)
    #     self.toolbar_layout.addWidget(self.strategy_box)

    def load_symbol(
        self,
        symbol: str,
        data: np.ndarray,
    ) -> None:
        """Load a new contract into the charting app.
        """
        # XXX: let's see if this causes mem problems
        self.chart = self._plot_cache.setdefault(symbol, LinkedSplitCharts())
        s = Symbol(key=symbol)

        # remove any existing plots
        if not self.h_layout.isEmpty():
            self.h_layout.removeWidget(self.chart)

        self.chart.plot(s, data)
        self.h_layout.addWidget(self.chart)
        return self.chart

    # TODO: add signalling painter system
    # def add_signals(self):
    #     self.chart.add_signals()


class LinkedSplitCharts(QtGui.QWidget):
    """Widget that holds a price chart plus indicators separated by splitters.
    """

    long_pen = pg.mkPen('#006000')
    long_brush = pg.mkBrush('#00ff00')
    short_pen = pg.mkPen('#600000')
    short_brush = pg.mkBrush('#ff0000')

    zoomIsDisabled = QtCore.pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.signals_visible = False
        self.indicators = []

        self.xaxis = FromTimeFieldDateAxis(orientation='bottom', splitter=self)
        # self.xaxis = pg.DateAxisItem()

        self.xaxis_ind = FromTimeFieldDateAxis(
            orientation='bottom', splitter=self)

        if _xaxis_at == 'bottom':
            self.xaxis.setStyle(showValues=False)
        else:
            self.xaxis_ind.setStyle(showValues=False)

        self.splitter = QtGui.QSplitter(QtCore.Qt.Vertical)
        self.splitter.setHandleWidth(5)

        self.layout = QtGui.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)

        self.layout.addWidget(self.splitter)

    def _update_sizes(self):
        min_h_ind = int(self.height() * 0.2 / len(self.indicators))
        sizes = [int(self.height() * 0.8)]
        sizes.extend([min_h_ind] * len(self.indicators))
        self.splitter.setSizes(sizes)  # , int(self.height()*0.2)

    def plot(
        self,
        symbol: Symbol,
        data: np.ndarray,
    ):
        """Start up and show price chart and all registered indicators.
        """
        self.digits = symbol.digits()

        cv = ChartView()
        self.chart = ChartPlotWidget(
            split_charts=self,
            parent=self.splitter,
            axisItems={'bottom': self.xaxis, 'right': PriceAxis()},
            viewBox=cv,
            # enableMenu=False,
        )
        # TODO: ``pyqtgraph`` doesn't pass through a parent to the
        # ``PlotItem`` by default; maybe we should PR this in?
        cv.splitter_widget = self
        self.chart.plotItem.vb.splitter_widget = self

        self.chart.getPlotItem().setContentsMargins(*CHART_MARGINS)
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)

        self.chart.draw_ohlc(data)

        # TODO: this is where we would load an indicator chain
        # XXX: note, if this isn't index aligned with
        # the source data the chart will go haywire.
        inds = [data.open]

        for d in inds:
            cv = ChartView()
            ind_chart = ChartPlotWidget(
                split_charts=self,
                parent=self.splitter,
                axisItems={'bottom': self.xaxis_ind, 'right': PriceAxis()},
                # axisItems={'top': self.xaxis_ind, 'right': PriceAxis()},
                viewBox=cv,
            )
            cv.splitter_widget = self
            self.chart.plotItem.vb.splitter_widget = self

            ind_chart.setFrameStyle(
                QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain
            )
            ind_chart.getPlotItem().setContentsMargins(*CHART_MARGINS)
            # self.splitter.addWidget(ind_chart)
            self.indicators.append((ind_chart, d))

            # link chart x-axis to main quotes chart
            ind_chart.setXLink(self.chart)

            # XXX: never do this lol
            # ind.setAspectLocked(1)
            ind_chart.draw_curve(d)

        self._update_sizes()

        ch = CrossHairItem(
            self.chart, [_ind for _ind, d in self.indicators], self.digits
        )
        self.chart.addItem(ch)


_min_points_to_show = 15
_min_bars_in_view = 10


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

    # TODO: can take a ``background`` color setting - maybe there's
    # a better one?

    def __init__(
        self,
        split_charts,
        **kwargs,
        # parent=None,
        # background='default',
        # plotItem=None,
        # **kargs
    ):
        """Configure chart display settings.
        """
        super().__init__(**kwargs)
        # XXX: label setting doesn't seem to work?
        # likely custom graphics need special handling

        # label = pg.LabelItem(justify='left')
        # self.addItem(label)
        # label.setText("Yo yoyo")
        # label.setText("<span style='font-size: 12pt'>x=")
        self.parent = split_charts

        # placeholder for source of data
        self._array = ohlc_zeros(1)

        # to be filled in when data is loaded
        self._graphics = {}

        # show only right side axes
        self.hideAxis('left')
        self.showAxis('right')

        # show background grid
        self.showGrid(x=True, y=True, alpha=0.4)

        self.plotItem.vb.setXRange(0, 0)

        # use cross-hair for cursor
        self.setCursor(QtCore.Qt.CrossCursor)

        # assign callback for rescaling y-axis automatically
        # based on y-range contents
        self.sigXRangeChanged.connect(self._update_yrange_limits)

    def set_view_limits(self, xfirst, xlast, ymin, ymax):
        # max_lookahead = _min_points_to_show - _min_bars_in_view

        # set panning limits
        # last = data[-1]['id']
        self.setLimits(
            # xMin=data[0]['id'],
            xMin=xfirst,
            # xMax=last + _min_points_to_show - 3,
            xMax=xlast + _min_points_to_show - 3,
            minXRange=_min_points_to_show,
            # maxYRange=highest-lowest,
            # yMin=data['low'].min() * 0.98,
            # yMax=data['high'].max() * 1.02,
            yMin=ymin * 0.98,
            yMax=ymax * 1.02,
        )

        # show last 50 points on startup
        # self.plotItem.vb.setXRange(last - 50, last + 50)
        self.plotItem.vb.setXRange(xlast - 50, xlast + 50)

        # fit y
        self._update_yrange_limits()

    def bars_range(self):
        """Return a range tuple for the bars present in view.
        """
        vr = self.viewRect()
        lbar = int(vr.left())
        rbar = int(min(vr.right(), len(self._array) - 1))
        return lbar, rbar

    def draw_ohlc(
        self,
        data: np.ndarray,
        # XXX: pretty sure this is dumb and we don't need an Enum
        style: ChartType = ChartType.BAR,
    ) -> None:
        """Draw OHLC datums to chart.
        """
        # remember it's an enum type..
        graphics = style.value()

        # adds all bar/candle graphics objects for each data point in
        # the np array buffer to be drawn on next render cycle
        graphics.draw_from_data(data)
        self._graphics['ohlc'] = graphics
        self.addItem(graphics)
        self._array = data

        # update view limits
        self.set_view_limits(
            data[0]['index'],
            data[-1]['index'],
            data['low'].min(),
            data['high'].max()
        )

        return graphics

    def draw_curve(
        self,
        data: np.ndarray,
    ) -> None:
        # draw the indicator as a plain curve
        curve = pg.PlotDataItem(data, antialias=True)
        self.addItem(curve)

        # update view limits
        self.set_view_limits(0, len(data)-1, data.min(), data.max())
        self._array = data

        return curve

    def _update_yrange_limits(self):
        """Callback for each y-range update.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.
        """
        # TODO: this can likely be ported in part to the built-ins:
        # self.setYRange(Quotes.low.min() * .98, Quotes.high.max() * 1.02)
        # self.setMouseEnabled(x=True, y=False)
        # self.setXRange(Quotes[0].id, Quotes[-1].id)
        # self.setAutoVisible(x=False, y=True)
        # self.enableAutoRange(x=False, y=True)

        lbar, rbar = self.bars_range()

        # if chart_parent.signals_visible:
        #     chart_parent._show_text_signals(lbar, rbar)

        bars = self._array[lbar:rbar]
        if not len(bars):
            # likely no data loaded yet
            return

        # TODO: should probably just have some kinda attr mark
        # that determines this behavior based on array type
        try:
            ylow = bars['low'].min()
            yhigh = bars['high'].max()
            std = np.std(bars['close'])
        except IndexError:
            # must be non-ohlc array?
            ylow = bars.min()
            yhigh = bars.max()
            std = np.std(bars)

        # view margins
        ylow *= 0.98
        yhigh *= 1.02

        chart = self
        chart.setLimits(
            yMin=ylow,
            yMax=yhigh,
            minYRange=std
        )
        chart.setYRange(ylow, yhigh)

    def enterEvent(self, ev):  # noqa
        # pg.PlotWidget.enterEvent(self, ev)
        self.sig_mouse_enter.emit(self)

    def leaveEvent(self, ev):  # noqa
        # pg.PlotWidget.leaveEvent(self, ev)
        self.sig_mouse_leave.emit(self)
        self.scene().leaveEvent(ev)


class ChartView(pg.ViewBox):
    """Price chart view box with interaction behaviors you'd expect from
    an interactive platform:

    - zoom on mouse scroll that auto fits y-axis
    - no vertical scrolling
    - zoom to a "fixed point" on the y-axis
    """
    def __init__(
        self,
        parent=None,
        **kwargs,
        # invertY=False,
    ):
        super().__init__(parent=parent, **kwargs)
        # disable vertical scrolling
        self.setMouseEnabled(x=True, y=False)
        self.splitter_widget = None

    def wheelEvent(self, ev, axis=None):
        """Override "center-point" location for scrolling.

        This is an override of the ``ViewBox`` method simply changing
        the center of the zoom to be the y-axis.

        TODO: PR a method into ``pyqtgraph`` to make this configurable
        """

        if axis in (0, 1):
            mask = [False, False]
            mask[axis] = self.state['mouseEnabled'][axis]
        else:
            mask = self.state['mouseEnabled'][:]

        # don't zoom more then the min points setting
        lbar, rbar = self.splitter_widget.chart.bars_range()
        # breakpoint()
        if ev.delta() >= 0 and rbar - lbar <= _min_points_to_show:
            return

        # actual scaling factor
        s = 1.02 ** (ev.delta() * -1/10)  # self.state['wheelScaleFactor'])
        s = [(None if m is False else s) for m in mask]

        # center = pg.Point(
        #     fn.invertQTransform(self.childGroup.transform()).map(ev.pos())
        # )

        # XXX: scroll "around" the right most element in the view
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


def main(symbol):
    """Entry point to spawn a chart app.
    """
    from datetime import datetime

    from ._exec import run_qtrio
    from ._source import from_df
    # uses pandas_datareader
    from .quantdom.loaders import get_quotes

    async def _main(widgets):
        """Main Qt-trio routine invoked by the Qt loop with
        the widgets ``dict``.
        """

        chart_app = widgets['main']
        quotes = get_quotes(
            symbol=symbol,
            date_from=datetime(1900, 1, 1),
            date_to=datetime(2030, 12, 31),
        )
        quotes = from_df(quotes)

        # spawn chart
        splitter_chart = chart_app.load_symbol(symbol, quotes)
        import itertools
        nums = itertools.cycle([315., 320., 325., 310., 3])

        def gen_nums():
            for i in itertools.count():
                yield quotes[-1].close + i
                yield quotes[-1].close - i

        chart = splitter_chart.chart

        nums = gen_nums()
        while True:
            await trio.sleep(0.1)
            new = next(nums)
            quotes[-1].close = new
            chart._graphics['ohlc'].update_last_bar({'last': new})

            # LOL this clearly isn't catching edge cases
            chart._update_yrange_limits()

        await trio.sleep_forever()

    run_qtrio(_main, (), ChartSpace)
