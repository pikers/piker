"""
High level Qt chart widgets.
"""
from typing import List, Optional

import trio
import numpy as np
import pyqtgraph as pg
from pyqtgraph import functions as fn
from PyQt5 import QtCore, QtGui

from ._axes import (
    DynamicDateAxis,
    PriceAxis,
)
from ._graphics import CrossHairItem, ChartType
from ._style import _xaxis_at
from ._source import Symbol


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
    """Widget that holds a central chart plus derived
    subcharts computed from the original data set apart
    by splitters for resizing.

    A single internal references to the data is maintained
    for each chart and can be updated externally.
    """
    long_pen = pg.mkPen('#006000')
    long_brush = pg.mkBrush('#00ff00')
    short_pen = pg.mkPen('#600000')
    short_brush = pg.mkBrush('#ff0000')

    zoomIsDisabled = QtCore.pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.signals_visible = False

        # main data source
        self._array = None

        self._ch = None  # crosshair graphics
        self._index = 0

        self.chart = None  # main (ohlc) chart
        self.indicators = []

        self.xaxis = DynamicDateAxis(
            orientation='bottom', linked_charts=self)

        self.xaxis_ind = DynamicDateAxis(
            orientation='bottom', linked_charts=self)

        if _xaxis_at == 'bottom':
            self.xaxis.setStyle(showValues=False)
        else:
            self.xaxis_ind.setStyle(showValues=False)

        self.splitter = QtGui.QSplitter(QtCore.Qt.Vertical)
        self.splitter.setHandleWidth(5)

        self.layout = QtGui.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)

        self.layout.addWidget(self.splitter)

    def set_split_sizes(
        self,
        prop: float = 0.2
    ) -> None:
        """Set the proportion of space allocated for linked subcharts.
        """
        major = 1 - prop
        # 20% allocated to consumer subcharts
        min_h_ind = int(self.height() * prop / len(self.indicators))
        sizes = [int(self.height() * major)]
        sizes.extend([min_h_ind] * len(self.indicators))
        self.splitter.setSizes(sizes)  # , int(self.height()*0.2)

    def plot(
        self,
        symbol: Symbol,
        array: np.ndarray,
        ohlc: bool = True,
    ):
        """Start up and show main (price) chart and all linked subcharts.
        """
        self.digits = symbol.digits()

        # XXX: this may eventually be a view onto shared mem
        # or some higher level type / API
        self._array = array

        cv = ChartView()
        self.chart = ChartPlotWidget(
            linked_charts=self,
            parent=self.splitter,
            axisItems={'bottom': self.xaxis, 'right': PriceAxis()},
            viewBox=cv,
            # enableMenu=False,
        )
        # TODO: ``pyqtgraph`` doesn't pass through a parent to the
        # ``PlotItem`` by default; maybe we should PR this in?
        cv.linked_charts = self
        self.chart.plotItem.vb.linked_charts = self

        self.chart.getPlotItem().setContentsMargins(*CHART_MARGINS)
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)

        if ohlc:
            self.chart.draw_ohlc(array)
        else:
            raise NotImplementedError(
                "Only OHLC linked charts are supported currently"
            )

        # TODO: this is where we would load an indicator chain
        # XXX: note, if this isn't index aligned with
        # the source data the chart will go haywire.
        inds = [('open', lambda a: a.close)]

        for name, func in inds:
            cv = ChartView()
            ind_chart = ChartPlotWidget(
                linked_charts=self,
                parent=self.splitter,
                axisItems={'bottom': self.xaxis_ind, 'right': PriceAxis()},
                # axisItems={'top': self.xaxis_ind, 'right': PriceAxis()},
                viewBox=cv,
            )
            # this name will be used to register the primary
            # graphics curve managed by the subchart
            ind_chart.name = name
            cv.linked_charts = self
            self.chart.plotItem.vb.linked_charts = self

            ind_chart.setFrameStyle(
                QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain
            )
            ind_chart.getPlotItem().setContentsMargins(*CHART_MARGINS)
            # self.splitter.addWidget(ind_chart)

            # compute historical subchart values from input array
            data = func(array)
            self.indicators.append((ind_chart, func))

            # link chart x-axis to main quotes chart
            ind_chart.setXLink(self.chart)

            # draw curve graphics
            ind_chart.draw_curve(data, name)

        self.set_split_sizes()

        ch = self._ch = CrossHairItem(
            self.chart,
            [_ind for _ind, d in self.indicators],
            self.digits
        )
        self.chart.addItem(ch)

    def update_from_quote(
        self,
        quote: dict
    ) -> List[pg.GraphicsObject]:
        """Update all linked chart graphics with a new quote
        datum.

        Return the modified graphics objects in a list.
        """
        # TODO: eventually we'll want to update bid/ask labels and other
        # data as subscribed by underlying UI consumers.
        last = quote['last']
        current = self._array[-1]

        # update ohlc (I guess we're enforcing this for now?)
        current['close'] = last
        current['high'] = max(current['high'], last)
        current['low'] = min(current['low'], last)

        # update the ohlc sequence graphics chart
        chart = self.chart
        # we send a reference to the whole updated array
        chart.update_from_array(self._array)

        # TODO: the "data" here should really be a function
        # and it should be managed and computed outside of this UI
        for chart, func in self.indicators:
            # process array in entirely every update
            # TODO: change this for streaming
            data = func(self._array)
            chart.update_from_array(data, chart.name)


_min_points_to_show = 15
_min_bars_in_view = 10


class ChartPlotWidget(pg.PlotWidget):
    """``GraphicsView`` subtype containing a single ``PlotItem``.

    - The added methods allow for plotting OHLC sequences from
      ``np.recarray``s with appropriate field names.
    - Overrides a ``pyqtgraph.PlotWidget`` (a ``GraphicsView`` containing
      a single ``PlotItem``) to intercept and and re-emit mouse enter/exit
      events.

    (Could be replaced with a ``pg.GraphicsLayoutWidget`` if we
    eventually want multiple plots managed together?)
    """
    sig_mouse_leave = QtCore.Signal(object)
    sig_mouse_enter = QtCore.Signal(object)

    # TODO: can take a ``background`` color setting - maybe there's
    # a better one?

    def __init__(
        self,
        linked_charts,
        **kwargs,
        # parent=None,
        # background='default',
        # plotItem=None,
    ):
        """Configure chart display settings.
        """
        super().__init__(**kwargs)
        self.parent = linked_charts
        # this is the index of that last input array entry and is
        # updated and used to figure out how many bars are in view
        self._xlast = 0

        # XXX: label setting doesn't seem to work?
        # likely custom graphics need special handling
        # label = pg.LabelItem(justify='left')
        # self.addItem(label)
        # label.setText("Yo yoyo")
        # label.setText("<span style='font-size: 12pt'>x=")

        # to be filled in when graphics are rendered
        # by name
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
        # based on ohlc contents
        self.sigXRangeChanged.connect(self._set_yrange)

    def _set_xlimits(
        self,
        xfirst: int,
        xlast: int
    ) -> None:
        """Set view limits (what's shown in the main chart "pane")
        based on max / min x / y coords.
        """
        # max_lookahead = _min_points_to_show - _min_bars_in_view

        # set panning limits
        self.setLimits(
            xMin=xfirst,
            xMax=xlast + _min_points_to_show - 3,
            minXRange=_min_points_to_show,
        )

    def bars_range(self):
        """Return a range tuple for the bars present in view.
        """
        vr = self.viewRect()
        lbar = int(vr.left())
        rbar = int(vr.right())
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
        self._graphics['main'] = graphics
        self.addItem(graphics)

        # set xrange limits
        self._xlast = xlast = data[-1]['index']
        self._set_xlimits(data[0]['index'], xlast)

        # show last 50 points on startup
        self.plotItem.vb.setXRange(xlast - 50, xlast + 50)

        return graphics

    def draw_curve(
        self,
        data: np.ndarray,
        name: Optional[str] = None,
    ) -> None:
        # draw the indicator as a plain curve
        curve = pg.PlotDataItem(data, antialias=True)
        self.addItem(curve)

        # register overlay curve with name
        if not self._graphics and name is None:
            name = 'main'
        self._graphics[name] = curve

        # set a "startup view"
        xlast = len(data)-1
        self._set_xlimits(0, xlast)

        # show last 50 points on startup
        self.plotItem.vb.setXRange(xlast - 50, xlast + 50)

        return curve

    def update_from_array(
        self,
        array: np.ndarray,
        name: str = 'main',
    ) -> None:
        self._xlast = len(array) - 1
        graphics = self._graphics[name]
        graphics.setData(array)
        # update view
        self._set_yrange()

    def _set_yrange(
        self,
    ) -> None:
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

        # figure out x-range bars on screen
        lbar, rbar = self.bars_range()

        # TODO: this should be some kind of numpy view api
        bars = self.parent._array[lbar:rbar]
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
    any interactive platform:

        - zoom on mouse scroll that auto fits y-axis
        - no vertical scrolling
        - zoom to a "fixed point" on the y-axis
    """
    def __init__(
        self,
        parent=None,
        **kwargs,
    ):
        super().__init__(parent=parent, **kwargs)
        # disable vertical scrolling
        self.setMouseEnabled(x=True, y=False)
        self.linked_charts = None

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
        lbar, rbar = self.linked_charts.chart.bars_range()
        if ev.delta() >= 0 and rbar - lbar <= _min_points_to_show:
            return

        # actual scaling factor
        s = 1.015 ** (ev.delta() * -1/20)  # self.state['wheelScaleFactor'])
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
        linked_charts = chart_app.load_symbol(symbol, quotes)

        # make some fake update data
        import itertools
        nums = itertools.cycle([315., 320., 325., 310., 3])

        def gen_nums():
            while True:
                yield quotes[-1].close + 1

        nums = gen_nums()

        await trio.sleep(10)
        while True:
            new = next(nums)
            quotes[-1].close = new
            # this updates the linked_charts internal array
            # and then passes that array to all subcharts to
            # render downstream graphics
            linked_charts.update_from_quote({'last': new})
            await trio.sleep(.1)

        await trio.sleep_forever()

    run_qtrio(_main, (), ChartSpace)
