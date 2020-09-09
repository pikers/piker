"""
High level Qt chart widgets.
"""
from typing import Tuple, Dict, Any, Optional
import time

from PyQt5 import QtCore, QtGui
import numpy as np
import pyqtgraph as pg
import tractor
import trio

from ._axes import (
    DynamicDateAxis,
    PriceAxis,
)
from ._graphics import CrossHair, BarItems
from ._axes import YSticky
from ._style import _xaxis_at, _min_points_to_show, hcolor
from ._source import Symbol
from .. import brokers
from .. import data
from ..log import get_logger
from ._exec import run_qtractor
from ._source import ohlc_dtype
from ._interaction import ChartView
from .. import fsp


log = get_logger(__name__)

# margins
CHART_MARGINS = (0, 0, 5, 3)


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
        self.toolbar_layout.setContentsMargins(5, 5, 10, 0)
        self.h_layout = QtGui.QHBoxLayout()

        # self.init_timeframes_ui()
        # self.init_strategy_ui()

        self.v_layout.addLayout(self.toolbar_layout)
        self.v_layout.addLayout(self.h_layout)
        self._chart_cache = {}

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
        self.window.setWindowTitle(f'piker chart {symbol}')
        linkedcharts = self._chart_cache.setdefault(
            symbol,
            LinkedSplitCharts()
        )
        s = Symbol(key=symbol)

        # remove any existing plots
        if not self.h_layout.isEmpty():
            self.h_layout.removeWidget(linkedcharts)

        main_chart = linkedcharts.plot_main(s, data)
        self.h_layout.addWidget(linkedcharts)
        return linkedcharts, main_chart

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
        self.signals_visible: bool = False
        self._array: np.ndarray = None  # main data source
        self._ch: CrossHair = None  # crosshair graphics
        self.chart: ChartPlotWidget = None  # main (ohlc) chart
        self.subplots: Dict[Tuple[str, ...], ChartPlotWidget] = {}

        self.xaxis = DynamicDateAxis(
            orientation='bottom',
            linked_charts=self
        )
        self.xaxis_ind = DynamicDateAxis(
            orientation='bottom',
            linked_charts=self
        )

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
        prop: float = 0.25  # proportion allocated to consumer subcharts
    ) -> None:
        """Set the proportion of space allocated for linked subcharts.
        """
        major = 1 - prop
        min_h_ind = int((self.height() * prop) / len(self.subplots))
        sizes = [int(self.height() * major)]
        sizes.extend([min_h_ind] * len(self.subplots))
        self.splitter.setSizes(sizes)  # , int(self.height()*0.2)

    def plot_main(
        self,
        symbol: Symbol,
        array: np.ndarray,
        ohlc: bool = True,
    ) -> 'ChartPlotWidget':
        """Start up and show main (price) chart and all linked subcharts.
        """
        self.digits = symbol.digits()

        # TODO: this should eventually be a view onto shared mem or some
        # higher level type / API
        self._array = array

        # add crosshairs
        self._ch = CrossHair(
            linkedsplitcharts=self,
            digits=self.digits
        )
        self.chart = self.add_plot(
            name=symbol.key,
            array=array,
            xaxis=self.xaxis,
            ohlc=True,
            _is_main=True,
        )
        # add crosshair graphic
        self.chart.addItem(self._ch)

        # style?
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)

        return self.chart

    def add_plot(
        self,
        name: str,
        array: np.ndarray,
        xaxis: DynamicDateAxis = None,
        ohlc: bool = False,
        _is_main: bool = False,
    ) -> 'ChartPlotWidget':
        """Add (sub)plots to chart widget by name.

        If ``name`` == ``"main"`` the chart will be the the primary view.
        """
        if self.chart is None and not _is_main:
            raise RuntimeError(
                "A main plot must be created first with `.plot_main()`")

        # source of our custom interactions
        cv = ChartView()
        cv.linked_charts = self

        # use "indicator axis" by default
        xaxis = self.xaxis_ind if xaxis is None else xaxis
        cpw = ChartPlotWidget(
            array=array,
            parent=self.splitter,
            axisItems={'bottom': xaxis, 'right': PriceAxis()},
            viewBox=cv,
        )
        # this name will be used to register the primary
        # graphics curve managed by the subchart
        cpw.name = name
        cpw.plotItem.vb.linked_charts = self

        cpw.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)
        cpw.getPlotItem().setContentsMargins(*CHART_MARGINS)

        # link chart x-axis to main quotes chart
        cpw.setXLink(self.chart)

        # draw curve graphics
        if ohlc:
            cpw.draw_ohlc(name, array)
        else:
            cpw.draw_curve(name, array)

        # add to cross-hair's known plots
        self._ch.add_plot(cpw)

        if not _is_main:
            # track by name
            self.subplots[name] = cpw

            # scale split regions
            self.set_split_sizes()

            # XXX: we need this right?
            # self.splitter.addWidget(cpw)

        return cpw


class ChartPlotWidget(pg.PlotWidget):
    """``GraphicsView`` subtype containing a single ``PlotItem``.

    - The added methods allow for plotting OHLC sequences from
      ``np.ndarray``s with appropriate field names.
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
        # the data view we generate graphics from
        array: np.ndarray,
        yrange: Optional[Tuple[float, float]] = None,
        **kwargs,
    ):
        """Configure chart display settings.
        """
        super().__init__(
            background=hcolor('papas_special'),
            # parent=None,
            # plotItem=None,
            # useOpenGL=True,
            **kwargs
        )
        self._array = array  # readonly view of data
        self._graphics = {}  # registry of underlying graphics
        self._labels = {}  # registry of underlying graphics
        self._ysticks = {}  # registry of underlying graphics
        self._yrange = yrange
        self._vb = self.plotItem.vb

        # show only right side axes
        self.hideAxis('left')
        self.showAxis('right')

        # show background grid
        self.showGrid(x=True, y=True, alpha=0.4)

        self.plotItem.vb.setXRange(0, 0)

        # use cross-hair for cursor
        self.setCursor(QtCore.Qt.CrossCursor)

        # Assign callback for rescaling y-axis automatically
        # based on data contents and ``ViewBox`` state.
        self.sigXRangeChanged.connect(self._set_yrange)

        vb = self._vb
        # for mouse wheel which doesn't seem to emit XRangeChanged
        vb.sigRangeChangedManually.connect(self._set_yrange)
        # for when the splitter(s) are resized
        vb.sigResized.connect(self._set_yrange)

    def _update_contents_label(self, index: int) -> None:
        if index > 0 and index < len(self._array):
            for name, (label, update) in self._labels.items():
                update(index)

    def _set_xlimits(
        self,
        xfirst: int,
        xlast: int
    ) -> None:
        """Set view limits (what's shown in the main chart "pane")
        based on max/min x/y coords.
        """
        self.setLimits(
            xMin=xfirst,
            xMax=xlast,
            minXRange=_min_points_to_show,
        )

    def view_range(self) -> Tuple[int, int]:
        vr = self.viewRect()
        return int(vr.left()), int(vr.right())

    def bars_range(self) -> Tuple[int, int, int, int]:
        """Return a range tuple for the bars present in view.
        """
        l, r = self.view_range()
        lbar = max(l, 0)
        rbar = min(r, len(self._array))
        return l, lbar, rbar, r

    def draw_ohlc(
        self,
        name: str,
        data: np.ndarray,
        # XXX: pretty sure this is dumb and we don't need an Enum
        style: pg.GraphicsObject = BarItems,
    ) -> pg.GraphicsObject:
        """Draw OHLC datums to chart.
        """
        graphics = style(self.plotItem)
        # adds all bar/candle graphics objects for each data point in
        # the np array buffer to be drawn on next render cycle
        self.addItem(graphics)
        # draw after to allow self.scene() to work...
        graphics.draw_from_data(data)

        self._graphics[name] = graphics

        # XXX: How to stack labels vertically?
        # Ogi says: "use ..."
        label = pg.LabelItem(
            justify='left',
            size='5pt',
        )
        self.scene().addItem(label)

        def update(index: int) -> None:
            label.setText(
                "{name}[{index}] -> O:{} H:{} L:{} C:{} V:{}".format(
                    *self._array[index].item()[2:],
                    name=name,
                    index=index,
                )
            )

        self._labels[name] = (label, update)
        self._update_contents_label(index=-1)

        # set xrange limits
        xlast = data[-1]['index']

        # show last 50 points on startup
        self.plotItem.vb.setXRange(xlast - 50, xlast + 50)

        self._add_sticky(name)

        return graphics

    def draw_curve(
        self,
        name: str,
        data: np.ndarray,
    ) -> pg.PlotDataItem:
        # draw the indicator as a plain curve
        curve = pg.PlotDataItem(
            data,
            antialias=True,
            # TODO: see how this handles with custom ohlcv bars graphics
            clipToView=True,
        )
        self.addItem(curve)

        # register overlay curve with name
        if not self._graphics and name is None:
            name = 'a_line_bby'

        self._graphics[name] = curve

        # XXX: How to stack labels vertically?
        label = pg.LabelItem(
            justify='left',
            size='5pt',
        )
        self.scene().addItem(label)

        def update(index: int) -> None:
            data = self._array[index]
            label.setText(f"{name} -> {data}")

        self._labels[name] = (label, update)
        self._update_contents_label(index=-1)

        # set a "startup view"
        xlast = len(data) - 1

        # show last 50 points on startup
        self.plotItem.vb.setXRange(xlast - 50, xlast + 50)

        # TODO: we should instead implement a diff based
        # "only update with new items" on the pg.PlotDataItem
        curve.update_from_array = curve.setData

        self._add_sticky(name)

        return curve

    def _add_sticky(
        self,
        name: str,
        # retreive: Callable[None, np.ndarray],
    ) -> YSticky:
        # add y-axis "last" value label
        last = self._ysticks[name] = YSticky(
            chart=self,
            parent=self.getAxis('right'),
            # digits=0,
            opacity=1,
            color=pg.mkPen(hcolor('gray'))
        )
        return last

    def update_from_array(
        self,
        name: str,
        array: np.ndarray,
        **kwargs,
    ) -> pg.GraphicsObject:

        graphics = self._graphics[name]
        graphics.update_from_array(array, **kwargs)

        return graphics

    def _set_yrange(
        self,
    ) -> None:
        """Set the viewable y-range based on embedded data.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.
        """
        l, lbar, rbar, r = self.bars_range()

        # figure out x-range in view such that user can scroll "off" the data
        # set up to the point where ``_min_points_to_show`` are left.
        # if l < lbar or r > rbar:
        bars_len = rbar - lbar
        view_len = r - l
        # TODO: logic to check if end of bars in view
        extra = view_len - _min_points_to_show
        begin = 0 - extra
        end = len(self._array) - 1 + extra

        log.trace(
            f"\nl: {l}, lbar: {lbar}, rbar: {rbar}, r: {r}\n"
            f"view_len: {view_len}, bars_len: {bars_len}\n"
            f"begin: {begin}, end: {end}, extra: {extra}"
        )
        self._set_xlimits(begin, end)

        # TODO: this should be some kind of numpy view api
        bars = self._array[lbar:rbar]
        if not len(bars):
            # likely no data loaded yet
            log.error(f"WTF bars_range = {lbar}:{rbar}")
            return

        # TODO: should probably just have some kinda attr mark
        # that determines this behavior based on array type
        try:
            ylow = bars['low'].min()
            yhigh = bars['high'].max()
            # std = np.std(bars['close'])
        except IndexError:
            # must be non-ohlc array?
            ylow = bars.min()
            yhigh = bars.max()
            # std = np.std(bars)

        # view margins: stay within 10% of the "true range"
        diff = yhigh - ylow
        ylow = ylow - (diff * 0.04)
        yhigh = yhigh + (diff * 0.01)

        # compute contents label "height" in view terms
        if self._labels:
            label = self._labels[self.name][0]
            rect = label.itemRect()
            tl, br = rect.topLeft(), rect.bottomRight()
            vb = self.plotItem.vb
            top, bottom = (vb.mapToView(tl).y(), vb.mapToView(br).y())
            label_h = top - bottom
            # print(f'label height {self.name}: {label_h}')
        else:
            label_h = 0

        chart = self
        chart.setLimits(
            yMin=ylow,
            yMax=yhigh + label_h,
            # minYRange=std
        )
        chart.setYRange(ylow, yhigh + label_h)

    def enterEvent(self, ev):  # noqa
        # pg.PlotWidget.enterEvent(self, ev)
        self.sig_mouse_enter.emit(self)

    def leaveEvent(self, ev):  # noqa
        # pg.PlotWidget.leaveEvent(self, ev)
        self.sig_mouse_leave.emit(self)
        self.scene().leaveEvent(ev)


async def add_new_bars(delay_s, linked_charts):
    """Task which inserts new bars into the ohlc every ``delay_s`` seconds.
    """
    # TODO: right now we'll spin printing bars if the last time
    # stamp is before a large period of no market activity.
    # Likely the best way to solve this is to make this task
    # aware of the instrument's tradable hours?

    # adjust delay to compensate for trio processing time
    ad = delay_s - 0.002

    price_chart = linked_charts.chart
    ohlc = price_chart._array

    async def sleep():
        """Sleep until next time frames worth has passed from last bar.
        """
        last_ts = ohlc[-1]['time']
        delay = max((last_ts + ad) - time.time(), 0)
        await trio.sleep(delay)

    # sleep for duration of current bar
    await sleep()

    while True:
        # TODO: bunch of stuff:
        # - I'm starting to think all this logic should be
        #   done in one place and "graphics update routines"
        #   should not be doing any length checking and array diffing.
        # - don't keep appending, but instead increase the
        #   underlying array's size less frequently
        # - handle odd lot orders
        # - update last open price correctly instead
        #   of copying it from last bar's close
        # - 5 sec bar lookback-autocorrection like tws does?

        def incr_ohlc_array(array: np.ndarray):
            (index, t, close) = array[-1][['index', 'time', 'close']]
            new_array = np.append(
                array,
                np.array(
                    [(index + 1, t + delay_s, close, close,
                      close, close, 0)],
                    dtype=array.dtype
                ),
            )
            return new_array

        # add new increment/bar
        start = time.time()
        ohlc = price_chart._array = incr_ohlc_array(ohlc)
        diff = time.time() - start
        print(f'array append took {diff}')

        # TODO: generalize this increment logic
        for name, chart in linked_charts.subplots.items():
            data = chart._array
            chart._array = np.append(
                data,
                np.array(data[-1], dtype=data.dtype)
            )

        # read value at "open" of bar
        # last_quote = ohlc[-1]
        # XXX: If the last bar has not changed print a flat line and
        # move to the next. This is a "animation" choice that we may not
        # keep.
        # if last_quote == ohlc[-1]:
            # log.debug("Printing flat line for {sym}")

        # update chart historical bars graphics
        price_chart.update_from_array(
            price_chart.name,
            ohlc,
            just_history=True
        )
        # resize view
        price_chart._set_yrange()

        for name, chart in linked_charts.subplots.items():
            chart.update_from_array(chart.name, chart._array)
            chart._set_yrange()

        # We **don't** update the bar right now
        # since the next quote that arrives should in the
        # tick streaming task
        await sleep()

        # TODO: should we update a graphics again time here?
        # Think about race conditions with data update task.
        # UPDATE: don't think this should matter know since the last bar
        # and the prior historical bars are being updated in 2 separate
        # steps now.


async def _async_main(
    sym: str,
    brokername: str,

    # implicit required argument provided by ``qtractor_run()``
    widgets: Dict[str, Any],

    # all kwargs are passed through from the CLI entrypoint
    loglevel: str = None,
) -> None:
    """Main Qt-trio routine invoked by the Qt loop with
    the widgets ``dict``.
    """
    chart_app = widgets['main']

    # historical data fetch
    brokermod = brokers.get_brokermod(brokername)

    async with brokermod.get_client() as client:
        # figure out the exact symbol
        bars = await client.bars(symbol=sym)

    # remember, msgpack-numpy's ``from_buffer` returns read-only array
    bars = np.array(bars[list(ohlc_dtype.names)])

    # load in symbol's ohlc data
    linked_charts, chart = chart_app.load_symbol(sym, bars)

    # determine ohlc delay between bars
    times = bars['time']

    # find expected time step between datums
    delay = times[-1] - times[times != times[-1]][-1]

    async with trio.open_nursery() as n:

        # load initial fsp chain (otherwise known as "indicators")
        n.start_soon(
            chart_from_fsp,
            linked_charts,
            'rsi',
            sym,
            bars,
            brokermod,
            loglevel,
        )

        # update last price sticky
        last_price_sticky = chart._ysticks[chart.name]
        last_price_sticky.update_from_data(
            *chart._array[-1][['index', 'close']]
        )

        # graphics update loop

        async with data.open_feed(
            brokername,
            [sym],
            loglevel=loglevel,
        ) as (fquote, stream):

            # wait for a first quote before we start any update tasks
            quote = await stream.__anext__()
            log.info(f'RECEIVED FIRST QUOTE {quote}')

            # start graphics tasks after receiving first live quote
            n.start_soon(add_new_bars, delay, linked_charts)

            async for quotes in stream:
                for sym, quote in quotes.items():
                    ticks = quote.get('ticks', ())
                    for tick in ticks:
                        if tick.get('type') == 'trade':

                            # TODO: eventually we'll want to update
                            # bid/ask labels and other data as
                            # subscribed by underlying UI consumers.
                            # last = quote.get('last') or quote['close']
                            last = tick['price']

                            # update ohlc (I guess we're enforcing this
                            # for now?) overwrite from quote
                            high, low = chart._array[-1][['high', 'low']]
                            chart._array[['high', 'low', 'close']][-1] = (
                                max(high, last),
                                min(low, last),
                                last,
                            )
                            chart.update_from_array(
                                chart.name,
                                chart._array,
                            )
                            # update sticky(s)
                            last_price_sticky.update_from_data(
                                *chart._array[-1][['index', 'close']])
                            chart._set_yrange()


async def chart_from_fsp(
    linked_charts,
    func_name,
    sym,
    bars,
    brokermod,
    loglevel,
) -> None:
    """Start financial signal processing in subactor.

    Pass target entrypoint and historical data.
    """
    async with tractor.open_nursery() as n:
        portal = await n.run_in_actor(
            f'fsp.{func_name}',  # name as title of sub-chart

            # subactor entrypoint
            fsp.stream_and_process,
            bars=bars,
            brokername=brokermod.name,
            symbol=sym,
            fsp_func_name=func_name,

            # tractor config
            loglevel=loglevel,
        )

        stream = await portal.result()

        # receive processed historical data-array as first message
        history = (await stream.__anext__())

        # TODO: enforce type checking here
        newbars = np.array(history)

        chart = linked_charts.add_plot(
            name=func_name,
            array=newbars,
        )

        # check for data length mis-allignment and fill missing values
        diff = len(chart._array) - len(linked_charts.chart._array)
        if diff < 0:
            data = chart._array
            chart._array = np.append(
                data,
                np.full(abs(diff), data[-1], dtype=data.dtype)
            )

        value = chart._array[-1]
        last_val_sticky = chart._ysticks[chart.name]
        last_val_sticky.update_from_data(-1, value)

        # update chart graphics
        async for value in stream:
            chart._array[-1] = value
            last_val_sticky.update_from_data(-1, value)
            chart._set_yrange()
            chart.update_from_array(chart.name, chart._array)
            chart._set_yrange()


def _main(
    sym: str,
    brokername: str,
    tractor_kwargs,
) -> None:
    """Sync entry point to start a chart app.
    """
    # Qt entry point
    run_qtractor(
       func=_async_main,
       args=(sym, brokername),
       main_widget=ChartSpace,
       tractor_kwargs=tractor_kwargs,
    )
