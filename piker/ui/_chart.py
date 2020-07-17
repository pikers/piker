"""
High level Qt chart widgets.
"""
from typing import List, Optional, Tuple
import time

from PyQt5 import QtCore, QtGui
from pyqtgraph import functions as fn
import numpy as np
import pyqtgraph as pg
import tractor
import trio

from ._axes import (
    DynamicDateAxis,
    PriceAxis,
)
from ._graphics import CrossHair, ChartType
from ._style import _xaxis_at
from ._source import Symbol
from .. import brokers
from .. import data
from ..log import get_logger


log = get_logger(__name__)

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
        self.window.setWindowTitle(f'piker chart {symbol}')
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
        self.signals_visible: bool = False
        self._array: np.ndarray = None  # main data source
        self._ch: CrossHair = None  # crosshair graphics
        self.chart: ChartPlotWidget = None  # main (ohlc) chart
        self.subplots: List[ChartPlotWidget] = []

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
        min_h_ind = int(self.height() * prop / len(self.subplots))
        sizes = [int(self.height() * major)]
        sizes.extend([min_h_ind] * len(self.subplots))
        self.splitter.setSizes(sizes)  # , int(self.height()*0.2)

    def plot(
        self,
        symbol: Symbol,
        array: np.ndarray,
        ohlc: bool = True,
    ) -> None:
        """Start up and show main (price) chart and all linked subcharts.
        """
        self.digits = symbol.digits()

        # XXX: this will eventually be a view onto shared mem
        # or some higher level type / API
        self._array = array

        # add crosshairs
        self._ch = CrossHair(
            parent=self,  #.chart,
            # subplots=[plot for plot, d in self.subplots],
            digits=self.digits
        )
        self.chart = self.add_plot(
            name='main',
            array=array,  #['close'],
            xaxis=self.xaxis,
            ohlc=True,
        )
        self.chart.addItem(self._ch)
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)

        # TODO: this is where we would load an indicator chain
        # XXX: note, if this isn't index aligned with
        # the source data the chart will go haywire.
        inds = [('open', lambda a: a['close'])]

        for name, func in inds:

            # compute historical subchart values from input array
            data = func(array)

            # create sub-plot
            ind_chart = self.add_plot(name=name, array=data)

            self.subplots.append((ind_chart, func))

        # scale split regions
        self.set_split_sizes()

    def add_plot(
        self,
        name: str,
        array: np.ndarray,
        xaxis: DynamicDateAxis = None,
        ohlc: bool = False,
    ) -> 'ChartPlotWidget':
        """Add (sub)plots to chart widget by name.

        If ``name`` == ``"main"`` the chart will be the the primary view.
        """
        cv = ChartView()
        # use "indicator axis" by default
        xaxis = self.xaxis_ind if xaxis is None else xaxis
        cpw = ChartPlotWidget(
            linked_charts=self,
            parent=self.splitter,
            axisItems={'bottom': xaxis, 'right': PriceAxis()},
            # axisItems={'top': self.xaxis_ind, 'right': PriceAxis()},
            viewBox=cv,
        )
        # this name will be used to register the primary
        # graphics curve managed by the subchart
        cpw.name = name
        cv.linked_charts = self
        cpw.plotItem.vb.linked_charts = self

        cpw.setFrameStyle(
            QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain
        )
        cpw.getPlotItem().setContentsMargins(*CHART_MARGINS)
        # self.splitter.addWidget(cpw)

        # link chart x-axis to main quotes chart
        cpw.setXLink(self.chart)

        # draw curve graphics
        if ohlc:
            cpw.draw_ohlc(array)
        else:
            cpw.draw_curve(array, name)

        # add to cross-hair's known plots
        self._ch.add_plot(cpw)

        return cpw

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
        last = quote.get('last') or quote['close']
        index, time, open, high, low, close, volume = self._array[-1]

        # update ohlc (I guess we're enforcing this for now?)
        # overwrite from quote
        self._array[-1] = (
            index,
            time,
            open,
            max(high, last),
            min(low, last),
            last,
            volume,
        )
        self.update_from_array(self._array)

    def update_from_array(
        self,
        array: np.ndarray,
        **kwargs,
    ) -> None:
        """Update all linked chart graphics with a new input array.

        Return the modified graphics objects in a list.
        """
        # update the ohlc sequence graphics chart
        # we send a reference to the whole updated array
        self.chart.update_from_array(array, **kwargs)

        # TODO: the "data" here should really be a function
        # and it should be managed and computed outside of this UI
        graphics = []
        for chart, func in self.subplots:
            # process array in entirely every update
            # TODO: change this for streaming
            data = func(array)
            graphic = chart.update_from_array(data, name=chart.name, **kwargs)
            graphics.append(graphic)

        return graphics


_min_points_to_show = 3


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

        # XXX: label setting doesn't seem to work?
        # likely custom graphics need special handling
        # label = pg.LabelItem(justify='left')
        # self.addItem(label)
        # label.setText("Yo yoyo")
        # label.setText("<span style='font-size: 12pt'>x=")

        # to be filled in when graphics are rendered by name
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
        rbar = min(r, len(self.parent._array))
        return l, lbar, rbar, r

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
        xlast = data[-1]['index']
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
        xlast = len(data) - 1

        # show last 50 points on startup
        self.plotItem.vb.setXRange(xlast - 50, xlast + 50)

        # TODO: we should instead implement a diff based
        # "only update with new items" on the pg.PlotDataItem
        curve.update_from_array = curve.setData

        return curve

    def update_from_array(
        self,
        array: np.ndarray,
        name: str = 'main',
        **kwargs,
    ) -> pg.GraphicsObject:
        graphics = self._graphics[name]
        graphics.update_from_array(array, **kwargs)

        # update view
        self._set_yrange()

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
        end = len(self.parent._array) - 1 + extra

        log.trace(
            f"\nl: {l}, lbar: {lbar}, rbar: {rbar}, r: {r}\n"
            f"view_len: {view_len}, bars_len: {bars_len}\n"
            f"begin: {begin}, end: {end}, extra: {extra}"
        )
        self._set_xlimits(begin, end)

        # TODO: this should be some kind of numpy view api
        bars = self.parent._array[lbar:rbar]
        if not len(bars):
            # likely no data loaded yet
            print(f"WTF bars_range = {lbar}:{rbar}")
            return
        elif lbar < 0:
            breakpoint()

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

        # view margins: stay within 10% of the "true range"
        diff = yhigh - ylow
        ylow = ylow - (diff * 0.1)
        yhigh = yhigh + (diff * 0.1)

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
        l, lbar, rbar, r = self.linked_charts.chart.bars_range()
        vl = r - l

        if ev.delta() > 0 and vl <= _min_points_to_show:
            log.trace("Max zoom bruh...")
            return
        if ev.delta() < 0 and vl >= len(self.linked_charts._array):
            log.trace("Min zoom bruh...")
            return

        # actual scaling factor
        s = 1.015 ** (ev.delta() * -1 / 20)  # self.state['wheelScaleFactor'])
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


def _main(
    sym: str,
    brokername: str,
    **qtractor_kwargs,
) -> None:
    """Entry point to spawn a chart app.
    """
    from ._exec import run_qtractor
    from ._source import ohlc_dtype

    async def _main(widgets):
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
        linked_charts = chart_app.load_symbol(sym, bars)

        # determine ohlc delay between bars
        times = bars['time']
        delay = times[-1] - times[-2]

        async def add_new_bars(delay_s):
            """Task which inserts new bars into the ohlc every ``delay_s`` seconds.
            """
            # adjust delay to compensate for trio processing time
            ad = delay_s - 0.002

            ohlc = linked_charts._array

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
                (index, t, close) = ohlc[-1][['index', 'time', 'close']]
                new = np.append(
                    ohlc,
                    np.array(
                        [(index + 1, t + delay, close, close,
                          close, close, 0)],
                        dtype=ohlc.dtype
                    ),
                )
                ohlc = linked_charts._array = new
                last_quote = ohlc[-1]

                # we **don't** update the bar right now
                # since the next quote that arrives should
                await sleep()

                # if the last bar has not changed print a flat line and
                # move to the next
                if last_quote == ohlc[-1]:
                    log.debug("Printing flat line for {sym}")
                    linked_charts.update_from_array(ohlc)

        async def stream_to_chart(func):

            async with tractor.open_nursery() as n:
                portal = await n.run_in_actor(
                    f'fsp_{func.__name__}',
                    func,
                    brokername=brokermod.name,
                    sym=sym,
                    loglevel='info',
                )
                stream = await portal.result()

                # retreive named layout and style instructions
                layout = await stream.__anext__()

                async for quote in stream:
                    ticks = quote.get('ticks')
                    if ticks:
                        for tick in ticks:
                            print(tick)

        async with trio.open_nursery() as n:
            from piker import fsp

            async with data.open_feed(brokername, [sym]) as stream:
                # start graphics tasks
                n.start_soon(add_new_bars, delay)
                n.start_soon(stream_to_chart, fsp.broker_latency)

                async for quote in stream:
                    # XXX: why are we getting both of these again?
                    ticks = quote.get('ticks')
                    if ticks:
                        for tick in ticks:
                            if tick['tickType'] in (48, 77):
                                linked_charts.update_from_quote(
                                    {'last': tick['price']}
                                )
                    # else:
                    #     linked_charts.update_from_quote(
                    #         {'last': quote['close']}
                    #     )

    run_qtractor(_main, (), ChartSpace, **qtractor_kwargs)
