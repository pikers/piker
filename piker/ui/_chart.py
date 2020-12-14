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
High level Qt chart widgets.
"""
from typing import Tuple, Dict, Any, Optional, Callable
from functools import partial

from PyQt5 import QtCore, QtGui
import numpy as np
import pyqtgraph as pg
import tractor
import trio

from ._axes import (
    DynamicDateAxis,
    PriceAxis,
)
from ._graphics import (
    CrossHair,
    ContentsLabel,
    BarItems,
    level_line,
    L1Labels,
)
from ._axes import YSticky
from ._style import (
    _font,
    hcolor,
    CHART_MARGINS,
    _xaxis_at,
    _min_points_to_show,
    _bars_from_right_in_follow_mode,
    _bars_to_left_in_follow_mode,
)
from ..data._source import Symbol, float_digits
from .. import brokers
from .. import data
from ..data import maybe_open_shm_array
from ..log import get_logger
from ._exec import run_qtractor, current_screen
from ._interaction import ChartView
from .. import fsp


log = get_logger(__name__)


class ChartSpace(QtGui.QWidget):
    """High level widget which contains layouts for organizing
    lower level charts as well as other widgets used to control
    or modify them.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.v_layout = QtGui.QVBoxLayout(self)
        self.v_layout.setContentsMargins(0, 0, 0, 0)
        self.v_layout.setSpacing(0)

        self.toolbar_layout = QtGui.QHBoxLayout()
        self.toolbar_layout.setContentsMargins(0, 0, 0, 0)

        self.h_layout = QtGui.QHBoxLayout()
        self.h_layout.setContentsMargins(0, 0, 0, 0)

        # self.init_timeframes_ui()
        # self.init_strategy_ui()
        self.v_layout.addLayout(self.toolbar_layout)
        self.v_layout.addLayout(self.h_layout)
        self._chart_cache = {}
        self.symbol_label: Optional[QtGui.QLabel] = None

    def init_search(self):
        self.symbol_label = label = QtGui.QLabel()
        label.setTextFormat(3)  # markdown
        label.setFont(_font.font)
        label.setMargin(0)
        # title = f'sym:   {self.symbol}'
        # label.setText(title)

        label.setAlignment(
            QtCore.Qt.AlignVCenter
            | QtCore.Qt.AlignLeft
        )
        self.v_layout.addWidget(label)

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
        ohlc: bool = True,
    ) -> None:
        """Load a new contract into the charting app.

        Expects a ``numpy`` structured array containing all the ohlcv fields.
        """
        # XXX: let's see if this causes mem problems
        self.window.setWindowTitle(f'piker chart {symbol}')

        # TODO: symbol search
        # # of course this doesn't work :eyeroll:
        # h = _font.boundingRect('Ag').height()
        # print(f'HEIGHT {h}')
        # self.symbol_label.setFixedHeight(h + 4)
        # self.v_layout.update()
        # self.symbol_label.setText(f'/`{symbol}`')

        linkedcharts = self._chart_cache.setdefault(
            symbol,
            LinkedSplitCharts()
        )
        s = Symbol(key=symbol)

        # remove any existing plots
        if not self.v_layout.isEmpty():
            self.v_layout.removeWidget(linkedcharts)

        main_chart = linkedcharts.plot_ohlc_main(s, data)

        self.v_layout.addWidget(linkedcharts)

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
        self._ch: CrossHair = None  # crosshair graphics
        self.chart: ChartPlotWidget = None  # main (ohlc) chart
        self.subplots: Dict[Tuple[str, ...], ChartPlotWidget] = {}

        self.xaxis = DynamicDateAxis(
            orientation='bottom',
            linked_charts=self
        )
        # if _xaxis_at == 'bottom':
        #     self.xaxis.setStyle(showValues=False)
        #     self.xaxis.hide()
        # else:
        #     self.xaxis_ind.setStyle(showValues=False)
        #     self.xaxis.hide()

        self.splitter = QtGui.QSplitter(QtCore.Qt.Vertical)
        self.splitter.setMidLineWidth(2)
        self.splitter.setHandleWidth(0)

        self.layout = QtGui.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.addWidget(self.splitter)

    def set_split_sizes(
        self,
        prop: float = 0.28  # proportion allocated to consumer subcharts
    ) -> None:
        """Set the proportion of space allocated for linked subcharts.
        """
        major = 1 - prop
        min_h_ind = int((self.height() * prop) / len(self.subplots))
        sizes = [int(self.height() * major)]
        sizes.extend([min_h_ind] * len(self.subplots))
        self.splitter.setSizes(sizes)  # , int(self.height()*0.2)

    def plot_ohlc_main(
        self,
        symbol: Symbol,
        array: np.ndarray,
        style: str = 'bar',
    ) -> 'ChartPlotWidget':
        """Start up and show main (price) chart and all linked subcharts.

        The data input struct array must include OHLC fields.
        """
        self.digits = symbol.digits()

        # add crosshairs
        self._ch = CrossHair(
            linkedsplitcharts=self,
            digits=self.digits
        )
        self.chart = self.add_plot(
            name=symbol.key,
            array=array,
            xaxis=self.xaxis,
            style=style,
            _is_main=True,
        )
        # add crosshair graphic
        self.chart.addItem(self._ch)

        # axis placement
        if _xaxis_at == 'bottom':
            self.chart.hideAxis('bottom')

        # style?
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)

        return self.chart

    def add_plot(
        self,
        name: str,
        array: np.ndarray,
        xaxis: DynamicDateAxis = None,
        style: str = 'line',
        _is_main: bool = False,
        **cpw_kwargs,
    ) -> 'ChartPlotWidget':
        """Add (sub)plots to chart widget by name.

        If ``name`` == ``"main"`` the chart will be the the primary view.
        """
        if self.chart is None and not _is_main:
            raise RuntimeError(
                "A main plot must be created first with `.plot_ohlc_main()`")

        # source of our custom interactions
        cv = ChartView()
        cv.linked_charts = self

        # use "indicator axis" by default
        if xaxis is None:
            xaxis = DynamicDateAxis(
                orientation='bottom',
                linked_charts=self
            )

        cpw = ChartPlotWidget(

            # this name will be used to register the primary
            # graphics curve managed by the subchart
            name=name,

            array=array,
            parent=self.splitter,
            axisItems={
                'bottom': xaxis,
                'right': PriceAxis(linked_charts=self)
            },
            viewBox=cv,
            cursor=self._ch,
            **cpw_kwargs,
        )

        # give viewbox a reference to primary chart
        # allowing for kb controls and interactions
        # (see our custom view in `._interactions.py`)
        cv.chart = cpw

        cpw.plotItem.vb.linked_charts = self
        cpw.setFrameStyle(QtGui.QFrame.StyledPanel)  # | QtGui.QFrame.Plain)
        cpw.hideButtons()
        # XXX: gives us outline on backside of y-axis
        cpw.getPlotItem().setContentsMargins(*CHART_MARGINS)

        # link chart x-axis to main quotes chart
        cpw.setXLink(self.chart)

        # add to cross-hair's known plots
        self._ch.add_plot(cpw)

        # draw curve graphics
        if style == 'bar':
            cpw.draw_ohlc(name, array)

        elif style == 'line':
            cpw.draw_curve(name, array)

        else:
            raise ValueError(f"Chart style {style} is currently unsupported")

        if not _is_main:
            # track by name
            self.subplots[name] = cpw

            # scale split regions
            self.set_split_sizes()

            # XXX: we need this right?
            # self.splitter.addWidget(cpw)
        else:
            assert style == 'bar', 'main chart must be OHLC'

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
        name: str,
        array: np.ndarray,
        static_yrange: Optional[Tuple[float, float]] = None,
        cursor: Optional[CrossHair] = None,
        **kwargs,
    ):
        """Configure chart display settings.
        """
        super().__init__(
            background=hcolor('papas_special'),
            # parent=None,
            # plotItem=None,
            # antialias=True,
            useOpenGL=True,
            **kwargs
        )

        self.name = name

        # self.setViewportMargins(0, 0, 0, 0)
        self._ohlc = array  # readonly view of ohlc data
        self.default_view()

        self._arrays = {}  # readonly view of overlays
        self._graphics = {}  # registry of underlying graphics
        self._overlays = set()  # registry of overlay curve names

        self._labels = {}  # registry of underlying graphics
        self._ysticks = {}  # registry of underlying graphics

        self._vb = self.plotItem.vb
        self._static_yrange = static_yrange  # for "known y-range style"

        self._view_mode: str = 'follow'
        self._cursor = cursor  # placehold for mouse

        # show only right side axes
        self.hideAxis('left')
        self.showAxis('right')

        # show background grid
        self.showGrid(x=True, y=True, alpha=0.5)

        # TODO: stick in config
        # use cross-hair for cursor?
        # self.setCursor(QtCore.Qt.CrossCursor)

        # Assign callback for rescaling y-axis automatically
        # based on data contents and ``ViewBox`` state.
        self.sigXRangeChanged.connect(self._set_yrange)

        # for mouse wheel which doesn't seem to emit XRangeChanged
        self._vb.sigRangeChangedManually.connect(self._set_yrange)

        # for when the splitter(s) are resized
        self._vb.sigResized.connect(self._set_yrange)

    def last_bar_in_view(self) -> bool:
        self._ohlc[-1]['index']

    def update_contents_labels(
        self,
        index: int,
        # array_name: str,
    ) -> None:
        if index >= 0 and index < self._ohlc[-1]['index']:
            for name, (label, update) in self._labels.items():

                if name is self.name:
                    array = self._ohlc
                else:
                    array = self._arrays[name]

                try:
                    update(index, array)
                except IndexError:
                    log.exception(f"Failed to update label: {name}")

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
        a = self._ohlc
        lbar = max(l, a[0]['index'])
        rbar = min(r, a[-1]['index'])
        # lbar = max(l, 0)
        # rbar = min(r, len(self._ohlc))
        return l, lbar, rbar, r

    def default_view(
        self,
        index: int = -1,
    ) -> None:
        """Set the view box to the "default" startup view of the scene.

        """
        xlast = self._ohlc[index]['index']
        print(xlast)
        begin = xlast - _bars_to_left_in_follow_mode
        end = xlast + _bars_from_right_in_follow_mode

        self.plotItem.vb.setXRange(
            min=begin,
            max=end,
            padding=0,
        )

    def increment_view(
        self,
    ) -> None:
        """Increment the data view one step to the right thus "following"
        the current time slot/step/bar.

        """
        l, r = self.view_range()
        self._vb.setXRange(
            min=l + 1,
            max=r + 1,
            # TODO: holy shit, wtf dude... why tf would this not be 0 by
            # default... speechless.
            padding=0,
        )

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

        self.add_contents_label(
            name,
            anchor_at=('top', 'left'),
            update_func=ContentsLabel.update_from_ohlc,
        )
        self.update_contents_labels(len(data) - 1)

        self._add_sticky(name)

        return graphics

    def draw_curve(
        self,
        name: str,
        data: np.ndarray,
        overlay: bool = False,
        color: str = 'default_light',
        add_label: bool = True,
        **pdi_kwargs,
    ) -> pg.PlotDataItem:
        """Draw a "curve" (line plot graphics) for the provided data in
        the input array ``data``.

        """
        _pdi_defaults = {
            'pen': pg.mkPen(hcolor(color)),
        }
        pdi_kwargs.update(_pdi_defaults)

        curve = pg.PlotDataItem(
            y=data[name],
            x=data['index'],
            # antialias=True,
            name=name,

            # TODO: see how this handles with custom ohlcv bars graphics
            # and/or if we can implement something similar for OHLC graphics
            clipToView=True,

            **pdi_kwargs,
        )
        self.addItem(curve)

        # register curve graphics and backing array for name
        self._graphics[name] = curve
        self._arrays[name] = data

        if overlay:
            anchor_at = ('bottom', 'left')
            self._overlays.add(name)

        else:
            anchor_at = ('top', 'left')

            # TODO: something instead of stickies for overlays
            # (we need something that avoids clutter on x-axis).
            self._add_sticky(name, bg_color='default_light')

        if add_label:
            self.add_contents_label(name, anchor_at=anchor_at)
            self.update_contents_labels(len(data) - 1)

        if self._cursor:
            self._cursor.add_curve_cursor(self, curve)

        return curve

    def add_contents_label(
        self,
        name: str,
        anchor_at: Tuple[str, str] = ('top', 'left'),
        update_func: Callable = ContentsLabel.update_from_value,
    ) -> ContentsLabel:

        label = ContentsLabel(chart=self, anchor_at=anchor_at)
        self._labels[name] = (
            # calls class method on instance
            label,
            partial(update_func, label, name)
        )
        label.show()

        return label

    def _add_sticky(
        self,
        name: str,
        bg_color='bracket',
        # retreive: Callable[None, np.ndarray],
    ) -> YSticky:
        # add y-axis "last" value label
        last = self._ysticks[name] = YSticky(
            chart=self,
            parent=self.getAxis('right'),
            # TODO: pass this from symbol data
            # digits=0,
            opacity=1,
            bg_color=bg_color,
        )
        return last

    def update_ohlc_from_array(
        self,
        name: str,
        array: np.ndarray,
        **kwargs,
    ) -> pg.GraphicsObject:
        """Update the named internal graphics from ``array``.

        """
        self._ohlc = array
        graphics = self._graphics[name]
        graphics.update_from_array(array, **kwargs)
        return graphics

    def update_curve_from_array(
        self,
        name: str,
        array: np.ndarray,
        **kwargs,
    ) -> pg.GraphicsObject:
        """Update the named internal graphics from ``array``.

        """
        if name not in self._overlays:
            self._ohlc = array
        else:
            self._arrays[name] = array

        curve = self._graphics[name]

        # TODO: we should instead implement a diff based
        # "only update with new items" on the pg.PlotCurveItem
        # one place to dig around this might be the `QBackingStore`
        # https://doc.qt.io/qt-5/qbackingstore.html
        curve.setData(y=array[name], x=array['index'], **kwargs)

        return curve

    def _set_yrange(
        self,
        *,
        yrange: Optional[Tuple[float, float]] = None,
    ) -> None:
        """Set the viewable y-range based on embedded data.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.

        """
        if self._static_yrange is not None:
            ylow, yhigh = self._static_yrange

        elif yrange is not None:
            ylow, yhigh = yrange

        else:
            # Determine max, min y values in viewable x-range from data.
            # Make sure min bars/datums on screen is adhered.

            l, lbar, rbar, r = self.bars_range()

            # figure out x-range in view such that user can scroll "off"
            # the data set up to the point where ``_min_points_to_show``
            # are left.
            view_len = r - l

            # TODO: logic to check if end of bars in view
            extra = view_len - _min_points_to_show
            begin = self._ohlc[0]['index'] - extra
            # end = len(self._ohlc) - 1 + extra
            end = self._ohlc[-1]['index'] - 1 + extra

            # XXX: test code for only rendering lines for the bars in view.
            # This turns out to be very very poor perf when scaling out to
            # many bars (think > 1k) on screen.
            # name = self.name
            # bars = self._graphics[self.name]
            # bars.draw_lines(
            #   istart=max(lbar, l), iend=min(rbar, r), just_history=True)

            # bars_len = rbar - lbar
            # log.trace(
            #     f"\nl: {l}, lbar: {lbar}, rbar: {rbar}, r: {r}\n"
            #     f"view_len: {view_len}, bars_len: {bars_len}\n"
            #     f"begin: {begin}, end: {end}, extra: {extra}"
            # )
            # self._set_xlimits(begin, end)

            # TODO: this should be some kind of numpy view api
            # bars = self._ohlc[lbar:rbar]

            a = self._ohlc
            ifirst = a[0]['index']
            bars = a[lbar - ifirst:rbar - ifirst]

            if not len(bars):
                # likely no data loaded yet or extreme scrolling?
                log.error(f"WTF bars_range = {lbar}:{rbar}")
                return

            # TODO: should probably just have some kinda attr mark
            # that determines this behavior based on array type
            try:
                ylow = np.nanmin(bars['low'])
                yhigh = np.nanmax(bars['high'])
            except (IndexError, ValueError):
                # likely non-ohlc array?
                bars = bars[self.name]
                ylow = np.nanmin(bars)
                yhigh = np.nanmax(bars)

        # view margins: stay within a % of the "true range"
        diff = yhigh - ylow
        ylow = ylow - (diff * 0.04)
        yhigh = yhigh + (diff * 0.04)

        # # compute contents label "height" in view terms
        # # to avoid having data "contents" overlap with them
        # if self._labels:
        #     label = self._labels[self.name][0]

        #     rect = label.itemRect()
        #     tl, br = rect.topLeft(), rect.bottomRight()
        #     vb = self.plotItem.vb

        #     try:
        #         # on startup labels might not yet be rendered
        #         top, bottom = (vb.mapToView(tl).y(), vb.mapToView(br).y())

        #         # XXX: magic hack, how do we compute exactly?
        #         label_h = (top - bottom) * 0.42

        #     except np.linalg.LinAlgError:
        #         label_h = 0
        # else:
        #     label_h = 0

        # # print(f'label height {self.name}: {label_h}')

        # if label_h > yhigh - ylow:
        #     label_h = 0
        # print(f"bounds (ylow, yhigh): {(ylow, yhigh)}")
        label_h = 0

        self.setLimits(
            yMin=ylow,
            yMax=yhigh + label_h,
        )
        self.setYRange(ylow, yhigh + label_h)

    def enterEvent(self, ev):  # noqa
        # pg.PlotWidget.enterEvent(self, ev)
        self.sig_mouse_enter.emit(self)

    def leaveEvent(self, ev):  # noqa
        # pg.PlotWidget.leaveEvent(self, ev)
        self.sig_mouse_leave.emit(self)
        self.scene().leaveEvent(ev)


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

    # attempt to configure DPI aware font size
    _font.configure_to_dpi(current_screen())

    # chart_app.init_search()

    # historical data fetch
    brokermod = brokers.get_brokermod(brokername)

    async with data.open_feed(
        brokername,
        [sym],
        loglevel=loglevel,
    ) as feed:

        ohlcv = feed.shm
        bars = ohlcv.array

        # load in symbol's ohlc data
        # await tractor.breakpoint()
        linked_charts, chart = chart_app.load_symbol(sym, bars)

        # plot historical vwap if available
        wap_in_history = False

        if brokermod._show_wap_in_history:

            if 'bar_wap' in bars.dtype.fields:
                wap_in_history = True
                chart.draw_curve(
                    name='bar_wap',
                    data=bars,
                    add_label=False,
                )

        chart._set_yrange()

        # TODO: a data view api that makes this less shit
        chart._shm = ohlcv

        # eventually we'll support some kind of n-compose syntax
        fsp_conf = {
            'vwap': {
                'overlay': True,
                'anchor': 'session',
            },
            'rsi': {
                'period': 14,
                'chart_kwargs': {
                    'static_yrange': (0, 100),
                },
            },

        }

        async with trio.open_nursery() as n:

            # load initial fsp chain (otherwise known as "indicators")
            n.start_soon(
                spawn_fsps,
                linked_charts,
                fsp_conf,
                sym,
                ohlcv,
                brokermod,
                loglevel,
            )

            # start graphics update loop(s)after receiving first live quote
            n.start_soon(
                chart_from_quotes,
                chart,
                feed.stream,
                ohlcv,
                wap_in_history,
            )

            # wait for a first quote before we start any update tasks
            quote = await feed.receive()
            log.info(f'Received first quote {quote}')

            n.start_soon(
                check_for_new_bars,
                feed,
                # delay,
                ohlcv,
                linked_charts
            )

            # probably where we'll eventually start the user input loop
            await trio.sleep_forever()


async def chart_from_quotes(
    chart: ChartPlotWidget,
    stream,
    ohlcv: np.ndarray,
    wap_in_history: bool = False,
) -> None:
    """The 'main' (price) chart real-time update loop.

    """
    # TODO: bunch of stuff:
    # - I'm starting to think all this logic should be
    #   done in one place and "graphics update routines"
    #   should not be doing any length checking and array diffing.
    # - handle odd lot orders
    # - update last open price correctly instead
    #   of copying it from last bar's close
    # - 5 sec bar lookback-autocorrection like tws does?

    # update last price sticky
    last_price_sticky = chart._ysticks[chart.name]
    last_price_sticky.update_from_data(
        *ohlcv.array[-1][['index', 'close']]
    )

    def maxmin():
        # TODO: implement this
        # https://arxiv.org/abs/cs/0610046
        # https://github.com/lemire/pythonmaxmin

        array = chart._ohlc
        ifirst = array[0]['index']

        last_bars_range = chart.bars_range()
        l, lbar, rbar, r = last_bars_range
        in_view = array[lbar - ifirst:rbar - ifirst]

        assert in_view.size

        mx, mn = np.nanmax(in_view['high']), np.nanmin(in_view['low'])

        # TODO: when we start using line charts, probably want to make
        # this an overloaded call on our `DataView
        # sym = chart.name
        # mx, mn = np.nanmax(in_view[sym]), np.nanmin(in_view[sym])

        return last_bars_range, mx, mn

    chart.default_view()

    last_bars_range, last_mx, last_mn = maxmin()

    last, volume = ohlcv.array[-1][['close', 'volume']]

    l1 = L1Labels(
        chart,
        # determine precision/decimal lengths
        digits=max(float_digits(last), 2),
        size_digits=min(float_digits(volume), 3)
    )

    # TODO:
    # - in theory we should be able to read buffer data faster
    # then msgs arrive.. needs some tinkering and testing

    # - if trade volume jumps above / below prior L1 price
    # levels this might be dark volume we need to
    # present differently?

    async for quotes in stream:
        for sym, quote in quotes.items():

            for tick in quote.get('ticks', ()):

                # print(f"CHART: {quote['symbol']}: {tick}")
                ticktype = tick.get('type')
                price = tick.get('price')
                size = tick.get('size')

                # compute max and min trade values to display in view
                # TODO: we need a streaming minmax algorithm here, see
                # def above.
                brange, mx_in_view, mn_in_view = maxmin()
                l, lbar, rbar, r = brange

                if ticktype in ('trade', 'utrade', 'last'):

                    array = ohlcv.array

                    # update price sticky(s)
                    last = array[-1]
                    last_price_sticky.update_from_data(
                        *last[['index', 'close']]
                    )

                    # plot bars
                    # update price bar
                    chart.update_ohlc_from_array(
                        chart.name,
                        array,
                    )

                    if wap_in_history:
                        # update vwap overlay line
                        chart.update_curve_from_array('bar_wap', ohlcv.array)

                # XXX: prettty sure this is correct?
                # if ticktype in ('trade', 'last'):
                if ticktype in ('last',):  # 'size'):

                    label = {
                        l1.ask_label.level: l1.ask_label,
                        l1.bid_label.level: l1.bid_label,
                    }.get(price)

                    if label is not None:
                        label.size = size
                        label.update_from_data(0, price)

                        # on trades should we be knocking down
                        # the relevant L1 queue?
                        # label.size -= size

                elif ticktype in ('ask', 'asize'):
                    l1.ask_label.size = size
                    l1.ask_label.update_from_data(0, price)

                    # update max price in view to keep ask on screen
                    mx_in_view = max(price, mx_in_view)

                elif ticktype in ('bid', 'bsize'):
                    l1.bid_label.size = size
                    l1.bid_label.update_from_data(0, price)

                    # update min price in view to keep bid on screen
                    mn_in_view = min(price, mn_in_view)

                if mx_in_view > last_mx or mn_in_view < last_mn:
                    chart._set_yrange(yrange=(mn_in_view, mx_in_view))
                    last_mx, last_mn = mx_in_view, mn_in_view

                if brange != last_bars_range:
                    # we **must always** update the last values due to
                    # the x-range change
                    last_mx, last_mn = mx_in_view, mn_in_view
                    last_bars_range = brange


async def spawn_fsps(
    linked_charts: LinkedSplitCharts,
    # fsp_func_name,
    fsps: Dict[str, str],
    sym,
    src_shm,
    brokermod,
    loglevel,
) -> None:
    """Start financial signal processing in subactor.

    Pass target entrypoint and historical data.

    """
    # spawns sub-processes which execute cpu bound FSP code
    async with tractor.open_nursery() as n:

        # spawns local task that consume and chart data streams from
        # sub-procs
        async with trio.open_nursery() as ln:

            # Currently we spawn an actor per fsp chain but
            # likely we'll want to pool them eventually to
            # scale horizonatlly once cores are used up.
            for fsp_func_name, conf in fsps.items():

                display_name = f'fsp.{fsp_func_name}'

                # TODO: load function here and introspect
                # return stream type(s)

                # TODO: should `index` be a required internal field?
                fsp_dtype = np.dtype([('index', int), (fsp_func_name, float)])

                key = f'{sym}.' + display_name

                # this is all sync currently
                shm, opened = maybe_open_shm_array(
                    key,
                    # TODO: create entry for each time frame
                    dtype=fsp_dtype,
                    readonly=True,
                )

                # XXX: fsp may have been opened by a duplicate chart. Error for
                # now until we figure out how to wrap fsps as "feeds".
                assert opened, f"A chart for {key} likely already exists?"

                conf['shm'] = shm

                # spawn closure, can probably define elsewhere
                async def spawn_fsp_daemon(
                    fsp_name: str,
                    display_name: str,
                    conf: dict,
                ):
                    """Start an fsp subactor async.

                    """
                    print(f'FSP NAME: {fsp_name}')
                    portal = await n.run_in_actor(

                        # name as title of sub-chart
                        display_name,

                        # subactor entrypoint
                        fsp.cascade,
                        brokername=brokermod.name,
                        src_shm_token=src_shm.token,
                        dst_shm_token=conf['shm'].token,
                        symbol=sym,
                        fsp_func_name=fsp_name,

                        # tractor config
                        loglevel=loglevel,
                    )

                    stream = await portal.result()

                    # receive last index for processed historical
                    # data-array as first msg
                    _ = await stream.receive()

                    conf['stream'] = stream
                    conf['portal'] = portal

                # new local task
                ln.start_soon(
                    spawn_fsp_daemon,
                    fsp_func_name,
                    display_name,
                    conf,
                )

            # blocks here until all daemons up

        # start and block on update loops
        async with trio.open_nursery() as ln:
            for fsp_func_name, conf in fsps.items():
                ln.start_soon(
                    update_signals,
                    linked_charts,
                    fsp_func_name,
                    conf,
                )


async def update_signals(
    linked_charts: LinkedSplitCharts,
    fsp_func_name: str,
    conf: Dict[str, Any],

) -> None:
    """FSP stream chart update loop.

    This is called once for each entry in the fsp
    config map.
    """
    shm = conf['shm']

    if conf.get('overlay'):
        chart = linked_charts.chart
        chart.draw_curve(
            name='vwap',
            data=shm.array,
            overlay=True,
        )
        last_val_sticky = None

    else:

        chart = linked_charts.add_plot(
            name=fsp_func_name,
            array=shm.array,

            # curve by default
            ohlc=False,

            # settings passed down to ``ChartPlotWidget``
            **conf.get('chart_kwargs', {})
            # static_yrange=(0, 100),
        )

        # display contents labels asap
        chart.update_contents_labels(
            len(shm.array) - 1,
            # fsp_func_name
        )

        # read last value
        array = shm.array
        value = array[fsp_func_name][-1]

        last_val_sticky = chart._ysticks[chart.name]
        last_val_sticky.update_from_data(-1, value)

        chart.update_curve_from_array(fsp_func_name, array)

        chart._shm = shm

    # TODO: figure out if we can roll our own `FillToThreshold` to
    # get brush filled polygons for OS/OB conditions.
    # ``pg.FillBetweenItems`` seems to be one technique using
    # generic fills between curve types while ``PlotCurveItem`` has
    # logic inside ``.paint()`` for ``self.opts['fillLevel']`` which
    # might be the best solution?
    # graphics = chart.update_from_array(chart.name, array[fsp_func_name])
    # graphics.curve.setBrush(50, 50, 200, 100)
    # graphics.curve.setFillLevel(50)

    # add moveable over-[sold/bought] lines
    # and labels only for the 70/30 lines
    level_line(chart, 20, show_label=False)
    level_line(chart, 30, orient_v='top')
    level_line(chart, 70, orient_v='bottom')
    level_line(chart, 80, orient_v='top', show_label=False)

    chart._set_yrange()

    stream = conf['stream']

    # update chart graphics
    async for value in stream:

        # read last
        array = shm.array
        value = array[-1][fsp_func_name]

        if last_val_sticky:
            last_val_sticky.update_from_data(-1, value)

        # update graphics
        chart.update_curve_from_array(fsp_func_name, array)


async def check_for_new_bars(feed, ohlcv, linked_charts):
    """Task which updates from new bars in the shared ohlcv buffer every
    ``delay_s`` seconds.
    """
    # TODO: right now we'll spin printing bars if the last time
    # stamp is before a large period of no market activity.
    # Likely the best way to solve this is to make this task
    # aware of the instrument's tradable hours?

    price_chart = linked_charts.chart
    price_chart.default_view()

    async for index in await feed.index_stream():

        # update chart historical bars graphics by incrementing
        # a time step and drawing the history and new bar

        # When appending a new bar, in the time between the insert
        # from the writing process and the Qt render call, here,
        # the index of the shm buffer may be incremented and the
        # (render) call here might read the new flat bar appended
        # to the buffer (since -1 index read). In that case H==L and the
        # body will be set as None (not drawn) on what this render call
        # *thinks* is the curent bar (even though it's reading data from
        # the newly inserted flat bar.
        #
        # HACK: We need to therefore write only the history (not the
        # current bar) and then either write the current bar manually
        # or place a cursor for visual cue of the current time step.

        price_chart.update_ohlc_from_array(
            price_chart.name,
            ohlcv.array,
            just_history=True,
        )

        # XXX: this puts a flat bar on the current time step
        # TODO: if we eventually have an x-axis time-step "cursor"
        # we can get rid of this since it is extra overhead.
        price_chart.update_ohlc_from_array(
            price_chart.name,
            ohlcv.array,
            just_history=False,
        )

        # resize view
        # price_chart._set_yrange()

        for name in price_chart._overlays:

            price_chart.update_curve_from_array(
                name,
                price_chart._arrays[name]
            )

            # # TODO: standard api for signal lookups per plot
            # if name in price_chart._ohlc.dtype.fields:

            #     # should have already been incremented above
            #     price_chart.update_curve_from_array(name, price_chart._ohlc)

        for name, chart in linked_charts.subplots.items():
            chart.update_curve_from_array(chart.name, chart._shm.array)
            # chart._set_yrange()

        # shift the view if in follow mode
        price_chart.increment_view()


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
