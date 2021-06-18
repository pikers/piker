# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
import time
from contextlib import AsyncExitStack
from typing import Tuple, Dict, Any, Optional, Callable
from types import ModuleType
from functools import partial

from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import Qt
from PyQt5.QtCore import QEvent
import numpy as np
import pyqtgraph as pg
import tractor
import trio

from .._daemon import (
    maybe_spawn_brokerd,
)
from ..brokers import get_brokermod
from ._axes import (
    DynamicDateAxis,
    PriceAxis,
    YAxisLabel,
)
from ._graphics._cursor import (
    Cursor,
    ContentsLabel,
)
from ._l1 import L1Labels
from ._graphics._ohlc import BarItems
from ._graphics._curve import FastAppendCurve
from ._style import (
    hcolor,
    CHART_MARGINS,
    _xaxis_at,
    _min_points_to_show,
    _bars_from_right_in_follow_mode,
    _bars_to_left_in_follow_mode,
)
from . import _search
from . import _event
from ..data._source import Symbol
from ..data._sharedmem import ShmArray
from ..data import maybe_open_shm_array
from .. import brokers
from .. import data
from ..log import get_logger
from ._exec import run_qtractor
from ._interaction import ChartView
from .order_mode import start_order_mode
from .. import fsp
from ..data import feed


log = get_logger(__name__)


class GodWidget(QtGui.QWidget):
    '''
    "Our lord and savior, the holy child of window-shua, there is no
    widget above thee." - 6|6

    The highest level composed widget which contains layouts for
    organizing lower level charts as well as other widgets used to
    control or modify them.

    '''
    def __init__(

        self,
        parent=None,

    ) -> None:

        super().__init__(parent)

        self.hbox = QtGui.QHBoxLayout(self)
        self.hbox.setContentsMargins(0, 0, 0, 0)
        self.hbox.setSpacing(2)

        self.vbox = QtGui.QVBoxLayout()
        self.vbox.setContentsMargins(0, 0, 0, 0)
        self.vbox.setSpacing(2)

        self.hbox.addLayout(self.vbox)

        # self.toolbar_layout = QtGui.QHBoxLayout()
        # self.toolbar_layout.setContentsMargins(0, 0, 0, 0)
        # self.vbox.addLayout(self.toolbar_layout)

        # self.init_timeframes_ui()
        # self.init_strategy_ui()
        # self.vbox.addLayout(self.hbox)

        self._chart_cache = {}
        self.linkedsplits: 'LinkedSplits' = None

        # assigned in the startup func `_async_main()`
        self._root_n: trio.Nursery = None

    def set_chart_symbol(
        self,
        symbol_key: str,  # of form <fqsn>.<providername>
        linkedsplits: 'LinkedSplits',  # type: ignore

    ) -> None:
        # re-sort org cache symbol list in LIFO order
        cache = self._chart_cache
        cache.pop(symbol_key, None)
        cache[symbol_key] = linkedsplits

    def get_chart_symbol(
        self,
        symbol_key: str,
    ) -> 'LinkedSplits':  # type: ignore
        return self._chart_cache.get(symbol_key)

    # def init_timeframes_ui(self):
    #     self.tf_layout = QtGui.QHBoxLayout()
    #     self.tf_layout.setSpacing(0)
    #     self.tf_layout.setContentsMargins(0, 12, 0, 0)
    #     time_frames = ('1M', '5M', '15M', '30M', '1H', '1D', '1W', 'MN')
    #     btn_prefix = 'TF'

    #     for tf in time_frames:
    #         btn_name = ''.join([btn_prefix, tf])
    #         btn = QtGui.QPushButton(tf)
    #         # TODO:
    #         btn.setEnabled(False)
    #         setattr(self, btn_name, btn)
    #         self.tf_layout.addWidget(btn)

    #     self.toolbar_layout.addLayout(self.tf_layout)

    # XXX: strat loader/saver that we don't need yet.
    # def init_strategy_ui(self):
    #     self.strategy_box = StrategyBoxWidget(self)
    #     self.toolbar_layout.addWidget(self.strategy_box)

    def load_symbol(

        self,
        providername: str,
        symbol_key: str,
        loglevel: str,
        ohlc: bool = True,
        reset: bool = False,

    ) -> trio.Event:
        '''Load a new contract into the charting app.

        Expects a ``numpy`` structured array containing all the ohlcv fields.

        '''
        # our symbol key style is always lower case
        symbol_key = symbol_key.lower()

        # fully qualified symbol name (SNS i guess is what we're making?)
        fqsn = '.'.join([symbol_key, providername])

        linkedsplits = self.get_chart_symbol(fqsn)

        order_mode_started = trio.Event()

        if not self.vbox.isEmpty():
            # XXX: this is CRITICAL especially with pixel buffer caching
            self.linkedsplits.hide()

            # XXX: pretty sure we don't need this
            # remove any existing plots?
            # XXX: ahh we might want to support cache unloading..
            self.vbox.removeWidget(self.linkedsplits)

        # switching to a new viewable chart
        if linkedsplits is None or reset:

            # we must load a fresh linked charts set
            linkedsplits = LinkedSplits(self)

            # spawn new task to start up and update new sub-chart instances
            self._root_n.start_soon(
                display_symbol_data,
                self,
                providername,
                symbol_key,
                loglevel,
                order_mode_started,
            )

            self.set_chart_symbol(fqsn, linkedsplits)

        else:
            # symbol is already loaded and ems ready
            order_mode_started.set()

        self.vbox.addWidget(linkedsplits)

        # chart is already in memory so just focus it
        if self.linkedsplits:
            self.linkedsplits.unfocus()

        # self.vbox.addWidget(linkedsplits)
        linkedsplits.show()
        linkedsplits.focus()
        self.linkedsplits = linkedsplits

        symbol = linkedsplits.symbol

        if symbol is not None:
            self.window.setWindowTitle(
                f'{symbol.key}@{symbol.brokers} '
                f'tick:{symbol.tick_size}'
            )

        return order_mode_started


class LinkedSplits(QtGui.QWidget):
    '''
    Widget that holds a central chart plus derived
    subcharts computed from the original data set apart
    by splitters for resizing.

    A single internal references to the data is maintained
    for each chart and can be updated externally.

    '''
    long_pen = pg.mkPen('#006000')
    long_brush = pg.mkBrush('#00ff00')
    short_pen = pg.mkPen('#600000')
    short_brush = pg.mkBrush('#ff0000')

    zoomIsDisabled = QtCore.pyqtSignal(bool)

    def __init__(

        self,
        godwidget: GodWidget,

    ) -> None:

        super().__init__()

        # self.signals_visible: bool = False
        self._cursor: Cursor = None  # crosshair graphics

        self.godwidget = godwidget
        self.chart: ChartPlotWidget = None  # main (ohlc) chart
        self.subplots: Dict[Tuple[str, ...], ChartPlotWidget] = {}

        self.godwidget = godwidget

        self.xaxis = DynamicDateAxis(
            orientation='bottom',
            linkedsplits=self
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

        # state tracker?
        self._symbol: Symbol = None

    @property
    def symbol(self) -> Symbol:
        return self._symbol

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

    def focus(self) -> None:
        if self.chart is not None:
            self.chart.focus()

    def unfocus(self) -> None:
        if self.chart is not None:
            self.chart.clearFocus()

    def plot_ohlc_main(
        self,
        symbol: Symbol,
        array: np.ndarray,
        style: str = 'bar',
    ) -> 'ChartPlotWidget':
        """Start up and show main (price) chart and all linked subcharts.

        The data input struct array must include OHLC fields.
        """
        # add crosshairs
        self._cursor = Cursor(
            linkedsplits=self,
            digits=symbol.digits(),
        )
        self.chart = self.add_plot(
            name=symbol.key,
            array=array,
            # xaxis=self.xaxis,
            style=style,
            _is_main=True,
        )
        # add crosshair graphic
        self.chart.addItem(self._cursor)

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
        cv = ChartView(name)
        cv.linkedsplits = self

        # use "indicator axis" by default
        if xaxis is None:
            xaxis = DynamicDateAxis(
                orientation='bottom',
                linkedsplits=self
            )

        cpw = ChartPlotWidget(

            # this name will be used to register the primary
            # graphics curve managed by the subchart
            name=name,

            array=array,
            parent=self.splitter,
            linkedsplits=self,
            axisItems={
                'bottom': xaxis,
                'right': PriceAxis(linkedsplits=self, orientation='right'),
                'left': PriceAxis(linkedsplits=self, orientation='left'),
            },
            viewBox=cv,
            cursor=self._cursor,
            **cpw_kwargs,
        )
        print(f'xaxis ps: {xaxis.pos()}')

        # give viewbox as reference to chart
        # allowing for kb controls and interactions on **this** widget
        # (see our custom view mode in `._interactions.py`)
        cv.chart = cpw

        cpw.plotItem.vb.linkedsplits = self
        cpw.setFrameStyle(QtGui.QFrame.StyledPanel)  # | QtGui.QFrame.Plain)
        cpw.hideButtons()
        # XXX: gives us outline on backside of y-axis
        cpw.getPlotItem().setContentsMargins(*CHART_MARGINS)

        # link chart x-axis to main quotes chart
        cpw.setXLink(self.chart)

        # add to cross-hair's known plots
        self._cursor.add_plot(cpw)

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
    '''
    ``GraphicsView`` subtype containing a single ``PlotItem``.

    - The added methods allow for plotting OHLC sequences from
      ``np.ndarray``s with appropriate field names.
    - Overrides a ``pyqtgraph.PlotWidget`` (a ``GraphicsView`` containing
      a single ``PlotItem``) to intercept and and re-emit mouse enter/exit
      events.

    (Could be replaced with a ``pg.GraphicsLayoutWidget`` if we
    eventually want multiple plots managed together?)

    '''
    sig_mouse_leave = QtCore.Signal(object)
    sig_mouse_enter = QtCore.Signal(object)

    _l1_labels: L1Labels = None

    mode_name: str = 'mode: view'

    # TODO: can take a ``background`` color setting - maybe there's
    # a better one?

    def __init__(
        self,
        # the data view we generate graphics from
        name: str,
        array: np.ndarray,
        linkedsplits: LinkedSplits,

        view_color: str = 'papas_special',
        pen_color: str = 'bracket',

        static_yrange: Optional[Tuple[float, float]] = None,
        cursor: Optional[Cursor] = None,

        **kwargs,
    ):
        """Configure chart display settings.
        """
        self.view_color = view_color
        self.pen_color = pen_color

        super().__init__(
            background=hcolor(view_color),
            # parent=None,
            # plotItem=None,
            # antialias=True,
            useOpenGL=True,
            **kwargs
        )
        self.name = name
        self._lc = linkedsplits

        # scene-local placeholder for book graphics
        # sizing to avoid overlap with data contents
        self._max_l1_line_len: float = 0

        # self.setViewportMargins(0, 0, 0, 0)
        self._ohlc = array  # readonly view of ohlc data

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
        # self.showAxis('left')

        # show background grid
        self.showGrid(x=False, y=True, alpha=0.3)

        self.default_view()

        # Assign callback for rescaling y-axis automatically
        # based on data contents and ``ViewBox`` state.
        # self.sigXRangeChanged.connect(self._set_yrange)

        # for mouse wheel which doesn't seem to emit XRangeChanged
        self._vb.sigRangeChangedManually.connect(self._set_yrange)

        # for when the splitter(s) are resized
        self._vb.sigResized.connect(self._set_yrange)

    def focus(self) -> None:
        # self.setFocus()
        self._vb.setFocus()

    def last_bar_in_view(self) -> int:
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
        begin = xlast - _bars_to_left_in_follow_mode
        end = xlast + _bars_from_right_in_follow_mode

        # remove any custom user yrange setttings
        if self._static_yrange == 'axis':
            self._static_yrange = None

        self.plotItem.vb.setXRange(
            min=begin,
            max=end,
            padding=0,
        )
        self._set_yrange()

    def increment_view(
        self,
    ) -> None:
        """
        Increment the data view one step to the right thus "following"
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
    ) -> pg.GraphicsObject:
        """
        Draw OHLC datums to chart.

        """
        graphics = BarItems(
            self.plotItem,
            pen_color=self.pen_color
        )

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

        # curve = pg.PlotDataItem(
        # curve = pg.PlotCurveItem(
        curve = FastAppendCurve(
            y=data[name],
            x=data['index'],
            # antialias=True,
            name=name,

            # XXX: pretty sure this is just more overhead
            # on data reads and makes graphics rendering no faster
            # clipToView=True,

            # TODO: see how this handles with custom ohlcv bars graphics
            # and/or if we can implement something similar for OHLC graphics
            # autoDownsample=True,
            # downsample=60,
            # downsampleMethod='subsample',

            **pdi_kwargs,
        )

        # XXX: see explanation for differenct caching modes:
        # https://stackoverflow.com/a/39410081
        # seems to only be useful if we don't re-generate the entire
        # QPainterPath every time
        # curve.curve.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

        # don't ever use this - it's a colossal nightmare of artefacts
        # and is disastrous for performance.
        # curve.setCacheMode(QtGui.QGraphicsItem.ItemCoordinateCache)

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
    ) -> YAxisLabel:

        # if the sticky is for our symbol
        # use the tick size precision for display
        sym = self._lc.symbol
        if name == sym.key:
            digits = sym.digits()
        else:
            digits = 2

        # add y-axis "last" value label
        last = self._ysticks[name] = YAxisLabel(
            chart=self,
            parent=self.getAxis('right'),
            # TODO: pass this from symbol data
            digits=digits,
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

        if len(array):
            # TODO: we should instead implement a diff based
            # "only update with new items" on the pg.PlotCurveItem
            # one place to dig around this might be the `QBackingStore`
            # https://doc.qt.io/qt-5/qbackingstore.html
            # curve.setData(y=array[name], x=array['index'], **kwargs)
            curve.update_from_array(x=array['index'], y=array[name], **kwargs)

        return curve

    def _set_yrange(
        self,
        *,
        yrange: Optional[Tuple[float, float]] = None,
        range_margin: float = 0.06,
    ) -> None:
        """Set the viewable y-range based on embedded data.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.

        """
        set_range = True

        if self._static_yrange == 'axis':
            set_range = False

        elif self._static_yrange is not None:
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
            # view_len = r - l

            # TODO: logic to check if end of bars in view
            # extra = view_len - _min_points_to_show

            # begin = self._ohlc[0]['index'] - extra

            # # end = len(self._ohlc) - 1 + extra
            # end = self._ohlc[-1]['index'] - 1 + extra

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
            bars = a[lbar - ifirst:rbar - ifirst + 1]

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

        if set_range:
            # view margins: stay within a % of the "true range"
            diff = yhigh - ylow
            ylow = ylow - (diff * range_margin)
            yhigh = yhigh + (diff * range_margin)

            self.setLimits(
                yMin=ylow,
                yMax=yhigh,
            )
            self.setYRange(ylow, yhigh)

    # def _label_h(self, yhigh: float, ylow: float) -> float:
    #     # compute contents label "height" in view terms
    #     # to avoid having data "contents" overlap with them
    #     if self._labels:
    #         label = self._labels[self.name][0]

    #         rect = label.itemRect()
    #         tl, br = rect.topLeft(), rect.bottomRight()
    #         vb = self.plotItem.vb

    #         try:
    #             # on startup labels might not yet be rendered
    #             top, bottom = (vb.mapToView(tl).y(), vb.mapToView(br).y())

    #             # XXX: magic hack, how do we compute exactly?
    #             label_h = (top - bottom) * 0.42

    #         except np.linalg.LinAlgError:
    #             label_h = 0
    #     else:
    #         label_h = 0

    #     # print(f'label height {self.name}: {label_h}')

    #     if label_h > yhigh - ylow:
    #         label_h = 0

    #     print(f"bounds (ylow, yhigh): {(ylow, yhigh)}")

    def enterEvent(self, ev):  # noqa
        # pg.PlotWidget.enterEvent(self, ev)
        self.sig_mouse_enter.emit(self)

    def leaveEvent(self, ev):  # noqa
        # pg.PlotWidget.leaveEvent(self, ev)
        self.sig_mouse_leave.emit(self)
        self.scene().leaveEvent(ev)


async def test_bed(
    ohlcv,
    chart,
    lc,
):
    from ._graphics._lines import order_line

    sleep = 6

    # from PyQt5.QtCore import QPointF
    vb = chart._vb
    # scene = vb.scene()

    # raxis = chart.getAxis('right')
    # vb_right = vb.boundingRect().right()

    last, i_end = ohlcv.array[-1][['close', 'index']]

    line = order_line(
        chart,
        level=last,
        level_digits=2
    )
    # eps = line.getEndpoints()

    # llabel = line._labels[1][1]

    line.update_labels({'level': last})
    return

    # rl = eps[1]
    # rlabel.setPos(rl)

    # ti = pg.TextItem(text='Fuck you')
    # ti.setPos(pg.Point(i_end, last))
    # ti.setParentItem(line)
    # ti.setAnchor(pg.Point(1, 1))
    # vb.addItem(ti)
    # chart.plotItem.addItem(ti)

    from ._label import Label

    txt = Label(
        vb,
        fmt_str='fuck {it}',
    )
    txt.format(it='boy')
    txt.place_on_scene('left')
    txt.set_view_y(last)

    # txt = QtGui.QGraphicsTextItem()
    # txt.setPlainText("FUCK YOU")
    # txt.setFont(_font.font)
    # txt.setDefaultTextColor(pg.mkColor(hcolor('bracket')))
    # # txt.setParentItem(vb)
    # w = txt.boundingRect().width()
    # scene.addItem(txt)

    # txt.setParentItem(line)
    # d_coords = vb.mapFromView(QPointF(i_end, last))
    # txt.setPos(vb_right - w, d_coords.y())
    # txt.show()
    # txt.update()

    # rlabel.setPos(vb_right - 2*w, d_coords.y())
    # rlabel.show()

    i = 0
    while True:
        await trio.sleep(sleep)
        await tractor.breakpoint()
        txt.format(it=f'dog_{i}')
        # d_coords = vb.mapFromView(QPointF(i_end, last))
        # txt.setPos(vb_right - w, d_coords.y())
        # txt.setPlainText(f"FUCK YOU {i}")
        i += 1


_clear_throttle_rate: int = 60  # Hz
_book_throttle_rate: int = 16  # Hz


async def chart_from_quotes(

    chart: ChartPlotWidget,
    stream: tractor.MsgStream,
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

        return last_bars_range, mx, max(mn, 0)

    chart.default_view()

    last_bars_range, last_mx, last_mn = maxmin()

    last, volume = ohlcv.array[-1][['close', 'volume']]

    symbol = chart._lc.symbol

    l1 = L1Labels(
        chart,
        # determine precision/decimal lengths
        digits=symbol.digits(),
        size_digits=symbol.lot_digits(),
    )
    chart._l1_labels = l1

    # TODO:
    # - in theory we should be able to read buffer data faster
    # then msgs arrive.. needs some tinkering and testing

    # - if trade volume jumps above / below prior L1 price
    # levels this might be dark volume we need to
    # present differently?

    tick_size = chart._lc.symbol.tick_size
    tick_margin = 2 * tick_size

    last_ask = last_bid = last_clear = time.time()
    chart.show()

    async for quotes in stream:

        # chart isn't actively shown so just skip render cycle
        if chart._lc.isHidden():
            continue

        for sym, quote in quotes.items():

            now = time.time()

            for tick in quote.get('ticks', ()):

                # print(f"CHART: {quote['symbol']}: {tick}")
                ticktype = tick.get('type')
                price = tick.get('price')
                size = tick.get('size')

                if ticktype == 'n/a' or price == -1:
                    # okkk..
                    continue

                # clearing price event
                if ticktype in ('trade', 'utrade', 'last'):

                    # throttle clearing price updates to ~ max 60 FPS
                    period = now - last_clear
                    if period <= 1/_clear_throttle_rate:
                        # faster then display refresh rate
                        continue

                    # print(f'passthrough {tick}\n{1/(now-last_clear)}')
                    # set time of last graphics update
                    last_clear = now

                    array = ohlcv.array

                    # update price sticky(s)
                    end = array[-1]
                    last_price_sticky.update_from_data(
                        *end[['index', 'close']]
                    )

                    # plot bars
                    # update price bar
                    chart.update_ohlc_from_array(
                        chart.name,
                        array,
                    )

                    if wap_in_history:
                        # update vwap overlay line
                        chart.update_curve_from_array(
                            'bar_wap', ohlcv.array)

                # l1 book events
                # throttle the book graphics updates at a lower rate
                # since they aren't as critical for a manual user
                # viewing the chart

                elif ticktype in ('ask', 'asize'):
                    if (now - last_ask) <= 1/_book_throttle_rate:
                        # print(f'skipping\n{tick}')
                        continue

                    # print(f'passthrough {tick}\n{1/(now-last_ask)}')
                    last_ask = now

                elif ticktype in ('bid', 'bsize'):
                    if (now - last_bid) <= 1/_book_throttle_rate:
                        continue

                    # print(f'passthrough {tick}\n{1/(now-last_bid)}')
                    last_bid = now

                # compute max and min trade values to display in view
                # TODO: we need a streaming minmax algorithm here, see
                # def above.
                brange, mx_in_view, mn_in_view = maxmin()
                l, lbar, rbar, r = brange

                mx = mx_in_view + tick_margin
                mn = mn_in_view - tick_margin

                # XXX: prettty sure this is correct?
                # if ticktype in ('trade', 'last'):
                if ticktype in ('last',):  # 'size'):

                    label = {
                        l1.ask_label.fields['level']: l1.ask_label,
                        l1.bid_label.fields['level']: l1.bid_label,
                    }.get(price)

                    if label is not None:
                        label.update_fields({'level': price, 'size': size})

                        # on trades should we be knocking down
                        # the relevant L1 queue?
                        # label.size -= size

                elif ticktype in ('ask', 'asize'):
                    l1.ask_label.update_fields({'level': price, 'size': size})

                elif ticktype in ('bid', 'bsize'):
                    l1.bid_label.update_fields({'level': price, 'size': size})

                # update min price in view to keep bid on screen
                mn = min(price - tick_margin, mn)
                # update max price in view to keep ask on screen
                mx = max(price + tick_margin, mx)

                if (mx > last_mx) or (
                    mn < last_mn
                ):
                    # print(f'new y range: {(mn, mx)}')

                    chart._set_yrange(
                        yrange=(mn, mx),
                        # TODO: we should probably scale
                        # the view margin based on the size
                        # of the true range? This way you can
                        # slap in orders outside the current
                        # L1 (only) book range.
                        # range_margin=0.1,
                    )

                last_mx, last_mn = mx, mn


async def spawn_fsps(

    linkedsplits: LinkedSplits,
    fsps: Dict[str, str],
    sym: str,
    src_shm: list,
    brokermod: ModuleType,
    group_status_key: str,
    loglevel: str,

) -> None:
    """Start financial signal processing in subactor.

    Pass target entrypoint and historical data.

    """

    linkedsplits.focus()

    # spawns sub-processes which execute cpu bound FSP code
    async with tractor.open_nursery(loglevel=loglevel) as n:

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
                # assert opened, f"A chart for {key} likely already exists?"

                conf['shm'] = shm

                portal = await n.start_actor(
                    enable_modules=['piker.fsp'],
                    name=display_name,
                )

                # init async
                ln.start_soon(
                    run_fsp,
                    portal,
                    linkedsplits,
                    brokermod,
                    sym,
                    src_shm,
                    fsp_func_name,
                    display_name,
                    conf,
                    group_status_key,
                )

        # blocks here until all fsp actors complete


async def run_fsp(

    portal: tractor._portal.Portal,
    linkedsplits: LinkedSplits,
    brokermod: ModuleType,
    sym: str,
    src_shm: ShmArray,
    fsp_func_name: str,
    display_name: str,
    conf: Dict[str, Any],
    group_status_key: str,

) -> None:
    """FSP stream chart update loop.

    This is called once for each entry in the fsp
    config map.
    """
    done = linkedsplits.window().status_bar.open_status(
        f'loading {display_name}..',
        group_key=group_status_key,
    )

    async with portal.open_stream_from(

        # subactor entrypoint
        fsp.cascade,

        # name as title of sub-chart
        brokername=brokermod.name,
        src_shm_token=src_shm.token,
        dst_shm_token=conf['shm'].token,
        symbol=sym,
        fsp_func_name=fsp_func_name,

    ) as stream:

        # receive last index for processed historical
        # data-array as first msg
        _ = await stream.receive()

        shm = conf['shm']

        if conf.get('overlay'):
            chart = linkedsplits.chart
            chart.draw_curve(
                name='vwap',
                data=shm.array,
                overlay=True,
            )
            last_val_sticky = None

        else:

            chart = linkedsplits.add_plot(
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

            # XXX: ONLY for sub-chart fsps, overlays have their
            # data looked up from the chart's internal array set.
            # TODO: we must get a data view api going STAT!!
            chart._shm = shm

            # should **not** be the same sub-chart widget
            assert chart.name != linkedsplits.chart.name

            # sticky only on sub-charts atm
            last_val_sticky = chart._ysticks[chart.name]

            # read from last calculated value
            array = shm.array
            value = array[fsp_func_name][-1]
            last_val_sticky.update_from_data(-1, value)

        chart._lc.focus()

        # works also for overlays in which case data is looked up from
        # internal chart array set....
        chart.update_curve_from_array(fsp_func_name, shm.array)

        # TODO: figure out if we can roll our own `FillToThreshold` to
        # get brush filled polygons for OS/OB conditions.
        # ``pg.FillBetweenItems`` seems to be one technique using
        # generic fills between curve types while ``PlotCurveItem`` has
        # logic inside ``.paint()`` for ``self.opts['fillLevel']`` which
        # might be the best solution?
        # graphics = chart.update_from_array(chart.name, array[fsp_func_name])
        # graphics.curve.setBrush(50, 50, 200, 100)
        # graphics.curve.setFillLevel(50)

        if fsp_func_name == 'rsi':
            from ._graphics._lines import level_line
            # add moveable over-[sold/bought] lines
            # and labels only for the 70/30 lines
            level_line(chart, 20)
            level_line(chart, 30, orient_v='top')
            level_line(chart, 70, orient_v='bottom')
            level_line(chart, 80, orient_v='top')

        chart._set_yrange()

        last = time.time()

        done()

        # update chart graphics
        async for value in stream:

            # chart isn't actively shown so just skip render cycle
            if chart._lc.isHidden():
                continue

            now = time.time()
            period = now - last

            # if period <= 1/30:
            if period <= 1/_clear_throttle_rate:
                # faster then display refresh rate
                # print(f'quote too fast: {1/period}')
                continue

            # TODO: provide a read sync mechanism to avoid this polling.
            # the underlying issue is that a backfill and subsequent shm
            # array first/last index update could result in an empty array
            # read here since the stream is never torn down on the
            # re-compute steps.
            read_tries = 2
            while read_tries > 0:
                try:
                    # read last
                    array = shm.array
                    value = array[-1][fsp_func_name]
                    break

                except IndexError:
                    read_tries -= 1
                    continue

            if last_val_sticky:
                last_val_sticky.update_from_data(-1, value)

            # update graphics
            chart.update_curve_from_array(fsp_func_name, array)

            # set time of last graphics update
            last = now


async def check_for_new_bars(feed, ohlcv, linkedsplits):
    """Task which updates from new bars in the shared ohlcv buffer every
    ``delay_s`` seconds.

    """
    # TODO: right now we'll spin printing bars if the last time
    # stamp is before a large period of no market activity.
    # Likely the best way to solve this is to make this task
    # aware of the instrument's tradable hours?

    price_chart = linkedsplits.chart
    price_chart.default_view()

    async with feed.index_stream() as stream:
        async for index in stream:

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

            # XXX: this puts a flat bar on the current time step
            # TODO: if we eventually have an x-axis time-step "cursor"
            # we can get rid of this since it is extra overhead.
            price_chart.update_ohlc_from_array(
                price_chart.name,
                ohlcv.array,
                just_history=False,
            )

            for name in price_chart._overlays:

                price_chart.update_curve_from_array(
                    name,
                    price_chart._arrays[name]
                )

            for name, chart in linkedsplits.subplots.items():
                chart.update_curve_from_array(chart.name, chart._shm.array)

            # shift the view if in follow mode
            price_chart.increment_view()


async def display_symbol_data(

    godwidget: GodWidget,
    provider: str,
    sym: str,
    loglevel: str,

    order_mode_started: trio.Event,

) -> None:
    '''Spawn a real-time displayed and updated chart for provider symbol.

    Spawned ``LinkedSplits`` chart widgets can remain up but hidden so
    that multiple symbols can be viewed and switched between extremely
    fast from a cached watch-list.

    '''
    sbar = godwidget.window.status_bar
    loading_sym_key = sbar.open_status(
        f'loading {sym}.{provider} -> ',
        group_key=True
    )

    # historical data fetch
    brokermod = brokers.get_brokermod(provider)

    async with(

        data.open_feed(
            provider,
            [sym],
            loglevel=loglevel,

            # 60 FPS to limit context switches
            tick_throttle=_clear_throttle_rate,

        ) as feed,

        trio.open_nursery() as n,
    ):

        ohlcv: ShmArray = feed.shm
        bars = ohlcv.array
        symbol = feed.symbols[sym]

        # load in symbol's ohlc data
        godwidget.window.setWindowTitle(
            f'{symbol.key}@{symbol.brokers} '
            f'tick:{symbol.tick_size}'
        )

        linkedsplits = godwidget.linkedsplits
        linkedsplits._symbol = symbol

        chart = linkedsplits.plot_ohlc_main(symbol, bars)
        chart.setFocus()

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

        # size view to data once at outset
        chart._set_yrange()

        # TODO: a data view api that makes this less shit
        chart._shm = ohlcv

        # TODO: eventually we'll support some kind of n-compose syntax
        fsp_conf = {
            'rsi': {
                'period': 14,
                'chart_kwargs': {
                    'static_yrange': (0, 100),
                },
            },

        }

        # make sure that the instrument supports volume history
        # (sometimes this is not the case for some commodities and
        # derivatives)
        volm = ohlcv.array['volume']
        if (
            np.all(np.isin(volm, -1)) or
            np.all(np.isnan(volm))
        ):
            log.warning(
                f"{sym} does not seem to have volume info,"
                " dropping volume signals")
        else:
            fsp_conf.update({
                'vwap': {
                    'overlay': True,
                    'anchor': 'session',
                },
            })

        # load initial fsp chain (otherwise known as "indicators")
        n.start_soon(
            spawn_fsps,
            linkedsplits,
            fsp_conf,
            sym,
            ohlcv,
            brokermod,
            loading_sym_key,
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

        # TODO: instead we should start based on instrument trading hours?
        # wait for a first quote before we start any update tasks
        # quote = await feed.receive()
        # log.info(f'Received first quote {quote}')

        n.start_soon(
            check_for_new_bars,
            feed,
            ohlcv,
            linkedsplits
        )

        await start_order_mode(chart, symbol, provider, order_mode_started)


async def load_providers(

    brokernames: list[str],
    loglevel: str,

) -> None:

    # TODO: seems like our incentive for brokerd caching lelel
    backends = {}

    async with AsyncExitStack() as stack:
        # TODO: spawn these async in nursery.
        # load all requested brokerd's at startup and load their
        # search engines.
        for broker in brokernames:

            log.info(f'loading brokerd for {broker}..')
            # spin up broker daemons for each provider
            portal = await stack.enter_async_context(
                maybe_spawn_brokerd(
                    broker,
                    loglevel=loglevel
                )
            )

            backends[broker] = portal

            await stack.enter_async_context(
                feed.install_brokerd_search(
                    portal,
                    get_brokermod(broker),
                )
            )

        # keep search engines up until cancelled
        await trio.sleep_forever()


async def _async_main(

    # implicit required argument provided by ``qtractor_run()``
    main_widget: GodWidget,

    sym: str,
    brokernames: str,
    loglevel: str,

) -> None:
    """
    Main Qt-trio routine invoked by the Qt loop with the widgets ``dict``.

    Provision the "main" widget with initial symbol data and root nursery.

    """

    godwidget = main_widget

    # attempt to configure DPI aware font size
    screen = godwidget.window.current_screen()

    # configure graphics update throttling based on display refresh rate
    global _clear_throttle_rate

    _clear_throttle_rate = min(
        round(screen.refreshRate()),
        _clear_throttle_rate,
    )
    log.info(f'Set graphics update rate to {_clear_throttle_rate} Hz')

    # TODO: do styling / themeing setup
    # _style.style_ze_sheets(godwidget)

    sbar = godwidget.window.status_bar
    starting_done = sbar.open_status('starting ze sexy chartz')

    async with (
        trio.open_nursery() as root_n,
    ):

        # set root nursery and task stack for spawning other charts/feeds
        # that run cached in the bg
        godwidget._root_n = root_n

        # setup search widget and focus main chart view at startup
        search = _search.SearchWidget(godwidget=godwidget)
        search.bar.unfocus()

        # add search singleton to global chart-space widget
        godwidget.hbox.addWidget(
            search,

            # alights to top and uses minmial space based on
            # search bar size hint (i think?)
            alignment=Qt.AlignTop
        )
        godwidget.search = search

        symbol, _, provider = sym.rpartition('.')

        # this internally starts a ``display_symbol_data()`` task above
        order_mode_ready = godwidget.load_symbol(provider, symbol, loglevel)

        # spin up a search engine for the local cached symbol set
        async with _search.register_symbol_search(

            provider_name='cache',
            search_routine=partial(
                _search.search_simple_dict,
                source=godwidget._chart_cache,
            ),
            # cache is super fast so debounce on super short period
            pause_period=0.01,

        ):
            # load other providers into search **after**
            # the chart's select cache
            root_n.start_soon(load_providers, brokernames, loglevel)

            await order_mode_ready.wait()

            # start handling search bar kb inputs
            async with (

                _event.open_handler(
                    search.bar,
                    event_types={QEvent.KeyPress},
                    async_handler=_search.handle_keyboard_input,
                    # let key repeats pass through for search
                    filter_auto_repeats=False,
                )
            ):
                # remove startup status text
                starting_done()
                await trio.sleep_forever()


def _main(
    sym: str,
    brokernames: [str],
    piker_loglevel: str,
    tractor_kwargs,
) -> None:
    """Sync entry point to start a chart app.

    """
    # Qt entry point
    run_qtractor(
        func=_async_main,
        args=(sym, brokernames, piker_loglevel),
        main_widget=GodWidget,
        tractor_kwargs=tractor_kwargs,
    )
