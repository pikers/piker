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

'''
High level chart-widget apis.

'''
from typing import Optional

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QFrame,
    QWidget,
    # QSizePolicy,
)
import numpy as np
import pyqtgraph as pg
import trio

from ._axes import (
    DynamicDateAxis,
    PriceAxis,
    YAxisLabel,
)
from ._cursor import (
    Cursor,
    ContentsLabel,
)
from ._l1 import L1Labels
from ._ohlc import BarItems
from ._curve import FastAppendCurve
from ._style import (
    hcolor,
    CHART_MARGINS,
    _xaxis_at,
    _min_points_to_show,
    _bars_from_right_in_follow_mode,
    _bars_to_left_in_follow_mode,
)
from ..data.feed import Feed
from ..data._source import Symbol
from ..log import get_logger
from ._interaction import ChartView
from ._forms import FieldsForm


log = get_logger(__name__)


class GodWidget(QWidget):
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

        self.hbox = QtWidgets.QHBoxLayout(self)
        self.hbox.setContentsMargins(0, 0, 0, 0)
        self.hbox.setSpacing(6)
        self.hbox.setAlignment(Qt.AlignTop)

        self.vbox = QtWidgets.QVBoxLayout()
        self.vbox.setContentsMargins(0, 0, 0, 0)
        self.vbox.setSpacing(2)
        self.vbox.setAlignment(Qt.AlignTop)

        self.hbox.addLayout(self.vbox)

        # self.toolbar_layout = QtWidgets.QHBoxLayout()
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
    #     self.tf_layout = QtWidgets.QHBoxLayout()
    #     self.tf_layout.setSpacing(0)
    #     self.tf_layout.setContentsMargins(0, 12, 0, 0)
    #     time_frames = ('1M', '5M', '15M', '30M', '1H', '1D', '1W', 'MN')
    #     btn_prefix = 'TF'

    #     for tf in time_frames:
    #         btn_name = ''.join([btn_prefix, tf])
    #         btn = QtWidgets.QPushButton(tf)
    #         # TODO:
    #         btn.setEnabled(False)
    #         setattr(self, btn_name, btn)
    #         self.tf_layout.addWidget(btn)

    #     self.toolbar_layout.addLayout(self.tf_layout)

    # XXX: strat loader/saver that we don't need yet.
    # def init_strategy_ui(self):
    #     self.strategy_box = StrategyBoxWidget(self)
    #     self.toolbar_layout.addWidget(self.strategy_box)

    async def load_symbol(
        self,

        providername: str,
        symbol_key: str,
        loglevel: str,

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
            # self.vbox.removeWidget(self.linkedsplits)

        # switching to a new viewable chart
        if linkedsplits is None or reset:
            from ._display import display_symbol_data

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

            # TODO:
            # - we'll probably want per-instrument/provider state here?
            #   change the order config form over to the new chart

            # XXX: since the pp config is a singleton widget we have to
            # also switch it over to the new chart's interal-layout
            # self.linkedsplits.chart.qframe.hbox.removeWidget(self.pp_pane)
            chart = linkedsplits.chart
            await chart.resume_all_feeds()

        # chart is already in memory so just focus it
        if self.linkedsplits:
            self.linkedsplits.unfocus()

        self.vbox.addWidget(linkedsplits)

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

    def focus(self) -> None:
        '''Focus the top level widget which in turn focusses the chart
        ala "view mode".

        '''
        # go back to view-mode focus (aka chart focus)
        self.clearFocus()
        self.linkedsplits.chart.setFocus()


class ChartnPane(QFrame):
    '''One-off ``QFrame`` composite which pairs a chart
    + sidepane (often a ``FieldsForm`` + other widgets if
    provided) forming a, sort of, "chart row" with a side panel
    for configuration and display of off-chart data.

    See composite widgets docs for deats:
    https://doc.qt.io/qt-5/qwidget.html#composite-widgets

    '''
    sidepane: FieldsForm
    hbox: QtWidgets.QHBoxLayout
    chart: Optional['ChartPlotWidget'] = None

    def __init__(
        self,

        sidepane: FieldsForm,
        parent=None,

    ) -> None:

        super().__init__(parent)

        self.sidepane = sidepane
        self.chart = None

        hbox = self.hbox = QtWidgets.QHBoxLayout(self)
        hbox.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.setSpacing(3)

        # self.setMaximumWidth()


class LinkedSplits(QWidget):
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
        self.cursor: Cursor = None  # crosshair graphics

        self.godwidget = godwidget
        self.chart: ChartPlotWidget = None  # main (ohlc) chart
        self.subplots: dict[tuple[str, ...], ChartPlotWidget] = {}

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

        self.splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        self.splitter.setMidLineWidth(0)
        self.splitter.setHandleWidth(2)

        self.layout = QtWidgets.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.addWidget(self.splitter)

        self._symbol: Symbol = None

    @property
    def symbol(self) -> Symbol:
        return self._symbol

    def set_split_sizes(
        self,
        prop: Optional[float] = None,

    ) -> None:
        '''Set the proportion of space allocated for linked subcharts.

        '''
        ln = len(self.subplots)

        if not prop:
            # proportion allocated to consumer subcharts
            if ln < 2:
                prop = 1/(.666 * 6)
            elif ln >= 2:
                prop = 3/8

        major = 1 - prop
        min_h_ind = int((self.height() * prop) / ln)

        sizes = [int(self.height() * major)]
        sizes.extend([min_h_ind] * ln)

        self.splitter.setSizes(sizes)

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
        sidepane: FieldsForm,

        style: str = 'bar',

    ) -> 'ChartPlotWidget':
        """Start up and show main (price) chart and all linked subcharts.

        The data input struct array must include OHLC fields.
        """
        # add crosshairs
        self.cursor = Cursor(
            linkedsplits=self,
            digits=symbol.tick_size_digits,
        )

        self.chart = self.add_plot(

            name=symbol.key,
            array=array,
            # xaxis=self.xaxis,
            style=style,
            _is_main=True,

            sidepane=sidepane,
        )
        # add crosshair graphic
        self.chart.addItem(self.cursor)

        # axis placement
        if _xaxis_at == 'bottom':
            self.chart.hideAxis('bottom')

        # style?
        self.chart.setFrameStyle(
            QFrame.StyledPanel |
            QFrame.Plain
        )

        return self.chart

    def add_plot(
        self,

        name: str,
        array: np.ndarray,

        array_key: Optional[str] = None,
        # xaxis: Optional[DynamicDateAxis] = None,
        style: str = 'line',
        _is_main: bool = False,

        sidepane: Optional[QWidget] = None,

        **cpw_kwargs,

    ) -> 'ChartPlotWidget':
        '''Add (sub)plots to chart widget by name.

        If ``name`` == ``"main"`` the chart will be the the primary view.

        '''
        if self.chart is None and not _is_main:
            raise RuntimeError(
                "A main plot must be created first with `.plot_ohlc_main()`")

        # source of our custom interactions
        cv = ChartView(name)
        cv.linkedsplits = self

        # use "indicator axis" by default

        # TODO: we gotta possibly assign this back
        # to the last subplot on removal of some last subplot

        xaxis = DynamicDateAxis(
            orientation='bottom',
            linkedsplits=self
        )

        if self.xaxis:
            self.xaxis.hide()
            self.xaxis = xaxis

        qframe = ChartnPane(sidepane=sidepane, parent=self.splitter)
        cpw = ChartPlotWidget(

            # this name will be used to register the primary
            # graphics curve managed by the subchart
            name=name,
            data_key=array_key or name,

            array=array,
            parent=qframe,
            linkedsplits=self,
            axisItems={
                'bottom': xaxis,
                'right': PriceAxis(linkedsplits=self, orientation='right'),
                'left': PriceAxis(linkedsplits=self, orientation='left'),
            },
            viewBox=cv,
            **cpw_kwargs,
        )

        qframe.chart = cpw
        qframe.hbox.addWidget(cpw)

        # so we can look this up and add back to the splitter
        # on a symbol switch
        cpw.qframe = qframe
        assert cpw.parent() == qframe

        # add sidepane **after** chart; place it on axis side
        qframe.hbox.addWidget(
            sidepane,
            alignment=Qt.AlignTop
        )
        cpw.sidepane = sidepane

        # give viewbox as reference to chart
        # allowing for kb controls and interactions on **this** widget
        # (see our custom view mode in `._interactions.py`)
        cv.chart = cpw

        cpw.plotItem.vb.linkedsplits = self
        cpw.setFrameStyle(
            QtWidgets.QFrame.StyledPanel
            # | QtWidgets.QFrame.Plain
        )

        cpw.hideButtons()

        # XXX: gives us outline on backside of y-axis
        cpw.getPlotItem().setContentsMargins(*CHART_MARGINS)

        # link chart x-axis to main chart
        # this is 1/2 of where the `Link` in ``LinkedSplit``
        # comes from ;)
        cpw.setXLink(self.chart)

        # add to cross-hair's known plots
        self.cursor.add_plot(cpw)

        # draw curve graphics
        if style == 'bar':
            cpw.draw_ohlc(name, array, array_key=array_key)

        elif style == 'line':
            cpw.draw_curve(
                name,
                array,
                array_key=array_key,
                color='default_lightest',
            )

        elif style == 'step':
            cpw.draw_curve(
                name,
                array,
                array_key=array_key,
                step_mode=True,
                color='davies',
                fill_color='davies',
            )

        else:
            raise ValueError(f"Chart style {style} is currently unsupported")

        if not _is_main:
            # track by name
            self.subplots[name] = cpw
            self.splitter.addWidget(qframe)
            # scale split regions
            self.set_split_sizes()

        else:
            assert style == 'bar', 'main chart must be OHLC'

        return cpw

    def resize_sidepanes(
        self,
    ) -> None:
        '''Size all sidepanes based on the OHLC "main" plot.

        '''
        for name, cpw in self.subplots.items():
            cpw.sidepane.setMinimumWidth(self.chart.sidepane.width())
            cpw.sidepane.setMaximumWidth(self.chart.sidepane.width())


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
    sig_mouse_leave = QtCore.pyqtSignal(object)
    sig_mouse_enter = QtCore.pyqtSignal(object)

    _l1_labels: L1Labels = None

    mode_name: str = 'view'

    # TODO: can take a ``background`` color setting - maybe there's
    # a better one?

    def __init__(
        self,

        # the "data view" we generate graphics from
        name: str,
        array: np.ndarray,
        data_key: str,
        linkedsplits: LinkedSplits,

        view_color: str = 'papas_special',
        pen_color: str = 'bracket',

        static_yrange: Optional[tuple[float, float]] = None,

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
            # useOpenGL=True,
            **kwargs
        )
        self.name = name
        self.data_key = data_key
        self.linked = linkedsplits

        # scene-local placeholder for book graphics
        # sizing to avoid overlap with data contents
        self._max_l1_line_len: float = 0

        # self.setViewportMargins(0, 0, 0, 0)
        # self._ohlc = array  # readonly view of ohlc data

        # readonly view of data arrays
        self._arrays = {
            'ohlc': array,
        }
        self._graphics = {}  # registry of underlying graphics
        self._overlays = set()  # registry of overlay curve names

        self._feeds: dict[Symbol, Feed] = {}

        self._labels = {}  # registry of underlying graphics
        self._ysticks = {}  # registry of underlying graphics

        self._vb = self.plotItem.vb
        self._static_yrange = static_yrange  # for "known y-range style"
        self._view_mode: str = 'follow'

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

    async def resume_all_feeds(self):
        for feed in self._feeds.values():
            await feed.resume()

    async def pause_all_feeds(self):
        for feed in self._feeds.values():
            await feed.pause()

    @property
    def view(self) -> ChartView:
        return self._vb

    def focus(self) -> None:
        self._vb.setFocus()

    def last_bar_in_view(self) -> int:
        self._arrays['ohlc'][-1]['index']

    def is_valid_index(self, index: int) -> bool:
        return index >= 0 and index < self._arrays['ohlc'][-1]['index']

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

    def view_range(self) -> tuple[int, int]:
        vr = self.viewRect()
        return int(vr.left()), int(vr.right())

    def bars_range(self) -> tuple[int, int, int, int]:
        """Return a range tuple for the bars present in view.
        """
        l, r = self.view_range()
        array = self._arrays['ohlc']
        lbar = max(l, array[0]['index'])
        rbar = min(r, array[-1]['index'])
        return l, lbar, rbar, r

    def default_view(
        self,
        index: int = -1,
    ) -> None:
        """Set the view box to the "default" startup view of the scene.

        """
        xlast = self._arrays['ohlc'][index]['index']
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

        array_key: Optional[str] = None,

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

        data_key = array_key or name
        self._graphics[data_key] = graphics

        self.linked.cursor.contents_labels.add_label(
            self,
            'ohlc',
            anchor_at=('top', 'left'),
            update_func=ContentsLabel.update_from_ohlc,
        )

        self._add_sticky(name)

        return graphics

    def draw_curve(
        self,

        name: str,
        data: np.ndarray,

        array_key: Optional[str] = None,
        overlay: bool = False,
        color: Optional[str] = None,
        add_label: bool = True,

        **pdi_kwargs,

    ) -> pg.PlotDataItem:
        """Draw a "curve" (line plot graphics) for the provided data in
        the input array ``data``.

        """
        pdi_kwargs.update({
            'color': color or self.pen_color or 'default_light'
        })

        data_key = array_key or name

        # curve = pg.PlotDataItem(
        # curve = pg.PlotCurveItem(
        curve = FastAppendCurve(
            y=data[data_key],
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
        # curve.curve.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # don't ever use this - it's a colossal nightmare of artefacts
        # and is disastrous for performance.
        # curve.setCacheMode(QtWidgets.QGraphicsItem.ItemCoordinateCache)

        self.addItem(curve)

        # register curve graphics and backing array for name
        self._graphics[name] = curve
        self._arrays[data_key or name] = data

        if overlay:
            anchor_at = ('bottom', 'left')
            self._overlays.add(name)

        else:
            anchor_at = ('top', 'left')

            # TODO: something instead of stickies for overlays
            # (we need something that avoids clutter on x-axis).
            self._add_sticky(name, bg_color='default_light')

        if self.linked.cursor:
            self.linked.cursor.add_curve_cursor(self, curve)

            if add_label:
                self.linked.cursor.contents_labels.add_label(
                    self,
                    data_key or name,
                    anchor_at=anchor_at
                )

        return curve

    def _add_sticky(
        self,

        name: str,
        bg_color='bracket',

    ) -> YAxisLabel:

        # if the sticky is for our symbol
        # use the tick size precision for display
        sym = self.linked.symbol
        if name == sym.key:
            digits = sym.tick_size_digits
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
        self._arrays['ohlc'] = array
        graphics = self._graphics[name]
        graphics.update_from_array(array, **kwargs)
        return graphics

    def update_curve_from_array(
        self,

        name: str,
        array: np.ndarray,
        array_key: Optional[str] = None,

        **kwargs,

    ) -> pg.GraphicsObject:
        """Update the named internal graphics from ``array``.

        """

        data_key = array_key or name
        if name not in self._overlays:
            self._arrays['ohlc'] = array
        else:
            self._arrays[data_key] = array

        curve = self._graphics[name]

        if len(array):
            # TODO: we should instead implement a diff based
            # "only update with new items" on the pg.PlotCurveItem
            # one place to dig around this might be the `QBackingStore`
            # https://doc.qt.io/qt-5/qbackingstore.html
            # curve.setData(y=array[name], x=array['index'], **kwargs)
            curve.update_from_array(
                x=array['index'],
                y=array[data_key],
                **kwargs
            )

        return curve

    def _set_yrange(
        self,
        *,
        yrange: Optional[tuple[float, float]] = None,
        range_margin: float = 0.06,
    ) -> None:
        '''Set the viewable y-range based on embedded data.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.

        '''
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

            # begin = self._arrays['ohlc'][0]['index'] - extra

            # # end = len(self._arrays['ohlc']) - 1 + extra
            # end = self._arrays['ohlc'][-1]['index'] - 1 + extra

            # XXX: test code for only rendering lines for the bars in view.
            # This turns out to be very very poor perf when scaling out to
            # many bars (think > 1k) on screen.
            # name = self.name
            # bars = self._graphics[self.name]
            # bars.draw_lines(
            #   istart=max(lbar, l), iend=min(rbar, r), just_history=True)

            # bars_len = rbar - lbar
            # log.debug(
            #     f"\nl: {l}, lbar: {lbar}, rbar: {rbar}, r: {r}\n"
            #     f"view_len: {view_len}, bars_len: {bars_len}\n"
            #     f"begin: {begin}, end: {end}, extra: {extra}"
            # )
            # self._set_xlimits(begin, end)

            # TODO: this should be some kind of numpy view api
            # bars = self._arrays['ohlc'][lbar:rbar]

            a = self._arrays['ohlc']
            ifirst = a[0]['index']
            bars = a[lbar - ifirst:rbar - ifirst + 1]

            if not len(bars):
                # likely no data loaded yet or extreme scrolling?
                log.error(f"WTF bars_range = {lbar}:{rbar}")
                return

            if self.data_key != self.linked.symbol.key:
                bars = bars[self.data_key]
                ylow = np.nanmin(bars)
                yhigh = np.nanmax(bars)
                # print(f'{(ylow, yhigh)}')
            else:
                # just the std ohlc bars
                ylow = np.nanmin(bars['low'])
                yhigh = np.nanmax(bars['high'])

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

    def get_index(self, time: float) -> int:

        # TODO: this should go onto some sort of
        # data-view strimg thinger..right?
        ohlc = self._shm.array

        # XXX: not sure why the time is so off here
        # looks like we're gonna have to do some fixing..
        indexes = ohlc['time'] >= time

        if any(indexes):
            return ohlc['index'][indexes][-1]
        else:
            return ohlc['index'][-1]
