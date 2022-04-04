# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
from __future__ import annotations
from typing import Optional, TYPE_CHECKING

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import (
    Qt,
    QLineF,
    # QPointF,
)
from PyQt5.QtWidgets import (
    QFrame,
    QWidget,
    QHBoxLayout,
    QVBoxLayout,
    QSplitter,
)
import numpy as np
import pyqtgraph as pg
import trio
from pydantic import BaseModel

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
)
from ..data.feed import Feed
from ..data._source import Symbol
from ..data._sharedmem import ShmArray
from ..log import get_logger
from ._interaction import ChartView
from ._forms import FieldsForm
from ._overlay import PlotItemOverlay

if TYPE_CHECKING:
    from ._display import DisplayState

log = get_logger(__name__)


class GodWidget(QWidget):
    '''
    "Our lord and savior, the holy child of window-shua, there is no
    widget above thee." - 6|6

    The highest level composed widget which contains layouts for
    organizing charts as well as other sub-widgets used to control or
    modify them.

    '''
    def __init__(

        self,
        parent=None,

    ) -> None:

        super().__init__(parent)

        self.hbox = QHBoxLayout(self)
        self.hbox.setContentsMargins(0, 0, 0, 0)
        self.hbox.setSpacing(6)
        self.hbox.setAlignment(Qt.AlignTop)

        self.vbox = QVBoxLayout()
        self.vbox.setContentsMargins(0, 0, 0, 0)
        self.vbox.setSpacing(2)
        self.vbox.setAlignment(Qt.AlignTop)

        self.hbox.addLayout(self.vbox)

        # self.toolbar_layout = QHBoxLayout()
        # self.toolbar_layout.setContentsMargins(0, 0, 0, 0)
        # self.vbox.addLayout(self.toolbar_layout)

        # self.init_timeframes_ui()
        # self.init_strategy_ui()
        # self.vbox.addLayout(self.hbox)

        self._chart_cache: dict[str, LinkedSplits] = {}
        self.linkedsplits: Optional[LinkedSplits] = None

        # assigned in the startup func `_async_main()`
        self._root_n: trio.Nursery = None

        self._widgets: dict[str, QWidget] = {}
        self._resizing: bool = False

    # def init_timeframes_ui(self):
    #     self.tf_layout = QHBoxLayout()
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

    def set_chart_symbol(
        self,
        symbol_key: str,  # of form <fqsn>.<providername>
        linkedsplits: LinkedSplits,  # type: ignore

    ) -> None:
        # re-sort org cache symbol list in LIFO order
        cache = self._chart_cache
        cache.pop(symbol_key, None)
        cache[symbol_key] = linkedsplits

    def get_chart_symbol(
        self,
        symbol_key: str,

    ) -> LinkedSplits:  # type: ignore
        return self._chart_cache.get(symbol_key)

    async def load_symbol(
        self,
        providername: str,
        symbol_key: str,
        loglevel: str,
        reset: bool = False,

    ) -> trio.Event:
        '''
        Load a new contract into the charting app.

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
            self.linkedsplits.unfocus()

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
            self.vbox.addWidget(linkedsplits)

            linkedsplits.show()
            linkedsplits.focus()
            await trio.sleep(0)

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

            # chart is already in memory so just focus it
            linkedsplits.show()
            linkedsplits.focus()
            linkedsplits.graphics_cycle()
            await trio.sleep(0)

            # resume feeds *after* rendering chart view asap
            chart.resume_all_feeds()

            # TODO: we need a check to see if the chart
            # last had the xlast in view, if so then shift so it's
            # still in view, if the user was viewing history then
            # do nothing yah?
            chart.default_view()

        self.linkedsplits = linkedsplits
        symbol = linkedsplits.symbol
        if symbol is not None:
            self.window.setWindowTitle(
                f'{symbol.front_fqsn()} '
                f'tick:{symbol.tick_size}'
            )

        return order_mode_started

    def focus(self) -> None:
        '''
        Focus the top level widget which in turn focusses the chart
        ala "view mode".

        '''
        # go back to view-mode focus (aka chart focus)
        self.clearFocus()
        self.linkedsplits.chart.setFocus()

    def resizeEvent(self, event: QtCore.QEvent) -> None:
        '''
        Top level god widget resize handler.

        Where we do UX magic to make things not suck B)

        '''
        if self._resizing:
            return

        self._resizing = True

        log.info('God widget resize')
        for name, widget in self._widgets.items():
            widget.on_resize()

        self._resizing = False


class ChartnPane(QFrame):
    '''
    One-off ``QFrame`` composite which pairs a chart
    + sidepane (often a ``FieldsForm`` + other widgets if
    provided) forming a, sort of, "chart row" with a side panel
    for configuration and display of off-chart data.

    See composite widgets docs for deats:
    https://doc.qt.io/qt-5/qwidget.html#composite-widgets

    '''
    sidepane: FieldsForm
    hbox: QHBoxLayout
    chart: Optional['ChartPlotWidget'] = None

    def __init__(
        self,

        sidepane: FieldsForm,
        parent=None,

    ) -> None:

        super().__init__(parent)

        self.sidepane = sidepane
        self.chart = None

        hbox = self.hbox = QHBoxLayout(self)
        hbox.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.setSpacing(3)


class LinkedSplits(QWidget):
    '''
    Composite that holds a central chart plus a set of (derived)
    subcharts (usually computed from the original data) arranged in
    a splitter for resizing.

    A single internal references to the data is maintained
    for each chart and can be updated externally.

    '''
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
        # placeholder for last appended ``PlotItem``'s bottom axis.
        self.xaxis_chart = None

        self.splitter = QSplitter(QtCore.Qt.Vertical)
        self.splitter.setMidLineWidth(0)
        self.splitter.setHandleWidth(2)

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.addWidget(self.splitter)

        # chart-local graphics state that can be passed to
        # a ``graphic_update_cycle()`` call by any task wishing to
        # update the UI for a given "chart instance".
        self.display_state: Optional[DisplayState] = None

        self._symbol: Symbol = None

    def graphics_cycle(self, **kwargs) -> None:
        from . import _display
        ds = self.display_state
        if ds:
            return _display.graphics_update_cycle(ds, **kwargs)

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

        # proportion allocated to consumer subcharts
        if not prop:
            prop = 3/8*5/8

            # if ln < 2:
            #     prop = 3/8*5/8

            # elif ln >= 2:
            #     prop = 3/8

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

    ) -> ChartPlotWidget:
        '''
        Start up and show main (price) chart and all linked subcharts.

        The data input struct array must include OHLC fields.

        '''
        # add crosshairs
        self.cursor = Cursor(
            linkedsplits=self,
            digits=symbol.tick_size_digits,
        )

        # NOTE: atm the first (and only) OHLC price chart for the symbol
        # is given a special reference but in the future there shouldn't
        # be no distinction since we will have multiple symbols per
        # view as part of "aggregate feeds".
        self.chart = self.add_plot(

            name=symbol.key,
            array=array,
            style=style,
            _is_main=True,

            sidepane=sidepane,
        )
        # add crosshair graphic
        self.chart.addItem(self.cursor)

        # axis placement
        if (
            _xaxis_at == 'bottom' and
            'bottom' in self.chart.plotItem.axes
        ):
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
        style: str = 'line',
        _is_main: bool = False,

        sidepane: Optional[QWidget] = None,

        **cpw_kwargs,

    ) -> ChartPlotWidget:
        '''
        Add (sub)plots to chart widget by key.

        '''
        if self.chart is None and not _is_main:
            raise RuntimeError(
                "A main plot must be created first with `.plot_ohlc_main()`")

        # use "indicator axis" by default

        # TODO: we gotta possibly assign this back
        # to the last subplot on removal of some last subplot
        xaxis = DynamicDateAxis(
            orientation='bottom',
            linkedsplits=self
        )
        axes = {
            'right': PriceAxis(linkedsplits=self, orientation='right'),
            'left': PriceAxis(linkedsplits=self, orientation='left'),
            'bottom': xaxis,
        }

        qframe = ChartnPane(
            sidepane=sidepane,
            parent=self.splitter,
        )
        cpw = ChartPlotWidget(

            # this name will be used to register the primary
            # graphics curve managed by the subchart
            name=name,
            data_key=array_key or name,

            array=array,
            parent=qframe,
            linkedsplits=self,
            axisItems=axes,
            **cpw_kwargs,
        )
        cpw.hideAxis('left')
        cpw.hideAxis('bottom')

        if self.xaxis_chart:
            self.xaxis_chart.hideAxis('bottom')

            # presuming we only want it at the true bottom of all charts.
            # XXX: uses new api from our ``pyqtgraph`` fork.
            # https://github.com/pikers/pyqtgraph/tree/plotitemoverlay_onto_pg_master
            # _ = self.xaxis_chart.removeAxis('bottom', unlink=False)
            # assert 'bottom' not in self.xaxis_chart.plotItem.axes

            self.xaxis_chart = cpw
            cpw.showAxis('bottom')

        if self.xaxis_chart is None:
            self.xaxis_chart = cpw

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

        cpw.plotItem.vb.linkedsplits = self
        cpw.setFrameStyle(
            QtWidgets.QFrame.StyledPanel
            # | QtWidgets.QFrame.Plain
        )

        # don't show the little "autoscale" A label.
        cpw.hideButtons()

        # XXX: gives us outline on backside of y-axis
        cpw.getPlotItem().setContentsMargins(*CHART_MARGINS)

        # link chart x-axis to main chart
        # this is 1/2 of where the `Link` in ``LinkedSplit``
        # comes from ;)
        cpw.setXLink(self.chart)

        add_label = False
        anchor_at = ('top', 'left')

        # draw curve graphics
        if style == 'bar':

            graphics, data_key = cpw.draw_ohlc(
                name,
                array,
                array_key=array_key
            )
            self.cursor.contents_labels.add_label(
                cpw,
                name,
                anchor_at=('top', 'left'),
                update_func=ContentsLabel.update_from_ohlc,
            )

        elif style == 'line':
            add_label = True
            graphics, data_key = cpw.draw_curve(
                name,
                array,
                array_key=array_key,
                color='default_light',
            )

        elif style == 'step':
            add_label = True
            graphics, data_key = cpw.draw_curve(
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

        # add to cross-hair's known plots
        # NOTE: add **AFTER** creating the underlying ``PlotItem``s
        # since we require that global (linked charts wide) axes have
        # been created!
        self.cursor.add_plot(cpw)

        if self.cursor and style != 'bar':
            self.cursor.add_curve_cursor(cpw, graphics)

            if add_label:
                self.cursor.contents_labels.add_label(
                    cpw,
                    data_key,
                    anchor_at=anchor_at,
                )

        self.resize_sidepanes()
        return cpw

    def resize_sidepanes(
        self,
    ) -> None:
        '''
        Size all sidepanes based on the OHLC "main" plot and its
        sidepane width.

        '''
        main_chart = self.chart
        if main_chart:
            sp_w = main_chart.sidepane.width()
            for name, cpw in self.subplots.items():
                cpw.sidepane.setMinimumWidth(sp_w)
                cpw.sidepane.setMaximumWidth(sp_w)


# class FlowsTable(pydantic.BaseModel):
#     '''
#     Data-AGGRegate: high level API onto multiple (categorized)
#     ``Flow``s with high level processing routines for
#     multi-graphics computations and display.

#     '''
#     flows: dict[str, np.ndarray] = {}


class Flow(BaseModel):
    '''
    (FinancialSignal-)Flow compound type which wraps a real-time
    graphics (curve) and its backing data stream together for high level
    access and control.

    '''
    class Config:
        arbitrary_types_allowed = True

    name: str
    plot: pg.PlotItem
    shm: Optional[ShmArray] = None  # may be filled in "later"


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
    def mk_vb(self, name: str) -> ChartView:
        cv = ChartView(name)
        cv.linkedsplits = self.linked
        return cv

    def __init__(
        self,

        # the "data view" we generate graphics from
        name: str,
        array: np.ndarray,
        data_key: str,
        linkedsplits: LinkedSplits,

        view_color: str = 'papas_special',
        pen_color: str = 'bracket',

        # TODO: load from config
        use_open_gl: bool = False,

        static_yrange: Optional[tuple[float, float]] = None,

        **kwargs,
    ):
        '''
        Configure initial display settings and connect view callback
        handlers.

        '''
        self.view_color = view_color
        self.pen_color = pen_color

        # NOTE: must be set bfore calling ``.mk_vb()``
        self.linked = linkedsplits

        # source of our custom interactions
        self.cv = cv = self.mk_vb(name)

        super().__init__(
            background=hcolor(view_color),
            viewBox=cv,
            # parent=None,
            # plotItem=None,
            # antialias=True,
            **kwargs
        )
        # give viewbox as reference to chart
        # allowing for kb controls and interactions on **this** widget
        # (see our custom view mode in `._interactions.py`)
        cv.chart = self

        # ensure internal pi matches
        assert self.cv is self.plotItem.vb

        self.useOpenGL(use_open_gl)
        self.name = name
        self.data_key = data_key or name

        # scene-local placeholder for book graphics
        # sizing to avoid overlap with data contents
        self._max_l1_line_len: float = 0

        # self.setViewportMargins(0, 0, 0, 0)
        # self._ohlc = array  # readonly view of ohlc data

        # TODO: move to Aggr above XD
        # readonly view of data arrays
        self._arrays = {
            self.data_key: array,
        }
        self._graphics = {}  # registry of underlying graphics

        # registry of overlay curve names
        self._flows: dict[str, Flow] = {}

        self._feeds: dict[Symbol, Feed] = {}

        self._labels = {}  # registry of underlying graphics
        self._ysticks = {}  # registry of underlying graphics

        self._static_yrange = static_yrange  # for "known y-range style"
        self._view_mode: str = 'follow'

        # show background grid
        self.showGrid(x=False, y=True, alpha=0.3)

        self.default_view()
        self.cv.enable_auto_yrange()

        self.pi_overlay: PlotItemOverlay = PlotItemOverlay(self.plotItem)

    def resume_all_feeds(self):
        for feed in self._feeds.values():
            self.linked.godwidget._root_n.start_soon(feed.resume)

    def pause_all_feeds(self):
        for feed in self._feeds.values():
            self.linked.godwidget._root_n.start_soon(feed.pause)

    @property
    def view(self) -> ChartView:
        return self.plotItem.vb

    def focus(self) -> None:
        self.view.setFocus()

    def last_bar_in_view(self) -> int:
        self._arrays[self.name][-1]['index']

    def is_valid_index(self, index: int) -> bool:
        return index >= 0 and index < self._arrays[self.name][-1]['index']

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
        '''
        Return a range tuple for the bars present in view.

        '''
        l, r = self.view_range()
        array = self._arrays[self.name]
        start, stop = self._xrange = (
            array[0]['index'],
            array[-1]['index'],
        )
        lbar = max(l, start)
        rbar = min(r, stop)
        return l, lbar, rbar, r

    def curve_width_pxs(
        self,
    ) -> float:
        _, lbar, rbar, _ = self.bars_range()
        return self.view.mapViewToDevice(
            QLineF(lbar, 0, rbar, 0)
        ).length()

    def pre_l1_xs(self) -> tuple[float, float]:
        '''
        Return the view x-coord for the value just before
        the L1 labels on the y-axis as well as the length
        of that L1 label from the y-axis.

        '''
        line_end, marker_right, yaxis_x = self.marker_right_points()
        view = self.view
        line = view.mapToView(
            QLineF(line_end, 0, yaxis_x, 0)
        )
        return line.x1(), line.length()

    def marker_right_points(
        self,
        marker_size: int = 20,

    ) -> (float, float, float):
        '''
        Return x-dimension, y-axis-aware, level-line marker oriented scene
        values.

        X values correspond to set the end of a level line, end of
        a paried level line marker, and the right most side of the "right"
        axis respectively.

        '''
        # TODO: compute some sensible maximum value here
        # and use a humanized scheme to limit to that length.
        l1_len = self._max_l1_line_len
        ryaxis = self.getAxis('right')

        r_axis_x = ryaxis.pos().x()
        up_to_l1_sc = r_axis_x - l1_len - 10

        marker_right = up_to_l1_sc - (1.375 * 2 * marker_size)
        line_end = marker_right - (6/16 * marker_size)

        return line_end, marker_right, r_axis_x

    def default_view(
        self,
        steps_on_screen: Optional[int] = None

    ) -> None:
        '''
        Set the view box to the "default" startup view of the scene.

        '''
        try:
            index = self._arrays[self.name]['index']
        except IndexError:
            log.warning(f'array for {self.name} not loaded yet?')
            return

        xfirst, xlast = index[0], index[-1]
        l, lbar, rbar, r = self.bars_range()

        marker_pos, l1_len = self.pre_l1_xs()
        end = xlast + l1_len + 1

        if (
            rbar < 0
            or l < xfirst
            or (rbar - lbar) < 6
        ):
            # set fixed bars count on screen that approx includes as
            # many bars as possible before a downsample line is shown.
            begin = xlast - round(6116 / 6)

        else:
            begin = end - (r - l)

        # for debugging
        # print(
        #     f'bars range: {brange}\n'
        #     f'xlast: {xlast}\n'
        #     f'marker pos: {marker_pos}\n'
        #     f'l1 len: {l1_len}\n'
        #     f'begin: {begin}\n'
        #     f'end: {end}\n'
        # )

        # remove any custom user yrange setttings
        if self._static_yrange == 'axis':
            self._static_yrange = None

        view = self.view
        view.setXRange(
            min=begin,
            max=end,
            padding=0,
        )
        view._set_yrange()
        self.view.maybe_downsample_graphics()
        try:
            self.linked.graphics_cycle()
        except IndexError:
            pass

    def increment_view(
        self,
        steps: int = 1,
        vb: Optional[ChartView] = None,

    ) -> None:
        """
        Increment the data view one step to the right thus "following"
        the current time slot/step/bar.

        """
        l, r = self.view_range()
        view = vb or self.view
        view.setXRange(
            min=l + steps,
            max=r + steps,

            # TODO: holy shit, wtf dude... why tf would this not be 0 by
            # default... speechless.
            padding=0,
        )

    def draw_ohlc(
        self,
        name: str,
        data: np.ndarray,

        array_key: Optional[str] = None,

    ) -> (pg.GraphicsObject, str):
        '''
        Draw OHLC datums to chart.

        '''
        graphics = BarItems(
            self.linked,
            self.plotItem,
            pen_color=self.pen_color,
            name=name,
        )

        # adds all bar/candle graphics objects for each data point in
        # the np array buffer to be drawn on next render cycle
        self.plotItem.addItem(graphics)

        # draw after to allow self.scene() to work...
        graphics.draw_from_data(data)

        data_key = array_key or name
        self._graphics[data_key] = graphics
        self._add_sticky(name, bg_color='davies')

        return graphics, data_key

    def overlay_plotitem(
        self,
        name: str,
        index: Optional[int] = None,
        axis_title: Optional[str] = None,
        axis_side: str = 'right',
        axis_kwargs: dict = {},

    ) -> pg.PlotItem:

        # Custom viewbox impl
        cv = self.mk_vb(name)
        cv.chart = self

        allowed_sides = {'left', 'right'}
        if axis_side not in allowed_sides:
            raise ValueError(f'``axis_side``` must be in {allowed_sides}')
        yaxis = PriceAxis(
            orientation=axis_side,
            linkedsplits=self.linked,
            **axis_kwargs,
        )

        pi = pg.PlotItem(
            parent=self.plotItem,
            name=name,
            enableMenu=False,
            viewBox=cv,
            axisItems={
                # 'bottom': xaxis,
                axis_side: yaxis,
            },
            default_axes=[],
        )
        pi.hideButtons()

        # cv.enable_auto_yrange(self.view)
        cv.enable_auto_yrange()

        # compose this new plot's graphics with the current chart's
        # existing one but with separate axes as neede and specified.
        self.pi_overlay.add_plotitem(
            pi,
            index=index,

            # only link x-axes,
            link_axes=(0,),
        )

        # add axis title
        # TODO: do we want this API to still work?
        # raxis = pi.getAxis('right')
        axis = self.pi_overlay.get_axis(pi, axis_side)
        axis.set_title(axis_title or name, view=pi.getViewBox())

        return pi

    def draw_curve(
        self,

        name: str,
        data: np.ndarray,

        array_key: Optional[str] = None,
        overlay: bool = False,
        color: Optional[str] = None,
        add_label: bool = True,

        **pdi_kwargs,

    ) -> (pg.PlotDataItem, str):
        '''
        Draw a "curve" (line plot graphics) for the provided data in
        the input array ``data``.

        '''
        color = color or self.pen_color or 'default_light'
        pdi_kwargs.update({
            'color': color
        })

        data_key = array_key or name

        # yah, we wrote our own B)
        curve = FastAppendCurve(
            y=data[data_key],
            x=data['index'],
            # antialias=True,
            name=name,

            # XXX: pretty sure this is just more overhead
            # on data reads and makes graphics rendering no faster
            # clipToView=True,

            **pdi_kwargs,
        )

        # XXX: see explanation for different caching modes:
        # https://stackoverflow.com/a/39410081
        # seems to only be useful if we don't re-generate the entire
        # QPainterPath every time
        # curve.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # don't ever use this - it's a colossal nightmare of artefacts
        # and is disastrous for performance.
        # curve.setCacheMode(QtWidgets.QGraphicsItem.ItemCoordinateCache)

        # register curve graphics and backing array for name
        self._graphics[name] = curve
        self._arrays[data_key] = data

        pi = self.plotItem

        # TODO: this probably needs its own method?
        if overlay:
            if isinstance(overlay, pg.PlotItem):
                if overlay not in self.pi_overlay.overlays:
                    raise RuntimeError(
                            f'{overlay} must be from `.plotitem_overlay()`'
                        )
                pi = overlay

            # anchor_at = ('bottom', 'left')
            self._flows[name] = Flow(name=name, plot=pi)

        else:
            # anchor_at = ('top', 'left')

            # TODO: something instead of stickies for overlays
            # (we need something that avoids clutter on x-axis).
            self._add_sticky(name, bg_color=color)

        # NOTE: this is more or less the RENDER call that tells Qt to
        # start showing the generated graphics-curves. This is kind of
        # of edge-triggered call where once added any
        # ``QGraphicsItem.update()`` calls are automatically displayed.
        # Our internal graphics objects have their own "update from
        # data" style method API that allows for real-time updates on
        # the next render cycle; just note a lot of the real-time
        # updates are implicit and require a bit of digging to
        # understand.
        pi.addItem(curve)

        return curve, data_key

    # TODO: make this a ctx mngr
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
            # parent=self.getAxis('right'),
            parent=self.pi_overlay.get_axis(self.plotItem, 'right'),
            # TODO: pass this from symbol data
            digits=digits,
            opacity=1,
            bg_color=bg_color,
        )
        return last

    def update_graphics_from_array(
        self,
        graphics_name: str,

        array: Optional[np.ndarray] = None,
        array_key: Optional[str] = None,

        use_vr: bool = True,
        render: bool = True,

        **kwargs,

    ) -> pg.GraphicsObject:
        '''
        Update the named internal graphics from ``array``.

        '''
        if array is not None:
            assert len(array)

        data_key = array_key or graphics_name
        if graphics_name not in self._flows:
            data_key = self.name

        if array is not None:
            # write array to internal graphics table
            self._arrays[data_key] = array
        else:
            array = self._arrays[data_key]

        # array key and graphics "name" might be different..
        graphics = self._graphics[graphics_name]

        # compute "in-view" indices
        l, lbar, rbar, r = self.bars_range()
        indexes = array['index']
        ifirst = indexes[0]
        ilast = indexes[-1]

        lbar_i = max(l, ifirst) - ifirst
        rbar_i = min(r, ilast) - ifirst

        # TODO: we could do it this way as well no?
        # to_draw = array[lbar - ifirst:(rbar - ifirst) + 1]
        in_view = array[lbar_i: rbar_i + 1]

        if (
            not in_view.size
            or not render
        ):
            return graphics

        if isinstance(graphics, BarItems):
            graphics.update_from_array(
                array,
                in_view,
                view_range=(lbar_i, rbar_i) if use_vr else None,

                **kwargs,
            )

        else:
            graphics.update_from_array(
                x=array['index'],
                y=array[data_key],

                x_iv=in_view['index'],
                y_iv=in_view[data_key],
                view_range=(lbar_i, rbar_i) if use_vr else None,

                **kwargs
            )

        return graphics

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

    # TODO: pretty sure we can just call the cursor
    # directly not? i don't wee why we need special "signal proxies"
    # for this lul..
    def enterEvent(self, ev):  # noqa
        # pg.PlotWidget.enterEvent(self, ev)
        self.sig_mouse_enter.emit(self)

    def leaveEvent(self, ev):  # noqa
        # pg.PlotWidget.leaveEvent(self, ev)
        self.sig_mouse_leave.emit(self)
        self.scene().leaveEvent(ev)

    def get_index(self, time: float) -> int:

        # TODO: this should go onto some sort of
        # data-view thinger..right?
        ohlc = self._shm.array

        # XXX: not sure why the time is so off here
        # looks like we're gonna have to do some fixing..
        indexes = ohlc['time'] >= time

        if any(indexes):
            return ohlc['index'][indexes][-1]
        else:
            return ohlc['index'][-1]

    def in_view(
        self,
        array: np.ndarray,

    ) -> np.ndarray:
        '''
        Slice an input struct array providing only datums
        "in view" of this chart.

        '''
        l, lbar, rbar, r = self.bars_range()
        ifirst = array[0]['index']
        # slice data by offset from the first index
        # available in the passed datum set.
        return array[lbar - ifirst:(rbar - ifirst) + 1]

    def maxmin(
        self,
        name: Optional[str] = None,
        bars_range: Optional[tuple[int, int, int, int]] = None,

    ) -> tuple[float, float]:
        '''
        Return the max and min y-data values "in view".

        If ``bars_range`` is provided use that range.

        '''
        l, lbar, rbar, r = bars_range or self.bars_range()
        # TODO: logic to check if end of bars in view
        # extra = view_len - _min_points_to_show
        # begin = self._arrays['ohlc'][0]['index'] - extra
        # # end = len(self._arrays['ohlc']) - 1 + extra
        # end = self._arrays['ohlc'][-1]['index'] - 1 + extra

        # bars_len = rbar - lbar
        # log.debug(
        #     f"\nl: {l}, lbar: {lbar}, rbar: {rbar}, r: {r}\n"
        #     f"view_len: {view_len}, bars_len: {bars_len}\n"
        #     f"begin: {begin}, end: {end}, extra: {extra}"
        # )

        # TODO: here we should instead look up the ``Flow.shm.array``
        # and read directly from shm to avoid copying to memory first
        # and then reading it again here.
        a = self._arrays.get(name or self.name)
        if a is None:
            return None

        ifirst = a[0]['index']
        bars = a[lbar - ifirst:(rbar - ifirst) + 1]

        if not len(bars):
            # likely no data loaded yet or extreme scrolling?
            log.error(f"WTF bars_range = {lbar}:{rbar}")
            return

        if (
            self.data_key == self.linked.symbol.key
        ):
            # ohlc sampled bars hi/lo lookup
            ylow = np.nanmin(bars['low'])
            yhigh = np.nanmax(bars['high'])

        else:
            view = bars[name or self.data_key]
            ylow = np.nanmin(view)
            yhigh = np.nanmax(view)

        # print(f'{(ylow, yhigh)}')
        return ylow, yhigh
