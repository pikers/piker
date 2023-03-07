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
from contextlib import (
    contextmanager as cm,
    ExitStack,
)
from typing import (
    Iterator,
    TYPE_CHECKING,
)

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
import pyqtgraph as pg
import trio

from ._axes import (
    DynamicDateAxis,
    PriceAxis,
)
from ._cursor import (
    Cursor,
    ContentsLabel,
)
from ..data._sharedmem import ShmArray
from ._ohlc import BarItems
from ._curve import (
    Curve,
    StepCurve,
)
from ._style import (
    hcolor,
    CHART_MARGINS,
    _xaxis_at,
    # _min_points_to_show,
)
from ..data.feed import (
    Feed,
    Flume,
)
from ..data._source import Symbol
from ..log import get_logger
from ._interaction import ChartView
from ._forms import FieldsForm
from ._overlay import PlotItemOverlay
from ._dataviz import Viz
from ._search import SearchWidget
from . import _pg_overrides as pgo

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
    search: SearchWidget
    mode_name: str = 'god'

    def __init__(

        self,
        parent=None,

    ) -> None:

        super().__init__(parent)

        self.search: SearchWidget | None = None

        self.hbox = QHBoxLayout(self)
        self.hbox.setContentsMargins(0, 0, 0, 0)
        self.hbox.setSpacing(6)
        self.hbox.setAlignment(Qt.AlignTop)

        self.vbox = QVBoxLayout()
        self.vbox.setContentsMargins(0, 0, 0, 0)
        self.vbox.setSpacing(2)
        self.vbox.setAlignment(Qt.AlignTop)

        self.hbox.addLayout(self.vbox)

        self._chart_cache: dict[
            str,
            tuple[LinkedSplits, LinkedSplits],
        ] = {}

        self.hist_linked: LinkedSplits | None = None
        self.rt_linked: LinkedSplits | None = None
        self._active_cursor: Cursor | None = None

        # assigned in the startup func `_async_main()`
        self._root_n: trio.Nursery = None

        self._widgets: dict[str, QWidget] = {}
        self._resizing: bool = False

        # TODO: do we need this, when would god get resized
        # and the window does not? Never right?!
        # self.reg_for_resize(self)

    # TODO: strat loader/saver that we don't need yet.
    # def init_strategy_ui(self):
    #     self.toolbar_layout = QHBoxLayout()
    #     self.toolbar_layout.setContentsMargins(0, 0, 0, 0)
    #     self.vbox.addLayout(self.toolbar_layout)
    #     self.strategy_box = StrategyBoxWidget(self)
    #     self.toolbar_layout.addWidget(self.strategy_box)

    @property
    def linkedsplits(self) -> LinkedSplits:
        return self.rt_linked

    def set_chart_symbols(
        self,
        group_key: tuple[str],  # of form <fqsn>.<providername>
        all_linked: tuple[LinkedSplits, LinkedSplits],  # type: ignore

    ) -> None:
        # re-sort org cache symbol list in LIFO order
        cache = self._chart_cache
        cache.pop(group_key, None)
        cache[group_key] = all_linked

    def get_chart_symbols(
        self,
        symbol_key: str,

    ) -> tuple[LinkedSplits, LinkedSplits]:  # type: ignore
        return self._chart_cache.get(symbol_key)

    async def load_symbols(
        self,
        fqsns: list[str],
        loglevel: str,
        reset: bool = False,

    ) -> trio.Event:
        '''
        Load a new contract into the charting app.

        Expects a ``numpy`` structured array containing all the ohlcv fields.

        '''
        # NOTE: for now we use the first symbol in the set as the "key"
        # for the overlay of feeds on the chart.
        group_key: tuple[str] = tuple(fqsns)

        all_linked = self.get_chart_symbols(group_key)
        order_mode_started = trio.Event()

        if not self.vbox.isEmpty():

            # XXX: seems to make switching slower?
            # qframe = self.hist_linked.chart.qframe
            # if qframe.sidepane is self.search:
            #     qframe.hbox.removeWidget(self.search)

            for linked in [self.rt_linked, self.hist_linked]:
                # XXX: this is CRITICAL especially with pixel buffer caching
                linked.hide()
                linked.unfocus()

                # XXX: pretty sure we don't need this
                # remove any existing plots?
                # XXX: ahh we might want to support cache unloading..
                # self.vbox.removeWidget(linked)

        # switching to a new viewable chart
        if all_linked is None or reset:
            from ._display import display_symbol_data

            # we must load a fresh linked charts set
            self.rt_linked = rt_charts = LinkedSplits(self)
            self.hist_linked = hist_charts = LinkedSplits(self)

            # spawn new task to start up and update new sub-chart instances
            self._root_n.start_soon(
                display_symbol_data,
                self,
                fqsns,
                loglevel,
                order_mode_started,
            )

            # self.vbox.addWidget(hist_charts)
            self.vbox.addWidget(rt_charts)
            self.set_chart_symbols(
                group_key,
                (hist_charts, rt_charts),
            )

            for linked in [hist_charts, rt_charts]:
                linked.show()
                linked.focus()

            await trio.sleep(0)

        else:
            # symbol is already loaded and ems ready
            order_mode_started.set()

            self.hist_linked, self.rt_linked = all_linked

            for linked in all_linked:
                # TODO:
                # - we'll probably want per-instrument/provider state here?
                #   change the order config form over to the new chart

                # chart is already in memory so just focus it
                linked.show()
                linked.focus()
                linked.graphics_cycle()
                await trio.sleep(0)

                # resume feeds *after* rendering chart view asap
                chart = linked.chart
                if chart:
                    chart.resume_all_feeds()

            # TODO: we need a check to see if the chart
            # last had the xlast in view, if so then shift so it's
            # still in view, if the user was viewing history then
            # do nothing yah?
            self.rt_linked.chart.main_viz.default_view(
                do_min_bars=True,
            )

        # if a history chart instance is already up then
        # set the search widget as its sidepane.
        hist_chart = self.hist_linked.chart
        if hist_chart:
            hist_chart.qframe.set_sidepane(self.search)

            # NOTE: this is really stupid/hard to follow.
            # we have to reposition the active position nav
            # **AFTER** applying the search bar as a sidepane
            # to the newly switched to symbol.
            await trio.sleep(0)

            # TODO: probably stick this in some kinda `LooknFeel` API?
            for tracker in self.rt_linked.mode.trackers.values():
                pp_nav = tracker.nav
                if tracker.live_pp.size:
                    pp_nav.show()
                    pp_nav.hide_info()
                else:
                    pp_nav.hide()

        # set window titlebar info
        symbol = self.rt_linked.symbol
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
        chart = self.rt_linked.chart
        if chart:
            chart.setFocus()

    def reg_for_resize(
        self,
        widget: QWidget,
    ) -> None:
        getattr(widget, 'on_resize')
        self._widgets[widget.mode_name] = widget

    def on_win_resize(self, event: QtCore.QEvent) -> None:
        '''
        Top level god widget handler from window (the real yaweh) resize
        events such that any registered widgets which wish to be
        notified are invoked using our pythonic `.on_resize()` method
        api.

        Where we do UX magic to make things not suck B)

        '''
        if self._resizing:
            return

        self._resizing = True

        log.info('God widget resize')
        for name, widget in self._widgets.items():
            widget.on_resize()

        self._resizing = False

    # on_resize = on_win_resize

    def get_cursor(self) -> Cursor:
        return self._active_cursor

    def iter_linked(self) -> Iterator[LinkedSplits]:
        for linked in [self.hist_linked, self.rt_linked]:
            yield linked

    def resize_all(self) -> None:
        '''
        Dynamic resize sequence: adjusts all sub-widgets/charts to
        sensible default ratios of what space is detected as available
        on the display / window.

        '''
        rt_linked = self.rt_linked
        rt_linked.set_split_sizes()
        self.rt_linked.resize_sidepanes()
        self.hist_linked.resize_sidepanes(from_linked=rt_linked)
        self.search.on_resize()


class ChartnPane(QFrame):
    '''
    One-off ``QFrame`` composite which pairs a chart
    + sidepane (often a ``FieldsForm`` + other widgets if
    provided) forming a, sort of, "chart row" with a side panel
    for configuration and display of off-chart data.

    See composite widgets docs for deats:
    https://doc.qt.io/qt-5/qwidget.html#composite-widgets

    '''
    sidepane: FieldsForm | SearchWidget
    hbox: QHBoxLayout
    chart: ChartPlotWidget | None = None

    def __init__(
        self,

        sidepane: FieldsForm,
        parent=None,

    ) -> None:

        super().__init__(parent)

        self._sidepane = sidepane
        self.chart = None

        hbox = self.hbox = QHBoxLayout(self)
        hbox.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.setSpacing(3)

    def set_sidepane(
        self,
        sidepane: FieldsForm | SearchWidget,
    ) -> None:

        # add sidepane **after** chart; place it on axis side
        self.hbox.addWidget(
            sidepane,
            alignment=Qt.AlignTop
        )
        self._sidepane = sidepane

    def sidepane(self) -> FieldsForm | SearchWidget:
        return self._sidepane


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
        self.subplots: dict[str, ChartPlotWidget] = {}

        self.godwidget = godwidget
        # placeholder for last appended ``PlotItem``'s bottom axis.
        self.xaxis_chart = None

        self.splitter = QSplitter(QtCore.Qt.Vertical)
        self.splitter.setMidLineWidth(0)
        self.splitter.setHandleWidth(2)
        self.splitter.splitterMoved.connect(self.on_splitter_adjust)

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.addWidget(self.splitter)

        # chart-local graphics state that can be passed to
        # a ``graphic_update_cycle()`` call by any task wishing to
        # update the UI for a given "chart instance".
        self.display_state: DisplayState | None = None

        self._symbol: Symbol = None

    def on_splitter_adjust(
        self,
        pos: int,
        index: int,
    ) -> None:
        # print(f'splitter moved pos:{pos}, index:{index}')
        godw = self.godwidget
        if self is godw.rt_linked:
            godw.search.on_resize()

    def graphics_cycle(self, **kwargs) -> None:
        from . import _display
        ds = self.display_state
        if ds:
            return _display.graphics_update_cycle(
                ds,
                ds.quotes,
                **kwargs,
            )

    @property
    def symbol(self) -> Symbol:
        return self._symbol

    def set_split_sizes(
        self,
        prop: float | None = None,

    ) -> None:
        '''
        Set the proportion of space allocated for linked subcharts.

        '''
        ln = len(self.subplots) or 1

        # proportion allocated to consumer subcharts
        if not prop:
            prop = 3/8

        h = self.height()
        histview_h = h * (4/11)
        h = h - histview_h

        major = 1 - prop
        min_h_ind = int((h * prop) / ln)
        sizes = [
            int(histview_h),
            int(h * major),
        ]

        # give all subcharts the same remaining proportional height
        sizes.extend([min_h_ind] * ln)

        if self.godwidget.rt_linked is self:
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
        shm: ShmArray,
        flume: Flume,
        sidepane: FieldsForm,

        style: str = 'ohlc_bar',

        **add_plot_kwargs,

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
            name=symbol.fqsn,
            shm=shm,
            flume=flume,
            style=style,
            _is_main=True,
            sidepane=sidepane,
            **add_plot_kwargs,
        )
        # add crosshair graphic
        self.chart.addItem(self.cursor)

        # style?
        self.chart.setFrameStyle(
            QFrame.StyledPanel |
            QFrame.Plain
        )

        return self.chart

    def add_plot(
        self,

        name: str,
        shm: ShmArray,
        flume: Flume,

        array_key: str | None = None,
        style: str = 'line',
        _is_main: bool = False,

        sidepane: QWidget | None = None,
        draw_kwargs: dict = {},

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
            None,
            orientation='bottom',
            linkedsplits=self
        )
        axes = {
            'right': PriceAxis(None, orientation='right'),
            'left': PriceAxis(None, orientation='left'),
            'bottom': xaxis,
        }

        if sidepane is not False:
            parent = qframe = ChartnPane(
                sidepane=sidepane,
                parent=self.splitter,
            )
        else:
            parent = self.splitter
            qframe = None

        cpw = ChartPlotWidget(

            # this name will be used to register the primary
            # graphics curve managed by the subchart
            name=name,
            data_key=array_key or name,

            parent=parent,
            linkedsplits=self,
            axisItems=axes,
            **cpw_kwargs,
        )
        # TODO: wow i can't believe how confusing garbage all this axes
        # stuff iss..
        for axis in axes.values():
            axis.pi = cpw.plotItem

        cpw.hideAxis('left')
        # cpw.removeAxis('left')
        cpw.hideAxis('bottom')

        if (
            _xaxis_at == 'bottom'
            and (
                self.xaxis_chart
                or (
                    not self.subplots
                    and self.xaxis_chart is None
                )
            )
        ):
            # hide the previous x-axis chart's bottom axis since we're
            # presumably being appended to the bottom subplot.
            if self.xaxis_chart:
                self.xaxis_chart.hideAxis('bottom')

            # presuming we only want it at the true bottom of all charts.
            # XXX: uses new api from our ``pyqtgraph`` fork.
            # https://github.com/pikers/pyqtgraph/tree/plotitemoverlay_onto_pg_master
            # _ = self.xaxis_chart.removeAxis('bottom', unlink=False)
            # assert 'bottom' not in self.xaxis_chart.plotItem.axes
            self.xaxis_chart = cpw
            cpw.showAxis('bottom')

        if qframe is not None:
            qframe.chart = cpw
            qframe.hbox.addWidget(cpw)

            # so we can look this up and add back to the splitter
            # on a symbol switch
            cpw.qframe = qframe
            assert cpw.parent() == qframe

            # add sidepane **after** chart; place it on axis side
            qframe.set_sidepane(sidepane)
            # qframe.hbox.addWidget(
            #     sidepane,
            #     alignment=Qt.AlignTop
            # )

            cpw.sidepane = sidepane

        cpw.plotItem.vb.linked = self
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
        cpw.cv.setXLink(self.chart)

        # NOTE: above is the same as the following,
        # link this subchart's axes to the main top level chart.
        # if self.chart:
        #     cpw.cv.linkView(0, self.chart.cv)

        add_label = False
        anchor_at = ('top', 'left')

        # draw curve graphics
        if style == 'ohlc_bar':

            viz = cpw.draw_ohlc(
                name,
                shm,
                flume=flume,
                array_key=array_key,
                **draw_kwargs,
            )
            self.cursor.contents_labels.add_label(
                cpw,
                name,
                anchor_at=('top', 'left'),
                update_func=ContentsLabel.update_from_ohlc,
            )

        elif style == 'line':
            add_label = True
            # graphics, data_key = cpw.draw_curve(
            viz = cpw.draw_curve(
                name,
                shm,
                flume,
                array_key=array_key,
                color='default_light',
                **draw_kwargs,
            )

        elif style == 'step':
            add_label = True
            # graphics, data_key = cpw.draw_curve(
            viz = cpw.draw_curve(
                name,
                shm,
                flume,
                array_key=array_key,
                step_mode=True,
                color='davies',
                fill_color='davies',
                **draw_kwargs,
            )

        else:
            raise ValueError(f"Chart style {style} is currently unsupported")

        # NOTE: back-link the new sub-chart to trigger y-autoranging in
        # the (ohlc parent) main chart for this linked set.
        # if self.chart:
        #     main_viz = self.chart.get_viz(self.chart.name)
        #     self.chart.view.enable_auto_yrange(
        #         src_vb=cpw.view,
        #         viz=main_viz,
        #     )

        graphics = viz.graphics
        data_key = viz.name

        if _is_main:
            assert style == 'ohlc_bar', 'main chart must be OHLC'
        else:
            # track by name
            self.subplots[name] = cpw
            if qframe is not None:
                self.splitter.addWidget(qframe)

        # add to cross-hair's known plots
        # NOTE: add **AFTER** creating the underlying ``PlotItem``s
        # since we require that global (linked charts wide) axes have
        # been created!
        if self.cursor:
            if (
                _is_main
                or style != 'ohlc_bar'
            ):
                self.cursor.add_plot(cpw)
                if style != 'ohlc_bar':
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
        from_linked: LinkedSplits | None = None,

    ) -> None:
        '''
        Size all sidepanes based on the OHLC "main" plot and its
        sidepane width.

        '''
        if from_linked:
            main_chart = from_linked.chart
        else:
            main_chart = self.chart

        if main_chart and main_chart.sidepane:
            sp_w = main_chart.sidepane.width()
            for name, cpw in self.subplots.items():
                cpw.sidepane.setMinimumWidth(sp_w)
                cpw.sidepane.setMaximumWidth(sp_w)

            if from_linked:
                self.chart.sidepane.setMinimumWidth(sp_w)


# TODO: a general rework of this widget-interface:
# - we should really drop using this type and instead just lever our
#   own override of `PlotItem`..
# - possibly rename to class -> MultiChart(pg.PlotWidget):
#   where the widget is responsible for containing management
#   harness for multi-Viz "view lists" and their associated mode-panes
#   (fsp chain, order ctl, feed queue-ing params, actor ctl, etc).

class ChartPlotWidget(pg.PlotWidget):
    '''
    ``PlotWidget`` subtype containing a ``.plotItem: PlotItem`` as well
    as a `.pi_overlay: PlotItemOverlay`` which helps manage and overlay flow
    graphics view multiple compose view boxes.

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

    mode_name: str = 'view'

    # TODO: can take a ``background`` color setting - maybe there's
    # a better one?
    def mk_vb(self, name: str) -> ChartView:
        cv = ChartView(name)
        # link new view to chart's view set
        cv.linked = self.linked
        return cv

    def __init__(
        self,

        # the "data view" we generate graphics from
        name: str,
        data_key: str,
        linkedsplits: LinkedSplits,

        view_color: str = 'papas_special',
        pen_color: str = 'bracket',

        # TODO: load from config
        use_open_gl: bool = False,

        static_yrange: tuple[float, float] | None = None,

        parent=None,
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
        self.sidepane: FieldsForm | None = None

        # source of our custom interactions
        self.cv = self.mk_vb(name)

        pi = pgo.PlotItem(
            viewBox=self.cv,
            name=name,
            **kwargs,
        )
        pi.chart_widget = self
        super().__init__(
            background=hcolor(view_color),
            viewBox=self.cv,
            # parent=None,
            # plotItem=None,
            # antialias=True,
            parent=parent,
            plotItem=pi,
            **kwargs
        )
        # give viewbox as reference to chart
        # allowing for kb controls and interactions on **this** widget
        # (see our custom view mode in `._interactions.py`)
        self.cv.chart = self

        self.pi_overlay: PlotItemOverlay = PlotItemOverlay(self.plotItem)

        # ensure internal pi matches
        assert self.cv is self.plotItem.vb

        self.useOpenGL(use_open_gl)
        self.name = name
        self.data_key = data_key or name

        # scene-local placeholder for book graphics
        # sizing to avoid overlap with data contents
        self._max_l1_line_len: float = 0

        # self.setViewportMargins(0, 0, 0, 0)

        # registry of overlay curve names
        self._vizs: dict[str, Viz] = {}

        self.feed: Feed | None = None

        self._labels = {}  # registry of underlying graphics
        self._ysticks = {}  # registry of underlying graphics

        self._static_yrange = static_yrange  # for "known y-range style"
        self._view_mode: str = 'follow'

        # show background grid
        self.showGrid(x=False, y=True, alpha=0.3)

        # indempotent startup flag for auto-yrange subsys
        # to detect the "first time" y-domain graphics begin
        # to be shown in the (main) graphics view.
        self._on_screen: bool = False

    def resume_all_feeds(self):
        feed = self.feed
        if feed:
            try:
                self.linked.godwidget._root_n.start_soon(feed.resume)
            except RuntimeError:
                # TODO: cancel the qtractor runtime here?
                raise

    def pause_all_feeds(self):
        feed = self.feed
        if feed:
            self.linked.godwidget._root_n.start_soon(feed.pause)

    @property
    def view(self) -> ChartView:
        return self.plotItem.vb

    def focus(self) -> None:
        self.view.setFocus()

    def pre_l1_xs(self) -> tuple[float, float]:
        '''
        Return the view x-coord for the value just before
        the L1 labels on the y-axis as well as the length
        of that L1 label from the y-axis.

        '''
        line_end, marker_right, yaxis_x = self.marker_right_points()
        line = self.view.mapToView(
            QLineF(line_end, 0, yaxis_x, 0)
        )
        linex, linelen = line.x1(), line.length()
        # print(
        #     f'line: {line}\n'
        #     f'linex: {linex}\n'
        #     f'linelen: {linelen}\n'
        # )
        return linex, linelen

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
        from ._l1 import L1Label
        l1_len = abs(L1Label._x_br_offset)
        ryaxis = self.getAxis('right')

        r_axis_x = ryaxis.pos().x()
        up_to_l1_sc = r_axis_x - l1_len
        marker_right = up_to_l1_sc - (1.375 * 2 * marker_size)
        # line_end = marker_right - (6/16 * marker_size)
        line_end = marker_right - marker_size

        # print(
        #     f'r_axis_x: {r_axis_x}\n'
        #     f'up_to_l1_sc: {up_to_l1_sc}\n'
        #     f'marker_right: {marker_right}\n'
        #     f'line_end: {line_end}\n'
        # )
        return line_end, marker_right, r_axis_x

    def increment_view(
        self,
        datums: int = 1,
        vb: ChartView | None = None,

    ) -> None:
        '''
        Increment the data view ``datums``` steps toward y-axis thus
        "following" the current time slot/step/bar.

        '''
        view = vb or self.view
        viz = self.main_viz
        l, r = viz.view_range()
        x_shift = viz.index_step() * datums

        if datums >= 300:
            print("FUCKING FIX THE GLOBAL STEP BULLSHIT")
            # breakpoint()
            return

        # should trigger broadcast on all overlays right?
        view.setXRange(
            min=l + x_shift,
            max=r + x_shift,

            # TODO: holy shit, wtf dude... why tf would this not be 0 by
            # default... speechless.
            padding=0,
        )

    def overlay_plotitem(
        self,
        name: str,
        index: int | None = None,
        axis_title: str | None = None,
        axis_side: str = 'right',
        axis_kwargs: dict = {},

    ) -> pgo.PlotItem:

        # Custom viewbox impl
        cv = self.mk_vb(name)
        cv.chart = self

        allowed_sides = {'left', 'right'}
        if axis_side not in allowed_sides:
            raise ValueError(f'``axis_side``` must be in {allowed_sides}')

        yaxis = PriceAxis(
            plotitem=None,
            orientation=axis_side,
            **axis_kwargs,
        )

        pi = pgo.PlotItem(
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
        # pi.vb.background.setOpacity(0)
        yaxis.pi = pi
        pi.chart_widget = self
        pi.hideButtons()

        # compose this new plot's graphics with the current chart's
        # existing one but with separate axes as neede and specified.
        self.pi_overlay.add_plotitem(
            pi,
            index=index,

            # only link x-axes and
            # don't relay any ``ViewBox`` derived event
            # handlers since we only care about keeping charts
            # x-synced on interaction (at least for now).
            link_axes=(0,),
        )

        # hide all axes not named by ``axis_side``
        for axname in (
            ({'bottom'} | allowed_sides) - {axis_side}
        ):
            try:
                pi.hideAxis(axname)
            except Exception:
                pass

        # add axis title
        # TODO: do we want this API to still work?
        # raxis = pi.getAxis('right')
        axis = self.pi_overlay.get_axis(pi, axis_side)
        axis.set_title(axis_title or name, view=pi.getViewBox())

        return pi

    def draw_curve(
        self,

        name: str,
        shm: ShmArray,
        flume: Flume,

        array_key: str | None = None,
        overlay: bool = False,
        color: str | None = None,
        add_label: bool = True,
        pi: pg.PlotItem | None = None,
        step_mode: bool = False,
        is_ohlc: bool = False,
        add_sticky: None | str = 'right',

        **graphics_kwargs,

    ) -> Viz:
        '''
        Draw a "curve" (line plot graphics) for the provided data in
        the input shm array ``shm``.

        '''
        color = color or self.pen_color or 'default_light'
        data_key = array_key or name

        pi = pi or self.plotItem

        if is_ohlc:
            graphics = BarItems(
                color=color,
                name=name,
                **graphics_kwargs,
            )

        else:
            curve_type = {
                None: Curve,
                'step': StepCurve,
                # TODO:
                # 'bars': BarsItems
            }['step' if step_mode else None]

            graphics = curve_type(
                name=name,
                color=color,
                **graphics_kwargs,
            )

        viz = self._vizs[data_key] = Viz(
            data_key,
            pi,
            shm,
            flume,

            is_ohlc=is_ohlc,
            # register curve graphics with this viz
            graphics=graphics,
        )

        # connect auto-yrange callbacks *from* this new
        # view **to** this parent and likewise *from* the
        # main/parent chart back *to* the created overlay.
        pi.vb.enable_auto_yrange(
            src_vb=self.view,
            viz=viz,
        )

        pi.viz = viz
        # so that viewboxes are associated 1-to-1 with
        # their parent plotitem
        pi.vb._viz = viz

        assert isinstance(viz.shm, ShmArray)

        # TODO: this probably needs its own method?
        if overlay:
            if isinstance(overlay, pgo.PlotItem):
                if overlay not in self.pi_overlay.overlays:
                    raise RuntimeError(
                            f'{overlay} must be from `.plotitem_overlay()`'
                        )
                pi = overlay

        if add_sticky:

            if pi is not self.plotItem:
                # overlay = self.pi_overlay
                # assert pi in overlay.overlays
                overlay = self.pi_overlay
                assert pi in overlay.overlays
                axis = overlay.get_axis(
                    pi,
                    add_sticky,
                )

            else:
                axis = pi.getAxis(add_sticky)

            if pi.name not in axis._stickies:

                # TODO: UGH! just make this not here! we should
                # be making the sticky from code which has access
                # to the ``Symbol`` instance..

                # if the sticky is for our symbol
                # use the tick size precision for display
                name = name or pi.name
                sym = self.linked.symbol
                digits = None
                if name == sym.key:
                    digits = sym.tick_size_digits

                # anchor_at = ('top', 'left')

                # TODO: something instead of stickies for overlays
                # (we need something that avoids clutter on x-axis).
                axis.add_sticky(
                    pi=pi,
                    fg_color='black',
                    # bg_color=color,
                    digits=digits,
                )

        # NOTE: this is more or less the RENDER call that tells Qt to
        # start showing the generated graphics-curves. This is kind of
        # of edge-triggered call where once added any
        # ``QGraphicsItem.update()`` calls are automatically displayed.
        # Our internal graphics objects have their own "update from
        # data" style method API that allows for real-time updates on
        # the next render cycle; just note a lot of the real-time
        # updates are implicit and require a bit of digging to
        # understand.
        pi.addItem(graphics)

        return viz

    def draw_ohlc(
        self,
        name: str,
        shm: ShmArray,
        flume: Flume,

        array_key: str | None = None,
        **draw_curve_kwargs,

    ) -> Viz:
        '''
        Draw OHLC datums to chart.

        '''
        return self.draw_curve(
            name,
            shm,
            flume,
            array_key=array_key,
            is_ohlc=True,
            **draw_curve_kwargs,
        )

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

    def get_viz(
        self,
        key: str,
    ) -> Viz:
        '''
        Try to get an underlying ``Viz`` by key.

        '''
        return self._vizs.get(key)

    @property
    def main_viz(self) -> Viz:
        return self.get_viz(self.name)

    def iter_vizs(self) -> Iterator[Viz]:
        return iter(self._vizs.values())

    @cm
    def reset_graphics_caches(self) -> None:
        '''
        Reset all managed ``Viz`` (flow) graphics objects
        Qt cache modes (to ``NoCache`` mode) on enter and
        restore on exit.

        '''
        with ExitStack() as stack:
            for viz in self.iter_vizs():
                stack.enter_context(
                    viz.graphics.reset_cache(),
                )

                # also reset any downsampled alt-graphics objects which
                # might be active.
                dsg = viz.ds_graphics
                if dsg:
                    stack.enter_context(
                        dsg.reset_cache(),
                    )
            try:
                yield
            finally:
                stack.close()
