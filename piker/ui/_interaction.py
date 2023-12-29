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
Chart view box primitives

'''
from __future__ import annotations
from contextlib import (
    asynccontextmanager,
    ExitStack,
)
from functools import partial
import time
from typing import (
    Callable,
    TYPE_CHECKING,
)

import pyqtgraph as pg
# NOTE XXX: pg is super annoying and re-implements it's own mouse
# event subsystem.. we should really look into re-working/writing
# this down the road.. Bo
from pyqtgraph.GraphicsScene import mouseEvents as mevs
# from pyqtgraph.GraphicsScene.mouseEvents import MouseDragEvent
from PyQt5.QtWidgets import QGraphicsSceneMouseEvent as gs_mouse
from PyQt5.QtGui import (
    QWheelEvent,
)
from PyQt5.QtCore import (
    Qt,
    QEvent,
)
from pyqtgraph import (
    ViewBox,
    Point,
    QtCore,
)
from pyqtgraph import functions as fn
import numpy as np
import trio

from ..log import get_logger
from ..toolz import (
    Profiler,
    pg_profile_enabled,
    ms_slower_then,
)
from .view_mode import overlay_viewlists
# from ._style import _min_points_to_show
from ._editors import SelectRect
from . import _event

if TYPE_CHECKING:
    # from ._search import (
    #     SearchWidget,
    # )
    from ._chart import (
        ChartnPane,
        ChartPlotWidget,
        GodWidget,
    )
    from ._dataviz import Viz
    from .order_mode import OrderMode
    from ._display import DisplayState


log = get_logger(__name__)

NUMBER_LINE = {
    Qt.Key_1,
    Qt.Key_2,
    Qt.Key_3,
    Qt.Key_4,
    Qt.Key_5,
    Qt.Key_6,
    Qt.Key_7,
    Qt.Key_8,
    Qt.Key_9,
    Qt.Key_0,
}

ORDER_MODE = {
    Qt.Key_A,
    Qt.Key_F,
    Qt.Key_D,
}


async def handle_viewmode_kb_inputs(

    view: ChartView,
    recv_chan: trio.abc.ReceiveChannel,
    dss: dict[str, DisplayState],

) -> None:

    order_mode: OrderMode = view.order_mode
    godw: GodWidget = order_mode.godw  # noqa

    # track edge triggered keys
    # (https://en.wikipedia.org/wiki/Interrupt#Triggering_methods)
    pressed: set[str] = set()

    last = time.time()
    action: str

    on_next_release: Callable | None = None

    # for quick key sequence-combo pattern matching
    # we have a min_tap period and these should not
    # ever be auto-repeats since we filter those at the
    # event filter level prior to the above mem chan.
    min_tap = 1/6
    fast_key_seq: list[str] = []
    fast_taps: dict[str, Callable] = {
        'cc': order_mode.cancel_all_orders,
    }

    async for kbmsg in recv_chan:
        event, etype, key, mods, text = kbmsg.to_tuple()
        log.debug(f'key: {key}, mods: {mods}, text: {text}')
        now = time.time()
        period = now - last

        # reset mods
        ctrl: bool = False
        shift: bool = False

        # press branch
        if etype in {QEvent.KeyPress}:

            pressed.add(key)

            if (
                # clear any old values not part of a "fast" tap sequence:
                # presumes the period since last tap is longer then our
                # min_tap period
                fast_key_seq and period >= min_tap or

                # don't support more then 2 key sequences for now
                len(fast_key_seq) > 2
            ):
                fast_key_seq.clear()

            # capture key to fast tap sequence if we either
            # have no previous keys or we do and the min_tap period is
            # met
            if (
                not fast_key_seq or
                period <= min_tap and fast_key_seq
            ):
                fast_key_seq.append(text)
                log.debug(f'fast keys seqs {fast_key_seq}')

            # mods run through
            if mods == Qt.ShiftModifier:
                shift = True

            if mods == Qt.ControlModifier:
                ctrl = True

            # UI REPL-shell, with ctrl-p (for "pause")
            if (
                ctrl
                and key in {
                    Qt.Key_P,
                }
            ):
                feed = order_mode.feed  # noqa
                chart = order_mode.chart  # noqa
                viz = chart.main_viz  # noqa
                vlm_chart = chart.linked.subplots['volume']  # noqa
                vlm_viz = vlm_chart.main_viz  # noqa
                dvlm_pi = vlm_chart._vizs['dolla_vlm'].plot  # noqa
                import tractor
                await tractor.pause()
                view.interact_graphics_cycle()

            # FORCE graphics reset-and-render of all currently
            # shown data `Viz`s for the current chart app.
            if (
                ctrl
                and key in {
                    Qt.Key_R,
                }
            ):
                fqme: str
                ds: DisplayState
                for fqme, ds in dss.items():

                    viz: Viz
                    for tf, viz in {
                        60: ds.hist_viz,
                        1: ds.viz,
                    }.items():
                        # TODO: only allow this when the data is IN VIEW!
                        # also, we probably can do this more efficiently
                        # / smarter by only redrawing the portion of the
                        # path necessary?
                        viz.reset_graphics()

            # ------ - ------
            # SEARCH MODE
            # ------ - ------
            # ctlr-<space>/<l> for "lookup", "search" -> open search tree
            if (
                ctrl
                and key in {
                    Qt.Key_L,
                    # Qt.Key_Space,
                }
            ):
                godw = view._chart.linked.godwidget
                godw.hist_linked.resize_sidepanes(from_linked=godw.rt_linked)
                godw.search.focus()

            # esc and ctrl-c
            if (
                key == Qt.Key_Escape
                or (
                    ctrl
                    and key == Qt.Key_C
                )
            ):
                # ctrl-c as cancel
                # https://forum.qt.io/topic/532/how-to-catch-ctrl-c-on-a-widget/9
                view.select_box.clear()
                view.linked.focus()

            # cancel order or clear graphics
            if (
                key == Qt.Key_C
                or key == Qt.Key_Delete
            ):

                order_mode.cancel_orders_under_cursor()

            # View modes
            if (
                ctrl
                and (
                    key == Qt.Key_Equal
                    or key == Qt.Key_I
                )
            ):
                view.wheelEvent(
                    ev=None,
                    axis=None,
                    delta=view.def_delta,
                )
            elif (
                ctrl
                and (
                    key == Qt.Key_Minus
                    or key == Qt.Key_O
                )
            ):
                view.wheelEvent(
                    ev=None,
                    axis=None,
                    delta=-view.def_delta,
                )

            elif (
                not ctrl
                and key == Qt.Key_R
            ):
                # NOTE: seems that if we don't yield a Qt render
                # cycle then the m4 downsampled curves will show here
                # without another reset..
                view._viz.default_view()
                view.interact_graphics_cycle()
                await trio.sleep(0)
                view.interact_graphics_cycle()

            if len(fast_key_seq) > 1:
                # begin matches against sequences
                func: Callable = fast_taps.get(''.join(fast_key_seq))
                if func:
                    func()
                    fast_key_seq.clear()

        # release branch
        elif etype in {QEvent.KeyRelease}:

            if on_next_release:
                on_next_release()
                on_next_release = None

            if key in pressed:
                pressed.remove(key)

        # QUERY/QUOTE MODE
        # ----------------
        if {Qt.Key_Q}.intersection(pressed):

            view.linked.cursor.in_query_mode = True

        else:
            view.linked.cursor.in_query_mode = False

        # SELECTION MODE
        # --------------
        if shift:
            if view.state['mouseMode'] == ViewBox.PanMode:
                view.setMouseMode(ViewBox.RectMode)
        else:
            view.setMouseMode(ViewBox.PanMode)

        # Toggle position config pane
        if (
            ctrl
            and key in {
                Qt.Key_Space,
            }
        ):
            # searchw: SearchWidget = godw.search
            # pp_pane = order_mode.current_pp.pane
            qframes: list[ChartnPane] = []

            for linked in (
                godw.rt_linked,
                godw.hist_linked,
            ):
                for chartw in (
                    [linked.chart]
                    +
                    list(linked.subplots.values())
                ):
                    qframes.append(
                        chartw.qframe
                    )

            # NOTE: place priority on FIRST hiding all
            # panes before showing them.
            # TODO: make this more "fancy"?
            # - maybe look at majority of hidden states and then
            #   flip based on that?
            # - move these loops into the chart APIs?
            # - store the UX-state for a given feed/symbol and
            #   apply when opening a new one (eg. if panes were
            #   hidden then also hide them on newly loaded mkt
            #   feeds).
            if not any(
                qf.sidepane.isHidden() for qf in qframes
            ):
                for qf in qframes:
                    qf.sidepane.hide()

            else:
                for qf in qframes:
                    qf.sidepane.show()

        # ORDER MODE
        # ----------
        # live vs. dark trigger + an action {buy, sell, alert}
        order_keys_pressed = ORDER_MODE.intersection(pressed)

        if order_keys_pressed:

            # TODO: it seems like maybe the composition should be
            # reversed here? Like, maybe we should have the nav have
            # access to the pos state and then make encapsulated logic
            # that shows the right stuff on screen instead or order mode
            # and position-related abstractions doing this?

            # show the pp size label only if there is
            # a non-zero pos existing
            tracker = order_mode.current_pp
            if tracker.live_pp.cumsize:
                tracker.nav.show()

            # TODO: show pp config mini-params in status bar widget
            # mode.pp_config.show()

            trigger_type: str = 'dark'
            if (
                # 's' for "submit" to activate "live" order
                Qt.Key_S in pressed or
                ctrl
            ):
                trigger_type: str = 'live'

            # order mode trigger "actions"
            if Qt.Key_D in pressed:  # for "damp eet"
                action = 'sell'

            elif Qt.Key_F in pressed:  # for "fillz eet"
                action = 'buy'

            elif Qt.Key_A in pressed:
                action = 'alert'
                trigger_type = 'live'

            order_mode.active = True

            # XXX: order matters here for line style!
            order_mode._trigger_type = trigger_type
            order_mode.stage_order(
                action,
                trigger_type=trigger_type,
            )

            prefix = trigger_type + '-' if action != 'alert' else ''
            view._chart.window().set_mode_name(f'{prefix}{action}')

        elif (
            (
                Qt.Key_S in pressed or
                order_keys_pressed or
                Qt.Key_O in pressed
            )
            and key in NUMBER_LINE
        ):
            # hot key to set order slots size.
            # change edit field to current number line value,
            # update the pp allocator bar, unhighlight the
            # field when ctrl is released.
            num = int(text)
            pp_pane = order_mode.pane
            pp_pane.on_ui_settings_change('slots', num)
            edit = pp_pane.form.fields['slots']
            edit.selectAll()
            # un-highlight on ctrl release
            on_next_release = edit.deselect
            pp_pane.update_status_ui(pp_pane.order_mode.current_pp)

        else:  # none active

            # hide pp label
            order_mode.current_pp.nav.hide_info()

            # if none are pressed, remove "staged" level
            # line under cursor position
            order_mode.lines.unstage_line()

            if view.hasFocus():
                # update mode label
                view._chart.window().set_mode_name('view')

            order_mode.active = False

        last = time.time()


async def handle_viewmode_mouse(

    view: ChartView,
    recv_chan: trio.abc.ReceiveChannel,
    dss: dict[str, DisplayState],

) -> None:

    async for msg in recv_chan:
        button = msg.button

        # XXX: ugggh ``pyqtgraph`` has its own mouse events..
        # so we can't overried this easily.
        # it's going to take probably some decent
        # reworking of the mouseClickEvent() handler.

        # if button == QtCore.Qt.RightButton and view.menuEnabled():
        #     event = mouseEvents.MouseClickEvent(msg.event)
        #     # event.accept()
        #     view.raiseContextMenu(event)

        if (
            view.order_mode.active and
            button == QtCore.Qt.LeftButton
        ):
            # when in order mode, submit execution
            # msg.event.accept()
            view.order_mode.submit_order()


class ChartView(ViewBox):
    '''
    Price chart view box with interaction behaviors you'd expect from
    any interactive platform:

        - zoom on mouse scroll that auto fits y-axis
        - vertical scrolling on y-axis
        - zoom on x to most recent in view datum
        - zoom on right-click-n-drag to cursor position

    '''
    mode_name: str = 'view'
    def_delta: float = 616 * 6
    def_scale_factor: float = 1.016 ** (def_delta * -1 / 20)
    # annots: dict[int, GraphicsObject] = {}

    def __init__(
        self,

        name: str,

        parent: pg.PlotItem = None,
        static_yrange: tuple[float, float] | None = None,
        **kwargs,

    ):
        super().__init__(
            parent=parent,
            name=name,
            # TODO: look into the default view padding
            # support that might replace somem of our
            # ``ChartPlotWidget._set_yrange()`
            # defaultPadding=0.,
            **kwargs
        )

        # for "known y-range style"
        self._static_yrange = static_yrange

        # disable vertical scrolling
        self.setMouseEnabled(
            x=True,
            y=True,
        )

        self.linked = None
        self._chart: ChartPlotWidget | None = None  # noqa

        # add our selection box annotator
        self.select_box = SelectRect(self)
        # self.select_box.add_to_view(self)
        # self.addItem(
        #     self.select_box,
        #     ignoreBounds=True,
        # )

        self.mode = None
        self.order_mode: bool = False

        self.setFocusPolicy(QtCore.Qt.StrongFocus)
        self._in_interact: trio.Event | None = None
        self._interact_stack: ExitStack = ExitStack()

        # TODO: probably just assign this whenever a new `PlotItem` is
        # allocated since they're 1to1 with views..
        self._viz: Viz | None = None
        self._yrange: tuple[float, float] | None = None

    def start_ic(
        self,
    ) -> None:
        '''
        Signal the beginning of a click-drag interaction
        to any interested task waiters.

        '''
        if self._in_interact is None:
            chart = self.chart
            try:
                self._in_interact = trio.Event()

                chart.pause_all_feeds()
                self._interact_stack.enter_context(
                    chart.reset_graphics_caches()
                )
            except RuntimeError:
                pass

    def signal_ic(
        self,
        *args,

    ) -> None:
        '''
        Signal the end of a click-drag interaction
        to any waiters.

        '''
        if self._in_interact:
            try:
                self._interact_stack.close()
                self.chart.resume_all_feeds()

                self._in_interact.set()
                self._in_interact = None
            except RuntimeError:
                pass

    @asynccontextmanager
    async def open_async_input_handler(
        self,
        **handler_kwargs,

    ) -> ChartView:

        async with (
            _event.open_handlers(
                [self],
                event_types={
                    QEvent.KeyPress,
                    QEvent.KeyRelease,
                },
                async_handler=partial(
                    handle_viewmode_kb_inputs,
                    **handler_kwargs,
                ),
            ),
            _event.open_handlers(
                [self],
                event_types={
                    gs_mouse.GraphicsSceneMousePress,
                },
                async_handler=partial(
                    handle_viewmode_mouse,
                    **handler_kwargs,
                ),
            ),
        ):
            yield self

    @property
    def chart(self) -> ChartPlotWidget:  # type: ignore # noqa
        return self._chart

    @chart.setter
    def chart(self, chart: ChartPlotWidget) -> None:  # type: ignore # noqa
        self._chart = chart
        self.select_box.chart = chart

    def wheelEvent(
        self,
        ev: QWheelEvent | None = None,
        axis: int | None = None,
        delta: float | None = None,
    ):
        '''
        Override "center-point" location for scrolling.

        This is an override of the ``ViewBox`` method simply changing
        the center of the zoom to be the y-axis.

        TODO: PR a method into ``pyqtgraph`` to make this configurable

        '''
        # NOTE: certain operations are only avail when this handler is
        # actually called on events.
        if ev is None:
            assert delta
            assert axis is None

        linked = self.linked
        if (
            not linked
        ):
            return

        if axis in (0, 1):
            mask = [False, False]
            mask[axis] = self.state['mouseEnabled'][axis]
        else:
            mask: list[bool] = self.state['mouseEnabled'][:]

        chart = self.linked.chart

        # don't zoom more then the min points setting
        viz = chart.get_viz(chart.name)
        _, vl, lbar, rbar, vr, r = viz.datums_range()

        # TODO: max/min zoom limits incorporating time step size.
        # rl = vr - vl
        # if ev.delta() > 0 and rl <= _min_points_to_show:
        #     log.warning("Max zoom bruh...")
        #     return
        # if (
        #     ev.delta() < 0
        #     and rl >= len(chart._vizs[chart.name].shm.array) + 666
        # ):
        #     log.warning("Min zoom bruh...")
        #     return

        # actual scaling factor
        delta: float = ev.delta() if ev else delta
        scale_factor: float = 1.016 ** (delta * -1 / 20)

        # NOTE: if elem is False -> None meaning "do not scale that
        # axis".
        scales: list[float | bool] = [
            (None if m is False else scale_factor)
            for m in mask
        ]

        if (
            # zoom happened on axis
            axis == 1

            # if already in axis zoom mode then keep it
            or self.chart._static_yrange == 'axis'
        ):
            self.chart._static_yrange = 'axis'
            self.setLimits(yMin=None, yMax=None)

            # print(scale_y)
            # pos = ev.pos()
            # lastPos = ev.lastPos()
            # dif = pos - lastPos
            # dif = dif * -1
            center = Point(
                fn.invertQTransform(
                    self.childGroup.transform()
                ).map(ev.pos())
            )
            # scale_y = 1.3 ** (center.y() * -1 / 20)
            self.scaleBy(scales, center)

        # zoom in view-box area
        else:
            # use right-most point of current curve graphic
            xl = viz.graphics.x_last()
            focal = min(
                xl,
                r,
            )

            self._resetTarget()

            # NOTE: scroll "around" the right most datum-element in view
            # gives the feeling of staying "pinned" in place.
            self.scaleBy(scales, focal)

            # XXX: the order of the next 2 lines i'm pretty sure
            # matters, we want the resize to trigger before the graphics
            # update, but i gotta feelin that because this one is signal
            # based (and thus not necessarily sync invoked right away)
            # that calling the resize method manually might work better.
            # self.sigRangeChangedManually.emit(mask)

            # XXX: without this is seems as though sometimes
            # when zooming in from far out (and maybe vice versa?)
            # the signal isn't being fired enough since if you pan
            # just after you'll see further downsampling code run
            # (pretty noticeable on the OHLC ds curve) but with this
            # that never seems to happen? Only question is how much this
            # "double work" is causing latency when these missing event
            # fires don't happen?
            self.interact_graphics_cycle()
            self.interact_graphics_cycle()

            if ev:
                ev.accept()

    def mouseDragEvent(
        self,
        ev: mevs.MouseDragEvent,
        axis: int | None = None,

    ) -> None:
        pos: Point = ev.pos()
        lastPos: Point = ev.lastPos()
        dif: Point = (pos - lastPos) * -1
        # dif: Point = pos - lastPos
        # dif: Point = dif * -1

        # NOTE: if axis is specified, event will only affect that axis.
        btn = ev.button()

        # Ignore axes if mouse is disabled
        mouseEnabled = np.array(
            self.state['mouseEnabled'],
            dtype=float,
        )
        mask = mouseEnabled.copy()
        if axis is not None:
            mask[1-axis] = 0.0

        # Scale or translate based on mouse button
        if btn & (
            QtCore.Qt.LeftButton | QtCore.Qt.MidButton
        ):
            # zoom y-axis ONLY when click-n-drag on it
            # if axis == 1:
            #     # set a static y range special value on chart widget to
            #     # prevent sizing to data in view.
            #     self.chart._static_yrange = 'axis'

            #     scale_y = 1.3 ** (dif.y() * -1 / 20)
            #     self.setLimits(yMin=None, yMax=None)

            #     # print(scale_y)
            #     self.scaleBy((0, scale_y))

            # SELECTION MODE
            if (
                self.state['mouseMode'] == ViewBox.RectMode
                and axis is None
            ):
                # XXX: WHY
                ev.accept()

                down_pos: Point = ev.buttonDownPos(
                    btn=btn,
                )
                scen_pos: Point = ev.scenePos()
                scen_down_pos: Point = ev.buttonDownScenePos(
                    btn=btn,
                )

                # This is the final position in the drag
                if ev.isFinish():

                    # import pdbp; pdbp.set_trace()

                    # NOTE: think of this as a `.mouse_drag_release()`
                    # (bc HINT that's what i called the shit ass
                    # method that wrapped this call [yes, as a single
                    # fucking call] originally.. you bish, guille)
                    # Bo.. oraleeee
                    self.select_box.set_scen_pos(
                        # down_pos,
                        # pos,
                        scen_down_pos,
                        scen_pos,
                    )

                    # this is the zoom transform cmd
                    ax = QtCore.QRectF(down_pos, pos)
                    ax = self.childGroup.mapRectFromParent(ax)
                    # self.showAxRect(ax)
                    # axis history tracking
                    self.axHistoryPointer += 1
                    self.axHistory = self.axHistory[
                        :self.axHistoryPointer] + [ax]

                else:
                    self.select_box.set_scen_pos(
                        # down_pos,
                        # pos,
                        scen_down_pos,
                        scen_pos,
                    )

                    # update shape of scale box
                    # self.updateScaleBox(ev.buttonDownPos(), ev.pos())
                    # breakpoint()
                    # self.updateScaleBox(
                    #     down_pos,
                    #     ev.pos(),
                    # )

            # PANNING MODE
            else:
                try:
                    self.start_ic()
                except RuntimeError:
                    pass

                if axis == 1:
                    self.chart._static_yrange = 'axis'

                tr = self.childGroup.transform()
                tr = fn.invertQTransform(tr)
                tr = tr.map(dif*mask) - tr.map(Point(0, 0))

                x = tr.x() if mask[0] == 1 else None
                y = tr.y() if mask[1] == 1 else None

                self._resetTarget()

                if x is not None or y is not None:
                    self.translateBy(x=x, y=y)

                # self.sigRangeChangedManually.emit(mask)
                    # self.state['mouseEnabled']
                # )
                self.interact_graphics_cycle()

                if ev.isFinish():
                    self.signal_ic()
                    # self._in_interact.set()
                    # self._in_interact = None
                    # self.chart.resume_all_feeds()

                # # XXX: WHY
                # ev.accept()

        # WEIRD "RIGHT-CLICK CENTER ZOOM" MODE
        elif btn & QtCore.Qt.RightButton:

            if self.state['aspectLocked'] is not False:
                mask[0] = 0

            dif = ev.screenPos() - ev.lastScreenPos()
            dif = np.array([dif.x(), dif.y()])
            dif[0] *= -1
            s = ((mask * 0.02) + 1) ** dif

            tr = self.childGroup.transform()
            tr = fn.invertQTransform(tr)

            x = s[0] if mouseEnabled[0] == 1 else None
            y = s[1] if mouseEnabled[1] == 1 else None

            center = Point(tr.map(ev.buttonDownPos(QtCore.Qt.RightButton)))
            self._resetTarget()
            self.scaleBy(x=x, y=y, center=center)

            # self.sigRangeChangedManually.emit(self.state['mouseEnabled'])
            self.interact_graphics_cycle()

        # XXX: WHY
        ev.accept()

    # def mouseClickEvent(self, event: QtCore.QEvent) -> None:
    #      '''This routine is rerouted to an async handler.
    #      '''
    #     pass

    def keyReleaseEvent(self, event: QtCore.QEvent) -> None:
        '''This routine is rerouted to an async handler.
        '''
        pass

    def keyPressEvent(self, event: QtCore.QEvent) -> None:
        '''This routine is rerouted to an async handler.
        '''
        pass

    def _set_yrange(
        self,
        *,

        yrange: tuple[float, float] | None = None,
        viz: Viz | None = None,

        # NOTE: this value pairs (more or less) with L1 label text
        # height offset from from the bid/ask lines.
        range_margin: float | None = 0.06,

        bars_range: tuple[int, int, int, int] | None = None,

        # flag to prevent triggering sibling charts from the same linked
        # set from recursion errors.
        autoscale_linked_plots: bool = False,
        name: str | None = None,

    ) -> None:
        '''
        Set the viewable y-range based on embedded data.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.

        '''
        name = self.name
        # print(f'YRANGE ON {name} -> yrange{yrange}')
        profiler = Profiler(
            msg=f'`ChartView._set_yrange()`: `{name}`',
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,
            delayed=True,
        )
        chart = self._chart

        # view has been set in 'axis' mode
        # meaning it can be panned and zoomed
        # arbitrarily on the y-axis:
        # - disable autoranging
        # - remove any y range limits
        if chart._static_yrange == 'axis':
            self.setLimits(yMin=None, yMax=None)
            return

        # static y-range has been set likely by
        # a specialized FSP configuration.
        elif chart._static_yrange is not None:
            ylow, yhigh = chart._static_yrange

        # range passed in by caller, usually a
        # maxmin detection algos inside the
        # display loop for re-draw efficiency.
        elif yrange is not None:
            ylow, yhigh = yrange

        # XXX: only compute the mxmn range
        # if none is provided as input!
        if not yrange:

            if not viz:
                breakpoint()

            out = viz.maxmin()
            if out is None:
                log.warning(f'No yrange provided for {name}!?')
                return
            (
                ixrng,
                _,
                yrange
            ) = out

            profiler(f'`{self.name}:Viz.maxmin()` -> {ixrng}=>{yrange}')

            if yrange is None:
                log.warning(f'No yrange provided for {name}!?')
                return

            ylow, yhigh = yrange

        # always stash last range for diffing by
        # incremental update calculations BEFORE adding
        # margin.
        self._yrange = ylow, yhigh

        # view margins: stay within a % of the "true range"
        if range_margin is not None:
            diff = yhigh - ylow
            ylow = max(
                ylow - (diff * range_margin),
                0,
            )
            yhigh = min(
                yhigh + (diff * range_margin),
                yhigh * (1 + range_margin),
            )

        # print(
        #     f'set limits {self.name}:\n'
        #     f'ylow: {ylow}\n'
        #     f'yhigh: {yhigh}\n'
        # )
        self.setYRange(
            ylow,
            yhigh,
            padding=0,
        )
        self.setLimits(
            yMin=ylow,
            yMax=yhigh,
        )
        self.update()

        # LOL: yet anothercucking pg buggg..
        # can't use `msg=f'setYRange({ylow}, {yhigh}')`
        profiler.finish()

    def enable_auto_yrange(
        self,
        viz: Viz,
        src_vb: ChartView | None = None,

    ) -> None:
        '''
        Assign callbacks for rescaling and resampling y-axis data
        automatically based on data contents and ``ViewBox`` state.

        '''
        if src_vb is None:
            src_vb = self

        # re-sampling trigger:
        # TODO: a smarter way to avoid calling this needlessly?
        # 2 things i can think of:
        # - register downsample-able graphics specially and only
        #   iterate those.
        # - only register this when certain downsample-able graphics are
        #   "added to scene".
        # src_vb.sigRangeChangedManually.connect(
        #     self.interact_graphics_cycle
        # )

        # widget-UIs/splitter(s) resizing
        src_vb.sigResized.connect(
            self.interact_graphics_cycle
        )

    def disable_auto_yrange(self) -> None:

        # XXX: not entirely sure why we can't de-reg this..
        self.sigResized.disconnect(
            self.interact_graphics_cycle
        )

    def x_uppx(self) -> float:
        '''
        Return the "number of x units" within a single
        pixel currently being displayed for relevant
        graphics items which are our children.

        '''
        graphics = [f.graphics for f in self._chart._vizs.values()]
        if not graphics:
            return 0

        for graphic in graphics:
            xvec = graphic.pixelVectors()[0]
            if xvec:
                return xvec.x()
        else:
            return 0

    def interact_graphics_cycle(
        self,
        *args,  # capture Qt signal (slot) inputs

        # debug_print: bool = False,
        do_linked_charts: bool = True,
        do_overlay_scaling: bool = True,

        yrange_kwargs: dict[
            str,
            tuple[float, float],
        ] | None = None,
 
    ):
        profiler = Profiler(
            msg=f'ChartView.interact_graphics_cycle() for {self.name}',
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,

            # XXX: important to avoid not seeing underlying
            # ``Viz.update_graphics()`` nested profiling likely
            # due to the way delaying works and garbage collection of
            # the profiler in the delegated method calls.
            delayed=True,

            # for hardcore latency checking, comment these flags above.
            # disabled=False,
            # ms_threshold=4,
        )

        linked = self.linked
        if (
            do_linked_charts
            and linked
        ):
            plots = {linked.chart.name: linked.chart}
            plots |= linked.subplots

        else:
            chart = self._chart
            plots = {chart.name: chart}

        # TODO: a faster single-loop-iterator way of doing this?
        return overlay_viewlists(
            self._viz,
            plots,
            profiler,
            do_overlay_scaling=do_overlay_scaling,
            do_linked_charts=do_linked_charts,
            yrange_kwargs=yrange_kwargs,
        )
