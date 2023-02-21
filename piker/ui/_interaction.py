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
Chart view box primitives

"""
from __future__ import annotations
from contextlib import asynccontextmanager
from math import (
    isinf,
)
import time
from typing import (
    Any,
    Callable,
    TYPE_CHECKING,
)

import pyqtgraph as pg
# from pyqtgraph.GraphicsScene import mouseEvents
from PyQt5.QtWidgets import QGraphicsSceneMouseEvent as gs_mouse
from PyQt5.QtCore import Qt, QEvent
from pyqtgraph import ViewBox, Point, QtCore
from pyqtgraph import functions as fn
import numpy as np
import trio

from ..log import get_logger
from .._profile import Profiler
from .._profile import pg_profile_enabled, ms_slower_then
from ..data.types import Struct
from ..data._pathops import slice_from_time
# from ._style import _min_points_to_show
from ._editors import SelectRect
from . import _event

if TYPE_CHECKING:
    from ._chart import ChartPlotWidget
    from ._dataviz import Viz
    # from ._overlay import PlotItemOverlay


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

) -> None:

    order_mode = view.order_mode

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

            # UI REPL-shell
            if (
                ctrl and key in {
                    Qt.Key_U,
                }
            ):
                import tractor
                god = order_mode.godw  # noqa
                feed = order_mode.feed  # noqa
                chart = order_mode.chart  # noqa
                vlm_chart = chart.linked.subplots['volume']  # noqa
                dvlm_pi = vlm_chart._vizs['dolla_vlm'].plot  # noqa
                await tractor.breakpoint()

            # SEARCH MODE #
            # ctlr-<space>/<l> for "lookup", "search" -> open search tree
            if (
                ctrl and key in {
                    Qt.Key_L,
                    Qt.Key_Space,
                }
            ):
                godw = view._chart.linked.godwidget
                godw.hist_linked.resize_sidepanes(from_linked=godw.rt_linked)
                godw.search.focus()

            # esc and ctrl-c
            if key == Qt.Key_Escape or (ctrl and key == Qt.Key_C):
                # ctrl-c as cancel
                # https://forum.qt.io/topic/532/how-to-catch-ctrl-c-on-a-widget/9
                view.select_box.clear()
                view.linked.focus()

            # cancel order or clear graphics
            if key == Qt.Key_C or key == Qt.Key_Delete:

                order_mode.cancel_orders_under_cursor()

            # View modes
            if key == Qt.Key_R:

                # TODO: set this for all subplots
                # edge triggered default view activation
                view.chart.default_view()

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
            ctrl and key in {
                Qt.Key_P,
            }
        ):
            pp_pane = order_mode.current_pp.pane
            if pp_pane.isHidden():
                pp_pane.show()
            else:
                pp_pane.hide()

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
            if tracker.live_pp.size:
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
        self.addItem(self.select_box, ignoreBounds=True)

        self.mode = None
        self.order_mode: bool = False

        self.setFocusPolicy(QtCore.Qt.StrongFocus)
        self._ic = None

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
        if self._ic is None:
            try:
                self.chart.pause_all_feeds()
                self._ic = trio.Event()
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
        if self._ic:
            try:
                self._ic.set()
                self._ic = None
                self.chart.resume_all_feeds()
            except RuntimeError:
                pass

    @asynccontextmanager
    async def open_async_input_handler(
        self,

    ) -> ChartView:

        async with (
            _event.open_handlers(
                [self],
                event_types={
                    QEvent.KeyPress,
                    QEvent.KeyRelease,
                },
                async_handler=handle_viewmode_kb_inputs,
            ),
            _event.open_handlers(
                [self],
                event_types={
                    gs_mouse.GraphicsSceneMousePress,
                },
                async_handler=handle_viewmode_mouse,
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
        ev,
        axis=None,
    ):
        '''
        Override "center-point" location for scrolling.

        This is an override of the ``ViewBox`` method simply changing
        the center of the zoom to be the y-axis.

        TODO: PR a method into ``pyqtgraph`` to make this configurable

        '''
        linked = self.linked
        if (
            not linked
        ):
            return

        if axis in (0, 1):
            mask = [False, False]
            mask[axis] = self.state['mouseEnabled'][axis]
        else:
            mask = self.state['mouseEnabled'][:]

        chart = self.linked.chart

        # don't zoom more then the min points setting
        viz = chart.get_viz(chart.name)
        vl, lbar, rbar, vr = viz.bars_range()

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
        s = 1.016 ** (ev.delta() * -1 / 20)  # self.state['wheelScaleFactor'])
        s = [(None if m is False else s) for m in mask]

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
            self.scaleBy(s, center)

        # zoom in view-box area
        else:
            # use right-most point of current curve graphic
            xl = viz.graphics.x_last()
            focal = min(
                xl,
                vr,
            )

            self._resetTarget()

            # NOTE: scroll "around" the right most datum-element in view
            # gives the feeling of staying "pinned" in place.
            self.scaleBy(s, focal)

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

            ev.accept()

    def mouseDragEvent(
        self,
        ev,
        axis: int | None = None,

    ) -> None:
        pos = ev.pos()
        lastPos = ev.lastPos()
        dif = pos - lastPos
        dif = dif * -1

        # NOTE: if axis is specified, event will only affect that axis.
        button = ev.button()

        # Ignore axes if mouse is disabled
        mouseEnabled = np.array(
            self.state['mouseEnabled'],
            dtype=np.float,
        )
        mask = mouseEnabled.copy()
        if axis is not None:
            mask[1-axis] = 0.0

        # Scale or translate based on mouse button
        if button & (
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

                down_pos = ev.buttonDownPos()

                # This is the final position in the drag
                if ev.isFinish():

                    self.select_box.mouse_drag_released(down_pos, pos)

                    ax = QtCore.QRectF(down_pos, pos)
                    ax = self.childGroup.mapRectFromParent(ax)

                    # this is the zoom transform cmd
                    self.showAxRect(ax)

                    # axis history tracking
                    self.axHistoryPointer += 1
                    self.axHistory = self.axHistory[
                        :self.axHistoryPointer] + [ax]

                else:
                    print('drag finish?')
                    self.select_box.set_pos(down_pos, pos)

                    # update shape of scale box
                    # self.updateScaleBox(ev.buttonDownPos(), ev.pos())
                    self.updateScaleBox(
                        down_pos,
                        ev.pos(),
                    )

            # PANNING MODE
            else:
                try:
                    self.start_ic()
                except RuntimeError:
                    pass
                # if self._ic is None:
                #     self.chart.pause_all_feeds()
                #     self._ic = trio.Event()

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
                    # self._ic.set()
                    # self._ic = None
                    # self.chart.resume_all_feeds()

                # # XXX: WHY
                # ev.accept()

        # WEIRD "RIGHT-CLICK CENTER ZOOM" MODE
        elif button & QtCore.Qt.RightButton:

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
        range_margin: float | None = 0.09,

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

        debug_print: bool = False,
        do_overlay_scaling: bool = True,
        do_linked_charts: bool = True,

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
        for chart_name, chart in plots.items():

            # Common `PlotItem` maxmin table; presumes that some path
            # graphics (and thus their backing data sets) are in the
            # same co-domain and view box (since the were added
            # a separate graphics objects to a common plot) and thus can
            # be sorted as one set per plot.
            mxmns_by_common_pi: dict[
                pg.PlotItem,
                tuple[float, float],
            ] = {}

            # proportional group auto-scaling per overlay set.
            # -> loop through overlays on each multi-chart widget
            #    and scale all y-ranges based on autoscale config.
            # -> for any "group" overlay we want to dispersion normalize
            #    and scale minor charts onto the major chart: the chart
            #    with the most dispersion in the set.
            major_viz: Viz = None
            major_mx: float = 0
            major_mn: float = float('inf')
            mx_disp: float = 0

            # collect certain flows have grapics objects **in seperate
            # plots/viewboxes** into groups and do a common calc to
            # determine auto-ranging input for `._set_yrange()`.
            # this is primarly used for our so called "log-linearized
            # multi-plot" overlay technique.
            overlay_table: dict[
                ViewBox,
                tuple[
                    Viz,
                    float,  # y start
                    float,  # y min
                    float,  # y max
                    float,  # y median
                    slice,  # in-view array slice
                    np.ndarray,  # in-view array
                ],
            ] = {}

            # ONLY auto-yrange the viz mapped to THIS view box
            if not do_overlay_scaling:
                viz = self._viz
                if debug_print:
                    print(f'ONLY ranging THIS viz: {viz.name}')

                out = _maybe_calc_yrange(
                    viz,
                    yrange_kwargs,
                    profiler,
                    chart_name,
                )
                if out is None:
                    continue

                read_slc, yrange_kwargs = out
                viz.plot.vb._set_yrange(**yrange_kwargs)
                profiler(f'{viz.name}@{chart_name} single curve yrange')

                # don't iterate overlays, just move to next chart
                continue

            # create a group overlay log-linearized y-range transform to
            # track and eventually inverse transform all overlay curves
            # to a common target max dispersion range.
            dnt = OverlayT()
            upt = OverlayT()

            if debug_print:
                print(
                    f'BEGIN UX GRAPHICS CYCLE: @{chart_name}\n'
                    +
                    '#'*100
                    +
                    '\n'
                )

            for name, viz in chart._vizs.items():

                out = _maybe_calc_yrange(
                    viz,
                    yrange_kwargs,
                    profiler,
                    chart_name,
                )
                if out is None:
                    continue

                read_slc, yrange_kwargs = out
                yrange = yrange_kwargs['yrange']
                pi = viz.plot

                # handle multiple graphics-objs per viewbox cases
                mxmn = mxmns_by_common_pi.get(pi)
                if mxmn:
                    yrange = mxmns_by_common_pi[pi] = (
                        min(yrange[0], mxmn[0]),
                        max(yrange[1], mxmn[1]),
                    )

                else:
                    mxmns_by_common_pi[pi] = yrange

                profiler(f'{viz.name}@{chart_name} common pi sort')

                # non-overlay group case
                if not viz.is_ohlc:
                    pi.vb._set_yrange(yrange=yrange)
                    profiler(
                        f'{viz.name}@{chart_name} simple std `._set_yrange()`'
                    )

                # handle overlay log-linearized group scaling cases
                # TODO: a better predicate here, likely something
                # to do with overlays and their settings..
                # TODO: we probably eventually might want some other
                # charts besides OHLC?
                else:
                    ymn, ymx = yrange

                    # determine start datum in view
                    arr = viz.shm.array
                    in_view = arr[read_slc]
                    if not in_view.size:
                        log.warning(f'{viz.name} not in view?')
                        continue

                    # row_start = arr[read_slc.start - 1]
                    row_start = arr[read_slc.start]

                    if viz.is_ohlc:
                        y_ref = row_start['open']
                    else:
                        y_ref = row_start[viz.name]

                    profiler(f'{viz.name}@{chart_name} MINOR curve median')

                    overlay_table[viz.plot.vb] = (
                        viz,
                        y_ref,
                        ymn,
                        ymx,
                        read_slc,
                        in_view,
                    )

                    key = 'open' if viz.is_ohlc else viz.name
                    start_t = in_view[0]['time']
                    r_down = (ymn - y_ref) / y_ref
                    r_up = (ymx - y_ref) / y_ref

                    msg = (
                        f'### {viz.name}@{chart_name} ###\n'
                        f'y_ref: {y_ref}\n'
                        f'down disp: {r_down}\n'
                        f'up disp: {r_up}\n'
                    )
                    profiler(msg)
                    if debug_print:
                        print(msg)

                    # track the "major" curve as the curve with most
                    # dispersion.
                    if (
                        dnt.rng is None
                        or (
                            r_down < dnt.rng
                            and r_down < 0
                        )
                    ):
                        dnt.viz = viz
                        dnt.rng = r_down
                        dnt.in_view = in_view
                        dnt.start_t = in_view[0]['time']
                        major_mn = ymn

                        msg = f'NEW DOWN: {viz.name}@{chart_name} r:{r_down}\n'
                        profiler(msg)
                        if debug_print:
                            print(msg)
                    else:
                        # minor in the down swing range so check that if
                        # we apply the current rng to the minor that it
                        # doesn't go outside the current range for the major
                        # otherwise we recompute the minor's range (when
                        # adjusted for it's intersect point to be the new
                        # major's range.
                        intersect = intersect_from_longer(
                            dnt.start_t,
                            dnt.in_view,
                            start_t,
                            in_view,
                        )
                        profiler(f'{viz.name}@{chart_name} intersect by t')

                        if intersect:
                            longer_in_view, _t, i = intersect

                            scaled_mn = dnt.apply_rng(y_ref)
                            if scaled_mn > ymn:
                                # after major curve scaling we detected
                                # the minor curve is still out of range
                                # so we need to adjust the major's range
                                # to include the new composed range.
                                y_maj_ref = longer_in_view[key]
                                new_major_ymn = (
                                    y_maj_ref
                                    *
                                    (1 + r_down)
                                )

                                # rewrite the major range to the new
                                # minor-pinned-to-major range and mark
                                # the transform as "virtual".
                                msg = (
                                    f'EXPAND DOWN bc {viz.name}@{chart_name}\n'
                                    f'y_start epoch time @ {_t}:\n'
                                    f'y_maj_ref @ {_t}: {y_maj_ref}\n'
                                    f'R: {dnt.rng} -> {r_down}\n'
                                    f'MN: {major_mn} -> {new_major_ymn}\n'
                                )
                                dnt.rng = r_down
                                major_mn = dnt.y_val = new_major_ymn
                                profiler(msg)
                                if debug_print:
                                    print(msg)

                    if (
                        upt.rng is None
                        or (
                            r_up > upt.rng
                            and r_up > 0
                        )
                    ):
                        upt.rng = r_up
                        upt.viz = viz
                        upt.in_view = in_view
                        upt.start_t = in_view[0]['time']
                        major_mx = ymx
                        msg = f'NEW UP: {viz.name}@{chart_name} r:{r_up}\n'
                        profiler(msg)
                        if debug_print:
                            print(msg)

                    else:
                        intersect = intersect_from_longer(
                            upt.start_t,
                            upt.in_view,
                            start_t,
                            in_view,
                        )
                        profiler(f'{viz.name}@{chart_name} intersect by t')

                        if intersect:
                            longer_in_view, _t, i = intersect

                            scaled_mx = upt.apply_rng(y_ref)
                            if scaled_mx < ymx:
                                # after major curve scaling we detected
                                # the minor curve is still out of range
                                # so we need to adjust the major's range
                                # to include the new composed range.
                                y_maj_ref = longer_in_view[key]
                                new_major_ymx = (
                                    y_maj_ref
                                    *
                                    (1 + r_up)
                                )

                                # rewrite the major range to the new
                                # minor-pinned-to-major range and mark
                                # the transform as "virtual".
                                msg = (
                                    f'EXPAND UP bc {viz.name}@{chart_name}:\n'
                                    f'y_maj_ref @ {_t}: {y_maj_ref}\n'
                                    f'R: {upt.rng} -> {r_up}\n'
                                    f'MX: {major_mx} -> {new_major_ymx}\n'
                                )
                                upt.rng = r_up
                                major_mx = upt.y_val = new_major_ymx
                                profiler(msg)
                                print(msg)

                    # find curve with max dispersion
                    disp = abs(ymx - ymn) / y_ref
                    if disp > mx_disp:
                        major_viz = viz
                        mx_disp = disp
                        major_mn = ymn
                        major_mx = ymx

                    profiler(f'{viz.name}@{chart_name} MINOR curve scale')

            # NOTE: if no there were no overlay charts
            # detected/collected (could be either no group detected or
            # chart with a single symbol, thus a single viz/overlay)
            # then we ONLY set the lone chart's (viz) yrange and short
            # circuit to the next chart in the linked charts loop. IOW
            # there's no reason to go through the overlay dispersion
            # scaling in the next loop below when only one curve is
            # detected.
            if (
                not mxmns_by_common_pi
                and len(overlay_table) < 2
            ):
                if debug_print:
                    print(f'ONLY ranging major: {viz.name}')

                out = _maybe_calc_yrange(
                    viz,
                    yrange_kwargs,
                    profiler,
                    chart_name,
                )
                if out is None:
                    continue

                read_slc, yrange_kwargs = out
                viz.plot.vb._set_yrange(**yrange_kwargs)
                profiler(f'{viz.name}@{chart_name} single curve yrange')

                # move to next chart in linked set since
                # no overlay transforming is needed.
                continue

            elif (
                mxmns_by_common_pi
                and not major_viz
            ):
                # move to next chart in linked set since
                # no overlay transforming is needed.
                continue

            profiler(f'<{chart_name}>.interact_graphics_cycle({name})')

            # if a minor curves scaling brings it "outside" the range of
            # the major curve (in major curve co-domain terms) then we
            # need to rescale the major to also include this range. The
            # below placeholder denotes when this occurs.
            # group_mxmn: None | tuple[float, float] = None

            # TODO: probably re-write this loop as a compiled cpython or
            # numba func.

            # conduct "log-linearized multi-plot" scalings for all groups
            for (
                view,
                (
                    viz,
                    y_start,
                    y_min,
                    y_max,
                    read_slc,
                    minor_in_view,
                )
            ) in overlay_table.items():

                key = 'open' if viz.is_ohlc else viz.name

                if (
                    isinf(ymx)
                    or isinf(ymn)
                ):
                    log.warning(
                        f'BAD ymx/ymn: {(ymn, ymx)}'
                    )
                    continue

                ymn = dnt.apply_rng(y_start)
                ymx = upt.apply_rng(y_start)

                # NOTE XXX: we have to set each curve's range once (and
                # ONLY ONCE) here since we're doing this entire routine
                # inside of a single render cycle (and apparently calling
                # `ViewBox.setYRange()` multiple times within one only takes
                # the first call as serious...) XD
                view._set_yrange(
                    yrange=(ymn, ymx),
                )
                profiler(f'{viz.name}@{chart_name} log-SCALE minor')

                if debug_print:
                    print(
                        '------------------------------\n'
                        f'LOGLIN SCALE CYCLE: {viz.name}@{chart_name}\n'
                        f'UP MAJOR C: {upt.viz.name} with disp: {upt.rng}\n'
                        f'DOWN MAJOR C: {dnt.viz.name} with disp: {dnt.rng}\n'
                        f'y_start: {y_start}\n'
                        f'y min: {y_min}\n'
                        f'y max: {y_max}\n'
                        f'T scaled ymn: {ymn}\n'
                        f'T scaled ymx: {ymx}\n'
                        '------------------------------\n'
                    )

            # profiler(f'{viz.name}@{chart_name} log-SCALE major')
            # major_mx, major_mn = group_mxmn
            # vrs = major_viz.plot.vb.viewRange()
            # if vrs[1][0] > major_mn:
            #     breakpoint()

            if debug_print:
                print(
                    f'END UX GRAPHICS CYCLE: @{chart_name}\n'
                    +
                    '#'*100
                    +
                    '\n'
                )
            if not do_linked_charts:
                return

        profiler.finish()


def _maybe_calc_yrange(
    viz: Viz,
    yrange_kwargs: dict[Viz, dict[str, Any]],
    profiler: Profiler,
    chart_name: str,

) -> tuple[
    slice,
    dict,
] | None:

    if not viz.render:
        return

    # pass in no array which will read and render from the last
    # passed array (normally provided by the display loop.)
    in_view, i_read_range, _ = viz.update_graphics()

    if not in_view:
        return

    profiler(f'{viz.name}@{chart_name} `Viz.update_graphics()`')

    # check if explicit yrange (kwargs) was passed in by the caller
    yrange_kwargs = yrange_kwargs.get(viz) if yrange_kwargs else None
    if yrange_kwargs is not None:
        read_slc = slice(*i_read_range)

    else:
        out = viz.maxmin(i_read_range=i_read_range)
        if out is None:
            log.warning(f'No yrange provided for {viz.name}!?')
            return
        (
            _,  # ixrng,
            read_slc,
            yrange
        ) = out
        profiler(f'{viz.name}@{chart_name} `Viz.maxmin()`')
        yrange_kwargs = {'yrange': yrange}

    return (
        read_slc,
        yrange_kwargs,
    )


class OverlayT(Struct):
    '''
    An overlay co-domain range transformer.

    Used to translate and apply a range from one y-range
    to another based on a returns logarithm:

    R(ymn, ymx, yref) = (ymx - yref)/yref

    which gives the log-scale multiplier, and

    ymx_t = yref * (1 + R)

    which gives the inverse to translate to the same value
    in the target co-domain.

    '''
    start_t: float | None = None
    viz: Viz = None

    # % "range" computed from some ref value to the mn/mx
    rng: float | None = None
    in_view: np.ndarray | None = None

    # pinned-minor curve modified mn and max for the major dispersion
    # curve due to one series being shorter and the pin + scaling from
    # that pin point causing the original range to have to increase.
    y_val: float | None = None

    def apply_rng(
        self,
        y_start: float,  # reference value for dispersion metric

    ) -> float:
        return y_start * (1 + self.rng)

    # def loglin_from_range(
    #     self,

    #     y_ref: float,  # reference value for dispersion metric
    #     mn: float,  # min y in target log-lin range
    #     mx: float,  # max y in target log-lin range
    #     offset: float,  # y-offset to start log-scaling from

    # ) -> tuple[float, float]:
    #     r_up = (mx - y_ref) / y_ref
    #     r_down = (mn - y_ref) / y_ref
    #     ymn = offset * (1 + r_down)
    #     ymx = offset * (1 + r_up)

    #     return ymn, ymx


def intersect_from_longer(
    start_t_first: float,
    in_view_first: np.ndarray,

    start_t_second: float,
    in_view_second: np.ndarray,

) -> np.ndarray:

    tdiff = start_t_first - start_t_second

    if tdiff == 0:
        return False

    i: int = 0

    # first time series has an "earlier" first time stamp then the 2nd.
    # aka 1st is "shorter" then the 2nd.
    if tdiff > 0:
        longer = in_view_second
        find_t = start_t_first
        i = 1

    # second time series has an "earlier" first time stamp then the 1st.
    # aka 2nd is "shorter" then the 1st.
    elif tdiff < 0:
        longer = in_view_first
        find_t = start_t_second
        i = 0

    slc = slice_from_time(
        arr=longer,
        start_t=find_t,
        stop_t=find_t,
    )
    return (
        longer[slc.start],
        find_t,
        i,
    )
