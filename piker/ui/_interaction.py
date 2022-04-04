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
# import itertools
import time
from typing import Optional, Callable

import pyqtgraph as pg
# from pyqtgraph.GraphicsScene import mouseEvents
from PyQt5.QtWidgets import QGraphicsSceneMouseEvent as gs_mouse
from PyQt5.QtCore import Qt, QEvent
from pyqtgraph import ViewBox, Point, QtCore
from pyqtgraph import functions as fn
import numpy as np
import trio

from ..log import get_logger
from .._profile import pg_profile_enabled, ms_slower_then
from ._style import _min_points_to_show
from ._editors import SelectRect
from . import _event
from ._ohlc import BarItems


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

    view: 'ChartView',
    recv_chan: trio.abc.ReceiveChannel,

) -> None:

    order_mode = view.order_mode

    # track edge triggered keys
    # (https://en.wikipedia.org/wiki/Interrupt#Triggering_methods)
    pressed: set[str] = set()

    last = time.time()
    trigger_mode: str
    action: str

    on_next_release: Optional[Callable] = None

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

            # SEARCH MODE #
            # ctlr-<space>/<l> for "lookup", "search" -> open search tree
            if (
                ctrl and key in {
                    Qt.Key_L,
                    Qt.Key_Space,
                }
            ):
                view._chart.linked.godwidget.search.focus()

            # esc and ctrl-c
            if key == Qt.Key_Escape or (ctrl and key == Qt.Key_C):
                # ctrl-c as cancel
                # https://forum.qt.io/topic/532/how-to-catch-ctrl-c-on-a-widget/9
                view.select_box.clear()

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

        # QUERY/QUOTE MODE #
        if {Qt.Key_Q}.intersection(pressed):

            view.linkedsplits.cursor.in_query_mode = True

        else:
            view.linkedsplits.cursor.in_query_mode = False

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

            # show the pp size label
            order_mode.current_pp.show()

            # TODO: show pp config mini-params in status bar widget
            # mode.pp_config.show()

            if (
                # 's' for "submit" to activate "live" order
                Qt.Key_S in pressed or
                ctrl
            ):
                trigger_type: str = 'live'

            else:
                trigger_type: str = 'dark'

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
            ) and
            key in NUMBER_LINE
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
            order_mode.current_pp.hide_info()

            # if none are pressed, remove "staged" level
            # line under cursor position
            order_mode.lines.unstage_line()

            if view.hasFocus():
                # update mode label
                view._chart.window().set_mode_name('view')

            order_mode.active = False

        last = time.time()


async def handle_viewmode_mouse(

    view: 'ChartView',
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
            # breakpoint()
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

    # "relay events" for making overlaid views work.
    # NOTE: these MUST be defined here (and can't be monkey patched
    # on later) due to signal construction requiring refs to be
    # in place during the run of meta-class machinery.
    mouseDragEventRelay = QtCore.Signal(object, object, object)
    wheelEventRelay = QtCore.Signal(object, object, object)

    event_relay_source: 'Optional[ViewBox]' = None
    relays: dict[str, QtCore.Signal] = {}

    def __init__(
        self,

        name: str,

        parent: pg.PlotItem = None,
        static_yrange: Optional[tuple[float, float]] = None,
        **kwargs,

    ):
        super().__init__(
            parent=parent,
            # TODO: look into the default view padding
            # support that might replace somem of our
            # ``ChartPlotWidget._set_yrange()`
            # defaultPadding=0.,
            **kwargs
        )
        # for "known y-range style"
        self._static_yrange = static_yrange
        self._maxmin = None

        # disable vertical scrolling
        self.setMouseEnabled(
            x=True,
            y=True,
        )

        self.linkedsplits = None
        self._chart: 'ChartPlotWidget' = None  # noqa

        # add our selection box annotator
        self.select_box = SelectRect(self)
        self.addItem(self.select_box, ignoreBounds=True)

        self.mode = None
        self.order_mode: bool = False

        self.setFocusPolicy(QtCore.Qt.StrongFocus)
        self._ic = None

    def start_ic(
        self,
    ) -> None:
        if self._ic is None:
            self.chart.pause_all_feeds()
            self._ic = trio.Event()

    def signal_ic(
        self,
        *args,
        # ev = None,
    ) -> None:
        if args:
            print(f'range change dun: {args}')
        else:
            print('proxy called')

        if self._ic:
            self._ic.set()
            self._ic = None
            self.chart.resume_all_feeds()

    @asynccontextmanager
    async def open_async_input_handler(
        self,

    ) -> 'ChartView':

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
    def chart(self) -> 'ChartPlotWidget':  # type: ignore # noqa
        return self._chart

    @chart.setter
    def chart(self, chart: 'ChartPlotWidget') -> None:  # type: ignore # noqa
        self._chart = chart
        self.select_box.chart = chart
        if self._maxmin is None:
            self._maxmin = chart.maxmin

    @property
    def maxmin(self) -> Callable:
        return self._maxmin

    @maxmin.setter
    def maxmin(self, callback: Callable) -> None:
        self._maxmin = callback

    def wheelEvent(
        self,
        ev,
        axis=None,
        relayed_from: ChartView = None,
    ):
        '''
        Override "center-point" location for scrolling.

        This is an override of the ``ViewBox`` method simply changing
        the center of the zoom to be the y-axis.

        TODO: PR a method into ``pyqtgraph`` to make this configurable

        '''
        if axis in (0, 1):
            mask = [False, False]
            mask[axis] = self.state['mouseEnabled'][axis]
        else:
            mask = self.state['mouseEnabled'][:]

        chart = self.linkedsplits.chart

        # don't zoom more then the min points setting
        l, lbar, rbar, r = chart.bars_range()
        vl = r - l

        if ev.delta() > 0 and vl <= _min_points_to_show:
            log.debug("Max zoom bruh...")
            return

        if ev.delta() < 0 and vl >= len(chart._arrays[chart.name]) + 666:
            log.debug("Min zoom bruh...")
            return

        # actual scaling factor
        s = 1.015 ** (ev.delta() * -1 / 20)  # self.state['wheelScaleFactor'])
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

        else:

            # center = pg.Point(
            #     fn.invertQTransform(self.childGroup.transform()).map(ev.pos())
            # )

            # XXX: scroll "around" the right most element in the view
            # which stays "pinned" in place.

            # furthest_right_coord = self.boundingRect().topRight()

            # yaxis = pg.Point(
            #     fn.invertQTransform(
            #         self.childGroup.transform()
            #     ).map(furthest_right_coord)
            # )

            # This seems like the most "intuitive option, a hybrid of
            # tws and tv styles
            last_bar = pg.Point(int(rbar)) + 1

            ryaxis = chart.getAxis('right')
            r_axis_x = ryaxis.pos().x()

            end_of_l1 = pg.Point(
                round(
                    chart.cv.mapToView(
                        pg.Point(r_axis_x - chart._max_l1_line_len)
                        # QPointF(chart._max_l1_line_len, 0)
                    ).x()
                )
            )  # .x()

            # self.state['viewRange'][0][1] = end_of_l1
            # focal = pg.Point((last_bar.x() + end_of_l1)/2)

            focal = min(
                last_bar,
                end_of_l1,
                key=lambda p: p.x()
            )
            # focal = pg.Point(last_bar.x() + end_of_l1)

            self._resetTarget()
            self.scaleBy(s, focal)
            self.sigRangeChangedManually.emit(mask)

            # self._ic.set()
            # self._ic = None
            # self.chart.resume_all_feeds()

            ev.accept()

    def mouseDragEvent(
        self,
        ev,
        axis: Optional[int] = None,
        relayed_from: ChartView = None,

    ) -> None:

        pos = ev.pos()
        lastPos = ev.lastPos()
        dif = pos - lastPos
        dif = dif * -1

        # NOTE: if axis is specified, event will only affect that axis.
        button = ev.button()

        # Ignore axes if mouse is disabled
        mouseEnabled = np.array(self.state['mouseEnabled'], dtype=np.float)
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
                # XXX: WHY
                ev.accept()

                self.start_ic()
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

                self.sigRangeChangedManually.emit(self.state['mouseEnabled'])

                if ev.isFinish():
                    print('DRAG FINISH')
                    self.signal_ic()
                    # self._ic.set()
                    # self._ic = None
                    # self.chart.resume_all_feeds()

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
            self.sigRangeChangedManually.emit(self.state['mouseEnabled'])

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

        yrange: Optional[tuple[float, float]] = None,
        range_margin: float = 0.06,
        bars_range: Optional[tuple[int, int, int, int]] = None,

        # flag to prevent triggering sibling charts from the same linked
        # set from recursion errors.
        autoscale_linked_plots: bool = True,
        name: Optional[str] = None,
        # autoscale_overlays: bool = False,

    ) -> None:
        '''
        Set the viewable y-range based on embedded data.

        This adds auto-scaling like zoom on the scroll wheel such
        that data always fits nicely inside the current view of the
        data set.

        '''
        set_range = True
        chart = self._chart

        # view has been set in 'axis' mode
        # meaning it can be panned and zoomed
        # arbitrarily on the y-axis:
        # - disable autoranging
        # - remove any y range limits
        if chart._static_yrange == 'axis':
            set_range = False
            self.setLimits(yMin=None, yMax=None)

        # static y-range has been set likely by
        # a specialized FSP configuration.
        elif chart._static_yrange is not None:
            ylow, yhigh = chart._static_yrange

        # range passed in by caller, usually a
        # maxmin detection algos inside the
        # display loop for re-draw efficiency.
        elif yrange is not None:
            ylow, yhigh = yrange

        # calculate max, min y values in viewable x-range from data.
        # Make sure min bars/datums on screen is adhered.
        else:
            br = bars_range or chart.bars_range()

            # TODO: maybe should be a method on the
            # chart widget/item?
            if autoscale_linked_plots:
                # avoid recursion by sibling plots
                linked = self.linkedsplits
                plots = list(linked.subplots.copy().values())
                main = linked.chart
                if main:
                    plots.append(main)

                for chart in plots:
                    if chart and not chart._static_yrange:
                        chart.cv._set_yrange(
                            bars_range=br,
                            autoscale_linked_plots=False,
                        )

        if set_range:

            yrange = self._maxmin()
            if yrange is None:
                return

            ylow, yhigh = yrange

            # view margins: stay within a % of the "true range"
            diff = yhigh - ylow
            ylow = ylow - (diff * range_margin)
            yhigh = yhigh + (diff * range_margin)

            # XXX: this often needs to be unset
            # to get different view modes to operate
            # correctly!
            self.setLimits(
                yMin=ylow,
                yMax=yhigh,
            )
            self.setYRange(ylow, yhigh)

    def enable_auto_yrange(
        self,
        src_vb: Optional[ChartView] = None,

    ) -> None:
        '''
        Assign callback for rescaling y-axis automatically
        based on data contents and ``ViewBox`` state.

        '''
        if src_vb is None:
            src_vb = self

        src_vb.sigXRangeChanged.connect(self._set_yrange)

        # TODO: a smarter way to avoid calling this needlessly?
        # 2 things i can think of:
        # - register downsample-able graphics specially and only
        #   iterate those.
        # - only register this when certain downsampleable graphics are
        #   "added to scene".
        src_vb.sigXRangeChanged.connect(self.maybe_downsample_graphics)

        # mouse wheel doesn't emit XRangeChanged
        src_vb.sigRangeChangedManually.connect(self._set_yrange)

        # splitter(s) resizing
        src_vb.sigResized.connect(self._set_yrange)

    def disable_auto_yrange(
        self,
    ) -> None:

        self._chart._static_yrange = 'axis'

    def xs_in_px(self) -> float:
        '''
        Return the "number of x units" within a single
        pixel currently being displayed for relevant
        graphics items which are our children.

        '''
        for graphic in self._chart._graphics.values():
            xvec = graphic.pixelVectors()[0]
            if xvec:
                xpx = xvec.x()
                if xpx:
                    return xpx
            else:
                continue
        return 1.0

    def maybe_downsample_graphics(self):

        profiler = pg.debug.Profiler(
            disabled=not pg_profile_enabled(),
            gt=3,
            delayed=True,
        )

        # TODO: a faster single-loop-iterator way of doing this XD
        chart = self._chart

        for name, graphics in chart._graphics.items():

            # TODO: make it so we don't have to do this XD
            # if name == 'volume':
            #     continue
            uppx = graphics.x_uppx()
            if (
                uppx and uppx > 16
                and self._ic is not None
            ):
                # don't bother updating since we're zoomed out bigly and
                # in a pan-interaction, in which case we shouldn't be
                # doing view-range based rendering (at least not yet).
                # print(f'{uppx} exiting early!')
                return

            use_vr = False
            if isinstance(graphics, BarItems):
                use_vr = True

            # pass in no array which will read and render from the last
            # passed array (normally provided by the display loop.)
            chart.update_graphics_from_array(
                name,
                use_vr=use_vr,
            )
            profiler(f'updated {name}')

        profiler.finish()
        # for graphic in graphics:
        #     ds_meth = getattr(graphic, 'maybe_downsample', None)
        #     if ds_meth:
        #         ds_meth()
