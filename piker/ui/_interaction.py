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
from contextlib import asynccontextmanager
from typing import Optional

import pyqtgraph as pg
from PyQt5.QtCore import Qt, QEvent
from pyqtgraph import ViewBox, Point, QtCore
from pyqtgraph import functions as fn
import numpy as np
import trio

from ..log import get_logger
from ._style import _min_points_to_show
from ._editors import SelectRect


log = get_logger(__name__)


async def handle_viewmode_inputs(

    view: 'ChartView',
    recv_chan: trio.abc.ReceiveChannel,

) -> None:

    async for event, etype, key, mods, text in recv_chan:
        log.debug(f'key: {key}, mods: {mods}, text: {text}')

        if etype in {QEvent.KeyPress}:

            # await view.on_key_press(text, key, mods)

            if mods == Qt.ShiftModifier:
                if view.state['mouseMode'] == ViewBox.PanMode:
                    view.setMouseMode(ViewBox.RectMode)

            # ctrl
            ctrl = False
            if mods == Qt.ControlModifier:
                ctrl = True
                view.mode._exec_mode = 'live'

            view._key_active = True

            # ctrl + alt
            # ctlalt = False
            # if (QtCore.Qt.AltModifier | QtCore.Qt.ControlModifier) == mods:
            #     ctlalt = True

            # ctlr-<space>/<l> for "lookup", "search" -> open search tree
            if ctrl and key in {
                Qt.Key_L,
                Qt.Key_Space,
            }:
                search = view._chart._lc.godwidget.search
                search.focus()

            # esc
            if key == Qt.Key_Escape or (ctrl and key == Qt.Key_C):
                # ctrl-c as cancel
                # https://forum.qt.io/topic/532/how-to-catch-ctrl-c-on-a-widget/9
                view.select_box.clear()

            # cancel order or clear graphics
            if key == Qt.Key_C or key == Qt.Key_Delete:
                # delete any lines under the cursor
                mode = view.mode
                for line in mode.lines.lines_under_cursor():
                    mode.book.cancel(uuid=line.oid)

            view._key_buffer.append(text)

            # View modes
            if key == Qt.Key_R:
                view.chart.default_view()

            # Order modes: stage orders at the current cursor level

            elif key == Qt.Key_D:  # for "damp eet"
                view.mode.set_exec('sell')

            elif key == Qt.Key_F:  # for "fillz eet"
                view.mode.set_exec('buy')

            elif key == Qt.Key_A:
                view.mode.set_exec('alert')

            else:
                # maybe propagate to parent widget
                # event.ignore()
                view._key_active = False

        elif etype in {QEvent.KeyRelease}:

            # await view.on_key_release(text, key, mods)
            if key == Qt.Key_Shift:
                # if view.state['mouseMode'] == ViewBox.RectMode:
                view.setMouseMode(ViewBox.PanMode)

            # ctlalt = False
            # if (QtCore.Qt.AltModifier | QtCore.Qt.ControlModifier) == mods:
            #     ctlalt = True

            # if view.state['mouseMode'] == ViewBox.RectMode:
            # if key == QtCore.Qt.Key_Space:
            if mods == Qt.ControlModifier or key == QtCore.Qt.Key_Control:
                view.mode._exec_mode = 'dark'

            if key in {Qt.Key_A, Qt.Key_F, Qt.Key_D}:
                # remove "staged" level line under cursor position
                view.mode.lines.unstage_line()

            view._key_active = False


class ChartView(ViewBox):
    '''
    Price chart view box with interaction behaviors you'd expect from
    any interactive platform:

        - zoom on mouse scroll that auto fits y-axis
        - vertical scrolling on y-axis
        - zoom on x to most recent in view datum
        - zoom on right-click-n-drag to cursor position

    '''
    mode_name: str = 'mode: view'

    def __init__(

        self,
        name: str,
        parent: pg.PlotItem = None,
        **kwargs,

    ):
        super().__init__(parent=parent, **kwargs)

        # disable vertical scrolling
        self.setMouseEnabled(x=True, y=False)

        self.linkedsplits = None
        self._chart: 'ChartPlotWidget' = None  # noqa

        # add our selection box annotator
        self.select_box = SelectRect(self)
        self.addItem(self.select_box, ignoreBounds=True)

        self.name = name
        self.mode = None

        # kb ctrls processing
        self._key_buffer = []
        self._key_active: bool = False

        self.setFocusPolicy(QtCore.Qt.StrongFocus)

    @asynccontextmanager
    async def open_async_input_handler(
        self,
    ) -> 'ChartView':
        from . import _event

        async with _event.open_handler(
            self,
            event_types={QEvent.KeyPress, QEvent.KeyRelease},
            async_handler=handle_viewmode_inputs,
        ):
            yield self

    @property
    def chart(self) -> 'ChartPlotWidget':  # type: ignore # noqa
        return self._chart

    @chart.setter
    def chart(self, chart: 'ChartPlotWidget') -> None:  # type: ignore # noqa
        self._chart = chart
        self.select_box.chart = chart

    def wheelEvent(self, ev, axis=None):
        '''Override "center-point" location for scrolling.

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

        if ev.delta() < 0 and vl >= len(chart._ohlc) + 666:
            log.debug("Min zoom bruh...")
            return

        # actual scaling factor
        s = 1.015 ** (ev.delta() * -1 / 20)  # self.state['wheelScaleFactor'])
        s = [(None if m is False else s) for m in mask]

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
                chart._vb.mapToView(
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
        ev.accept()
        self.sigRangeChangedManually.emit(mask)

    def mouseDragEvent(
        self,
        ev,
        axis: Optional[int] = None,
    ) -> None:
        #  if axis is specified, event will only affect that axis.
        ev.accept()  # we accept all buttons
        button = ev.button()

        pos = ev.pos()
        lastPos = ev.lastPos()
        dif = pos - lastPos
        dif = dif * -1

        # Ignore axes if mouse is disabled
        mouseEnabled = np.array(self.state['mouseEnabled'], dtype=np.float)
        mask = mouseEnabled.copy()
        if axis is not None:
            mask[1-axis] = 0.0

        # Scale or translate based on mouse button
        if button & (QtCore.Qt.LeftButton | QtCore.Qt.MidButton):

            # zoom y-axis ONLY when click-n-drag on it
            if axis == 1:
                # set a static y range special value on chart widget to
                # prevent sizing to data in view.
                self.chart._static_yrange = 'axis'

                scale_y = 1.3 ** (dif.y() * -1 / 20)
                self.setLimits(yMin=None, yMax=None)

                # print(scale_y)
                self.scaleBy((0, scale_y))

            if self.state['mouseMode'] == ViewBox.RectMode:

                down_pos = ev.buttonDownPos()

                # This is the final position in the drag
                if ev.isFinish():

                    self.select_box.mouse_drag_released(down_pos, pos)

                    # ax = QtCore.QRectF(down_pos, pos)
                    # ax = self.childGroup.mapRectFromParent(ax)
                    # print(ax)

                    # this is the zoom transform cmd
                    # self.showAxRect(ax)

                    # self.axHistoryPointer += 1
                    # self.axHistory = self.axHistory[
                    #     :self.axHistoryPointer] + [ax]
                else:
                    self.select_box.set_pos(down_pos, pos)

                    # update shape of scale box
                    # self.updateScaleBox(ev.buttonDownPos(), ev.pos())
            else:
                # default bevavior: click to pan view

                tr = self.childGroup.transform()
                tr = fn.invertQTransform(tr)
                tr = tr.map(dif*mask) - tr.map(Point(0, 0))

                x = tr.x() if mask[0] == 1 else None
                y = tr.y() if mask[1] == 1 else None

                self._resetTarget()

                if x is not None or y is not None:
                    self.translateBy(x=x, y=y)

                self.sigRangeChangedManually.emit(self.state['mouseEnabled'])

        elif button & QtCore.Qt.RightButton:

            # right click zoom to center behaviour

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

    def mouseClickEvent(self, ev):
        """Full-click callback.

        """
        button = ev.button()
        # pos = ev.pos()

        if button == QtCore.Qt.RightButton and self.menuEnabled():
            ev.accept()
            self.raiseContextMenu(ev)

        elif button == QtCore.Qt.LeftButton:
            # when in order mode, submit execution
            if self._key_active:
                ev.accept()
                self.mode.submit_exec()

    def keyReleaseEvent(self, event: QtCore.QEvent) -> None:
        '''This routine is rerouted to an async handler.
        '''
        pass

    def keyPressEvent(self, event: QtCore.QEvent) -> None:
        '''This routine is rerouted to an async handler.
        '''
        pass
