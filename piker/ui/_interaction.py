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
UX interaction customs.
"""
from typing import Optional

import pyqtgraph as pg
from pyqtgraph import ViewBox, Point, QtCore, QtGui
from pyqtgraph import functions as fn
import numpy as np

from ..log import get_logger
from ._style import _min_points_to_show, hcolor, _font


log = get_logger(__name__)


class SelectRect(QtGui.QGraphicsRectItem):

    def __init__(
        self,
        viewbox: ViewBox,
        color: str = 'dad_blue',
    ) -> None:
        super().__init__(0, 0, 1, 1)

        # self.rbScaleBox = QtGui.QGraphicsRectItem(0, 0, 1, 1)
        self.vb = viewbox
        self._chart: 'ChartPlotWidget' = None  # noqa

        # override selection box color
        color = QtGui.QColor(hcolor(color))
        self.setPen(fn.mkPen(color, width=1))
        color.setAlpha(66)
        self.setBrush(fn.mkBrush(color))
        self.setZValue(1e9)
        self.hide()
        self._label = None

        label = self._label = QtGui.QLabel()
        label.setTextFormat(0)  # markdown
        label.setFont(_font.font)
        label.setMargin(0)
        label.setAlignment(
            QtCore.Qt.AlignLeft
            # | QtCore.Qt.AlignVCenter
        )

        # proxy is created after containing scene is initialized
        self._label_proxy = None
        self._abs_top_right = None

        # TODO: "swing %" might be handy here (data's max/min # % change)
        self._contents = [
            'change: {pchng:.2f} %',
            'range: {rng:.2f}',
            'bars: {nbars}',
            'max: {dmx}',
            'min: {dmn}',
            # 'time: {nbars}m',  # TODO: compute this per bar size
            'sigma: {std:.2f}',
        ]

    @property
    def chart(self) -> 'ChartPlotWidget':  # noqa
        return self._chart

    @chart.setter
    def chart(self, chart: 'ChartPlotWidget') -> None:  # noqa
        self._chart = chart
        chart.sigRangeChanged.connect(self.update_on_resize)
        palette = self._label.palette()

        # TODO: get bg color working
        palette.setColor(
            self._label.backgroundRole(),
            # QtGui.QColor(chart.backgroundBrush()),
            QtGui.QColor(hcolor('papas_special')),
        )

    def update_on_resize(self, vr, r):
        """Re-position measure label on view range change.

        """
        if self._abs_top_right:
            self._label_proxy.setPos(
                self.vb.mapFromView(self._abs_top_right)
            )

    def mouse_drag_released(
        self,
        p1: QtCore.QPointF,
        p2: QtCore.QPointF
    ) -> None:
        """Called on final button release for mouse drag with start and
        end positions.

        """
        self.set_pos(p1, p2)

    def set_pos(
        self,
        p1: QtCore.QPointF,
        p2: QtCore.QPointF
    ) -> None:
        """Set position of selection rectagle and accompanying label, move
        label to match.

        """
        if self._label_proxy is None:
            # https://doc.qt.io/qt-5/qgraphicsproxywidget.html
            self._label_proxy = self.vb.scene().addWidget(self._label)

        start_pos = self.vb.mapToView(p1)
        end_pos = self.vb.mapToView(p2)

        # map to view coords and update area
        r = QtCore.QRectF(start_pos, end_pos)

        # old way; don't need right?
        # lr = QtCore.QRectF(p1, p2)
        # r = self.vb.childGroup.mapRectFromParent(lr)

        self.setPos(r.topLeft())
        self.resetTransform()
        self.scale(r.width(), r.height())
        self.show()

        y1, y2 = start_pos.y(), end_pos.y()
        x1, x2 = start_pos.x(), end_pos.x()

        # TODO: heh, could probably use a max-min streamin algo here too
        _, xmn = min(y1, y2), min(x1, x2)
        ymx, xmx = max(y1, y2), max(x1, x2)

        pchng = (y2 - y1) / y1 * 100
        rng = abs(y1 - y2)

        ixmn, ixmx = round(xmn), round(xmx)
        nbars = ixmx - ixmn + 1

        data = self._chart._ohlc[ixmn:ixmx]

        if len(data):
            std = data['close'].std()
            dmx = data['high'].max()
            dmn = data['low'].min()
        else:
            dmn = dmx = std = np.nan

        # update label info
        self._label.setText('\n'.join(self._contents).format(
            pchng=pchng, rng=rng, nbars=nbars,
            std=std, dmx=dmx, dmn=dmn,
        ))

        # print(f'x2, y2: {(x2, y2)}')
        # print(f'xmn, ymn: {(xmn, ymx)}')

        label_anchor = Point(xmx + 2, ymx)

        # XXX: in the drag bottom-right -> top-left case we don't
        # want the label to overlay the box.
        # if (x2, y2) == (xmn, ymx):
        #     # could do this too but needs to be added after coords transform
        #     # label_anchor = Point(x2, y2 + self._label.height())
        #     label_anchor = Point(xmn, ymn)

        self._abs_top_right = label_anchor
        self._label_proxy.setPos(self.vb.mapFromView(label_anchor))
        self._label.show()

    def clear(self):
        """Clear the selection box from view.

        """
        self._label.hide()
        self.hide()


class ChartView(ViewBox):
    """Price chart view box with interaction behaviors you'd expect from
    any interactive platform:

        - zoom on mouse scroll that auto fits y-axis
        - no vertical scrolling
        - zoom to a "fixed point" on the y-axis
    """
    def __init__(
        self,
        parent: pg.PlotItem = None,
        **kwargs,
    ):
        super().__init__(parent=parent, **kwargs)
        # disable vertical scrolling
        self.setMouseEnabled(x=True, y=False)
        self.linked_charts = None
        self.select_box = SelectRect(self)
        self.addItem(self.select_box, ignoreBounds=True)
        self._chart: 'ChartPlotWidget' = None  # noqa

    @property
    def chart(self) -> 'ChartPlotWidget':  # noqa
        return self._chart

    @chart.setter
    def chart(self, chart: 'ChartPlotWidget') -> None:  # noqa
        self._chart = chart
        self.select_box.chart = chart

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
            log.debug("Max zoom bruh...")
            return

        if ev.delta() < 0 and vl >= len(self.linked_charts.chart._ohlc) + 666:
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
        last_bar = pg.Point(int(rbar))

        self._resetTarget()
        self.scaleBy(s, last_bar)
        ev.accept()
        self.sigRangeChangedManually.emit(mask)

    def mouseDragEvent(
        self,
        ev,
        axis: Optional[int] = None,
    ) -> None:
        #  if axis is specified, event will only affect that axis.
        ev.accept()  # we accept all buttons

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
        if ev.button() & (QtCore.Qt.LeftButton | QtCore.Qt.MidButton):

            # zoom only y-axis when click-n-drag on it
            if axis == 1:
                # set a static y range special value on chart widget to
                # prevent sizing to data in view.
                self._chart._static_yrange = 'axis'

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
                tr = self.childGroup.transform()
                tr = fn.invertQTransform(tr)
                tr = tr.map(dif*mask) - tr.map(Point(0, 0))

                x = tr.x() if mask[0] == 1 else None
                y = tr.y() if mask[1] == 1 else None

                self._resetTarget()
                if x is not None or y is not None:
                    self.translateBy(x=x, y=y)
                self.sigRangeChangedManually.emit(self.state['mouseEnabled'])

        elif ev.button() & QtCore.Qt.RightButton:

            # print "vb.rightDrag"
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

    def keyReleaseEvent(self, ev):
        # print(f'release: {ev.text().encode()}')
        ev.accept()
        if ev.key() == QtCore.Qt.Key_Shift:
            if self.state['mouseMode'] == ViewBox.RectMode:
                self.setMouseMode(ViewBox.PanMode)

    def keyPressEvent(self, ev):
        """
        This routine should capture key presses in the current view box.
        """
        # print(ev.text().encode())
        ev.accept()

        if ev.modifiers() == QtCore.Qt.ShiftModifier:
            if self.state['mouseMode'] == ViewBox.PanMode:
                self.setMouseMode(ViewBox.RectMode)

        # ctl
        if ev.modifiers() == QtCore.Qt.ControlModifier:
            # print("CTRL")
            # TODO: ctrl-c as cancel?
            # https://forum.qt.io/topic/532/how-to-catch-ctrl-c-on-a-widget/9
            # if ev.text() == 'c':
            #     self.rbScaleBox.hide()
            pass

        # alt
        if ev.modifiers() == QtCore.Qt.AltModifier:
            pass
            # print("ALT")

        # esc
        if ev.key() == QtCore.Qt.Key_Escape:
            self.select_box.clear()

        if ev.text() == 'r':
            self.chart.default_view()

        # Leaving this for light reference purposes

        # Key presses are used only when mouse mode is RectMode
        # The following events are implemented:
        # ctrl-A : zooms out to the default "full" view of the plot
        # ctrl-+ : moves forward in the zooming stack (if it exists)
        # ctrl-- : moves backward in the zooming stack (if it exists)

        #     self.scaleHistory(-1)
        # elif ev.text() in ['+', '=']:
        #     self.scaleHistory(1)
        # elif ev.key() == QtCore.Qt.Key_Backspace:
        #     self.scaleHistory(len(self.axHistory))
        else:
            ev.ignore()
