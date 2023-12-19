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
Higher level annotation editors.

"""
from __future__ import annotations
from collections import defaultdict
from typing import (
    TYPE_CHECKING
)

import pyqtgraph as pg
from pyqtgraph import (
    ViewBox,
    Point,
    QtCore,
    QtWidgets,
)
from PyQt5.QtGui import (
    QColor,
)
from PyQt5.QtWidgets import (
    QLabel,
)

from pyqtgraph import functions as fn
from PyQt5.QtCore import QPointF
import numpy as np

from piker.types import Struct
from ._style import hcolor, _font
from ._lines import LevelLine
from ..log import get_logger

if TYPE_CHECKING:
    from ._chart import GodWidget


log = get_logger(__name__)


class ArrowEditor(Struct):

    godw: GodWidget = None  # type: ignore # noqa
    _arrows: dict[str, list[pg.ArrowItem]] = {}

    def add(
        self,
        plot: pg.PlotItem,
        uid: str,
        x: float,
        y: float,
        color='default',
        pointing: str | None = None,

    ) -> pg.ArrowItem:
        '''
        Add an arrow graphic to view at given (x, y).

        '''
        angle = {
            'up': 90,
            'down': -90,
            None: 180,  # pointing to right (as in an alert)
        }[pointing]

        # scale arrow sizing to dpi-aware font
        size = _font.font.pixelSize() * 0.8

        arrow = pg.ArrowItem(
            angle=angle,
            baseAngle=0,
            headLen=size,
            headWidth=size/2,
            tailLen=None,
            pxMode=True,

            # coloring
            pen=pg.mkPen(hcolor('papas_special')),
            brush=pg.mkBrush(hcolor(color)),
        )
        arrow.setPos(x, y)
        self._arrows.setdefault(uid, []).append(arrow)

        # render to view
        plot.addItem(arrow)

        return arrow

    def remove(self, arrow) -> bool:
        for linked in self.godw.iter_linked():
            linked.chart.plotItem.removeItem(arrow)


class LineEditor(Struct):
    '''
    The great editor of linez.

    '''
    godw: GodWidget = None  # type: ignore # noqa
    _order_lines: defaultdict[str, LevelLine] = defaultdict(list)
    _active_staged_line: LevelLine = None

    def stage_line(
        self,
        line: LevelLine,

    ) -> LevelLine:
        '''
        Stage a line at the current chart's cursor position
        and return it.

        '''
        # add a "staged" cursor-tracking line to view
        # and cash it in a a var
        if self._active_staged_line:
            self.unstage_line()

        self._active_staged_line = line

        return line

    def unstage_line(self) -> LevelLine:
        '''
        Inverse of ``.stage_line()``.

        '''
        cursor = self.godw.get_cursor()
        if not cursor:
            return None

        # delete "staged" cursor tracking line from view
        line = self._active_staged_line
        if line:
            try:
                cursor._trackers.remove(line)
            except KeyError:
                # when the current cursor doesn't have said line
                # registered (probably means that user held order mode
                # key while panning to another view) then we just
                # ignore the remove error.
                pass
            line.delete()

        self._active_staged_line = None

        # show the crosshair y line and label
        cursor.show_xhair()

    def submit_lines(
        self,
        lines: list[LevelLine],
        uuid: str,

    ) -> LevelLine:

        # staged_line = self._active_staged_line
        # if not staged_line:
        #     raise RuntimeError("No line is currently staged!?")

        # for now, until submission reponse arrives
        for line in lines:
            line.hide_labels()

        # register for later lookup/deletion
        self._order_lines[uuid] += lines

        return lines

    def commit_line(self, uuid: str) -> list[LevelLine]:
        '''
        Commit a "staged line" to view.

        Submits the line graphic under the cursor as a (new) permanent
        graphic in view.

        '''
        lines = self._order_lines[uuid]
        if lines:
            for line in lines:
                line.show_labels()
                line.hide_markers()
                log.debug(f'Level active for level: {line.value()}')
                # TODO: other flashy things to indicate the order is active

        return lines

    def lines_under_cursor(self) -> list[LevelLine]:
        '''
        Get the line(s) under the cursor position.

        '''
        # Delete any hoverable under the cursor
        return self.godw.get_cursor()._hovered

    def all_lines(self) -> list[LevelLine]:
        all_lines = []
        for lines in list(self._order_lines.values()):
            all_lines.extend(lines)

        return all_lines

    def remove_line(
        self,
        line: LevelLine = None,
        uuid: str = None,

    ) -> LevelLine | None:
        '''Remove a line by refernce or uuid.

        If no lines or ids are provided remove all lines under the
        cursor position.

        '''
        # try to look up line from our registry
        lines = self._order_lines.pop(uuid, None)
        if lines:
            cursor = self.godw.get_cursor()
            if cursor:
                for line in lines:
                    # if hovered remove from cursor set
                    hovered = cursor._hovered
                    if line in hovered:
                        hovered.remove(line)

                    log.debug(f'deleting {line} with oid: {uuid}')
                    line.delete()

                    # make sure the xhair doesn't get left off
                    # just because we never got a un-hover event
                    cursor.show_xhair()

        else:
            log.warning(f'Could not find line for {line}')

        return lines


class SelectRect(QtWidgets.QGraphicsRectItem):

    def __init__(
        self,
        viewbox: ViewBox,
        color: str = 'dad_blue',
    ) -> None:
        super().__init__(0, 0, 1, 1)

        # self.rbScaleBox = QGraphicsRectItem(0, 0, 1, 1)
        self.vb = viewbox
        self._chart: 'ChartPlotWidget' = None  # noqa

        # override selection box color
        color = QColor(hcolor(color))
        self.setPen(fn.mkPen(color, width=1))
        color.setAlpha(66)
        self.setBrush(fn.mkBrush(color))
        self.setZValue(1e9)
        self.hide()
        self._label = None

        label = self._label = QLabel()
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
            # QColor(chart.backgroundBrush()),
            QColor(hcolor('papas_special')),
        )

    def update_on_resize(self, vr, r):
        '''
        Re-position measure label on view range change.

        '''
        if self._abs_top_right:
            self._label_proxy.setPos(
                self.vb.mapFromView(self._abs_top_right)
            )

    def set_pos(
        self,
        p1: QPointF,
        p2: QPointF
    ) -> None:
        """Set position of selection rect and accompanying label, move
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
        self.setRect(r)
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

        chart = self._chart
        data = chart.get_viz(chart.name).shm.array[ixmn:ixmx]

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
        # self._label.show()

    def clear(self):
        """Clear the selection box from view.

        """
        self._label.hide()
        self.hide()
