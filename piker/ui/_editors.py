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
    Sequence,
    TYPE_CHECKING,
)

import pyqtgraph as pg
from pyqtgraph import (
    ViewBox,
    Point,
    QtCore,
    QtWidgets,
)
from PyQt5.QtCore import (
    QPointF,
    QRectF,
)
from PyQt5.QtGui import (
    QColor,
    QTransform,
)
from PyQt5.QtWidgets import (
    QGraphicsProxyWidget,
    QGraphicsScene,
    QLabel,
)

from pyqtgraph import functions as fn
import numpy as np

from piker.types import Struct
from ._style import (
    hcolor,
    _font,
)
from ._lines import LevelLine
from ..log import get_logger

if TYPE_CHECKING:
    from ._chart import (
        GodWidget,
        ChartPlotWidget,
    )
    from ._interaction import ChartView


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
        color: str = 'default',
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


def as_point(
    pair: Sequence[float, float] | QPointF,
) -> list[QPointF, QPointF]:
    '''
    Case any input tuple of floats to a a list of `QPoint` objects
    for use in Qt geometry routines.

    '''
    if isinstance(pair, QPointF):
        return pair

    return QPointF(pair[0], pair[1])


# TODO: maybe implement better, something something RectItemProxy??
# -[ ] dig into details of how proxy's work?
#    https://doc.qt.io/qt-5/qgraphicsscene.html#addWidget
# -[ ] consider using `.addRect()` maybe?

class SelectRect(QtWidgets.QGraphicsRectItem):
    '''
    A data-view "selection rectangle": the most fundamental
    geometry for annotating data views.

    - https://doc.qt.io/qt-5/qgraphicsrectitem.html
    - https://doc.qt.io/qt-6/qgraphicsrectitem.html

    '''
    def __init__(
        self,
        viewbox: ViewBox,
        color: str | None = None,
    ) -> None:
        super().__init__(0, 0, 1, 1)

        # self.rbScaleBox = QGraphicsRectItem(0, 0, 1, 1)
        self.vb: ViewBox = viewbox

        self._chart: ChartPlotWidget | None = None  # noqa

        # TODO: maybe allow this to be dynamic via a method?
        #l override selection box color
        color: str = color or 'dad_blue'
        color = QColor(hcolor(color))

        self.setPen(fn.mkPen(color, width=1))
        color.setAlpha(66)
        self.setBrush(fn.mkBrush(color))
        self.setZValue(1e9)

        label = self._label = QLabel()
        label.setTextFormat(0)  # markdown
        label.setFont(_font.font)
        label.setMargin(0)
        label.setAlignment(
            QtCore.Qt.AlignLeft
            # | QtCore.Qt.AlignVCenter
        )
        label.hide()  # always right after init

        # proxy is created after containing scene is initialized
        self._label_proxy: QGraphicsProxyWidget | None = None
        self._abs_top_right: Point | None = None

        # TODO: "swing %" might be handy here (data's max/min
        # # % change)?
        self._contents: list[str] = [
            'change: {pchng:.2f} %',
            'range: {rng:.2f}',
            'bars: {nbars}',
            'max: {dmx}',
            'min: {dmn}',
            # 'time: {nbars}m',  # TODO: compute this per bar size
            'sigma: {std:.2f}',
        ]

        self.add_to_view(viewbox)
        self.hide()

    def add_to_view(
        self,
        view: ChartView,
    ) -> None:
        '''
        Self-defined view hookup impl which will
        also re-assign the internal ref.

        '''
        view.addItem(
            self,
            ignoreBounds=True,
        )
        if self.vb is not view:
            self.vb = view

    @property
    def chart(self) -> ChartPlotWidget:  # noqa
        return self._chart

    @chart.setter
    def chart(self, chart: ChartPlotWidget) -> None:  # noqa
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

    def set_scen_pos(
        self,
        scen_p1: QPointF,
        scen_p2: QPointF,

        update_label: bool = True,

    ) -> None:
        '''
        Set position from scene coords of selection rect (normally
        from mouse position) and accompanying label, move label to
        match.

        '''
        # NOTE XXX: apparently just setting it doesn't work!?
        # i have no idea why but it's pretty weird we have to do
        # this transform thing which was basically pulled verbatim
        # from the `pg.ViewBox.updateScaleBox()` method.
        view_rect: QRectF = self.vb.childGroup.mapRectFromScene(
            QRectF(
                scen_p1, 
                scen_p2,
            )
        )
        self.setPos(view_rect.topLeft())
        # XXX: does not work..!?!?
        # https://doc.qt.io/qt-5/qgraphicsrectitem.html#setRect
        # self.setRect(view_rect)

        tr = QTransform.fromScale(
            view_rect.width(),
            view_rect.height(),
        )
        self.setTransform(tr)

        # XXX: never got this working, was always offset
        # / transformed completely wrong (and off to the far right
        # from the cursor?)
        # self.set_view_pos(
        #     view_rect=view_rect,
        #     # self.vwqpToView(p1),
        #     # self.vb.mapToView(p2),
        #     # start_pos=self.vb.mapToScene(p1),
        #     # end_pos=self.vb.mapToScene(p2),
        # )
        self.show()

        if update_label:
            self.init_label(view_rect)

    def set_view_pos(
        self,

        start_pos: QPointF | Sequence[float, float] | None = None,
        end_pos: QPointF | Sequence[float, float] | None = None,
        view_rect: QRectF | None = None,

        update_label: bool = True,

    ) -> None:
        '''
        Set position from `ViewBox` coords (i.e. from the actual
        data domain) of rect (and any accompanying label which is
        moved to match).

        '''
        if self._chart is None:
            raise RuntimeError(
                'You MUST assign a `SelectRect.chart: ChartPlotWidget`!'
            )

        if view_rect is None:
            # ensure point casting
            start_pos: QPointF = as_point(start_pos)
            end_pos: QPointF = as_point(end_pos)

            # map to view coords and update area
            view_rect = QtCore.QRectF(
                start_pos,
                end_pos,
            )

        self.setPos(view_rect.topLeft())

        # NOTE: SERIOUSLY NO IDEA WHY THIS WORKS...
        # but it does and all the other commented stuff above
        # dint, dawg..

        # self.resetTransform()
        # self.setRect(view_rect)

        tr = QTransform.fromScale(
            view_rect.width(),
            view_rect.height(),
        )
        self.setTransform(tr)

        if update_label:
            self.init_label(view_rect)

        print(
            'SelectRect modify:\n'
            f'QRectF: {view_rect}\n'
            f'start_pos: {start_pos}\n'
            f'end_pos: {end_pos}\n'
        )
        self.show()

    def init_label(
        self,
        view_rect: QRectF,
   ) -> QLabel:

        # should be init-ed in `.__init__()`
        label: QLabel = self._label
        cv: ChartView = self.vb

        # https://doc.qt.io/qt-5/qgraphicsproxywidget.html
        if self._label_proxy is None:
            scen: QGraphicsScene = cv.scene()
            # NOTE: specifically this is passing a widget
            # pointer to the scene's `.addWidget()` as per,
            # https://doc.qt.io/qt-5/qgraphicsproxywidget.html#embedding-a-widget-with-qgraphicsproxywidget
            self._label_proxy: QGraphicsProxyWidget = scen.addWidget(label)

        # get label startup coords
        tl: QPointF = view_rect.topLeft()
        br: QPointF = view_rect.bottomRight()

        x1, y1 = tl.x(), tl.y()
        x2, y2 = br.x(), br.y()

        # TODO: to remove, previous label corner point unpacking
        # x1, y1 = start_pos.x(), start_pos.y()
        # x2, y2 = end_pos.x(), end_pos.y()
        # y1, y2 = start_pos.y(), end_pos.y()
        # x1, x2 = start_pos.x(), end_pos.x()

        # TODO: heh, could probably use a max-min streamin algo
        # here too?
        _, xmn = min(y1, y2), min(x1, x2)
        ymx, xmx = max(y1, y2), max(x1, x2)

        pchng = (y2 - y1) / y1 * 100
        rng = abs(y1 - y2)

        ixmn, ixmx = round(xmn), round(xmx)
        nbars = ixmx - ixmn + 1

        chart: ChartPlotWidget = self._chart
        data: np.ndarray = chart.get_viz(
            chart.name
        ).shm.array[ixmn:ixmx]

        if len(data):
            std: float = data['close'].std()
            dmx: float = data['high'].max()
            dmn: float = data['low'].min()
        else:
            dmn = dmx = std = np.nan

        # update label info
        label.setText('\n'.join(self._contents).format(
            pchng=pchng,
            rng=rng,
            nbars=nbars,
            std=std,
            dmx=dmx,
            dmn=dmn,
        ))

        # print(f'x2, y2: {(x2, y2)}')
        # print(f'xmn, ymn: {(xmn, ymx)}')

        label_anchor = Point(
            xmx + 2,
            ymx,
        )

        # XXX: in the drag bottom-right -> top-left case we don't
        # want the label to overlay the box.
        # if (x2, y2) == (xmn, ymx):
        #     # could do this too but needs to be added after coords transform
        #     # label_anchor = Point(x2, y2 + self._label.height())
        #     label_anchor = Point(xmn, ymn)

        self._abs_top_right: Point = label_anchor
        self._label_proxy.setPos(
            cv.mapFromView(label_anchor)
        )
        label.show()

    def hide(self):
        '''
        Clear the selection box from its graphics scene but
        don't delete it permanently.

        '''
        super().hide()
        self._label.hide()

    # TODO: ensure noone else using dis.
    clear = hide

    def delete(self) -> None:
        '''
        De-allocate this rect from its rendering graphics scene.

        Like a permanent hide.

        '''
        scen: QGraphicsScene = self.scene()
        if scen is None:
            return

        scen.removeItem(self)
        if (
            self._label
            and
            self._label_proxy

        ):
            scen.removeItem(self._label_proxy)
