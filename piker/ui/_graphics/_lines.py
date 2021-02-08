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
Lines for orders, alerts, L2.

"""
from typing import Tuple, Dict, Any, Optional

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QPointF

from .._style import (
    hcolor,
    _down_2_font_inches_we_like,
    # _font,
    # DpiAwareFont
)
from .._axes import YAxisLabel


class LevelLabel(YAxisLabel):
    """Y-axis oriented label that sticks to where it's placed despite
    chart resizing and supports displaying multiple fields.

    """
    _w_margin = 4
    _h_margin = 3

    # adjustment "further away from" parent axis
    _x_offset = 0

    # fields to be displayed
    # class fields:
    level: float = 0.0
    digits: int = 2
    size: float = 2.0
    size_digits: int = int(2.0)

    def __init__(
        self,
        chart,
        *args,
        color: str = 'bracket',
        orient_v: str = 'bottom',
        orient_h: str = 'left',
        **kwargs
    ) -> None:
        super().__init__(
            chart,
            *args,
            use_arrow=False,
            **kwargs
        )

        # TODO: this is kinda cludgy
        self._hcolor = None
        self.color = color

        # orientation around axis options
        self._orient_v = orient_v
        self._orient_h = orient_h
        self._v_shift = {
            'top': 1.,
            'bottom': 0,
            'middle': 1 / 2.
        }[orient_v]

        self._h_shift = {
            'left': -1., 'right': 0
        }[orient_h]

        self._fmt_fields: Dict[str, Dict[str, Any]] = {}
        self._use_extra_fields: bool = False

    @property
    def color(self):
        return self._hcolor

    @color.setter
    def color(self, color: str) -> None:
        self._hcolor = color
        self._pen = self.pen = pg.mkPen(hcolor(color))

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        level: float,  # data for text
        offset: int = 1  # if have margins, k?
    ) -> None:

        # write contents, type specific
        h, w = self.set_label_str(level)

        # this triggers ``.paint()`` implicitly or no?
        self.setPos(QPointF(
            self._h_shift * w - self._x_offset,
            abs_pos.y() - (self._v_shift * h) - offset
        ))
        # trigger .paint()
        self.update()

        self.level = level

    def set_label_str(self, level: float):
        # use space as e3 delim
        label_str = (f'{level:,.{self.digits}f}   ').replace(',', ' ')

        # XXX: not huge on this approach but we need a more formal
        # way to define "label fields" that i don't have the brain space
        # for atm.. it's at least a **lot** better then the wacky
        # internals of InfLinelabel or wtv.

        # mutate label to contain any extra defined format fields
        if self._use_extra_fields:
            for fmt_str, fields in self._fmt_fields.items():
                label_str = fmt_str.format(
                    **{f: getattr(self, f) for f in fields}) + label_str

        self.label_str = label_str

        br = self.boundingRect()
        h, w = br.height(), br.width()
        return h, w

    def size_hint(self) -> Tuple[None, None]:
        return None, None

    def draw(
        self,
        p: QtGui.QPainter,
        rect: QtCore.QRectF
    ) -> None:
        p.setPen(self._pen)

        if self._orient_v == 'bottom':
            lp, rp = rect.topLeft(), rect.topRight()
            # p.drawLine(rect.topLeft(), rect.topRight())

        elif self._orient_v == 'top':
            lp, rp = rect.bottomLeft(), rect.bottomRight()

        p.drawLine(lp.x(), lp.y(), rp.x(), rp.y())

    def highlight(self, pen) -> None:
        self._pen = pen
        self.update()

    def unhighlight(self):
        self._pen = self.pen
        self.update()

    # def view_size(self):
    #     """Widgth and height of this label in view box coordinates.

    #     """
    #     return self.height()
    #     self._chart.mapFromView(QPointF(index, value)),


# global for now but probably should be
# attached to chart instance?
_max_l1_line_len: float = 0


class L1Label(LevelLabel):

    size: float = 0
    size_digits: int = 3

    text_flags = (
        QtCore.Qt.TextDontClip
        | QtCore.Qt.AlignLeft
    )

    def set_label_str(self, level: float) -> None:
        """Reimplement the label string write to include the level's order-queue's
        size in the text, eg. 100 x 323.3.

        """
        h, w = super().set_label_str(level)

        # Set a global "max L1 label length" so we can look it up
        # on order lines and adjust their labels not to overlap with it.
        global _max_l1_line_len
        _max_l1_line_len = max(_max_l1_line_len, w)

        return h, w


class L1Labels:
    """Level 1 bid ask labels for dynamic update on price-axis.

    """
    max_value: float = '100.0 x 100 000.00'

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa
        digits: int = 2,
        size_digits: int = 3,
        font_size_inches: float = _down_2_font_inches_we_like,
    ) -> None:

        self.chart = chart

        self.bid_label = L1Label(
            chart=chart,
            parent=chart.getAxis('right'),
            opacity=1,
            font_size_inches=font_size_inches,
            bg_color='papas_special',
            fg_color='bracket',
            orient_v='bottom',
        )
        self.bid_label.size_digits = size_digits
        self.bid_label.digits = digits
        # self.bid_label._size_br_from_str(self.max_value)

        self.ask_label = L1Label(
            chart=chart,
            parent=chart.getAxis('right'),
            opacity=1,
            font_size_inches=font_size_inches,
            bg_color='papas_special',
            fg_color='bracket',
            orient_v='top',
        )
        self.ask_label.size_digits = size_digits
        self.ask_label.digits = digits
        # self.ask_label._size_br_from_str(self.max_value)

        self.bid_label._use_extra_fields = True
        self.ask_label._use_extra_fields = True

        self.bid_label._fmt_fields['{size:.{size_digits}f} x '] = {
            'size', 'size_digits'}
        self.ask_label._fmt_fields['{size:.{size_digits}f} x '] = {
            'size', 'size_digits'}


# TODO: probably worth investigating if we can
# make .boundingRect() faster:
# https://stackoverflow.com/questions/26156486/determine-bounding-rect-of-line-in-qt
class LevelLine(pg.InfiniteLine):

    # TODO: fill in these slots for orders
    # .sigPositionChangeFinished.emit(self)

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # type: ignore # noqa
        label: LevelLabel,
        color: str = 'default',
        highlight_color: str = 'default_light',
        hl_on_hover: bool = True,
        dotted: bool = False,
        adjust_to_l1: bool = False,
        always_show_label: bool = False,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.label = label

        self.sigPositionChanged.connect(self.set_level)

        self._chart = chart
        self._hoh = hl_on_hover
        self._dotted = dotted

        self._hcolor = None
        self.color = color

        # TODO: for when we want to move groups of lines?
        self._track_cursor: bool = False
        self._adjust_to_l1 = adjust_to_l1
        self._always_show_label = always_show_label

        # testing markers
        # self.addMarker('<|', 0.1, 3)
        # self.addMarker('<|>', 0.2, 3)
        # self.addMarker('>|', 0.3, 3)
        # self.addMarker('>|<', 0.4, 3)
        # self.addMarker('>|<', 0.5, 3)
        # self.addMarker('^', 0.6, 3)
        # self.addMarker('v', 0.7, 3)
        # self.addMarker('o', 0.8, 3)

    @property
    def color(self):
        return self._hcolor

    @color.setter
    def color(self, color: str) -> None:
        # set pens to new color
        self._hcolor = color
        pen = pg.mkPen(hcolor(color))
        hoverpen = pg.mkPen(hcolor(color + '_light'))

        if self._dotted:
            pen.setStyle(QtCore.Qt.DashLine)
            hoverpen.setStyle(QtCore.Qt.DashLine)

        # set regular pen
        self.setPen(pen)

        # use slightly thicker highlight for hover pen
        hoverpen.setWidth(2)
        self.hoverPen = hoverpen

    def set_level(self) -> None:

        label = self.label

        # TODO: a better way to accomplish this...
        if self._adjust_to_l1:
            label._x_offset = _max_l1_line_len

        label.update_from_data(0, self.value())

    def on_tracked_source(
        self,
        x: int,
        y: float
    ) -> None:
        # XXX: this is called by our ``Cursor`` type once this
        # line is set to track the cursor: for every movement
        # this callback is invoked to reposition the line
        self.movable = True
        self.setPos(y)  # implictly calls ``.set_level()``
        self.update()

    def setMouseHover(self, hover: bool) -> None:
        """Mouse hover callback.

        """
        # XXX: currently we'll just return if _hoh is False
        if self.mouseHovering == hover:
            return

        self.mouseHovering = hover

        chart = self._chart

        if hover:
            # highlight if so configured
            if self._hoh:
                self.currentPen = self.hoverPen
                self.label.highlight(self.hoverPen)

            # add us to cursor state
            chart._cursor.add_hovered(self)

            self.label.show()
            # TODO: hide y-crosshair?
            # chart._cursor.graphics[chart]['hl'].hide()

            # self.setCursor(QtCore.Qt.OpenHandCursor)
            # self.setCursor(QtCore.Qt.DragMoveCursor)
        else:
            self.currentPen = self.pen
            self.label.unhighlight()

            chart._cursor._hovered.remove(self)

            if not self._always_show_label:
                self.label.hide()

        # highlight any attached label

        self.update()

    def mouseDragEvent(self, ev):
        chart = self._chart
        # hide y-crosshair
        chart._cursor.graphics[chart]['hl'].hide()

        # highlight
        self.currentPen = self.hoverPen
        self.label.highlight(self.hoverPen)

        # normal tracking behavior
        super().mouseDragEvent(ev)

        # This is the final position in the drag
        if ev.isFinish():
            # show y-crosshair again
            chart = self._chart
            chart._cursor.graphics[chart]['hl'].show()

    def mouseDoubleClickEvent(
        self,
        ev: QtGui.QMouseEvent,
    ) -> None:
        print(f'double click {ev}')

    def delete(self) -> None:
        """Remove this line from containing chart/view/scene.

        """
        scene = self.scene()
        if scene:
            # self.label.parent.scene().removeItem(self.label)
            scene.removeItem(self.label)

        self._chart.plotItem.removeItem(self)

    def getEndpoints(self):
        """Get line endpoints at view edges.

        Stolen from InfLineLabel.

        """
        # calculate points where line intersects view box
        # (in line coordinates)
        lr = self.boundingRect()
        pt1 = pg.Point(lr.left(), 0)
        pt2 = pg.Point(lr.right(), 0)

        return pt1, pt2


def level_line(
    chart: 'ChartPlogWidget',  # noqa
    level: float,
    digits: int = 1,
    color: str = 'default',

    # size 4 font on 4k screen scaled down, so small-ish.
    font_size_inches: float = _down_2_font_inches_we_like,

    # whether or not the line placed in view should highlight
    # when moused over (aka "hovered")
    hl_on_hover: bool = True,

    # line style
    dotted: bool = False,

    adjust_to_l1: bool = False,

    always_show_label: bool = False,

    **linelabelkwargs
) -> LevelLine:
    """Convenience routine to add a styled horizontal line to a plot.

    """
    label = LevelLabel(
        chart=chart,
        parent=chart.getAxis('right'),
        # TODO: pass this from symbol data
        digits=digits,
        opacity=0.616,
        font_size_inches=font_size_inches,
        color=color,

        # TODO: make this take the view's bg pen
        bg_color='papas_special',
        fg_color=color,
        **linelabelkwargs
    )
    label.update_from_data(0, level)

    # by default, the label must be shown by client code
    label.hide()

    # TODO: can we somehow figure out a max value from the parent axis?
    label._size_br_from_str(label.label_str)

    line = LevelLine(
        chart,
        label,

        color=color,
        # lookup "highlight" equivalent
        highlight_color=color + '_light',

        movable=True,
        angle=0,

        dotted=dotted,

        # UX related options

        hl_on_hover=hl_on_hover,

        # makes order line labels offset from their parent axis
        # such that they don't collide with the L1/L2 lines/prices
        # that are displayed on the axis
        adjust_to_l1=adjust_to_l1,

        # when set to True the label is always shown instead of just on
        # highlight (which is a privacy thing for orders)
        always_show_label=always_show_label,
    )

    # activate/draw label
    line.setValue(level)  # it's just .setPos() right?
    line.set_level()

    chart.plotItem.addItem(line)

    return line


def order_line(
    *args,
    size: Optional[int] = None,
    size_digits: int = 0,
    **kwargs,
) -> LevelLine:
    """Convenience routine to add a line graphic representing an order execution
    submitted to the EMS via the chart's "order mode".

    """
    line = level_line(*args, adjust_to_l1=True, **kwargs)
    line.label._fmt_fields['{size:.{size_digits}f} x '] = {
        'size', 'size_digits'}

    if size is not None:

        line.label._use_extra_fields = True
        line.label.size = size
        line.label.size_digits = size_digits

    return line
