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
from typing import Tuple, Optional, List

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QPointF

from .._label import Label, vbr_left, right_axis
from .._style import (
    hcolor,
    _down_2_font_inches_we_like,
)
from .._axes import YAxisLabel


class LevelLabel(YAxisLabel):
    """Y-axis (vertically) oriented, horizontal label that sticks to
    where it's placed despite chart resizing and supports displaying
    multiple fields.

    """
    _x_margin = 0
    _y_margin = 0

    # adjustment "further away from" anchor point
    _x_offset = 9
    _y_offset = 0

    # fields to be displayed in the label string
    _fields = {
        'level': 0,
        'level_digits': 2,
    }
    # default label template is just a y-level with so much precision
    _fmt_str = '{level:,.{level_digits}f}'

    def __init__(
        self,
        chart,
        parent,

        color: str = 'bracket',

        orient_v: str = 'bottom',
        orient_h: str = 'left',

        opacity: float = 0,

        # makes order line labels offset from their parent axis
        # such that they don't collide with the L1/L2 lines/prices
        # that are displayed on the axis
        adjust_to_l1: bool = False,

        **axis_label_kwargs,
    ) -> None:

        super().__init__(
            chart,
            parent=parent,
            use_arrow=False,
            opacity=opacity,
            **axis_label_kwargs
        )

        # TODO: this is kinda cludgy
        self._hcolor: pg.Pen = None
        self.color: str = color

        # orientation around axis options
        self._orient_v = orient_v
        self._orient_h = orient_h

        self._adjust_to_l1 = adjust_to_l1

        self._v_shift = {
            'top': -1.,
            'bottom': 0.,
            'middle': 1 / 2.
        }[orient_v]

        self._h_shift = {
            'left': -1.,
            'right': 0.
        }[orient_h]

        self.fields = self._fields.copy()
        # ensure default format fields are in correct
        self.set_fmt_str(self._fmt_str, self.fields)

    @property
    def color(self):
        return self._hcolor

    @color.setter
    def color(self, color: str) -> None:
        self._hcolor = color
        self._pen = self.pen = pg.mkPen(hcolor(color))

    def update_on_resize(self, vr, r):
        """Tiis is a ``.sigRangeChanged()`` handler.

        """
        self.update_fields(self.fields)

    def update_fields(
        self,
        fields: dict = None,
    ) -> None:
        """Update the label's text contents **and** position from
        a view box coordinate datum.

        """
        self.fields.update(fields)
        level = self.fields['level']

        # map "level" to local coords
        abs_xy = self._chart.mapFromView(QPointF(0, level))

        self.update_label(
            abs_xy,
            self.fields,
        )

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        fields: dict,
    ) -> None:

        # write contents, type specific
        h, w = self.set_label_str(fields)

        if self._adjust_to_l1:
            self._x_offset = _max_l1_line_len

        self.setPos(QPointF(
            self._h_shift * (w + self._x_offset),
            abs_pos.y() + self._v_shift * h
        ))

    def set_fmt_str(
        self,
        fmt_str: str,
        fields: dict,
    ) -> (str, str):
        # test that new fmt str can be rendered
        self._fmt_str = fmt_str
        self.set_label_str(fields)
        self.fields.update(fields)
        return fmt_str, self.label_str

    def set_label_str(
        self,
        fields: dict,
    ):
        # use space as e3 delim
        self.label_str = self._fmt_str.format(**fields).replace(',', ' ')

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

        rect = self.rect

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


# global for now but probably should be
# attached to chart instance?
_max_l1_line_len: float = 0


class L1Label(LevelLabel):

    text_flags = (
        QtCore.Qt.TextDontClip
        | QtCore.Qt.AlignLeft
    )

    def set_label_str(
        self,
        fields: dict,
    ) -> None:
        """Make sure the max L1 line module var is kept up to date.

        """
        h, w = super().set_label_str(fields)

        # Set a global "max L1 label length" so we can look it up
        # on order lines and adjust their labels not to overlap with it.
        global _max_l1_line_len
        _max_l1_line_len = max(_max_l1_line_len, w)

        return h, w


class L1Labels:
    """Level 1 bid ask labels for dynamic update on price-axis.

    """
    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa
        digits: int = 2,
        size_digits: int = 3,
        font_size_inches: float = _down_2_font_inches_we_like,
    ) -> None:

        self.chart = chart

        raxis = chart.getAxis('right')
        kwargs = {
            'chart': chart,
            'parent': raxis,

            'opacity': 1,
            'font_size_inches': font_size_inches,
            'fg_color': chart.pen_color,
            'bg_color': chart.view_color,
        }

        fmt_str = (
            '{size:.{size_digits}f} x '
            '{level:,.{level_digits}f}'
        )
        fields = {
            'level': 0,
            'level_digits': digits,
            'size': 0,
            'size_digits': size_digits,
        }

        bid = self.bid_label = L1Label(
            orient_v='bottom',
            **kwargs,
        )
        bid.set_fmt_str(fmt_str=fmt_str, fields=fields)
        bid.show()

        ask = self.ask_label = L1Label(
            orient_v='top',
            **kwargs,
        )
        ask.set_fmt_str(fmt_str=fmt_str, fields=fields)
        ask.show()


# TODO: probably worth investigating if we can
# make .boundingRect() faster:
# https://stackoverflow.com/questions/26156486/determine-bounding-rect-of-line-in-qt
class LevelLine(pg.InfiniteLine):

    # TODO: fill in these slots for orders
    # .sigPositionChangeFinished.emit(self)

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # type: ignore # noqa

        color: str = 'default',
        highlight_color: str = 'default_light',

        hl_on_hover: bool = True,
        dotted: bool = False,
        always_show_labels: bool = False,

    ) -> None:

        super().__init__(
            movable=True,
            angle=0,
            label=None,  # don't use the shitty ``InfLineLabel``
        )

        self._chart = chart
        self._hoh = hl_on_hover
        self._dotted = dotted
        self._hcolor: str = None

        # the float y-value in the view coords
        self.level: float = 0

        # list of labels anchored at one of the 2 line endpoints
        # inside the viewbox
        self._labels: List[(int, Label)] = []

        # whenever this line is moved trigger label updates
        self.sigPositionChanged.connect(self.on_pos_change)

        # sets color to value triggering pen creation
        self.color = color

        # TODO: for when we want to move groups of lines?
        self._track_cursor: bool = False
        self._always_show_labels = always_show_labels

        # # indexed by int
        # self._endpoints = (None, None)

        # testing markers
        # self.addMarker('<|', 0.1, 3)
        # self.addMarker('<|>', 0.2, 3)
        # self.addMarker('>|', 0.3, 3)
        # self.addMarker('>|<', 0.4, 3)
        # self.addMarker('>|<', 0.5, 3)
        # self.addMarker('^', 0.6, 3)
        # self.addMarker('v', 0.7, 3)
        # self.addMarker('o', 0.8, 3)

    def txt_offsets(self) -> Tuple[int, int]:
        return 0, 0

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

    def add_label(
        self,

        # by default we only display the line's level value
        # in the label
        fmt_str: str = (
            '{level:,.{level_digits}f}'
        ),
        side: str = 'right',

        font_size_inches: float = _down_2_font_inches_we_like,
        color: str = None,
        bg_color: str = None,

        **label_kwargs,
    ) -> LevelLabel:
        """Add a ``LevelLabel`` anchored at one of the line endpoints in view.

        """
        vb = self.getViewBox()

        label = Label(
            view=vb,
            fmt_str=fmt_str,
            color=self.color,
        )

        if side == 'right':
            label.set_x_anchor_func(right_axis(self._chart, label))
        elif side == 'left':
            label.set_x_anchor_func(vbr_left(label))

        self._labels.append((side, label))

        return label

    def on_pos_change(
        self,
        line: 'LevelLine',  # noqa
    ) -> None:
        """Position changed handler.

        """
        self.update_labels({'level': self.value()})

    def update_labels(
        self,
        fields_data: dict,
    ) -> None:

        for at, label in self._labels:
            label.color = self.color

            label.fields.update(fields_data)

            level = fields_data.get('level')
            if level:
                label.set_view_y(level)

            label.render()

        self.update()

    def hide_labels(self) -> None:
        for at, label in self._labels:
            label.hide()

    def show_labels(self) -> None:
        for at, label in self._labels:
            label.show()

    def set_level(
        self,
        level: float,
    ) -> None:
        self.setPos(level)
        self.level = self.value()
        self.update()

    def on_tracked_source(
        self,
        x: int,
        y: float
    ) -> None:
        # XXX: this is called by our ``Cursor`` type once this
        # line is set to track the cursor: for every movement
        # this callback is invoked to reposition the line
        self.movable = True
        self.set_level(y)  # implictly calls reposition handler

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

                # for at, label in self._labels:
                #     label.highlight(self.hoverPen)

            # add us to cursor state
            cur = chart._cursor
            cur.add_hovered(self)
            cur.graphics[chart]['yl'].hide()
            cur.graphics[chart]['hl'].hide()

            for at, label in self._labels:
                label.show()

            # TODO: hide y-crosshair?
            # chart._cursor.graphics[chart]['hl'].hide()

            # self.setCursor(QtCore.Qt.OpenHandCursor)
            # self.setCursor(QtCore.Qt.DragMoveCursor)
        else:
            self.currentPen = self.pen

            cur = chart._cursor
            cur._hovered.remove(self)
            if self not in cur._trackers:
                g = cur.graphics[chart]
                g['yl'].show()
                g['hl'].show()

            if not self._always_show_labels:
                for at, label in self._labels:
                    label.hide()
                    # label.unhighlight()

        # highlight any attached label

        self.update()

    def mouseDragEvent(self, ev):
        chart = self._chart

        # hide y-crosshair
        graphics = chart._cursor.graphics[chart]
        graphics['hl'].hide()
        graphics['yl'].hide()

        # highlight
        self.currentPen = self.hoverPen
        # self.label.highlight(self.hoverPen)
        for at, label in self._labels:
            # label.highlight(self.hoverPen)
            label.show()

        # normal tracking behavior
        super().mouseDragEvent(ev)

        # This is the final position in the drag
        if ev.isFinish():
            # show y-crosshair again
            graphics['hl'].show()
            graphics['yl'].show()

    def delete(self) -> None:
        """Remove this line from containing chart/view/scene.

        """
        scene = self.scene()
        if scene:
            # self.label.parent.scene().removeItem(self.label)
            for at, label in self._labels:
                label.delete()

            self._labels.clear()

        self._chart.plotItem.removeItem(self)

    def mouseDoubleClickEvent(
        self,
        ev: QtGui.QMouseEvent,
    ) -> None:

        # TODO: enter labels edit mode
        print(f'double click {ev}')


def level_line(
    chart: 'ChartPlogWidget',  # noqa
    level: float,

    color: str = 'default',

    # size 4 font on 4k screen scaled down, so small-ish.
    font_size_inches: float = _down_2_font_inches_we_like,

    # whether or not the line placed in view should highlight
    # when moused over (aka "hovered")
    hl_on_hover: bool = True,

    # line style
    dotted: bool = False,

    # label fields and options
    digits: int = 1,

    always_show_labels: bool = False,

    add_label: bool = True,

    orient_v: str = 'bottom',

) -> LevelLine:
    """Convenience routine to add a styled horizontal line to a plot.

    """

    line = LevelLine(
        chart,
        color=color,
        # lookup "highlight" equivalent
        highlight_color=color + '_light',

        dotted=dotted,

        # UX related options
        hl_on_hover=hl_on_hover,

        # when set to True the label is always shown instead of just on
        # highlight (which is a privacy thing for orders)
        always_show_labels=always_show_labels,
    )

    chart.plotItem.addItem(line)

    if add_label:

        label = line.add_label(
            side='right',
            opacity=1,
        )
        label.orient_v = orient_v

        line.update_labels({'level': level, 'level_digits': 2})
        label.render()

        line.hide_labels()

    # activate/draw label
    line.set_level(level)

    return line


def order_line(
    chart,
    level: float,
    level_digits: float,

    size: Optional[int] = 1,
    size_digits: int = 0,

    submit_price: float = None,

    order_status: str = 'dark',
    order_type: str = 'limit',

    opacity=0.616,

    orient_v: str = 'bottom',

    **line_kwargs,
) -> LevelLine:
    """Convenience routine to add a line graphic representing an order
    execution submitted to the EMS via the chart's "order mode".

    """
    line = level_line(
        chart,
        level,
        add_label=False,
        **line_kwargs
    )

    llabel = line.add_label(
        side='left',
        fmt_str='{order_status}-{order_type}:{submit_price}',
    )
    llabel.fields = {
        'order_status': order_status,
        'order_type': order_type,
        'submit_price': submit_price,
    }
    llabel.orient_v = orient_v
    llabel.render()
    llabel.show()

    rlabel = line.add_label(
        side='right',
        fmt_str=(
            '{size:.{size_digits}f} x '
            '{level:,.{level_digits}f}'
        ),
    )
    rlabel.fields = {
        'size': size,
        'size_digits': size_digits,
        'level': level,
        'level_digits': level_digits,
    }

    rlabel.orient_v = orient_v
    rlabel.render()
    rlabel.show()

    # sanity check
    line.update_labels({'level': level})

    return line
