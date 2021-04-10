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
Double auction top-of-book (L1) graphics.

"""
from typing import Tuple

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QPointF

from ._axes import YAxisLabel
from ._style import hcolor


class LevelLabel(YAxisLabel):
    """Y-axis (vertically) oriented, horizontal label that sticks to
    where it's placed despite chart resizing and supports displaying
    multiple fields.


    TODO: replace the rectangle-text part with our new ``Label`` type.

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
    _fmt_str = '{level:,.{level_digits}f} '

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
            self._x_offset = self._chart._max_l1_line_len

        self.setPos(QPointF(
            self._h_shift * (w + self._x_offset),
            abs_pos.y() + self._v_shift * h
        ))
        # XXX: definitely need this!
        self.update()

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

        # Set a global "max L1 label length" so we can
        # look it up on order lines and adjust their
        # labels not to overlap with it.
        chart = self._chart
        chart._max_l1_line_len: float = max(
            chart._max_l1_line_len,
            w
        )

        return h, w


class L1Labels:
    """Level 1 bid ask labels for dynamic update on price-axis.

    """
    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa
        digits: int = 2,
        size_digits: int = 3,
        font_size: str = 'small',
    ) -> None:

        self.chart = chart

        raxis = chart.getAxis('right')
        kwargs = {
            'chart': chart,
            'parent': raxis,

            'opacity': 1,
            'font_size': font_size,
            'fg_color': chart.pen_color,
            'bg_color': chart.view_color,
        }

        fmt_str = (
            ' {size:.{size_digits}f} x '
            '{level:,.{level_digits}f} '
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
