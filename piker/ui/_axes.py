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
Chart axes graphics and behavior.
"""

from typing import List, Tuple, Optional

import pandas as pd
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QPointF

from ._style import DpiAwareFont, hcolor, _font
from ..data._source import float_digits

_axis_pen = pg.mkPen(hcolor('bracket'))


class Axis(pg.AxisItem):
    """A better axis that sizes to typical tick contents considering font size.

    """
    def __init__(
        self,
        linked_charts,
        typical_max_str: str = '100 000.000',
        min_tick: int = 2,
        **kwargs
    ) -> None:

        super().__init__(**kwargs)

        # XXX: pretty sure this makes things slower
        # self.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

        self.linked_charts = linked_charts
        self._min_tick = min_tick

        self.setTickFont(_font.font)
        self.setStyle(**{
            'textFillLimits': [(0, 0.616)],
            'tickFont': _font.font,
            # offset of text *away from* axis line in px
            'tickTextOffset': 6,
        })

        self.setTickFont(_font.font)
        self.setPen(_axis_pen)
        self.typical_br = _font._qfm.boundingRect(typical_max_str)

        # size the pertinent axis dimension to a "typical value"
        self.resize()

    def set_min_tick(self, size: int) -> None:
        self._min_tick = size


class PriceAxis(Axis):

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, orientation='right', **kwargs)
        self.setStyle(**{
            # offset of text *away from* axis line in px
            'tickTextOffset': 9,
        })

    def resize(self) -> None:
        self.setWidth(self.typical_br.width())

    # XXX: drop for now since it just eats up h space

    def tickStrings(self, vals, scale, spacing):

        # TODO: figure out how to enforce min tick spacing by passing
        # it into the parent type
        digits = max(float_digits(spacing * scale), self._min_tick)

        # print(f'vals: {vals}\nscale: {scale}\nspacing: {spacing}')
        # print(f'digits: {digits}')

        return [
            ('{value:,.{digits}f}').format(
                digits=digits,
                value=v,
            ).replace(',', ' ') for v in vals
        ]


class DynamicDateAxis(Axis):

    # time formats mapped by seconds between bars
    tick_tpl = {
        60 * 60 * 24: '%Y-%b-%d',
        60: '%H:%M',
        30: '%H:%M:%S',
        5: '%H:%M:%S',
        1: '%H:%M:%S',
    }

    def resize(self) -> None:
        self.setHeight(self.typical_br.height() + 1)

    def _indexes_to_timestrs(
        self,
        indexes: List[int],
    ) -> List[str]:

        # try:
        chart = self.linked_charts.chart
        bars = chart._ohlc
        shm = self.linked_charts.chart._shm
        first = shm._first.value

        bars_len = len(bars)
        times = bars['time']

        epochs = times[list(
            map(
                int,
                filter(
                    lambda i: i > 0 and i < bars_len,
                    (i-first for i in indexes)
                )
            )
        )]

        # TODO: **don't** have this hard coded shift to EST
        dts = pd.to_datetime(epochs, unit='s')  # - 4*pd.offsets.Hour()

        delay = times[-1] - times[-2]
        return dts.strftime(self.tick_tpl[delay])

    def tickStrings(self, values: List[float], scale, spacing):
        return self._indexes_to_timestrs(values)


class AxisLabel(pg.GraphicsObject):

    _x_margin = 0
    _y_margin = 0

    def __init__(
        self,
        parent: Axis,
        digits: int = 2,
        bg_color: str = 'bracket',
        fg_color: str = 'black',
        opacity: int = 0,
        use_arrow: bool = True,
        font_size_inches: Optional[float] = None,
    ) -> None:

        super().__init__(parent)
        self.setFlag(self.ItemIgnoresTransformations)

        # XXX: pretty sure this is faster
        self.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

        self.parent = parent
        self.opacity = opacity
        self.label_str = ''
        self.digits = digits

        self._txt_br: QtCore.QRect = None

        self._dpifont = DpiAwareFont(size_in_inches=font_size_inches)
        self._dpifont.configure_to_dpi()

        self.bg_color = pg.mkColor(hcolor(bg_color))
        self.fg_color = pg.mkColor(hcolor(fg_color))

        self._use_arrow = use_arrow

        # create triangle path
        self.path = None
        self.rect = None

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget
    ) -> None:
        # p.setCompositionMode(QtGui.QPainter.CompositionMode_SourceOver)

        if self.label_str:

            # if not self.rect:
            self._size_br_from_str(self.label_str)

            p.setFont(self._dpifont.font)
            p.setPen(self.fg_color)
            p.setOpacity(self.opacity)

            # can be overrided in subtype
            self.draw(p, self.rect)

            p.drawText(self.rect, self.text_flags, self.label_str)

    def draw(
        self,
        p: QtGui.QPainter,
        rect: QtCore.QRectF
    ) -> None:
        # this adds a nice black outline around the label for some odd
        # reason; ok by us
        # p.setOpacity(self.opacity)
        p.drawRect(self.rect)

        if self._use_arrow:

            if not self.path:
                self._draw_arrow_path()

            p.drawPath(self.path)
            p.fillPath(self.path, pg.mkBrush(self.bg_color))

        p.fillRect(self.rect, self.bg_color)


    def boundingRect(self):  # noqa
        if self.label_str:
            self._size_br_from_str(self.label_str)

            # if self.path:
            #     self.tl = self.path.controlPointRect().topLeft()
            if not self.path:
                self.tl = self.rect.topLeft()

            return QtCore.QRectF(
                self.tl,
                self.rect.bottomRight(),
            )

        return QtCore.QRectF()

        # return self.rect or QtCore.QRectF()

    def _size_br_from_str(self, value: str) -> None:
        """Do our best to render the bounding rect to a set margin
        around provided string contents.

        """
        # size the filled rect to text and/or parent axis
        # if not self._txt_br:
        #     # XXX: this can't be c
        #     self._txt_br = self._dpifont.boundingRect(value)

        txt_br = self._txt_br = self._dpifont.boundingRect(value)
        txt_h, txt_w = txt_br.height(), txt_br.width()
        h, w = self.size_hint()

        self.rect = QtCore.QRectF(
            0, 0,
            (w or txt_w) + self._x_margin/2,
            (h or txt_h) + self._y_margin/2,
        )
        # print(self.rect)
        # hb = self.path.controlPointRect()
        # hb_size = hb.size()

        return self.rect

# _common_text_flags = (
#     QtCore.Qt.TextDontClip |
#     QtCore.Qt.AlignCenter |
#     QtCore.Qt.AlignTop |
#     QtCore.Qt.AlignHCenter |
#     QtCore.Qt.AlignVCenter
# )


class XAxisLabel(AxisLabel):
    _x_margin = 8

    text_flags = (
        QtCore.Qt.TextDontClip
        | QtCore.Qt.AlignCenter
    )

    def size_hint(self) -> Tuple[float, float]:
        # size to parent axis height
        return self.parent.height(), None

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        value: float,  # data for text
        offset: int = 0  # if have margins, k?
    ) -> None:

        timestrs = self.parent._indexes_to_timestrs([int(value)])

        if not timestrs.any():
            return

        pad = 1*' '
        self.label_str = pad + timestrs[0] + pad

        y_offset = self.parent.style['tickTextOffset'][1]

        w = self.boundingRect().width()
        self.setPos(QPointF(
            abs_pos.x() - w/2,
            y_offset/2,
        ))
        self.update()

    def _draw_arrow_path(self):
        y_offset = self.parent.style['tickTextOffset'][1]
        path = QtGui.QPainterPath()
        h, w = self.rect.height(), self.rect.width()
        # middle = (w + self._y_margin)/2
        # middle = (w + self._x_margin)/2
        middle = w/2 - 0.5
        # aw = (h + self._x_margin)/2
        aw = h/2
        left = middle - aw
        right = middle + aw
        path.moveTo(left, 0)
        path.lineTo(middle, -y_offset)
        path.lineTo(right, 0)
        path.closeSubpath()
        self.path = path
        self.tl = QtCore.QPointF(0, -y_offset)


class YAxisLabel(AxisLabel):
    _y_margin = 4

    text_flags = (
        QtCore.Qt.AlignLeft
        # QtCore.Qt.AlignHCenter
        | QtCore.Qt.AlignVCenter
        | QtCore.Qt.TextDontClip
    )

    def __init__(
        self,
        chart,
        *args,
        **kwargs
    ) -> None:

        super().__init__(*args, **kwargs)

        self._chart = chart
        chart.sigRangeChanged.connect(self.update_on_resize)
        self._last_datum = (None, None)

        # pull text offset from axis from parent axis
        self.x_offset = self.parent.style['tickTextOffset'][0]

    def size_hint(self) -> Tuple[float, float]:
        # size to parent axis width
        return None, self.parent.width()

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        value: float,  # data for text

        # on odd dimension and/or adds nice black line
        x_offset: Optional[int] = None
    ) -> None:

        # this is read inside ``.paint()``
        self.label_str = '{value:,.{digits}f}'.format(
            digits=self.digits, value=value).replace(',', ' ')

        # pull text offset from axis from parent axis
        x_offset = x_offset or self.x_offset

        br = self.boundingRect()
        h = br.height()
        self.setPos(QPointF(
            x_offset,
            abs_pos.y() - h / 2 - self._y_margin / 2
        ))
        self.update()

    def update_on_resize(self, vr, r):
        """Tiis is a ``.sigRangeChanged()`` handler.

        """
        index, last = self._last_datum
        if index is not None:
            self.update_from_data(index, last)

    def update_from_data(
        self,
        index: int,
        value: float,
    ) -> None:
        self._last_datum = (index, value)
        self.update_label(
            self._chart.mapFromView(QPointF(index, value)),
            value
        )

    def _draw_arrow_path(self):
        x_offset = self.parent.style['tickTextOffset'][0]
        path = QtGui.QPainterPath()
        h = self.rect.height()
        path.moveTo(0, 0)
        path.lineTo(-x_offset - 2, h/2.)
        path.lineTo(0, h)
        path.closeSubpath()
        self.path = path
        self.tl = path.controlPointRect().topLeft()
