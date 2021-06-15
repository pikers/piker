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
from math import floor

import pandas as pd
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QPointF

from ._style import DpiAwareFont, hcolor, _font
from ..data._source import float_digits

_axis_pen = pg.mkPen(hcolor('bracket'))


class Axis(pg.AxisItem):
    """A better axis that sizes tick contents considering font size.

    """
    def __init__(
        self,
        linkedsplits,
        typical_max_str: str = '100 000.000',
        min_tick: int = 2,
        **kwargs
    ) -> None:

        super().__init__(**kwargs)

        # XXX: pretty sure this makes things slower
        # self.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

        self.linkedsplits = linkedsplits
        self._min_tick = min_tick
        self._dpi_font = _font

        self.setTickFont(_font.font)
        font_size = self._dpi_font.font.pixelSize()

        if self.orientation in ('bottom',):
            text_offset = floor(0.25 * font_size)

        elif self.orientation in ('left', 'right'):
            text_offset = floor(font_size / 2)

        self.setStyle(**{
            'textFillLimits': [(0, 0.5)],
            'tickFont': self._dpi_font.font,

            # offset of text *away from* axis line in px
            # use approx. half the font pixel size (height)
            'tickTextOffset': text_offset,
        })

        self.setTickFont(_font.font)
        self.setPen(_axis_pen)
        self.typical_br = _font._qfm.boundingRect(typical_max_str)

        # size the pertinent axis dimension to a "typical value"
        self.size_to_values()

    def size_to_values(self) -> None:
        pass

    def set_min_tick(self, size: int) -> None:
        self._min_tick = size

    def txt_offsets(self) -> Tuple[int, int]:
        return tuple(self.style['tickTextOffset'])


class PriceAxis(Axis):

    def size_to_values(self) -> None:
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

    def size_to_values(self) -> None:
        self.setHeight(self.typical_br.height() + 1)

    def _indexes_to_timestrs(
        self,
        indexes: List[int],
    ) -> List[str]:

        # try:
        chart = self.linkedsplits.chart
        bars = chart._ohlc
        shm = self.linkedsplits.chart._shm
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
        parent: pg.GraphicsItem,
        digits: int = 2,

        bg_color: str = 'bracket',
        fg_color: str = 'black',
        opacity: int = 1,  # XXX: seriously don't set this to 0
        font_size: str = 'default',

        use_arrow: bool = True,

    ) -> None:

        super().__init__()
        self.setParentItem(parent)

        self.setFlag(self.ItemIgnoresTransformations)

        # XXX: pretty sure this is faster
        self.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

        self._parent = parent

        self.opacity = opacity
        self.label_str = ''
        self.digits = digits

        self._txt_br: QtCore.QRect = None

        self._dpifont = DpiAwareFont(font_size=font_size)
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
        """Draw a filled rectangle based on the size of ``.label_str`` text.

        Subtypes can customize further by overloading ``.draw()``.

        """
        # p.setCompositionMode(QtGui.QPainter.CompositionMode_SourceOver)

        if self.label_str:

            # if not self.rect:
            self._size_br_from_str(self.label_str)

            # can be overrided in subtype
            self.draw(p, self.rect)

            p.setFont(self._dpifont.font)
            p.setPen(self.fg_color)
            p.drawText(self.rect, self.text_flags, self.label_str)


    def draw(
        self,
        p: QtGui.QPainter,
        rect: QtCore.QRectF
    ) -> None:

        if self._use_arrow:
            if not self.path:
                self._draw_arrow_path()

            p.drawPath(self.path)
            p.fillPath(self.path, pg.mkBrush(self.bg_color))

        # this adds a nice black outline around the label for some odd
        # reason; ok by us
        p.setOpacity(self.opacity)

        # this cause the L1 labels to glitch out if used 
        # in the subtype and it will leave a small black strip
        # with the arrow path if done before the above
        p.fillRect(self.rect, self.bg_color)


    def boundingRect(self):  # noqa
        """Size the graphics space from the text contents.

        """
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

        # allow subtypes to specify a static width and height
        h, w = self.size_hint()

        self.rect = QtCore.QRectF(
            0, 0,
            (w or txt_w) + self._x_margin /2,
            (h or txt_h) + self._y_margin /2,
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
        return self._parent.height(), None

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        value: float,  # data for text
        offset: int = 0  # if have margins, k?
    ) -> None:

        timestrs = self._parent._indexes_to_timestrs([int(value)])

        if not timestrs.any():
            return

        pad = 1*' '
        self.label_str = pad + timestrs[0] + pad

        _, y_offset = self._parent.txt_offsets()

        w = self.boundingRect().width()

        self.setPos(QPointF(
            abs_pos.x() - w/2,
            y_offset/2,
        ))
        self.update()

    def _draw_arrow_path(self):
        y_offset = self._parent.style['tickTextOffset'][1]
        path = QtGui.QPainterPath()
        h, w = self.rect.height(), self.rect.width()
        middle = w/2 - 0.5
        aw = h/2
        left = middle - aw
        right = middle + aw
        path.moveTo(left, 0)
        path.lineTo(middle, -y_offset)
        path.lineTo(right, 0)
        path.closeSubpath()
        self.path = path

        # top left point is local origin and tip of the arrow path
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
        if getattr(self._parent, 'txt_offsets', False):
            self.x_offset, y_offset = self._parent.txt_offsets()

    def size_hint(self) -> Tuple[float, float]:
        # size to parent axis width
        return None, self._parent.width()

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
        _save_last: bool = True,
    ) -> None:
        """Update the label's text contents **and** position from
        a view box coordinate datum.

        """
        if _save_last:
            self._last_datum = (index, value)

        self.update_label(
            self._chart.mapFromView(QPointF(index, value)),
            value
        )

    def _draw_arrow_path(self):
        x_offset = self._parent.style['tickTextOffset'][0]
        path = QtGui.QPainterPath()
        h = self.rect.height()
        path.moveTo(0, 0)
        path.lineTo(-x_offset - h/4, h/2.)
        path.lineTo(0, h)
        path.closeSubpath()
        self.path = path
        self.tl = path.controlPointRect().topLeft()
