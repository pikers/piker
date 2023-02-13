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
from __future__ import annotations
from functools import lru_cache
from typing import Optional, Callable
from math import floor

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QPointF

from . import _pg_overrides as pgo
from ..data._source import float_digits
from ._label import Label
from ._style import DpiAwareFont, hcolor, _font
from ._interaction import ChartView

_axis_pen = pg.mkPen(hcolor('bracket'))


class Axis(pg.AxisItem):
    '''
    A better axis that sizes tick contents considering font size.

    Also includes tick values lru caching originally proposed in but never
    accepted upstream:
    https://github.com/pyqtgraph/pyqtgraph/pull/2160

    '''
    def __init__(
        self,
        plotitem: pgo.PlotItem,
        typical_max_str: str = '100 000.000  ',
        text_color: str = 'bracket',
        lru_cache_tick_strings: bool = True,
        **kwargs

    ) -> None:
        super().__init__(
            # textPen=textPen,
            **kwargs
        )

        # XXX: pretty sure this makes things slower
        # self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        self.pi = plotitem
        self._dpi_font = _font

        self.setTickFont(_font.font)
        font_size = self._dpi_font.font.pixelSize()

        style_conf = {
            'textFillLimits': [(0, 0.5)],
            'tickFont': self._dpi_font.font,

        }
        text_offset = None
        if self.orientation in ('bottom',):
            text_offset = floor(0.25 * font_size)

        elif self.orientation in ('left', 'right'):
            text_offset = floor(font_size / 2)

        if text_offset:
            style_conf.update({
                # offset of text *away from* axis line in px
                # use approx. half the font pixel size (height)
                'tickTextOffset': text_offset,
            })

        self.setStyle(**style_conf)
        self.setTickFont(_font.font)

        # NOTE: this is for surrounding "border"
        self.setPen(_axis_pen)

        # this is the text color
        self.text_color = text_color

        # generate a bounding rect based on sizing to a "typical"
        # maximum length-ed string defined as init default.
        self.typical_br = _font._qfm.boundingRect(typical_max_str)

        # size the pertinent axis dimension to a "typical value"
        self.size_to_values()

        # NOTE: requires override ``.tickValues()`` method seen below.
        if lru_cache_tick_strings:
            self.tickStrings = lru_cache(
                maxsize=2**20
            )(self.tickStrings)

        # axis "sticky" labels
        self._stickies: dict[str, YAxisLabel] = {}

    # NOTE: only overriden to cast tick values entries into tuples
    # for use with the lru caching.
    def tickValues(
        self,
        minVal: float,
        maxVal: float,
        size: int,

    ) -> list[tuple[float, tuple[str]]]:
        '''
        Repack tick values into tuples for lru caching.

        '''
        ticks = []
        for scalar, values in super().tickValues(minVal, maxVal, size):
            ticks.append((
                scalar,
                tuple(values),  # this
            ))

        return ticks

    @property
    def text_color(self) -> str:
        return self._text_color

    @text_color.setter
    def text_color(self, text_color: str) -> None:
        self.setTextPen(pg.mkPen(hcolor(text_color)))
        self._text_color = text_color

    def size_to_values(self) -> None:
        pass

    def txt_offsets(self) -> tuple[int, int]:
        return tuple(self.style['tickTextOffset'])

    def add_sticky(
        self,
        pi: pgo.PlotItem,
        name: None | str = None,
        digits: None | int = 2,
        bg_color='default',
        fg_color='black',

    ) -> YAxisLabel:

        # if the sticky is for our symbol
        # use the tick size precision for display
        name = name or pi.name
        digits = digits or 2

        # TODO: ``._ysticks`` should really be an attr on each
        # ``PlotItem`` now instead of the containing widget (because of
        # overlays) ?

        # add y-axis "last" value label
        sticky = self._stickies[name] = YAxisLabel(
            pi=pi,
            parent=self,
            digits=digits,  # TODO: pass this from symbol data
            opacity=0.9,  # slight see-through
            bg_color=bg_color,
            fg_color=fg_color,
        )

        pi.sigRangeChanged.connect(sticky.update_on_resize)
        return sticky


class PriceAxis(Axis):

    def __init__(
        self,
        *args,
        min_tick: int = 2,
        title: str = '',
        formatter: Optional[Callable[[float], str]] = None,
        **kwargs

    ) -> None:
        super().__init__(*args, **kwargs)
        self.formatter = formatter
        self._min_tick: int = min_tick
        self.title = None

    def set_title(
        self,
        title: str,
        view: Optional[ChartView] = None,
        color: Optional[str] = None,

    ) -> Label:
        '''
        Set a sane UX label using our built-in ``Label``.

        '''
        # XXX: built-in labels but they're huge, and placed weird..
        # self.setLabel(title)
        # self.showLabel()

        label = self.title = Label(
            view=view or self.linkedView(),
            fmt_str=title,
            color=color or self.text_color,
            parent=self,
            # update_on_range_change=False,
        )

        def below_axis() -> QPointF:
            return QPointF(
                0,
                self.size().height(),
            )

        # XXX: doesn't work? have to pass it above
        # label.txt.setParent(self)
        label.scene_anchor = below_axis
        label.render()
        label.show()
        label.update()
        return label

    def set_min_tick(
        self,
        size: int
    ) -> None:
        self._min_tick = size

    def size_to_values(self) -> None:
        self.setWidth(self.typical_br.width())

    # XXX: drop for now since it just eats up h space

    def tickStrings(
        self,
        vals: tuple[float],
        scale: float,
        spacing: float,

    ) -> list[str]:
        # TODO: figure out how to enforce min tick spacing by passing it
        # into the parent type
        digits = max(
            float_digits(spacing * scale),
            self._min_tick,
        )
        if self.title:
            self.title.update()

        # print(f'vals: {vals}\nscale: {scale}\nspacing: {spacing}')
        # print(f'digits: {digits}')

        if not self.formatter:
            return [
                ('{value:,.{digits}f}').format(
                    digits=digits,
                    value=v,
                ).replace(',', ' ') for v in vals
            ]
        else:
            return list(map(self.formatter, vals))


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
        indexes: list[int],

    ) -> list[str]:

        # XX: ARGGGGG AG:LKSKDJF:LKJSDFD
        chart = self.pi.chart_widget

        viz = chart._vizs[chart.name]
        shm = viz.shm
        array = shm.array
        times = array['time']
        i_0, i_l = times[0], times[-1]

        # edge cases
        if (
            not indexes
            or
            (indexes[0] < i_0
             and indexes[-1] < i_l)
            or
            (indexes[0] > i_0
             and indexes[-1] > i_l)
        ):
            return []

        if viz.index_field == 'index':
            arr_len = times.shape[0]
            first = shm._first.value
            epochs = times[
                list(
                    map(
                        int,
                        filter(
                            lambda i: i > 0 and i < arr_len,
                            (i - first for i in indexes)
                        )
                    )
                )
            ]
        else:
            epochs = list(map(int, indexes))

        # TODO: **don't** have this hard coded shift to EST
        # delay = times[-1] - times[-2]
        dts = np.array(
            epochs,
            dtype='datetime64[s]',
        )

        # see units listing:
        # https://numpy.org/devdocs/reference/arrays.datetime.html#datetime-units
        return list(np.datetime_as_string(dts))

        # TODO: per timeframe formatting?
        # - we probably need this based on zoom now right?
        # prec = self.np_dt_precision[delay]
        # return dts.strftime(self.tick_tpl[delay])

    def tickStrings(
        self,
        values: tuple[float],
        scale: float,
        spacing: float,

    ) -> list[str]:

        return self._indexes_to_timestrs(values)

        # NOTE: handy for debugging the lru cache
        # info = self.tickStrings.cache_info()
        # print(info)


class AxisLabel(pg.GraphicsObject):

    # relative offsets *OF* the bounding rect relative
    # to parent graphics object.
    # eg.  <parent>| => <_x_br_offset> => | <text> |
    _x_br_offset: float = 0
    _y_br_offset: float = 0

    # relative offsets of text *within* bounding rect
    # eg. | <_x_margin> => <text> |
    _x_margin: float = 0
    _y_margin: float = 0

    # multiplier of the text content's height in order
    # to force a larger (y-dimension) bounding rect.
    _y_txt_h_scaling: float = 1

    def __init__(
        self,
        parent: pg.GraphicsItem,
        digits: int = 2,

        bg_color: str = 'default',
        fg_color: str = 'black',
        opacity: int = .8,  # XXX: seriously don't set this to 0
        font_size: str = 'default',

        use_arrow: bool = True,

    ) -> None:

        super().__init__()
        self.setParentItem(parent)

        self.setFlag(self.ItemIgnoresTransformations)
        self.setZValue(100)

        # XXX: pretty sure this is faster
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

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

        self._pw = self.pixelWidth()

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget

    ) -> None:
        '''
        Draw a filled rectangle based on the size of ``.label_str`` text.

        Subtypes can customize further by overloading ``.draw()``.

        '''
        if self.label_str:

            # if not self.rect:
            self._size_br_from_str(self.label_str)

            # can be overrided in subtype
            self.draw(p, self.rect)

            p.setFont(self._dpifont.font)
            p.setPen(self.fg_color)
            p.drawText(
                self.rect,
                self.text_flags,
                self.label_str,
            )

    def draw(
        self,
        p: QtGui.QPainter,
        rect: QtCore.QRectF
    ) -> None:

        p.setOpacity(self.opacity)

        if self._use_arrow:
            if not self.path:
                self._draw_arrow_path()

            p.drawPath(self.path)
            p.fillPath(self.path, pg.mkBrush(self.bg_color))

        # this cause the L1 labels to glitch out if used in the subtype
        # and it will leave a small black strip with the arrow path if
        # done before the above
        p.fillRect(
            self.rect,
            self.bg_color,
        )

    def boundingRect(self):  # noqa
        '''
        Size the graphics space from the text contents.

        '''
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

    # TODO: but the input probably needs to be the "len" of
    # the current text value:
    @lru_cache
    def _size_br_from_str(
        self,
        value: str

    ) -> tuple[float, float]:
        '''
        Do our best to render the bounding rect to a set margin
        around provided string contents.

        '''
        # size the filled rect to text and/or parent axis
        # if not self._txt_br:
        #     # XXX: this can't be called until stuff is rendered?
        #     self._txt_br = self._dpifont.boundingRect(value)

        txt_br = self._txt_br = self._dpifont.boundingRect(value)
        txt_h, txt_w = txt_br.height(), txt_br.width()
        # print(f'wsw: {self._dpifont.boundingRect(" ")}')

        # allow subtypes to override width and height
        h, w = self.size_hint()

        self.rect = QtCore.QRectF(

            # relative bounds offsets
            self._x_br_offset,
            self._y_br_offset,

            (w or txt_w) + self._x_margin / 2,

            (h or txt_h) * self._y_txt_h_scaling + (self._y_margin / 2),
        )
        # print(self.rect)
        # hb = self.path.controlPointRect()
        # hb_size = hb.size()

        return (self.rect.width(), self.rect.height())

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

    def size_hint(self) -> tuple[float, float]:
        # size to parent axis height
        return self._parent.height(), None

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        value: float,  # data for text
        offset: int = 0  # if have margins, k?

    ) -> None:

        timestrs = self._parent._indexes_to_timestrs([int(value)])

        if not len(timestrs):
            return

        pad = 1*' '
        self.label_str = pad + str(timestrs[0]) + pad

        _, y_offset = self._parent.txt_offsets()

        w = self.boundingRect().width()

        self.setPos(
            QPointF(
                abs_pos.x() - w/2 - self._pw,
                y_offset/2,
            )
        )
        self.update()

    def _draw_arrow_path(self):
        y_offset = self._parent.style['tickTextOffset'][1]
        path = QtGui.QPainterPath()
        h, w = self.rect.height(), self.rect.width()
        middle = w/2 - self._pw * 0.5
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
    _y_margin: int = 4

    text_flags = (
        QtCore.Qt.AlignLeft
        # QtCore.Qt.AlignHCenter
        | QtCore.Qt.AlignVCenter
        | QtCore.Qt.TextDontClip
    )

    def __init__(
        self,
        pi: pgo.PlotItem,
        *args,
        **kwargs
    ) -> None:

        super().__init__(*args, **kwargs)

        self._pi = pi
        pi.sigRangeChanged.connect(self.update_on_resize)

        self._last_datum = (None, None)

        self.x_offset = 0
        # pull text offset from axis from parent axis
        if getattr(self._parent, 'txt_offsets', False):
            self.x_offset, y_offset = self._parent.txt_offsets()

    def size_hint(self) -> tuple[float, float]:
        # size to parent axis width(-ish)
        wsh = self._dpifont.boundingRect(' ').height() / 2
        return (
            None,
            self._parent.size().width() - wsh,
        )

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        value: float,  # data for text

        # on odd dimension and/or adds nice black line
        x_offset: int = 0,

    ) -> None:

        # this is read inside ``.paint()``
        self.label_str = '{value:,.{digits}f}'.format(
            digits=self.digits, value=value).replace(',', ' ')

        # pull text offset from axis from parent axis
        x_offset = x_offset or self.x_offset

        br = self.boundingRect()
        h = br.height()

        self.setPos(
            QPointF(
                x_offset,
                abs_pos.y() - h / 2 - self._pw,
            )
        )
        self.update()

    def update_on_resize(self, vr, r):
        '''
        This is a ``.sigRangeChanged()`` handler.

        '''
        index, last = self._last_datum
        if index is not None:
            self.update_from_data(index, last)

    def update_from_data(
        self,
        index: int,
        value: float,
        _save_last: bool = True,

    ) -> None:
        '''
        Update the label's text contents **and** position from
        a view box coordinate datum.

        '''
        if _save_last:
            self._last_datum = (index, value)

        self.update_label(
            self._pi.mapFromView(QPointF(index, value)),
            value
        )

    def _draw_arrow_path(self):
        x_offset = self._parent.style['tickTextOffset'][0]
        path = QtGui.QPainterPath()
        h = self.rect.height()
        path.moveTo(0, 0)
        path.lineTo(-x_offset - h/4, h/2. - self._pw/2)
        path.lineTo(0, h)
        path.closeSubpath()
        self.path = path
        self.tl = path.controlPointRect().topLeft()
