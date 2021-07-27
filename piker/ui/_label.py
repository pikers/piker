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
Non-shitty labels that don't re-invent the wheel.

"""
from inspect import isfunction
from typing import Callable, Optional

import pyqtgraph as pg
from PyQt5 import QtGui, QtWidgets
from PyQt5.QtCore import QPointF, QRectF

from ._style import (
    DpiAwareFont,
    hcolor,
)


class Label:
    """
    A plain ol' "scene label" using an underlying ``QGraphicsTextItem``.

    After hacking for many days on multiple "label" systems inside
    ``pyqtgraph`` yet again we're left writing our own since it seems
    all of those are over complicated, ad-hoc, transform-mangling,
    messes which can't accomplish the simplest things via their inputs
    (such as pinning to the left hand side of a view box).

    Here we do the simple thing where the label uses callables to figure
    out the (x, y) coordinate "pin point": nice and simple.

    This type is another effort (see our graphics) to start making
    small, re-usable label components that can actually be used to build
    production grade UIs...

    """

    def __init__(

        self,
        view: pg.ViewBox,
        fmt_str: str,

        color: str = 'default_light',
        x_offset: float = 0,
        font_size: str = 'small',
        opacity: float = 1,
        fields: dict = {},
        update_on_range_change: bool = True,

    ) -> None:

        vb = self.vb = view
        self._fmt_str = fmt_str
        self._view_xy = QPointF(0, 0)

        self.scene_anchor: Optional[Callable[..., QPointF]] = None

        self._x_offset = x_offset

        txt = self.txt = QtWidgets.QGraphicsTextItem()
        vb.scene().addItem(txt)

        # configure font size based on DPI
        dpi_font = DpiAwareFont(
            font_size=font_size,
        )
        dpi_font.configure_to_dpi()
        txt.setFont(dpi_font.font)

        txt.setOpacity(opacity)

        # register viewbox callbacks
        if update_on_range_change:
            vb.sigRangeChanged.connect(self.on_sigrange_change)

        self._hcolor: str = ''
        self.color = color

        self.fields = fields
        self.orient_v = 'bottom'

        self._anchor_func = self.txt.pos().x

        # not sure if this makes a diff
        self.txt.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # TODO: edit and selection support
        # https://doc.qt.io/qt-5/qt.html#TextInteractionFlag-enum
        # self.setTextInteractionFlags(QtGui.Qt.TextEditorInteraction)

    @property
    def color(self):
        return self._hcolor

    @color.setter
    def color(self, color: str) -> None:
        self.txt.setDefaultTextColor(pg.mkColor(hcolor(color)))
        self._hcolor = color

    def update(self) -> None:
        '''Update this label either by invoking its
        user defined anchoring function, or by positioning
        to the last recorded data view coordinates.

        '''
        # move label in scene coords to desired position
        anchor = self.scene_anchor
        if anchor:
            self.txt.setPos(anchor())

        else:
            # position based on last computed view coordinate
            self.set_view_pos(self._view_xy.y())

    def on_sigrange_change(self, vr, r) -> None:
        return self.update()

    @property
    def w(self) -> float:
        return self.txt.boundingRect().width()

    def scene_br(self) -> QRectF:
        txt = self.txt
        return txt.mapToScene(
            txt.boundingRect()
        ).boundingRect()

    @property
    def h(self) -> float:
        return self.txt.boundingRect().height()

    def vbr(self) -> QRectF:
        return self.vb.boundingRect()

    def set_x_anchor_func(
        self,
        func: Callable,
    ) -> None:
        assert isinstance(func(), float)
        self._anchor_func = func

    def set_view_pos(
        self,

        y: float,
        x: Optional[float] = None,

    ) -> None:

        if x is None:
            scene_x = self._anchor_func() or self.txt.pos().x()
            x = self.vb.mapToView(QPointF(scene_x, scene_x)).x()

        # get new (inside the) view coordinates / position
        self._view_xy = QPointF(x, y)

        # map back to the outer UI-land "scene" coordinates
        s_xy = self.vb.mapFromView(self._view_xy)

        if self.orient_v == 'top':
            s_xy = QPointF(s_xy.x(), s_xy.y() - self.h)

        # move label in scene coords to desired position
        self.txt.setPos(s_xy)

        assert s_xy == self.txt.pos()

    @property
    def fmt_str(self) -> str:
        return self._fmt_str

    @fmt_str.setter
    def fmt_str(self, fmt_str: str) -> None:
        self._fmt_str = fmt_str

    def format(
        self,
        **fields: dict

    ) -> str:

        out = {}

        # this is hacky support for single depth
        # calcs of field data from field data
        # ex. to calculate a $value = price * size
        for k, v in fields.items():

            if isfunction(v):
                out[k] = v(fields)

            else:
                out[k] = v

        text = self._fmt_str.format(**out)

        # for large numbers with a thousands place
        text = text.replace(',', ' ')

        self.txt.setPlainText(text)

    def render(self) -> None:
        self.format(**self.fields)

    def show(self) -> None:
        self.txt.show()

    def hide(self) -> None:
        self.txt.hide()

    def delete(self) -> None:
        self.vb.scene().removeItem(self.txt)
