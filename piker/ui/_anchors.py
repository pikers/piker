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

'''
Anchor funtions for UI placement of annotions.

'''
from __future__ import annotations
from typing import Callable, TYPE_CHECKING

from PyQt5.QtCore import QPointF
from PyQt5.QtWidgets import QGraphicsPathItem

if TYPE_CHECKING:
    from ._chart import ChartPlotWidget
    from ._label import Label


def vbr_left(
    label: Label,

) -> Callable[..., float]:
    '''
    Return a closure which gives the scene x-coordinate for the leftmost
    point of the containing view box.

    '''
    return label.vbr().left


def right_axis(

    chart: ChartPlotWidget,  # noqa
    label: Label,

    side: str = 'left',
    offset: float = 10,
    avoid_book: bool = True,
    # width: float = None,

) -> Callable[..., float]:
    '''Return a position closure which gives the scene x-coordinate for
    the x point on the right y-axis minus the width of the label given
    it's contents.

    '''
    ryaxis = chart.getAxis('right')

    if side == 'left':

        if avoid_book:
            def right_axis_offset_by_w() -> float:

                # l1 spread graphics x-size
                l1_len = chart._max_l1_line_len

                # sum of all distances "from" the y-axis
                right_offset = l1_len + label.w + offset

                return ryaxis.pos().x() - right_offset

        else:
            def right_axis_offset_by_w() -> float:

                return ryaxis.pos().x() - (label.w + offset)

        return right_axis_offset_by_w

    elif 'right':

        # axis_offset = ryaxis.style['tickTextOffset'][0]

        def on_axis() -> float:

            return ryaxis.pos().x()  # + axis_offset - 2

        return on_axis


def gpath_pin(

    gpath: QGraphicsPathItem,
    label: Label,  # noqa

    location_description: str = 'right-of-path-centered',
    use_right_of_pp_label: bool = False,

) -> QPointF:

    # get actual arrow graphics path
    path_br = gpath.mapToScene(gpath.path()).boundingRect()

    # label.vb.locate(label.txt)  #, children=True)

    if location_description == 'right-of-path-centered':
        return path_br.topRight() - QPointF(label.h/16, label.h / 3)

    if location_description == 'left-of-path-centered':
        return path_br.topLeft() - QPointF(label.w, label.h / 6)

    elif location_description == 'below-path-left-aligned':
        return path_br.bottomLeft() - QPointF(0, label.h / 6)

    elif location_description == 'below-path-right-aligned':
        return path_br.bottomRight() - QPointF(label.w, label.h / 6)


def pp_tight_and_right(
    label: Label

) -> QPointF:
    '''
    Place *just* right of the pp label.

    '''
    # txt = label.txt
    return label.txt.pos() + QPointF(label.w - label.h/3, 0)
