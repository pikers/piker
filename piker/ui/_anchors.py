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
from typing import Callable

from PyQt5.QtCore import QPointF
from PyQt5.QtGui import QGraphicsPathItem

from ._label import Label


def marker_right_points(

    chart: 'ChartPlotWidget',  # noqa
    marker_size: int = 20,

) -> (float, float, float):
    '''Return x-dimension, y-axis-aware, level-line marker oriented scene values.

    X values correspond to set the end of a level line, end of
    a paried level line marker, and the right most side of the "right"
    axis respectively.

    '''
    l1_len = chart._max_l1_line_len
    ryaxis = chart.getAxis('right')

    r_axis_x = ryaxis.pos().x()
    up_to_l1_sc = r_axis_x - l1_len

    marker_right = up_to_l1_sc - (1.375 * 2 * marker_size)
    line_end = marker_right - (6/16 * marker_size)

    return line_end, marker_right, r_axis_x


def vbr_left(
    label: Label,

) -> Callable[..., float]:
    """Return a closure which gives the scene x-coordinate for the
    leftmost point of the containing view box.

    """
    return label.vbr().left


def right_axis(

    chart: 'ChartPlotWidget',  # noqa
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


def update_pp_nav(

    chartview: 'ChartView',  # noqa
    line: 'LevelLine',  # noqa

) -> None:
    '''Show a pp off-screen indicator for a level label.

    This is like in fps games where you have a gps "nav" indicator
    but your teammate is outside the range of view, except in 2D, on
    the y-dimension.

    '''
    vr = chartview.state['viewRange']
    ymn, ymx = vr[1]
    level = line.value()

    marker = line._marker
    label = marker.label

    _, marker_right, _ = marker_right_points(line._chart)

    if level > ymx:  # pin to top of view
        marker.setPos(
            QPointF(
                marker_right,
                marker._height/3,
            )
        )

    elif level < ymn:  # pin to bottom of view

        marker.setPos(
            QPointF(
                marker_right,
                chartview.height() - 4/3*marker._height,
            )
        )

    else:
        # pp line is viewable so show marker normally
        marker.update()

    # re-anchor label (i.e. trigger call of ``arrow_tr()`` from above
    label.update()


def gpath_pin(

    gpath: QGraphicsPathItem,
    label: Label,  # noqa

    location_description: str = 'right-of-path-centered',
    use_right_of_pp_label: bool = False,

) -> QPointF:

    # get actual arrow graphics path
    path_br = gpath.mapToScene(gpath.path()).boundingRect()

    # vb.locate(arrow_path)  #, children=True)

    if location_description == 'right-of-path-centered':
        return path_br.topRight() - QPointF(0, label.h / 3)

    elif location_description == 'below-path-left-aligned':
        return path_br.bottomLeft() - QPointF(0, label.h / 6)

    elif location_description == 'below-path-right-aligned':
        return path_br.bottomRight() - QPointF(label.w, label.h / 6)
