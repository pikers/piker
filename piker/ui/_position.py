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
Position info and display

"""
from typing import Optional, Dict, Any, Callable
from functools import partial
from math import floor

from pyqtgraph import Point, functions as fn
from pydantic import BaseModel
from PyQt5 import QtGui, QtWidgets
from PyQt5.QtCore import QPointF
from PyQt5.QtGui import QGraphicsPathItem

from ._annotate import mk_marker_path
from ._anchors import (
    marker_right_points,
    gpath_pin,
    # keep_marker_in_view,
)
from ._label import Label
from ._lines import LevelLine, level_line
from ._style import _font
from ..data._source import Symbol


class Position(BaseModel):
    '''Basic pp representation with attached fills history.

    '''
    symbol: Symbol
    size: float
    avg_price: float  # TODO: contextual pricing
    fills: Dict[str, Any] = {}


class LevelMarker(QGraphicsPathItem):
    '''An arrow marker path graphich which redraws itself
    to the specified view coordinate level on each paint cycle.

    '''
    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa
        style: str,
        get_level: Callable[..., float],
        size: float = 20,
        keep_in_view: bool = True,

    ) -> None:

        self._style = None
        self.size = size

        # get polygon and scale
        super().__init__()

        # interally generates and scales path
        self.style = style
        # path = mk_marker_path(style)

        # self.scale(size, size)

        self.chart = chart
        # chart.getViewBox().scene().addItem(self)

        self.get_level = get_level
        self.scene_x = lambda: marker_right_points(chart)[1]
        self.level: float = 0
        self.keep_in_view = keep_in_view

        # get the path for the opaque path **without** weird
        # surrounding margin
        self.path_br = self.mapToScene(
            self.path()
        ).boundingRect()


    @property
    def style(self) -> str:
        return self._style

    @style.setter
    def style(self, value: str) -> None:
        if self._style != value:
            polygon = mk_marker_path(value)
            self.setPath(polygon)
            self._style = value
            self.scale(self.size, self.size)

    def delete(self) -> None:
        self.scene().removeItem(self)

    @property
    def h(self) -> float:
        return self.path_br.height()

    @property
    def w(self) -> float:
        return self.path_br.width()

    def position_in_view(
        self,
        # level: float,

    ) -> None:
        '''Show a pp off-screen indicator for a level label.

        This is like in fps games where you have a gps "nav" indicator
        but your teammate is outside the range of view, except in 2D, on
        the y-dimension.

        '''
        level = self.get_level()

        view = self.chart.getViewBox()
        vr = view.state['viewRange']
        ymn, ymx = vr[1]

        # _, marker_right, _ = marker_right_points(line._chart)
        x = self.scene_x()

        if level > ymx:  # pin to top of view
            self.setPos(
                QPointF(
                    x,
                    self.h/3,
                )
            )

        elif level < ymn:  # pin to bottom of view

            self.setPos(
                QPointF(
                    x,
                    view.height() - 4/3*self.h,
                )
            )

        else:
        #     # pp line is viewable so show marker normally
        #     self.update()
            self.setPos(
                x,
                self.chart.view.mapFromView(
                    QPointF(0, self.get_level())
                ).y()
            )

        # marker = line._marker
        if getattr(self, 'label', None):
            label = self.label

            # re-anchor label (i.e. trigger call of ``arrow_tr()`` from above
            label.update()

    def paint(
        self,

        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget

    ) -> None:
        '''Core paint which we override to always update
        our marker position in scene coordinates from a
        view cooridnate "level".

        '''
        if self.keep_in_view:
            self.position_in_view()

        else:

            # just place at desired level even if not in view
            self.setPos(
                self.scene_x(),
                self.mapToScene(QPointF(0, self.get_level())).y()
            )

        return super().paint(p, opt, w)


class PositionInfo:

    # inputs
    chart: 'ChartPlotWidget'  # noqa
    info: dict

    # allocated
    pp_label: Label
    size_label: Label
    info_label: Label
    line: Optional[LevelLine] = None

    _color: str = 'default_light'

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa

    ) -> None:

        # from . import _lines

        self.chart = chart
        self.info = {}
        self.pp_label = None

        view = chart.getViewBox()

        # create placeholder 'up' level arrow
        self._level_marker = None
        self._level_marker = self.level_marker(size=1)

        # literally 'pp' label that's always in view
        self.pp_label = pp_label = Label(
            view=view,
            fmt_str='pp',
            color=self._color,
            update_on_range_change=False,
        )

        self._level_marker.label = pp_label

        pp_label.scene_anchor = partial(
            gpath_pin,
            gpath=self._level_marker,
            label=pp_label,
        )
        pp_label.render()
        pp_label.show()

        self.size_label = size_label = Label(

            view=view,
            color=self._color,

            # this is "static" label
            # update_on_range_change=False,
            fmt_str='\n'.join((
                '{entry_size} x',
            )),

            fields={
                'entry_size': 0,
            },
        )
        size_label.render()
        # size_label.scene_anchor = self.align_to_marker
        size_label.scene_anchor = partial(
            gpath_pin,
            location_description='left-of-path-centered',
            gpath=self._level_marker,
            label=size_label,
        )
        size_label.hide()

        # self.info_label = info_label = Label(

        #     view=view,
        #     color=self._color,

        #     # this is "static" label
        #     # update_on_range_change=False,

        #     fmt_str='\n'.join((
        #         # '{entry_size}x ',
        #         '{percent_pnl} % PnL',
        #         # '{percent_of_port}% of port',
        #         '${base_unit_value}',
        #     )),

        #     fields={
        #         # 'entry_size': 0,
        #         'percent_pnl': 0,
        #         'percent_of_port': 2,
        #         'base_unit_value': '1k',
        #     },
        # )
        # info_label.scene_anchor = lambda: self.size_label.txt.pos()
        # + QPointF(0, self.size_label.h)
        # info_label.render()
        # info_label.hide()

    def level(self) -> float:
        if self.line:
            return self.line.value()
        else:
            return 0

    def show(self) -> None:
        self.pp_label.show()
        self.size_label.show()
        # self.info_label.show()
        if self.line:
            self.line.show()

    def hide(self) -> None:
        # self.pp_label.hide()
        self.size_label.hide()
        # self.info_label.hide()

        # if self.line:
        #     self.line.hide()

    def level_marker(
        self,
        size: float,

    ) -> QGraphicsPathItem:

        if self._level_marker:
            self._level_marker.delete()

        # arrow marker
        # scale marker size with dpi-aware font size
        font_size = _font.font.pixelSize()

        # scale marker size with dpi-aware font size
        arrow_size = floor(1.375 * font_size)

        if size > 0:
            style = '|<'
            direction = 'up'

        elif size < 0:
            style = '>|'
            direction = 'down'

        arrow = LevelMarker(
            chart=self.chart,
            style=style,
            get_level=self.level,
            size=arrow_size,
        )
        # _, marker_right, _ = marker_right_points(self.chart)
        # arrow.scene_x = marker_right

        # monkey-cache height for sizing on pp nav-hub
        # arrow._height = path_br.height()
        # arrow._width = path_br.width()
        arrow._direction = direction

        self.chart.getViewBox().scene().addItem(arrow)
        arrow.show()

        # arrow.label = self.pp_label

        # inside ``LevelLine.pain()`` this is updates...
        # we need a better way to have the label updated as frequenty
        # as every paint call? Maybe use a better slot then the range
        # change?
        # self._level_marker.label = self.pp_label

        return arrow

    def position_line(
        self,

        size: float,
        level: float,

        orient_v: str = 'bottom',

    ) -> LevelLine:
        '''Convenience routine to add a line graphic representing an order
        execution submitted to the EMS via the chart's "order mode".

        '''
        self.line = line = level_line(
            self.chart,
            level,
            color=self._color,
            add_label=False,
            hl_on_hover=False,
            movable=False,
            hide_xhair_on_hover=False,
            use_marker_margin=True,
            only_show_markers_on_hover=False,
            always_show_labels=True,
        )

        if size > 0:
            style = '|<'
        elif size < 0:
            style = '>|'

        self._level_marker.style = style

        # last_direction = self._level_marker._direction
        # if (
        #     size < 0 and last_direction == 'up'
        # ):
        #     self._level_marker = self.level_marker(size)
        marker = self._level_marker

        # add path to scene
        # line.getViewBox().scene().addItem(marker)

        # set marker color to same as line
        marker.setPen(line.currentPen)
        marker.setBrush(fn.mkBrush(line.currentPen.color()))
        marker.level = level
        marker.update()
        marker.show()

        #  hide position marker when out of view (for now)
        vb = line.getViewBox()
        vb.sigRangeChanged.connect(marker.position_in_view)

        line._labels.append(self.pp_label)

        # XXX: uses new marker drawing approach
        # line.add_marker(self._level_marker)
        line.set_level(level)

        # sanity check
        line.update_labels({'level': level})

        # vb.sigRangeChanged.connect(
        #     partial(keep_marker_in_view, chartview=vb, line=line)
        # )

        return line

    # order line endpoint anchor
    def align_to_marker(self) -> QPointF:

        pp_line = self.line
        if pp_line:

            line_ep = pp_line.scene_endpoint()
            # print(line_ep)

            y_level_scene = line_ep.y()
            # pp_y = pp_label.txt.pos().y()

            # if y_level_scene > pp_y:
            #     y_level_scene = pp_y

            # elif y_level_scene
            mkr_pos = self._level_marker.pos()

            left_of_mkr = QPointF(
                # line_ep.x() - self.size_label.w,
                mkr_pos.x() - self.size_label.w,
                mkr_pos.y(),
                # self._level_marker
                # max(0, y_level_scene),
                # min(
                #     pp_label.txt.pos().y()
                # ),
            )
            return left_of_mkr

            # return QPointF(

            #     marker_right_points(chart)[2] - pp_label.w ,
            #     view.height() - pp_label.h,
            #     # br.x() - pp_label.w,
            #     # br.y(),
            # )

        else:
            # pp = _lines._pp_label.txt
            # scene_rect = pp.mapToScene(pp.boundingRect()).boundingRect()
            # br = scene_rect.bottomRight()

            return QPointF(0, 0)

    def update_line(
        self,

        price: float,
        size: float,

    ) -> None:
        '''Update personal position level line.


        '''
        # do line update
        line = self.line

        if line is None and size:

            # create and show a pp line
            line = self.line = self.position_line(
                level=price,
                size=size,
            )
            line.show()

        elif line:

            if size != 0.0:
                line.set_level(price)
                self._level_marker.lelvel = price
                self._level_marker.update()
                line.update_labels({'size': size})
                line.show()

            else:
                # remove pp line from view
                line.delete()
                self.line = None

    def update(
        self,

        avg_price: float,
        size: float,

    ) -> None:
        '''Update graphics and data from average price and size.

        '''
        self.update_line(avg_price, size)

        self._level_marker.level = avg_price
        self._level_marker.update()  # trigger paint

        # info updates
        self.info['avg_price'] = avg_price
        self.info['size'] = size

        # label updates
        self.size_label.fields['entry_size'] = size
        self.size_label.render()

        # self.info_label.fields['size'] = size
        # self.info_label.render()
