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
from typing import Optional
from functools import partial
from math import floor

from pyqtgraph import functions as fn
from pydantic import BaseModel
# from PyQt5.QtCore import QPointF
# from PyQt5.QtGui import QGraphicsPathItem

from ._annotate import LevelMarker
from ._anchors import (
    pp_tight_and_right,  # wanna keep it straight in the long run
    gpath_pin,
)
from ..clearing._messages import BrokerdPosition, Status
from ..data._source import Symbol
from ._label import Label
from ._lines import LevelLine, level_line
from ._style import _font


class Position(BaseModel):
    '''Basic pp (personal position) data representation with attached
    fills history.

    This type should be IPC wire ready?

    '''
    symbol: Symbol

    # last size and avg entry price
    size: float
    avg_price: float  # TODO: contextual pricing

    # ordered record of known constituent trade messages
    fills: list[Status] = []


class PositionTracker:
    '''Track and display a real-time position for a single symbol
    on a chart.

    '''
    # inputs
    chart: 'ChartPlotWidget'  # noqa

    # allocated
    info: Position
    pp_label: Label
    size_label: Label
    line: Optional[LevelLine] = None

    _color: str = 'default_light'

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa

    ) -> None:

        self.chart = chart
        self.info = Position(
            symbol=chart.linked.symbol,
            size=0,
            avg_price=0,
        )

        self.pp_label = None

        view = chart.getViewBox()

        # literally 'pp' label that's always in view
        self.pp_label = pp_label = Label(
            view=view,
            fmt_str='pp',
            color=self._color,
            update_on_range_change=False,
        )

        # create placeholder 'up' level arrow
        self._level_marker = None
        self._level_marker = self.level_marker(size=1)

        pp_label.scene_anchor = partial(
            gpath_pin,
            gpath=self._level_marker,
            label=pp_label,
        )
        pp_label.render()

        self.size_label = size_label = Label(
            view=view,
            color=self._color,

            # this is "static" label
            # update_on_range_change=False,
            fmt_str='\n'.join((
                ':{entry_size:.0f}',
            )),

            fields={
                'entry_size': 0,
            },
        )
        size_label.render()

        size_label.scene_anchor = partial(
            pp_tight_and_right,
            label=self.pp_label,
        )

        # size_label.scene_anchor = lambda: (
        #     self.pp_label.txt.pos() + QPointF(self.pp_label.w, 0)
        # )
        # size_label.scene_anchor = lambda: (
        #     self.pp_label.scene_br().bottomRight() - QPointF(
        #     self.size_label.w, self.size_label.h/3)
        # )

        # TODO: if we want to show more position-y info?
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

    def update_graphics(
        self,
        marker: LevelMarker

    ) -> None:
        '''Update all labels.

        Meant to be called from the maker ``.paint()``
        for immediate, lag free label draws.

        '''
        self.pp_label.update()
        self.size_label.update()

    def update(
        self,
        msg: BrokerdPosition,

    ) -> None:
        '''Update graphics and data from average price and size.

        '''
        avg_price, size = msg['avg_price'], msg['size']
        # info updates
        self.info.avg_price = avg_price
        self.info.size = size

        self.update_line(avg_price, size)

        # label updates
        self.size_label.fields['entry_size'] = size
        self.size_label.render()

        if size == 0:
            self.hide()

        else:
            self._level_marker.level = avg_price

            # these updates are critical to avoid lag on view/scene changes
            self._level_marker.update()  # trigger paint
            self.pp_label.update()
            self.size_label.update()

            self.show()

            # don't show side and status widgets unless
            # order mode is "engaged" (which done via input controls)
            self.hide_info()

    def level(self) -> float:
        if self.line:
            return self.line.value()
        else:
            return 0

    def show(self) -> None:
        if self.info.size:
            self.line.show()
            self._level_marker.show()
            self.pp_label.show()
            self.size_label.show()

    def hide(self) -> None:
        self.pp_label.hide()
        self._level_marker.hide()
        self.size_label.hide()
        if self.line:
            self.line.hide()

    def hide_info(self) -> None:
        '''Hide details (right now just size label?) of position.

        '''
        # TODO: add remove status bar widgets here
        self.size_label.hide()

    # TODO: move into annoate module
    def level_marker(
        self,
        size: float,

    ) -> LevelMarker:

        if self._level_marker:
            self._level_marker.delete()

        # arrow marker
        # scale marker size with dpi-aware font size
        font_size = _font.font.pixelSize()

        # scale marker size with dpi-aware font size
        arrow_size = floor(1.375 * font_size)

        if size > 0:
            style = '|<'

        elif size < 0:
            style = '>|'

        arrow = LevelMarker(
            chart=self.chart,
            style=style,
            get_level=self.level,
            size=arrow_size,
            on_paint=self.update_graphics,
        )

        self.chart.getViewBox().scene().addItem(arrow)
        arrow.show()

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

        marker = self._level_marker
        marker.style = style

        # set marker color to same as line
        marker.setPen(line.currentPen)
        marker.setBrush(fn.mkBrush(line.currentPen.color()))
        marker.level = level
        marker.update()
        marker.show()

        # show position marker on view "edge" when out of view
        vb = line.getViewBox()
        vb.sigRangeChanged.connect(marker.position_in_view)

        line.set_level(level)

        return line

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
                self._level_marker.level = price
                self._level_marker.update()
                # line.update_labels({'size': size})
                line.show()

            else:
                # remove pp line from view
                line.delete()
                self.line = None
