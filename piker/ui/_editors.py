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
Higher level annotation editors.

"""
from dataclasses import dataclass, field
from typing import Optional

import pyqtgraph as pg

from ._style import hcolor, _font
from ._graphics._lines import order_line, LevelLine
from ..log import get_logger


log = get_logger(__name__)


@dataclass
class ArrowEditor:

    chart: 'ChartPlotWidget'  # noqa
    _arrows: field(default_factory=dict)

    def add(
        self,
        uid: str,
        x: float,
        y: float,
        color='default',
        pointing: Optional[str] = None,
    ) -> pg.ArrowItem:
        """Add an arrow graphic to view at given (x, y).

        """
        angle = {
            'up': 90,
            'down': -90,
            None: 180,  # pointing to right (as in an alert)
        }[pointing]

        # scale arrow sizing to dpi-aware font
        size = _font.font.pixelSize() * 0.8

        arrow = pg.ArrowItem(
            angle=angle,
            baseAngle=0,
            headLen=size,
            headWidth=size/2,
            tailLen=None,
            pxMode=True,

            # coloring
            pen=pg.mkPen(hcolor('papas_special')),
            brush=pg.mkBrush(hcolor(color)),
        )
        arrow.setPos(x, y)

        self._arrows[uid] = arrow

        # render to view
        self.chart.plotItem.addItem(arrow)

        return arrow

    def remove(self, arrow) -> bool:
        self.chart.plotItem.removeItem(arrow)


# global store of order-lines graphics
# keyed by uuid4 strs - used to sync draw
# order lines **after** the order is 100%
# active in emsd
_order_lines: dict[str, LevelLine] = {}


@dataclass
class LineEditor:
    """The great editor of linez..

    """
    # TODO: drop this?
    # view: 'ChartView'

    _order_lines: field(default_factory=_order_lines)
    chart: 'ChartPlotWidget' = None  # type: ignore # noqa
    _active_staged_line: LevelLine = None
    _stage_line: LevelLine = None

    def stage_line(
        self,
        action: str,

        color: str = 'alert_yellow',
        hl_on_hover: bool = False,
        dotted: bool = False,

        # fields settings
        size: Optional[int] = None,
    ) -> LevelLine:
        """Stage a line at the current chart's cursor position
        and return it.

        """
        # chart.setCursor(QtCore.Qt.PointingHandCursor)

        chart = self.chart._cursor.active_plot
        cursor = chart._cursor
        y = chart._cursor._datum_xy[1]

        symbol = chart._lc.symbol

        # line = self._stage_line
        # if not line:
        # add a "staged" cursor-tracking line to view
        # and cash it in a a var
        if self._active_staged_line:
            self.unstage_line()

        line = order_line(
            chart,

            level=y,
            level_digits=symbol.digits(),
            size=size,
            size_digits=symbol.lot_digits(),

            # just for the stage line to avoid
            # flickering while moving the cursor
            # around where it might trigger highlight
            # then non-highlight depending on sensitivity
            always_show_labels=True,

            # kwargs
            color=color,
            # don't highlight the "staging" line
            hl_on_hover=hl_on_hover,
            dotted=dotted,
            exec_type='dark' if dotted else 'live',
            action=action,
            show_markers=True,

            # prevent flickering of marker while moving/tracking cursor
            only_show_markers_on_hover=False,
        )

        self._active_staged_line = line

        # hide crosshair y-line and label
        cursor.hide_xhair()

        # add line to cursor trackers
        cursor._trackers.add(line)

        return line

    def unstage_line(self) -> LevelLine:
        """Inverse of ``.stage_line()``.

        """
        # chart = self.chart._cursor.active_plot
        # # chart.setCursor(QtCore.Qt.ArrowCursor)
        cursor = self.chart._cursor

        # delete "staged" cursor tracking line from view
        line = self._active_staged_line
        if line:
            cursor._trackers.remove(line)
            line.delete()

        self._active_staged_line = None

        # show the crosshair y line and label
        cursor.show_xhair()

    def create_order_line(
        self,
        uuid: str,
        level: float,
        chart: 'ChartPlotWidget',  # noqa
        size: float,
        action: str,
    ) -> LevelLine:

        line = self._active_staged_line
        if not line:
            raise RuntimeError("No line is currently staged!?")

        sym = chart._lc.symbol

        line = order_line(
            chart,

            # label fields default values
            level=level,
            level_digits=sym.digits(),

            size=size,
            size_digits=sym.lot_digits(),

            # LevelLine kwargs
            color=line.color,
            dotted=line._dotted,

            show_markers=True,
            only_show_markers_on_hover=True,

            action=action,
        )

        # for now, until submission reponse arrives
        line.hide_labels()

        # register for later lookup/deletion
        self._order_lines[uuid] = line

        return line

    def commit_line(self, uuid: str) -> LevelLine:
        """Commit a "staged line" to view.

        Submits the line graphic under the cursor as a (new) permanent
        graphic in view.

        """
        try:
            line = self._order_lines[uuid]
        except KeyError:
            log.warning(f'No line for {uuid} could be found?')
            return
        else:
            assert line.oid == uuid
            line.show_labels()

            # TODO: other flashy things to indicate the order is active

            log.debug(f'Level active for level: {line.value()}')

            return line

    def lines_under_cursor(self):
        """Get the line(s) under the cursor position.

        """
        # Delete any hoverable under the cursor
        return self.chart._cursor._hovered

    def remove_line(
        self,
        line: LevelLine = None,
        uuid: str = None,
    ) -> LevelLine:
        """Remove a line by refernce or uuid.

        If no lines or ids are provided remove all lines under the
        cursor position.

        """
        if line:
            uuid = line.oid

        # try to look up line from our registry
        line = self._order_lines.pop(uuid, None)
        if line:

            # if hovered remove from cursor set
            hovered = self.chart._cursor._hovered
            if line in hovered:
                hovered.remove(line)

                # make sure the xhair doesn't get left off
                # just because we never got a un-hover event
                self.chart._cursor.show_xhair()

            line.delete()
            return line
