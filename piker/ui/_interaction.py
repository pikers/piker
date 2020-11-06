# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
UX interaction customs.
"""
import pyqtgraph as pg
from pyqtgraph import functions as fn

from ..log import get_logger
from ._style import _min_points_to_show


log = get_logger(__name__)


class ChartView(pg.ViewBox):
    """Price chart view box with interaction behaviors you'd expect from
    any interactive platform:

        - zoom on mouse scroll that auto fits y-axis
        - no vertical scrolling
        - zoom to a "fixed point" on the y-axis
    """
    def __init__(
        self,
        parent=None,
        **kwargs,
    ):
        super().__init__(parent=parent, **kwargs)
        # disable vertical scrolling
        self.setMouseEnabled(x=True, y=False)
        self.linked_charts = None

    def wheelEvent(self, ev, axis=None):
        """Override "center-point" location for scrolling.

        This is an override of the ``ViewBox`` method simply changing
        the center of the zoom to be the y-axis.

        TODO: PR a method into ``pyqtgraph`` to make this configurable
        """

        if axis in (0, 1):
            mask = [False, False]
            mask[axis] = self.state['mouseEnabled'][axis]
        else:
            mask = self.state['mouseEnabled'][:]

        # don't zoom more then the min points setting
        l, lbar, rbar, r = self.linked_charts.chart.bars_range()
        vl = r - l

        if ev.delta() > 0 and vl <= _min_points_to_show:
            log.debug("Max zoom bruh...")
            return

        if ev.delta() < 0 and vl >= len(self.linked_charts._array) + 666:
            log.debug("Min zoom bruh...")
            return

        # actual scaling factor
        s = 1.015 ** (ev.delta() * -1 / 20)  # self.state['wheelScaleFactor'])
        s = [(None if m is False else s) for m in mask]

        # center = pg.Point(
        #     fn.invertQTransform(self.childGroup.transform()).map(ev.pos())
        # )

        # XXX: scroll "around" the right most element in the view
        # which stays "pinned" in place.

        # furthest_right_coord = self.boundingRect().topRight()

        # yaxis = pg.Point(
        #     fn.invertQTransform(
        #         self.childGroup.transform()
        #     ).map(furthest_right_coord)
        # )

        # This seems like the most "intuitive option, a hybrdid of
        # tws and tv styles
        last_bar = pg.Point(rbar)

        self._resetTarget()
        self.scaleBy(s, last_bar)
        ev.accept()
        self.sigRangeChangedManually.emit(mask)
