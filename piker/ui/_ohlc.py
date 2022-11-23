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
Super fast OHLC sampling graphics types.

"""
from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
)

import numpy as np
import pyqtgraph as pg
from PyQt5 import (
    QtGui,
    QtWidgets,
)
from PyQt5.QtCore import (
    QLineF,
    QRectF,
)

from PyQt5.QtGui import QPainterPath

from .._profile import pg_profile_enabled, ms_slower_then
from ._style import hcolor
from ..log import get_logger
from .._profile import Profiler

if TYPE_CHECKING:
    from ._chart import LinkedSplits


log = get_logger(__name__)


def bar_from_ohlc_row(
    row: np.ndarray,
    # 0.5 is no overlap between arms, 1.0 is full overlap
    w: float = 0.43

) -> tuple[QLineF]:
    '''
    Generate the minimal ``QLineF`` lines to construct a single
    OHLC "bar" for use in the "last datum" of a series.

    '''
    open, high, low, close, index = row[
        ['open', 'high', 'low', 'close', 'index']]

    # TODO: maybe consider using `QGraphicsLineItem` ??
    # gives us a ``.boundingRect()`` on the objects which may make
    # computing the composite bounding rect of the last bars + the
    # history path faster since it's done in C++:
    # https://doc.qt.io/qt-5/qgraphicslineitem.html

    # high -> low vertical (body) line
    if low != high:
        hl = QLineF(index, low, index, high)
    else:
        # XXX: if we don't do it renders a weird rectangle?
        # see below for filtering this later...
        hl = None

    # NOTE: place the x-coord start as "middle" of the drawing range such
    # that the open arm line-graphic is at the left-most-side of
    # the index's range according to the view mapping coordinates.

    # open line
    o = QLineF(index - w, open, index, open)

    # close line
    c = QLineF(index, close, index + w, close)

    return [hl, o, c]


class BarItems(pg.GraphicsObject):
    '''
    "Price range" bars graphics rendered from a OHLC sampled sequence.

    '''
    def __init__(
        self,
        linked: LinkedSplits,
        plotitem: 'pg.PlotItem',  # noqa
        color: str = 'bracket',
        last_bar_color: str = 'original',

        name: Optional[str] = None,

    ) -> None:
        super().__init__()
        self.linked = linked
        # XXX: for the mega-lulz increasing width here increases draw
        # latency...  so probably don't do it until we figure that out.
        self._color = color
        self.bars_pen = pg.mkPen(hcolor(color), width=1)
        self.last_bar_pen = pg.mkPen(hcolor(last_bar_color), width=2)
        self._name = name

        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        self.path = QPainterPath()
        self._last_bar_lines: Optional[tuple[QLineF, ...]] = None

    def x_uppx(self) -> int:
        # we expect the downsample curve report this.
        return 0

    # Qt docs: https://doc.qt.io/qt-5/qgraphicsitem.html#boundingRect
    def boundingRect(self):
        # profiler = Profiler(
        #     msg=f'BarItems.boundingRect(): `{self._name}`',
        #     disabled=not pg_profile_enabled(),
        #     ms_threshold=ms_slower_then,
        # )

        # TODO: Can we do rect caching to make this faster
        # like `pg.PlotCurveItem` does? In theory it's just
        # computing max/min stuff again like we do in the udpate loop
        # anyway. Not really sure it's necessary since profiling already
        # shows this method is faf.

        # boundingRect _must_ indicate the entire area that will be
        # drawn on or else we will get artifacts and possibly crashing.
        # (in this case, QPicture does all the work of computing the
        # bounding rect for us).

        # apparently this a lot faster says the docs?
        # https://doc.qt.io/qt-5/qpainterpath.html#controlPointRect
        hb = self.path.controlPointRect()
        hb_tl, hb_br = (
            hb.topLeft(),
            hb.bottomRight(),
        )
        mn_y = hb_tl.y()
        mx_y = hb_br.y()
        most_left = hb_tl.x()
        most_right = hb_br.x()
        # profiler('calc path vertices')

        # need to include last bar height or BR will be off
        # OHLC line segments: [hl, o, c]
        last_lines: tuple[QLineF] | None = self._last_bar_lines
        if last_lines:
            (
                hl,
                o,
                c,
            ) = last_lines
            most_right = c.x2() + 1
            ymx = ymn = c.y2()

            if hl:
                y1, y2 = hl.y1(), hl.y2()
                ymn = min(y1, y2)
                ymx = max(y1, y2)
                mx_y = max(ymx, mx_y)
                mn_y = min(ymn, mn_y)
                # profiler('calc last bar vertices')

        return QRectF(
            most_left,
            mn_y,
            most_right - most_left + 1,
            mx_y - mn_y,
        )

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget

    ) -> None:

        profiler = Profiler(
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,
        )

        # p.setCompositionMode(0)

        # TODO: one thing we could try here is pictures being drawn of
        # a fixed count of bars such that based on the viewbox indices we
        # only draw the "rounded up" number of "pictures worth" of bars
        # as is necesarry for what's in "view". Not sure if this will
        # lead to any perf gains other then when zoomed in to less bars
        # in view.
        p.setPen(self.last_bar_pen)
        if self._last_bar_lines:
            p.drawLines(*tuple(filter(bool, self._last_bar_lines)))
            profiler('draw last bar')

        p.setPen(self.bars_pen)
        p.drawPath(self.path)
        profiler(f'draw history path: {self.path.capacity()}')

    def draw_last_datum(
        self,
        path: QPainterPath,
        src_data: np.ndarray,
        render_data: np.ndarray,
        reset: bool,
        array_key: str,

        fields: list[str] = [
            'index',
            'open',
            'high',
            'low',
            'close',
        ],

    ) -> None:

        # relevant fields
        ohlc = src_data[fields]
        # last_row = ohlc[-1:]

        # individual values
        last_row = i, o, h, l, last = ohlc[-1]

        # times = src_data['time']
        # if times[-1] - times[-2]:
        #     breakpoint()

        # generate new lines objects for updatable "current bar"
        self._last_bar_lines = bar_from_ohlc_row(last_row)

        # assert i == graphics.start_index - 1
        # assert i == last_index
        body, larm, rarm = self._last_bar_lines

        # XXX: is there a faster way to modify this?
        rarm.setLine(rarm.x1(), last, rarm.x2(), last)

        # writer is responsible for changing open on "first" volume of bar
        larm.setLine(larm.x1(), o, larm.x2(), o)

        if l != h:  # noqa

            if body is None:
                body = self._last_bar_lines[0] = QLineF(i, l, i, h)
            else:
                # update body
                body.setLine(i, l, i, h)

            # XXX: pretty sure this is causing an issue where the
            # bar has a large upward move right before the next
            # sample and the body is getting set to None since the
            # next bar is flat but the shm array index update wasn't
            # read by the time this code runs. Iow we're doing this
            # removal of the body for a bar index that is now out of
            # date / from some previous sample. It's weird though
            # because i've seen it do this to bars i - 3 back?

        # return ohlc['time'], ohlc['close']
        return ohlc['index'], ohlc['close']
