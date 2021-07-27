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
Annotations for ur faces.

"""
from typing import Callable

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QPointF
from PyQt5.QtWidgets import QGraphicsPathItem
from pyqtgraph import Point, functions as fn, Color
import numpy as np

from ._anchors import marker_right_points


def mk_marker_path(

    style: str,

) -> QGraphicsPathItem:
    """Add a marker to be displayed on the line wrapped in a ``QGraphicsPathItem``
    ready to be placed using scene coordinates (not view).

    **Arguments**
    style        String indicating the style of marker to add:
                  ``'<|'``, ``'|>'``, ``'>|'``, ``'|<'``, ``'<|>'``,
                  ``'>|<'``, ``'^'``, ``'v'``, ``'o'``
    size          Size of the marker in pixels.

    """
    path = QtGui.QPainterPath()

    if style == 'o':
        path.addEllipse(QtCore.QRectF(-0.5, -0.5, 1, 1))

    # arrow pointing away-from the top of line
    if '<|' in style:
        p = QtGui.QPolygonF([Point(0.5, 0), Point(0, -0.5), Point(-0.5, 0)])
        path.addPolygon(p)
        path.closeSubpath()

    # arrow pointing away-from the bottom of line
    if '|>' in style:
        p = QtGui.QPolygonF([Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0)])
        path.addPolygon(p)
        path.closeSubpath()

    # arrow pointing in-to the top of line
    if '>|' in style:
        p = QtGui.QPolygonF([Point(0.5, -0.5), Point(0, 0), Point(-0.5, -0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    # arrow pointing in-to the bottom of line
    if '|<' in style:
        p = QtGui.QPolygonF([Point(0.5, 0.5), Point(0, 0), Point(-0.5, 0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    if '^' in style:
        p = QtGui.QPolygonF([Point(0, -0.5), Point(0.5, 0), Point(0, 0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    if 'v' in style:
        p = QtGui.QPolygonF([Point(0, -0.5), Point(-0.5, 0), Point(0, 0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    # self._maxMarkerSize = max([m[2] / 2. for m in self.markers])

    return path


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

        # get polygon and scale
        super().__init__()
        self.scale(size, size)

        # interally generates path
        self._style = None
        self.style = style

        self.chart = chart

        self.get_level = get_level
        self.scene_x = lambda: marker_right_points(chart)[1]
        self.level: float = 0
        self.keep_in_view = keep_in_view

        assert self.path_br

    @property
    def style(self) -> str:
        return self._style

    @style.setter
    def style(self, value: str) -> None:
        if self._style != value:
            polygon = mk_marker_path(value)
            self.setPath(polygon)
            self._style = value

            # get the path for the opaque path **without** weird
            # surrounding margin
            self.path_br = self.mapToScene(
                self.path()
            ).boundingRect()

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
            # pp line is viewable so show marker normally
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

        else:  # just place at desired level even if not in view
            self.setPos(
                self.scene_x(),
                self.mapToScene(QPointF(0, self.get_level())).y()
            )

        return super().paint(p, opt, w)


def qgo_draw_markers(

    markers: list,
    color: Color,
    p: QtGui.QPainter,
    left: float,
    right: float,
    right_offset: float,

) -> float:
    """Paint markers in ``pg.GraphicsItem`` style by first
    removing the view transform for the painter, drawing the markers
    in scene coords, then restoring the view coords.

    """
    # paint markers in native coordinate system
    orig_tr = p.transform()

    start = orig_tr.map(Point(left, 0))
    end = orig_tr.map(Point(right, 0))
    up = orig_tr.map(Point(left, 1))

    dif = end - start
    # length = Point(dif).length()
    angle = np.arctan2(dif.y(), dif.x()) * 180 / np.pi

    p.resetTransform()

    p.translate(start)
    p.rotate(angle)

    up = up - start
    det = up.x() * dif.y() - dif.x() * up.y()
    p.scale(1, 1 if det > 0 else -1)

    p.setBrush(fn.mkBrush(color))
    # p.setBrush(fn.mkBrush(self.currentPen.color()))
    tr = p.transform()

    sizes = []
    for path, pos, size in markers:
        p.setTransform(tr)

        # XXX: we drop the "scale / %" placement
        # x = length * pos
        x = right_offset

        p.translate(x, 0)
        p.scale(size, size)
        p.drawPath(path)
        sizes.append(size)

    p.setTransform(orig_tr)
    return max(sizes)
