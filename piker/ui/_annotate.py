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
from PyQt5 import QtCore, QtGui
from PyQt5.QtGui import QGraphicsPathItem
from pyqtgraph import Point, functions as fn, Color
import numpy as np


def mk_marker(
    style,
    size: float = 20.0,
    use_qgpath: bool = True,
) -> QGraphicsPathItem:
    """Add a marker to be displayed on the line wrapped in a ``QGraphicsPathItem``
    ready to be placed using scene coordinates (not view).

    **Arguments**
    style        String indicating the style of marker to add:
                  ``'<|'``, ``'|>'``, ``'>|'``, ``'|<'``, ``'<|>'``,
                  ``'>|<'``, ``'^'``, ``'v'``, ``'o'``
    size          Size of the marker in pixels. Default is 10.0.

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

    if use_qgpath:
        path = QGraphicsPathItem(path)
        path.scale(size, size)

    return path


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
