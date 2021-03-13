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
Lines for orders, alerts, L2.

"""
from typing import Tuple, Optional, List

import pyqtgraph as pg
from pyqtgraph import Point, functions as fn
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtGui import QGraphicsPathItem
from PyQt5.QtCore import QPointF
import numpy as np

from .._label import Label, vbr_left, right_axis
from .._style import (
    hcolor,
    _down_2_font_inches_we_like,
)


def mk_marker(
    marker,
    size: float = 20.0
) -> QGraphicsPathItem:
    """Add a marker to be displayed on the line wrapped in a ``QGraphicsPathItem``
    ready to be placed using scene coordinates (not view).

    **Arguments**
    marker        String indicating the style of marker to add:
                  ``'<|'``, ``'|>'``, ``'>|'``, ``'|<'``, ``'<|>'``,
                  ``'>|<'``, ``'^'``, ``'v'``, ``'o'``
    size          Size of the marker in pixels. Default is 10.0.

    """
    path = QtGui.QPainterPath()

    if marker == 'o':
        path.addEllipse(QtCore.QRectF(-0.5, -0.5, 1, 1))

    # arrow pointing away-from the top of line
    if '<|' in marker:
        p = QtGui.QPolygonF([Point(0.5, 0), Point(0, -0.5), Point(-0.5, 0)])
        path.addPolygon(p)
        path.closeSubpath()

    # arrow pointing away-from the bottom of line
    if '|>' in marker:
        p = QtGui.QPolygonF([Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0)])
        path.addPolygon(p)
        path.closeSubpath()

    # arrow pointing in-to the top of line
    if '>|' in marker:
        p = QtGui.QPolygonF([Point(0.5, -0.5), Point(0, 0), Point(-0.5, -0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    # arrow pointing in-to the bottom of line
    if '|<' in marker:
        p = QtGui.QPolygonF([Point(0.5, 0.5), Point(0, 0), Point(-0.5, 0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    if '^' in marker:
        p = QtGui.QPolygonF([Point(0, -0.5), Point(0.5, 0), Point(0, 0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    if 'v' in marker:
        p = QtGui.QPolygonF([Point(0, -0.5), Point(-0.5, 0), Point(0, 0.5)])
        path.addPolygon(p)
        path.closeSubpath()

    # self._maxMarkerSize = max([m[2] / 2. for m in self.markers])

    return QGraphicsPathItem(path)


# TODO: probably worth investigating if we can
# make .boundingRect() faster:
# https://stackoverflow.com/questions/26156486/determine-bounding-rect-of-line-in-qt
class LevelLine(pg.InfiniteLine):

    # TODO: fill in these slots for orders
    # available parent signals
    # sigDragged(self)
    # sigPositionChangeFinished(self)
    # sigPositionChanged(self)

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # type: ignore # noqa

        color: str = 'default',
        highlight_color: str = 'default_light',

        hl_on_hover: bool = True,
        dotted: bool = False,
        always_show_labels: bool = False,
        hide_xhair_on_hover: bool = True,
        movable: bool = True,

    ) -> None:

        super().__init__(
            movable=movable,
            angle=0,

            # don't use the shitty ``InfLineLabel``
            label=None,
        )

        self._chart = chart
        self._hoh = hl_on_hover
        self._dotted = dotted
        self._hide_xhair_on_hover = hide_xhair_on_hover

        self._marker = None

        if dotted:
            self._style = QtCore.Qt.DashLine
        else:
            self._style = QtCore.Qt.SolidLine

        self._hcolor: str = None

        # the float y-value in the view coords
        self.level: float = 0

        # list of labels anchored at one of the 2 line endpoints
        # inside the viewbox
        self._labels: List[(int, Label)] = []
        self._markers: List[(int, Label)] = []

        # whenever this line is moved trigger label updates
        self.sigPositionChanged.connect(self.on_pos_change)

        # sets color to value triggering pen creation
        self._hl_color = highlight_color
        self.color = color

        # TODO: for when we want to move groups of lines?
        self._track_cursor: bool = False
        self._always_show_labels = always_show_labels

        self._on_drag_start = lambda l: None
        self._on_drag_end = lambda l: None

        self._y_incr_mult = 1 / chart._lc._symbol.tick_size

    def txt_offsets(self) -> Tuple[int, int]:
        return 0, 0

    @property
    def color(self):
        return self._hcolor

    @color.setter
    def color(self, color: str) -> None:
        # set pens to new color
        self._hcolor = color
        pen = pg.mkPen(hcolor(color))
        hoverpen = pg.mkPen(hcolor(self._hl_color))

        pen.setStyle(self._style)
        hoverpen.setStyle(self._style)

        # set regular pen
        self.setPen(pen)

        # use slightly thicker highlight for hover pen
        hoverpen.setWidth(2)
        self.hoverPen = hoverpen

    def add_label(
        self,

        # by default we only display the line's level value
        # in the label
        fmt_str: str = (
            '{level:,.{level_digits}f}'
        ),
        side: str = 'right',
        side_of_axis: str = 'left',
        x_offset: float = 50,

        font_size_inches: float = _down_2_font_inches_we_like,
        color: str = None,
        bg_color: str = None,
        avoid_book: bool = True,

        **label_kwargs,
    ) -> Label:
        """Add a ``LevelLabel`` anchored at one of the line endpoints in view.

        """
        label = Label(
            view=self.getViewBox(),
            fmt_str=fmt_str,
            color=self.color,
        )

        # set anchor callback
        if side == 'right':
            label.set_x_anchor_func(
                right_axis(
                    self._chart,
                    label,
                    side=side_of_axis,
                    offset=x_offset,
                    avoid_book=avoid_book,
                )
            )

        elif side == 'left':
            label.set_x_anchor_func(vbr_left(label))

        self._labels.append((side, label))

        return label

    def on_pos_change(
        self,
        line: 'LevelLine',  # noqa
    ) -> None:
        """Position changed handler.

        """
        self.update_labels({'level': self.value()})

    def update_labels(
        self,
        fields_data: dict,
    ) -> None:

        for at, label in self._labels:
            label.color = self.color
            # print(f'color is {self.color}')

            label.fields.update(fields_data)

            level = fields_data.get('level')
            if level:
                label.set_view_y(level)

            label.render()

        self.update()

    def hide_labels(self) -> None:
        for at, label in self._labels:
            label.hide()

    def show_labels(self) -> None:
        for at, label in self._labels:
            label.show()

    def set_level(
        self,
        level: float,
    ) -> None:
        last = self.value()

        # if the position hasn't changed then ``.update_labels()``
        # will not be called by a non-triggered `.on_pos_change()`,
        # so we need to call it manually to avoid mismatching
        # label-to-line color when the line is updated but not
        # "moved".
        if level == last:
            self.update_labels({'level': level})

        self.setPos(level)
        self.level = self.value()
        self.update()

    def on_tracked_source(
        self,
        x: int,
        y: float
    ) -> None:
        # XXX: this is called by our ``Cursor`` type once this
        # line is set to track the cursor: for every movement
        # this callback is invoked to reposition the line
        self.movable = True
        self.set_level(y)  # implictly calls reposition handler

    # TODO: just put this in the hoverEvent handler
    def set_mouser_hover(self, hover: bool) -> None:
        """Mouse hover callback.

        """
        # XXX: currently we'll just return if _hoh is False
        if self.mouseHovering == hover:
            return

        self.mouseHovering = hover

        chart = self._chart
        cur = chart._cursor

        if hover:
            # highlight if so configured
            if self._hoh:

                self.currentPen = self.hoverPen

                # only disable cursor y-label updates
                # if we're highlighting a line
                cur._y_label_update = False

            # add us to cursor state
            cur.add_hovered(self)

            if self._hide_xhair_on_hover:
                cur.hide_xhair(
                    # set y-label to current value
                    y_label_level=self.value(),

                    # fg_color=self._hcolor,
                    # bg_color=self._hcolor,
                )

            # if we want highlighting of labels
            # it should be delegated into this method
            self.show_labels()

        else:
            cur._y_label_update = True

            self.currentPen = self.pen

            cur._hovered.remove(self)

            if self not in cur._trackers:
                cur.show_xhair()

            if not self._always_show_labels:
                for at, label in self._labels:
                    label.hide()
                    label.txt.update()
                    # label.unhighlight()

        # highlight any attached label

        self.update()

    def mouseDragEvent(self, ev):
        """Override the ``InfiniteLine`` handler since we need more
        detailed control and start end signalling.

        """
        chart = self._chart

        # hide y-crosshair
        chart._cursor.hide_xhair()

        # highlight
        self.currentPen = self.hoverPen
        self.show_labels()

        # XXX: normal tracking behavior pulled out from parent type
        if self.movable and ev.button() == QtCore.Qt.LeftButton:
            ev.accept()

            if ev.isStart():
                self.moving = True
                down_pos = ev.buttonDownPos()
                self.cursorOffset = self.pos() - self.mapToParent(down_pos)
                self.startPosition = self.pos()

                self._on_drag_start(self)

            if not self.moving:
                return

            pos = self.cursorOffset + self.mapToParent(ev.pos())

            # TODO: we should probably figure out a std api
            # for this kind of thing given we already have
            # it on the cursor system...

            # round to nearest symbol tick
            m = self._y_incr_mult
            self.setPos(
                QPointF(
                    self.pos().x(),  # don't allow shifting horizontally
                    round(pos.y() * m) / m
                )
            )

            self.sigDragged.emit(self)

            if ev.isFinish():
                self.moving = False
                self.sigPositionChangeFinished.emit(self)
                self._on_drag_end(self)

        # This is the final position in the drag
        if ev.isFinish():
            # show y-crosshair again
            chart._cursor.show_xhair()

    def delete(self) -> None:
        """Remove this line from containing chart/view/scene.

        """
        scene = self.scene()
        if scene:
            for at, label in self._labels:
                label.delete()

            self._labels.clear()

            if self._marker:
                self.scene().removeItem(self._marker)

        # remove from chart/cursor states
        chart = self._chart
        cur = chart._cursor

        if self in cur._hovered:
            cur._hovered.remove(self)

        chart.plotItem.removeItem(self)

    def mouseDoubleClickEvent(
        self,
        ev: QtGui.QMouseEvent,
    ) -> None:

        # TODO: enter labels edit mode
        print(f'double click {ev}')

    def draw_markers(
        self,
        p: QtGui.QPainter,
        left: float,
        right: float,
        right_offset: float,
    ) -> None:
        # paint markers in native coordinate system
        tr = p.transform()
        p.resetTransform()

        start = tr.map(Point(left, 0))
        end = tr.map(Point(right, 0))
        up = tr.map(Point(left, 1))
        dif = end - start
        # length = Point(dif).length()
        angle = np.arctan2(dif.y(), dif.x()) * 180 / np.pi

        p.translate(start)
        p.rotate(angle)

        up = up - start
        det = up.x() * dif.y() - dif.x() * up.y()
        p.scale(1, 1 if det > 0 else -1)

        p.setBrush(fn.mkBrush(self.currentPen.color()))
        tr = p.transform()
        for path, pos, size in self.markers:
            p.setTransform(tr)

            # XXX: we drop the "scale / %" placement
            # x = length * pos
            x = right_offset

            p.translate(x, 0)
            p.scale(size, size)
            p.drawPath(path)

    def right_point(
        self,
    ) -> float:

        chart = self._chart
        l1_len = chart._max_l1_line_len
        ryaxis = chart.getAxis('right')

        if self.markers:
            size = self.markers[0][2]
        else:
            size = 0

        r_axis_x = ryaxis.pos().x()
        right_offset = l1_len + size + 10
        right_scene_coords = r_axis_x - right_offset

        right_view_coords = chart._vb.mapToView(
            Point(right_scene_coords, 0)).x()

        return (
            right_scene_coords,
            right_view_coords,
            right_offset,
        )

    def paint(
        self,
        p: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget
    ) -> None:
        """Core paint which we override (yet again)
        from pg..

        """
        p.setRenderHint(p.Antialiasing)

        vb_left, vb_right = self._endPoints
        pen = self.currentPen
        # pen.setJoinStyle(QtCore.Qt.MiterJoin)
        p.setPen(pen)

        rsc, rvc, rosc = self.right_point()

        p.drawLine(
            Point(vb_left, 0),
            Point(rvc, 0)
        )

        # this seems slower when moving around
        # order lines.. not sure wtf is up with that.
        # for now we're just using it on the position line.
        if self._marker:
            scene_pos = QPointF(rsc, self.scene_y())
            self._marker.setPos(scene_pos)

            # somehow this is adding a lot of lag, but without
            # if we're getting weird trail artefacs grrr.
            # gotta be some kinda boundingRect problem yet again
            # self._marker.update()

        if self.markers:
            self.draw_markers(
                p,
                vb_left,
                vb_right,
                rsc
            )

    def scene_y(self) -> float:
        return self.getViewBox().mapFromView(Point(0, self.value())).y()

    def add_marker(
        self,
        path: QtGui.QGraphicsPathItem,
    ) -> None:

        # chart = self._chart
        vb = self.getViewBox()
        vb.scene().addItem(path)

        self._marker = path

        rsc, rvc, rosc = self.right_point()

        self._marker.setPen(self.currentPen)
        self._marker.setBrush(fn.mkBrush(self.currentPen.color()))
        self._marker.scale(20, 20)
        # y_in_sc = chart._vb.mapFromView(Point(0, self.value())).y()
        path.setPos(QPointF(rsc, self.scene_y()))

    def hoverEvent(self, ev):
        """Gawd, basically overriding it all at this point...

        """
        if (not ev.isExit()) and ev.acceptDrags(QtCore.Qt.LeftButton):
            self.set_mouser_hover(True)
        else:
            self.set_mouser_hover(False)


def level_line(
    chart: 'ChartPlogWidget',  # noqa
    level: float,
    color: str = 'default',

    # size 4 font on 4k screen scaled down, so small-ish.
    font_size_inches: float = _down_2_font_inches_we_like,

    # whether or not the line placed in view should highlight
    # when moused over (aka "hovered")
    hl_on_hover: bool = True,

    # line style
    dotted: bool = False,

    # label fields and options
    digits: int = 1,

    always_show_labels: bool = False,

    add_label: bool = True,

    orient_v: str = 'bottom',

    **kwargs,

) -> LevelLine:
    """Convenience routine to add a styled horizontal line to a plot.

    """
    hl_color = color + '_light' if hl_on_hover else color

    line = LevelLine(
        chart,
        color=color,

        # lookup "highlight" equivalent
        highlight_color=hl_color,

        dotted=dotted,

        # UX related options
        hl_on_hover=hl_on_hover,

        # when set to True the label is always shown instead of just on
        # highlight (which is a privacy thing for orders)
        always_show_labels=always_show_labels,

        **kwargs,
    )

    chart.plotItem.addItem(line)

    if add_label:

        label = line.add_label(
            side='right',
            opacity=1,
            x_offset=0,
            avoid_book=False,
        )
        label.orient_v = orient_v

        line.update_labels({'level': level, 'level_digits': 2})
        label.render()

        line.hide_labels()

    # activate/draw label
    line.set_level(level)

    return line


def order_line(
    chart,
    level: float,
    level_digits: float,
    action: str,  # buy or sell

    size: Optional[int] = 1,
    size_digits: int = 0,
    show_markers: bool = False,
    submit_price: float = None,
    exec_type: str = 'dark',
    order_type: str = 'limit',
    orient_v: str = 'bottom',

    **line_kwargs,

) -> LevelLine:
    """Convenience routine to add a line graphic representing an order
    execution submitted to the EMS via the chart's "order mode".

    """
    line = level_line(
        chart,
        level,
        add_label=False,
        **line_kwargs
    )

    if show_markers:
        # add arrow marker on end of line nearest y-axis
        marker_style, marker_size = {
            'buy': ('|<', 20),
            'sell': ('>|', 20),
            'alert': ('^', 12),
        }[action]

        # XXX: not sure wtf but this is somehow laggier
        # when tested manually staging an order..
        # I would assume it's to do either with come kinda
        # conflict of the ``QGraphicsPathItem`` with the custom
        # object or that those types are just slower in general...
        # Pretty annoying to say the least.
        # line.add_marker(mk_marker(marker_style))

        line.addMarker(
            marker_style,
            # the "position" here is now ignored since we modified
            # internals to pin markers to the right end of the line
            0.9,
            marker_size
        )

    orient_v = 'top' if action == 'sell' else 'bottom'

    if action == 'alert':
        # completely different labelling for alerts
        fmt_str = 'alert => {level}'

        # for now, we're just duplicating the label contents i guess..
        llabel = line.add_label(
            side='left',
            fmt_str=fmt_str,
        )
        llabel.fields = {
            'level': level,
            'level_digits': level_digits,
        }
        llabel.orient_v = orient_v
        llabel.render()
        llabel.show()

        # right before L1 label
        rlabel = line.add_label(
            side='right',
            side_of_axis='left',
            fmt_str=fmt_str,
        )
        rlabel.fields = {
            'level': level,
            'level_digits': level_digits,
        }

        rlabel.orient_v = orient_v
        rlabel.render()
        rlabel.show()

    else:
        # left side label
        llabel = line.add_label(
            side='left',
            fmt_str='{exec_type}-{order_type}: ${$value}',
        )
        llabel.fields = {
            'order_type': order_type,
            'level': level,
            '$value': lambda f: f['level'] * f['size'],
            'size': size,
            'exec_type': exec_type,
        }
        llabel.orient_v = orient_v
        llabel.render()
        llabel.show()

        # right before L1 label
        rlabel = line.add_label(
            side='right',
            side_of_axis='left',
            fmt_str=(
                '{size:.{size_digits}f} x'
            ),
        )
        rlabel.fields = {
            'size': size,
            'size_digits': size_digits,
        }

        rlabel.orient_v = orient_v
        rlabel.render()
        rlabel.show()

        # axis_label = line.add_label(
        #     side='right',
        #     side_of_axis='left',
        #     x_offset=0,
        #     avoid_book=False,
        #     fmt_str=(
        #         '{level:,.{level_digits}f}'
        #     ),
        # )
        # axis_label.fields = {
        #     'level': level,
        #     'level_digits': level_digits,
        # }

        # axis_label.orient_v = orient_v
        # axis_label.render()
        # axis_label.show()

    # sanity check
    line.update_labels({'level': level})

    return line


def position_line(
    chart,
    size: float,

    level: float,

    orient_v: str = 'bottom',

) -> LevelLine:
    """Convenience routine to add a line graphic representing an order
    execution submitted to the EMS via the chart's "order mode".

    """
    line = level_line(
        chart,
        level,
        color='default_light',
        add_label=False,
        hl_on_hover=False,
        movable=False,
        always_show_labels=False,
        hide_xhair_on_hover=False,
    )
    if size > 0:
        arrow_path = mk_marker('|<')

    elif size < 0:
        arrow_path = mk_marker('>|')

    line.add_marker(arrow_path)

    rlabel = line.add_label(
        side='left',
        fmt_str='{direction}: {size}\n${$:.2f}',
    )
    rlabel.fields = {
        'direction': 'long' if size > 0 else 'short',
        '$': size * level,
        'size': size,
    }
    rlabel.orient_v = orient_v
    rlabel.render()
    rlabel.show()

    # sanity check
    line.update_labels({'level': level})

    return line
