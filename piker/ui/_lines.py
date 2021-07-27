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
from functools import partial
from math import floor
from typing import Tuple, Optional, List

import pyqtgraph as pg
from pyqtgraph import Point, functions as fn
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QPointF
from PyQt5.QtGui import QGraphicsPathItem

from ._annotate import mk_marker_path, qgo_draw_markers
from ._anchors import (
    marker_right_points,
    vbr_left,
    right_axis,
    gpath_pin,
)
from ._label import Label
from ._style import hcolor, _font


# TODO: probably worth investigating if we can
# make .boundingRect() faster:
# https://stackoverflow.com/questions/26156486/determine-bounding-rect-of-line-in-qt
class LevelLine(pg.InfiniteLine):

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # type: ignore # noqa

        # style
        color: str = 'default',
        highlight_color: str = 'default_light',
        dotted: bool = False,
        # marker_size: int = 20,

        # UX look and feel opts
        always_show_labels: bool = False,
        hl_on_hover: bool = True,
        hide_xhair_on_hover: bool = True,
        only_show_markers_on_hover: bool = True,
        use_marker_margin: bool = False,

        movable: bool = True,

    ) -> None:

        # TODO: at this point it's probably not worth the inheritance
        # any more since  we've reimplemented ``.pain()`` among other
        # things..
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
        # self._default_mkr_size = marker_size
        self._moh = only_show_markers_on_hover
        self.show_markers: bool = True  # presuming the line is hovered at init

        # should line go all the way to far end or leave a "margin"
        # space for other graphics (eg. L1 book)
        self.use_marker_margin: bool = use_marker_margin

        if dotted:
            self._style = QtCore.Qt.DashLine
        else:
            self._style = QtCore.Qt.SolidLine

        self._hcolor: str = None

        # the float y-value in the view coords
        self.level: float = 0

        # list of labels anchored at one of the 2 line endpoints
        # inside the viewbox
        self._labels: List[Label] = []
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
        self._right_end_sc: float = 0

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

        for label in self._labels:

            label.color = self.color
            # print(f'color is {self.color}')

            label.fields.update(fields_data)

            level = fields_data.get('level')
            if level:
                label.set_view_pos(y=level)

            label.render()

        self.update()

    def hide_labels(self) -> None:
        for label in self._labels:
            label.hide()

    def show_labels(self) -> None:
        for label in self._labels:
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

    def mouseDragEvent(self, ev):
        """Override the ``InfiniteLine`` handler since we need more
        detailed control and start end signalling.

        """
        cursor = self._chart.linked.cursor

        # hide y-crosshair
        cursor.hide_xhair()

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
            cursor.show_xhair()

    def delete(self) -> None:
        """Remove this line from containing chart/view/scene.

        """
        scene = self.scene()
        if scene:
            for label in self._labels:
                label.delete()

            # gc managed labels?
            self._labels.clear()

            if self._marker:
                self.scene().removeItem(self._marker)

        # remove from chart/cursor states
        chart = self._chart
        cur = chart.linked.cursor

        if self in cur._hovered:
            cur._hovered.remove(self)

        chart.plotItem.removeItem(self)

    def mouseDoubleClickEvent(
        self,
        ev: QtGui.QMouseEvent,
    ) -> None:

        # TODO: enter labels edit mode
        print(f'double click {ev}')

    def right_point(
        self,
    ) -> float:

        chart = self._chart
        l1_len = chart._max_l1_line_len
        ryaxis = chart.getAxis('right')
        up_to_l1_sc = ryaxis.pos().x() - l1_len

        return up_to_l1_sc

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

        # these are in viewbox coords
        vb_left, vb_right = self._endPoints
        vb = self.getViewBox()

        line_end, marker_right, r_axis_x = marker_right_points(self._chart)

        if self.show_markers and self.markers:

            p.setPen(self.pen)
            qgo_draw_markers(
                self.markers,
                self.pen.color(),
                p,
                vb_left,
                vb_right,
                marker_right,
            )
            # marker_size = self.markers[0][2]
            self._maxMarkerSize = max([m[2] / 2. for m in self.markers])

        # this seems slower when moving around
        # order lines.. not sure wtf is up with that.
        # for now we're just using it on the position line.
        elif self._marker:

            # TODO: make this label update part of a scene-aware-marker
            # composed annotation
            self._marker.setPos(
                QPointF(marker_right, self.scene_y())
            )
            if hasattr(self._marker, 'label'):
                self._marker.label.update()

        elif not self.use_marker_margin:
            # basically means **don't** shorten the line with normally
            # reserved space for a direction marker but, leave small
            # blank gap for style
            line_end = r_axis_x - 10

        line_end_view = vb.mapToView(Point(line_end, 0)).x()

        # self.currentPen.setJoinStyle(QtCore.Qt.MiterJoin)
        p.setPen(self.currentPen)
        p.drawLine(
            Point(vb_left, 0),
            Point(line_end_view, 0)
        )
        self._right_end_sc = line_end

    def hide(self) -> None:
        super().hide()
        if self._marker:
            self._marker.hide()
            # self._marker.label.hide()

    def show(self) -> None:
        super().show()
        if self._marker:
            self._marker.show()
            # self._marker.label.show()

    def scene_y(self) -> float:
        return self.getViewBox().mapFromView(
            Point(0, self.value())
        ).y()

    def scene_endpoint(self) -> QPointF:

        if not self._right_end_sc:
            line_end, _, _ = marker_right_points(self._chart)
            self._right_end_sc = line_end - 10

        return QPointF(self._right_end_sc, self.scene_y())

    def add_marker(
        self,
        path: QtWidgets.QGraphicsPathItem,

    ) -> QtWidgets.QGraphicsPathItem:

        # add path to scene
        self.getViewBox().scene().addItem(path)

        self._marker = path

        rsc = self.right_point()

        self._marker.setPen(self.currentPen)
        self._marker.setBrush(fn.mkBrush(self.currentPen.color()))
        # y_in_sc = chart._vb.mapFromView(Point(0, self.value())).y()
        path.setPos(QPointF(rsc, self.scene_y()))

        return path

    def hoverEvent(self, ev):
        """Mouse hover callback.

        """
        cur = self._chart.linked.cursor

        # hovered
        if (not ev.isExit()) and ev.acceptDrags(QtCore.Qt.LeftButton):

            # if already hovered we don't need to run again
            if self.mouseHovering is True:
                return

            if self._moh:
                self.show_markers = True

                if self._marker:
                    self._marker.show()

            # highlight if so configured
            if self._hoh:

                self.currentPen = self.hoverPen

                if self not in cur._trackers:
                    # only disable cursor y-label updates
                    # if we're highlighting a line
                    cur._y_label_update = False

            # add us to cursor state
            cur.add_hovered(self)

            if self._hide_xhair_on_hover:
                cur.hide_xhair(
                    # set y-label to current value
                    y_label_level=self.value(),
                    just_vertical=True,

                    # fg_color=self._hcolor,
                    # bg_color=self._hcolor,
                )

            # if we want highlighting of labels
            # it should be delegated into this method
            self.show_labels()

            self.mouseHovering = True

        # un-hovered
        else:
            if self.mouseHovering is False:
                return

            cur._y_label_update = True

            self.currentPen = self.pen

            cur._hovered.remove(self)

            if self._moh:
                self.show_markers = False

                if self._marker:
                    self._marker.hide()

            if self not in cur._trackers:
                cur.show_xhair(y_label_level=self.value())

            if not self._always_show_labels:
                for label in self._labels:
                    label.hide()
                    label.txt.update()
                    # label.unhighlight()

            self.mouseHovering = False

        self.update()


def level_line(
    chart: 'ChartPlotWidget',  # noqa
    level: float,

    # line style
    dotted: bool = False,
    color: str = 'default',

    # ux
    hl_on_hover: bool = True,

    # label fields and options
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

        label = Label(

            view=line.getViewBox(),

            # by default we only display the line's level value
            # in the label
            fmt_str=('{level:,.{level_digits}f}'),
            color=color,
        )

        # anchor to right side (of view ) label
        label.set_x_anchor_func(
            right_axis(
                chart,
                label,
                side='left',  # side of axis
                offset=0,
                avoid_book=False,
            )
        )

        # add to label set which will be updated on level changes
        line._labels.append(label)

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
    size_digits: int = 1,
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
        use_marker_margin=True,
        # only_show_markers_on_hover=True,
        **line_kwargs
    )

    if show_markers:
        font_size = _font.font.pixelSize()

        # scale marker size with dpi-aware font size
        arrow_size = floor(1.375 * font_size)
        alert_size = arrow_size * 0.666

        # add arrow marker on end of line nearest y-axis
        marker_style, marker_size = {
            'buy': ('|<', arrow_size),
            'sell': ('>|', arrow_size),
            'alert': ('v', alert_size),
        }[action]

        # this fixes it the artifact issue! .. of course, bounding rect stuff
        line._maxMarkerSize = marker_size

        # use ``QPathGraphicsItem``s to draw markers in scene coords
        # instead of the old way that was doing the same but by
        # resetting the graphics item transform intermittently

        # the old way which is still somehow faster?
        path = QGraphicsPathItem(
            mk_marker_path(
                marker_style,
                # the "position" here is now ignored since we modified
                # internals to pin markers to the right end of the line
                # marker_size,

                # uncommment for the old transform / .paint() marker method
                # use_qgpath=False,
            )
        )
        path.scale(marker_size, marker_size)

        # XXX: this is our new approach but seems slower?
        path = line.add_marker(path)

        # XXX: old
        # path = line.add_marker(mk_marker(marker_style, marker_size))

        line._marker = path

        assert not line.markers

        # # manually append for later ``InfiniteLine.paint()`` drawing
        # # XXX: this was manually tested as faster then using the
        # # QGraphicsItem around a painter path.. probably needs further
        # # testing to figure out why tf that's true.
        # line.markers.append((path, 0, marker_size))

    orient_v = 'top' if action == 'sell' else 'bottom'

    if action == 'alert':

        llabel = Label(

            view=line.getViewBox(),
            color=line.color,

            # completely different labelling for alerts
            fmt_str='alert => {level}',
        )

        # for now, we're just duplicating the label contents i guess..
        line._labels.append(llabel)

        # anchor to left side of view / line
        llabel.set_x_anchor_func(vbr_left(llabel))

        llabel.fields = {
            'level': level,
            'level_digits': level_digits,
        }
        llabel.orient_v = orient_v
        llabel.render()
        llabel.show()

        path.label = llabel

    else:

        rlabel = Label(

            view=line.getViewBox(),

            # display the order pos size, which is some multiple
            # of the user defined base unit size
            fmt_str=('x{size:.0f}'),
            # fmt_str=('{size:.{size_digits}f}'),  # old
            color=line.color,
        )
        path.label = rlabel

        rlabel.scene_anchor = partial(
            gpath_pin,
            location_description='right-of-path-centered',
            gpath=path,
            label=rlabel,
        )

        line._labels.append(rlabel)

        rlabel.fields = {
            'size': size,
            # 'size_digits': size_digits,
        }

        rlabel.orient_v = orient_v
        rlabel.render()
        rlabel.show()

    # sanity check
    line.update_labels({'level': level})

    return line
