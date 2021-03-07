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
from PyQt5 import QtCore, QtGui

from .._label import Label, vbr_left, right_axis
from .._style import (
    hcolor,
    _down_2_font_inches_we_like,
)


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

    ) -> None:

        super().__init__(
            movable=True,
            angle=0,
            label=None,  # don't use the shitty ``InfLineLabel``
        )

        self._chart = chart
        self._hoh = hl_on_hover
        self._dotted = dotted

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

        # whenever this line is moved trigger label updates
        self.sigPositionChanged.connect(self.on_pos_change)

        # sets color to value triggering pen creation
        self.color = color

        # TODO: for when we want to move groups of lines?
        self._track_cursor: bool = False
        self._always_show_labels = always_show_labels

        self._on_drag_start = lambda l: None
        self._on_drag_end = lambda l: None

        # testing markers
        # self.addMarker('<|', 0.1, 3)
        # self.addMarker('<|>', 0.2, 3)
        # self.addMarker('>|', 0.3, 3)
        # self.addMarker('>|<', 0.4, 3)
        # self.addMarker('>|<', 0.5, 3)
        # self.addMarker('^', 0.6, 3)
        # self.addMarker('v', 0.7, 3)
        # self.addMarker('o', 0.8, 3)

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
        hoverpen = pg.mkPen(hcolor(color + '_light'))

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

        font_size_inches: float = _down_2_font_inches_we_like,
        color: str = None,
        bg_color: str = None,

        **label_kwargs,
    ) -> Label:
        """Add a ``LevelLabel`` anchored at one of the line endpoints in view.

        """
        vb = self.getViewBox()

        label = Label(
            view=vb,
            fmt_str=fmt_str,
            color=self.color,
        )

        if side == 'right':
            label.set_x_anchor_func(right_axis(self._chart, label))
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

    def setMouseHover(self, hover: bool) -> None:
        """Mouse hover callback.

        """
        # XXX: currently we'll just return if _hoh is False
        if self.mouseHovering == hover:
            return

        self.mouseHovering = hover

        chart = self._chart

        if hover:
            # highlight if so configured
            if self._hoh:
                self.currentPen = self.hoverPen

                # for at, label in self._labels:
                #     label.highlight(self.hoverPen)

            # add us to cursor state
            cur = chart._cursor
            cur.add_hovered(self)
            cur.graphics[chart]['yl'].hide()
            cur.graphics[chart]['hl'].hide()

            for at, label in self._labels:
                label.show()

            # TODO: hide y-crosshair?
            # chart._cursor.graphics[chart]['hl'].hide()

            # self.setCursor(QtCore.Qt.OpenHandCursor)
            # self.setCursor(QtCore.Qt.DragMoveCursor)
        else:
            self.currentPen = self.pen

            cur = chart._cursor
            cur._hovered.remove(self)

            if self not in cur._trackers:
                g = cur.graphics[chart]
                g['yl'].show()
                g['hl'].show()

            if not self._always_show_labels:
                for at, label in self._labels:
                    label.hide()
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
        for at, label in self._labels:
            # label.highlight(self.hoverPen)
            label.show()

        # XXX: normal tracking behavior pulled out from parent type
        if self.movable and ev.button() == QtCore.Qt.LeftButton:

            if ev.isStart():
                self.moving = True
                self.cursorOffset = self.pos() - self.mapToParent(
                    ev.buttonDownPos())
                self.startPosition = self.pos()
                self._on_drag_start(self)

            ev.accept()

            if not self.moving:
                return

            self.setPos(self.cursorOffset + self.mapToParent(ev.pos()))
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

) -> LevelLine:
    """Convenience routine to add a styled horizontal line to a plot.

    """

    line = LevelLine(
        chart,
        color=color,
        # lookup "highlight" equivalent
        highlight_color=color + '_light',

        dotted=dotted,

        # UX related options
        hl_on_hover=hl_on_hover,

        # when set to True the label is always shown instead of just on
        # highlight (which is a privacy thing for orders)
        always_show_labels=always_show_labels,
    )

    chart.plotItem.addItem(line)

    if add_label:

        label = line.add_label(
            side='right',
            opacity=1,
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

    size: Optional[int] = 1,
    size_digits: int = 0,

    submit_price: float = None,

    order_status: str = 'dark',
    order_type: str = 'limit',

    opacity=0.616,

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

    llabel = line.add_label(
        side='left',
        fmt_str='{order_status}-{order_type}:{submit_price}',
    )
    llabel.fields = {
        'order_status': order_status,
        'order_type': order_type,
        'submit_price': submit_price,
    }
    llabel.orient_v = orient_v
    llabel.render()
    llabel.show()

    rlabel = line.add_label(
        side='right',
        fmt_str=(
            '{size:.{size_digits}f} x '
            '{level:,.{level_digits}f}'
        ),
    )
    rlabel.fields = {
        'size': size,
        'size_digits': size_digits,
        'level': level,
        'level_digits': level_digits,
    }

    rlabel.orient_v = orient_v
    rlabel.render()
    rlabel.show()

    # sanity check
    line.update_labels({'level': level})

    return line
