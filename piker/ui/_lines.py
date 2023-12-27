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
from __future__ import annotations
from functools import partial
from math import floor
from typing import (
    Callable,
    TYPE_CHECKING,
)

import pyqtgraph as pg
from pyqtgraph import Point, functions as fn
from PyQt5 import (
    QtCore,
    QtGui,
)
from PyQt5.QtWidgets import (
    QGraphicsPathItem,
    QStyleOptionGraphicsItem,
    QGraphicsItem,
    QGraphicsScene,
    QWidget,
)
from PyQt5.QtCore import QPointF

from ._annotate import LevelMarker
from ._anchors import (
    vbr_left,
    right_axis,
    gpath_pin,
)
from ..calc import humanize
from ._label import Label
from ._style import hcolor, _font

if TYPE_CHECKING:
    from ._cursor import Cursor


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

        # UX look and feel opts
        always_show_labels: bool = False,
        highlight_on_hover: bool = True,
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
        self.highlight_on_hover = highlight_on_hover
        self._dotted = dotted
        self._hide_xhair_on_hover = hide_xhair_on_hover

        # callback that can be assigned by user code
        # to get updates from each level change
        self._on_level_change: Callable[[float], None] = lambda y: None

        self._marker = None
        self.only_show_markers_on_hover = only_show_markers_on_hover
        self.track_marker_pos: bool = False

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
        self._labels: list[Label] = []
        self._markers: list[(int, Label)] = []

        # whenever this line is moved trigger label updates
        self.sigPositionChanged.connect(self.on_pos_change)

        # sets color to value triggering pen creation
        self._hl_color = highlight_color
        self.color = color

        # TODO: for when we want to move groups of lines?
        self._track_cursor: bool = False
        self.always_show_labels = always_show_labels

        self._on_drag_start = lambda lvln: None
        self._on_drag_end = lambda lvln: None

        self._y_incr_mult = float(1 / chart.linked.mkt.size_tick)
        self._right_end_sc: float = 0

        # use px caching
        self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

    def txt_offsets(self) -> tuple[int, int]:
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
        level = self.value()
        self.update_labels({'level': level})
        self.set_level(level, called_from_on_pos_change=True)

    def update_labels(
        self,
        fields_data: dict,

    ) -> None:

        for label in self._labels:

            label.color = self.color
            label.fields.update(fields_data)
            label.render()

            level = fields_data.get('level')
            if level:
                label.set_view_pos(y=level)

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
        called_from_on_pos_change: bool = False,

    ) -> None:

        if not called_from_on_pos_change:
            last = self.value()

            # if the position hasn't changed then ``.update_labels()``
            # will not be called by a non-triggered `.on_pos_change()`,
            # so we need to call it manually to avoid mismatching
            # label-to-line color when the line is updated but not
            # from a "moved" event.
            if level == last:
                self.update_labels({'level': level})

            self.setPos(level)

        self.level = self.value()
        self.update()

        # invoke any user code
        self._on_level_change(level)

    def on_tracked_source(
        self,

        x: int,
        y: float

    ) -> None:
        '''
        Chart coordinates cursor tracking callback.

        this is called by our ``Cursor`` type once this line is set to
        track the cursor: for every movement this callback is invoked to
        reposition the line with the current view coordinates.

        '''
        self.movable = True
        self.set_level(y)  # implictly calls reposition handler

    def mouseDragEvent(self, ev):
        '''
        Override the ``InfiniteLine`` handler since we need more
        detailed control and start end signalling.

        '''
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

    def get_cursor(self) -> Cursor | None:

        chart = self._chart
        cur = chart.linked.cursor
        if self in cur._hovered:
            return cur

        return None

    def delete(self) -> None:
        '''
        Remove this line from containing chart/view/scene.

        '''
        scene: QGraphicsScene = self.scene()
        if scene:
            for label in self._labels:
                label.delete()

            # gc managed labels?
            self._labels.clear()

            if self._marker:
                self.scene().removeItem(self._marker)

        # remove from chart/cursor states
        chart = self._chart
        cur = self.get_cursor()
        if cur:
            cur._hovered.remove(self)

        chart.plotItem.removeItem(self)

    def mouseDoubleClickEvent(
        self,
        ev: QtGui.QMouseEvent,

    ) -> None:
        # TODO: enter labels edit mode
        print(f'double click {ev}')

    def paint(
        self,

        p: QtGui.QPainter,
        opt: QStyleOptionGraphicsItem,
        w: QWidget

    ) -> None:
        '''
        Core paint which we override (yet again)
        from pg..

        '''
        p.setRenderHint(p.Antialiasing)

        # these are in viewbox coords
        vb_left, vb_right = self._endPoints
        vb = self.getViewBox()

        line_end, marker_right, r_axis_x = self._chart.marker_right_points()

        # (legacy) NOTE: at one point this seemed slower when moving around
        # order lines.. not sure if that's still true or why but we've
        # dropped the original hacky `.pain()` transform stuff for inf
        # line markers now - check the git history if it needs to be
        # reverted.
        if self._marker:
            if self.track_marker_pos:
                # make the line end at the marker's x pos
                line_end = marker_right = self._marker.pos().x()

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
        mkr = self._marker
        if mkr:
            mkr.hide()

    def show(self) -> None:
        super().show()
        if self._marker:
            self._marker.show()

    def scene_y(self) -> float:
        return self.getViewBox().mapFromView(
            Point(0, self.value())
        ).y()

    def scene_endpoint(self) -> QPointF:

        if not self._right_end_sc:
            line_end, _, _ = self._chart.marker_right_points()
            self._right_end_sc = line_end - 10

        return QPointF(self._right_end_sc, self.scene_y())

    def add_marker(
        self,
        path: QGraphicsPathItem,

    ) -> QGraphicsPathItem:

        self._marker = path
        self._marker.setPen(self.currentPen)
        self._marker.setBrush(fn.mkBrush(self.currentPen.color()))
        # add path to scene
        self.getViewBox().scene().addItem(path)

        # place to just-left of L1 labels
        rsc = self._chart.pre_l1_xs()[0]
        path.setPos(QPointF(rsc, self.scene_y()))

        return path

    @property
    def marker(self) -> LevelMarker:
        return self._marker

    def hoverEvent(self, ev):
        '''
        Mouse hover callback.

        '''
        cur = self._chart.linked.cursor

        # hovered
        if (
            not ev.isExit()
            and ev.acceptDrags(QtCore.Qt.LeftButton)
        ):
            # if already hovered we don't need to run again
            if self.mouseHovering is True:
                return

            if self.only_show_markers_on_hover:
                self.show_markers()

            # highlight if so configured
            if self.highlight_on_hover:

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

            if self.only_show_markers_on_hover:
                self.hide_markers()

            if self not in cur._trackers:
                cur.show_xhair(y_label_level=self.value())

            if not self.always_show_labels:
                self.hide_labels()

            self.mouseHovering = False

        self.update()

    def hide_markers(self) -> None:
        if self._marker:
            self._marker.hide()
            self._marker.label.hide()

    def show_markers(self) -> None:
        if self._marker:
            self._marker.show()


def level_line(

    chart: 'ChartPlotWidget',  # noqa
    level: float,

    # line style
    dotted: bool = False,
    color: str = 'default',

    # ux
    highlight_on_hover: bool = True,

    # label fields and options
    always_show_labels: bool = False,
    add_label: bool = True,
    orient_v: str = 'bottom',
    **kwargs,

) -> LevelLine:
    '''
    Convenience routine to add a styled horizontal line to a plot.

    '''
    hl_color = color + '_light' if highlight_on_hover else color

    line = LevelLine(
        chart,
        color=color,

        # lookup "highlight" equivalent
        highlight_color=hl_color,

        dotted=dotted,

        # UX related options
        highlight_on_hover=highlight_on_hover,

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

        # keep pp label details private until
        # the user edge triggers "order mode"
        line.hide_labels()

    # activate/draw label
    line.set_level(level)

    return line


def order_line(

    chart,
    level: float,
    action: str | None = 'buy',  # buy or sell

    marker_style: str | None = None,
    level_digits: float | None = 3,
    size: int | None = 1,
    size_digits: int = 1,
    show_markers: bool = False,
    submit_price: float = None,
    orient_v: str = 'bottom',

    **line_kwargs,

) -> LevelLine:
    '''
    Convenience routine to add a line graphic representing an order
    execution submitted to the EMS via the chart's "order mode".

    '''
    line = level_line(
        chart,
        level,
        add_label=False,
        use_marker_margin=True,
        **line_kwargs
    )

    font_size = _font.font.pixelSize()

    # scale marker size with dpi-aware font size
    marker_size = floor(1.375 * font_size)

    orient_v = 'top' if action == 'sell' else 'bottom'

    if action == 'alert':

        label = Label(

            view=line.getViewBox(),
            color=line.color,

            # completely different labelling for alerts
            fmt_str='alert => {level}',
        )

        # for now, we're just duplicating the label contents i guess..
        line._labels.append(label)

        # anchor to left side of view / line
        label.set_x_anchor_func(vbr_left(label))

        label.fields = {
            'level': level,
            'level_digits': level_digits,
        }

        marker_size = marker_size * 0.666

    else:
        view = line.getViewBox()

        # far-side label
        label = Label(
            view=view,
            # display the order pos size, which is some multiple
            # of the user defined base unit size
            fmt_str=(
                '{account_text}{size:.{size_digits}f}u{fiat_text}'
            ),
            color=line.color,
        )

        label.set_x_anchor_func(vbr_left(label))

        line._labels.append(label)

        def maybe_show_fiat_text(fields: dict) -> str:
            fiat_size = fields.get('fiat_size')
            if not fiat_size:
                return ''

            return f' ~ ${humanize(fiat_size)}'

        def maybe_show_account_name(fields: dict) -> str:
            account = fields.get('account')
            if not account:
                return ''

            return f'{account}: '

        label.fields = {
            'size': size,
            'size_digits': 0,
            'fiat_size': None,
            'fiat_text': maybe_show_fiat_text,
            'account': None,
            'account_text': maybe_show_account_name,
        }

    label.orient_v = orient_v
    label.render()
    label.show()

    if show_markers:
        # add arrow marker on end of line nearest y-axis
        marker_style = marker_style or {
            'buy': '|<',
            'sell': '>|',
            'alert': 'v',
        }[action]

        # the old way which is still somehow faster?
        marker = LevelMarker(
            chart=chart,
            style=marker_style,
            get_level=line.value,  # callback
            size=marker_size,
            keep_in_view=False,
        )

        # XXX: this is our new approach but seems slower?
        marker = line.add_marker(marker)

        # XXX: DON'T COMMENT THIS!
        # this fixes it the artifact issue!
        # .. of course, bounding rect stuff
        line._maxMarkerSize = marker_size

        assert line._marker is marker
        assert not line.markers

        # above we use ``QPathGraphicsItem``s directly to draw markers
        # in scene coords instead of the way ``InfiniteLine`` does it
        # internally: by resetting the graphics item transform
        # intermittently inside ``.paint()`` which we've copied and
        # seperated as ``qgo_draw_markers()`` if we ever want to go back
        # to it; likely we can remove this.

        # manually append for later ``InfiniteLine.paint()`` drawing
        # XXX: this was manually tested as faster then using the
        # QGraphicsItem around a painter path.. probably needs further
        # testing to figure out why tf that's true.
        # line.markers.append((marker, 0, marker_size))

        if action != 'alert':

            # add a partial position label if we also added a level
            # marker
            pp_size_label = Label(
                view=view,
                color=line.color,

                # this is "static" label
                # update_on_range_change=False,
                fmt_str='\n'.join((
                    '{slots_used:.1f}x',
                )),

                fields={
                    'slots_used': 0,
                },
            )
            pp_size_label.render()
            pp_size_label.show()

            line._labels.append(pp_size_label)

            # TODO: pretty sure one of the reasons these "label
            # updatess" are a bit "jittery" is because we aren't
            # leveraging the "scene coordinates hierarchy" stuff:
            # i.e. using some parent object as the coord "origin"
            # which i presume would result in better pixel caching
            # results? def something to dig into..
            pp_size_label.scene_anchor = partial(
                gpath_pin,
                gpath=marker,
                label=pp_size_label,
            )
            # XXX: without this the pp proportion label next the marker
            # seems to lag?  this is the same issue we had with position
            # lines which we handle with ``.update_graphcis()``.
            marker._on_paint = lambda marker: pp_size_label.update()

        # XXX: THIS IS AN UNTYPED MONKEY PATCH!?!?!
        marker.label = label

    # sanity check
    line.update_labels({'level': level})

    return line


# TODO: should probably consider making this a more general
# purpose class method on the type?
def copy_from_order_line(

    chart: 'ChartPlotWidget',  # noqa
    line: LevelLine

) -> LevelLine:

    return order_line(

        chart,

        # label fields default values
        level=line.value(),
        marker_style=line._marker.style,

        # LevelLine kwargs
        color=line.color,
        dotted=line._dotted,

        show_markers=line.show_markers,
        only_show_markers_on_hover=line.only_show_markers_on_hover,
    )
