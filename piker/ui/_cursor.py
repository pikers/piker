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
Mouse interaction graphics

"""
from __future__ import annotations
from functools import partial
from typing import (
    Callable,
    TYPE_CHECKING,
)

import inspect
import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import QPointF, QRectF

from ._style import (
    _xaxis_at,
    hcolor,
    _font_small,
    _font,
)
from ._axes import (
    YAxisLabel,
    XAxisLabel,
)
from ..log import get_logger

if TYPE_CHECKING:
    from ._chart import (
        ChartPlotWidget,
        LinkedSplits,
    )


log = get_logger(__name__)

# XXX: these settings seem to result in really decent mouse scroll
# latency (in terms of perceived lag in cross hair) so really be sure
# there's an improvement if you want to change it!

_mouse_rate_limit = 60  # TODO; should we calc current screen refresh rate?
_debounce_delay = 0
_ch_label_opac = 1


# TODO: we need to handle the case where index is outside
# the underlying datums range
class LineDot(pg.CurvePoint):

    def __init__(
        self,

        curve: pg.PlotCurveItem,
        index: int,

        plot: ChartPlotWidget,  # type: ingore # noqa
        pos=None,
        color: str = 'bracket',

    ) -> None:
        # scale from dpi aware font size
        size = int(_font.px_size * 0.375)

        pg.CurvePoint.__init__(
            self,
            curve,
            index=index,
            pos=pos,
            rotate=False,
        )
        self._plot = plot

        # TODO: get pen from curve if not defined?
        cdefault = hcolor(color)
        pen = pg.mkPen(cdefault)
        brush = pg.mkBrush(cdefault)

        # presuming this is fast since it's built in?
        dot = self.dot = QtWidgets.QGraphicsEllipseItem(
            QtCore.QRectF(-size / 2, -size / 2, size, size)
        )
        # if we needed transformable dot?
        # dot.translate(-size*0.5, -size*0.5)
        dot.setPen(pen)
        dot.setBrush(brush)
        dot.setParentItem(self)

        # keep a static size
        self.setFlag(self.ItemIgnoresTransformations)

    def event(
        self,
        ev: QtCore.QEvent,

    ) -> bool:

        if (
            not isinstance(ev, QtCore.QDynamicPropertyChangeEvent)
            or self.curve() is None
        ):
            return False

        # TODO: get rid of this ``.getData()`` and
        # make a more pythonic api to retreive backing
        # numpy arrays...
        # (x, y) = self.curve().getData()
        # index = self.property('index')
        # # first = self._plot._arrays['ohlc'][0]['index']
        # # first = x[0]
        # # i = index - first
        # if index:
        #     i = round(index - x[0])
        #     if i > 0 and i < len(y):
        #         newPos = (index, y[i])
        #         QtWidgets.QGraphicsItem.setPos(
        #             self,
        #             *newPos,
        #         )
        #         return True

        return False


# TODO: change this into our own ``_label.Label``
class ContentsLabel(pg.LabelItem):
    """Label anchored to a ``ViewBox`` typically for displaying
    datum-wise points from the "viewed" contents.

    """
    _corner_anchors = {
        'top': 0,
        'left': 0,
        'bottom': 1,
        'right': 1,
    }

    # XXX: fyi naming here is confusing / opposite to coords
    _corner_margins = {
        ('top', 'left'): (-2, lambda font_size: -font_size*0.25),
        ('top', 'right'): (2, lambda font_size: -font_size*0.25),

        ('bottom', 'left'): (-2, lambda font_size: font_size),
        ('bottom', 'right'): (2, lambda font_size: font_size),
    }

    def __init__(
        self,

        # chart: ChartPlotWidget,  # noqa
        view: pg.ViewBox,

        anchor_at: str = ('top', 'right'),
        justify_text: str = 'left',
        font_size: int | None = None,

    ) -> None:

        font_size = font_size or _font_small.px_size

        super().__init__(
            justify=justify_text,
            size=f'{str(font_size)}px'
        )

        # anchor to viewbox
        self.setParentItem(view)

        self.vb = view
        view.scene().addItem(self)

        v, h = anchor_at
        index = (self._corner_anchors[h], self._corner_anchors[v])
        margins = self._corner_margins[(v, h)]

        ydim = margins[1]
        if inspect.isfunction(margins[1]):
            margins = margins[0], ydim(font_size)

        self.anchor(itemPos=index, parentPos=index, offset=margins)

    def update_from_ohlc(
        self,

        name: str,
        ix: int,
        array: np.ndarray,

    ) -> None:
        # this being "html" is the dumbest shit :eyeroll:

        self.setText(
            "<b>i</b>:{index}<br/>"
            # NB: these fields must be indexed in the correct order via
            # the slice syntax below.
            "<b>epoch</b>:{}<br/>"
            "<b>O</b>:{}<br/>"
            "<b>H</b>:{}<br/>"
            "<b>L</b>:{}<br/>"
            "<b>C</b>:{}<br/>"
            "<b>V</b>:{}<br/>"
            "<b>wap</b>:{}".format(
                *array[ix][
                    [
                        'time',
                        'open',
                        'high',
                        'low',
                        'close',
                        'volume',
                        'bar_wap',
                    ]
                ],
                name=name,
                index=ix,
            )
        )

    def update_from_value(
        self,

        name: str,
        ix: int,
        array: np.ndarray,

    ) -> None:
        data = array[ix][name]
        self.setText(f"{name}: {data:.2f}")


class ContentsLabels:
    '''Collection of labels that span a ``LinkedSplits`` set of chart plots
    and can be updated from the underlying data from an x-index value sent
    as input from a cursor or other query mechanism.

    '''
    def __init__(
        self,
        linkedsplits: LinkedSplits,  # type: ignore # noqa

    ) -> None:

        self.linkedsplits = linkedsplits
        self._labels: list[(
            'CharPlotWidget',  # type: ignore # noqa
            str,
            ContentsLabel,
            Callable
        )] = []

    def update_labels(
        self,
        x_in: int,

    ) -> None:
        for chart, name, label, update in self._labels:

            viz = chart.get_viz(name)
            array = viz.shm.array
            index = array[viz.index_field]
            start = index[0]
            stop = index[-1]

            if not (
                x_in >= start
                and x_in <= stop
            ):
                # out of range
                print('WTF out of range?')
                continue

            # call provided update func with data point
            try:
                label.show()
                ix = np.searchsorted(index, x_in)
                if ix > len(array):
                    breakpoint()
                update(ix, array)

            except IndexError:
                log.exception(f"Failed to update label: {name}")

    def hide(self) -> None:
        for chart, name, label, update in self._labels:
            label.hide()

    def add_label(

        self,
        chart: ChartPlotWidget,  # type: ignore # noqa
        name: str,
        anchor_at: tuple[str, str] = ('top', 'left'),
        update_func: Callable = ContentsLabel.update_from_value,

    ) -> ContentsLabel:

        label = ContentsLabel(
            view=chart.view,
            anchor_at=anchor_at,
        )
        self._labels.append(
            (chart, name, label, partial(update_func, label, name))
        )
        label.hide()

        return label


class Cursor(pg.GraphicsObject):
    '''
    Multi-plot cursor for use on a ``LinkedSplits`` chart (set).

    '''
    def __init__(

        self,
        linkedsplits: LinkedSplits,  # noqa
        digits: int = 0

    ) -> None:

        super().__init__()

        self.linked = linkedsplits
        self.graphics: dict[str, pg.GraphicsObject] = {}
        self.xaxis_label: XAxisLabel | None = None
        self.always_show_xlabel: bool = True
        self.plots: list['PlotChartWidget'] = []  # type: ignore # noqa
        self.active_plot = None
        self.digits: int = digits
        self._datum_xy: tuple[int, float] = (0, 0)

        self._hovered: set[pg.GraphicsObject] = set()
        self._trackers: set[pg.GraphicsObject] = set()

        # XXX: not sure why these are instance variables?
        # It's not like we can change them on the fly..?
        self.pen = pg.mkPen(
            color=hcolor('bracket'),
            style=QtCore.Qt.DashLine,
        )
        self.lines_pen = pg.mkPen(
            color=hcolor('davies'),
            style=QtCore.Qt.DashLine,
        )

        # value used for rounding y-axis discreet tick steps
        # computing once, up front, here cuz why not
        self._y_incr_mult = 1 / self.linked._symbol.tick_size

        # line width in view coordinates
        self._lw = self.pixelWidth() * self.lines_pen.width()

        # xhair label's color name
        self.label_color: str = 'bracket'

        self._y_label_update: bool = True

        self.contents_labels = ContentsLabels(self.linked)
        self._in_query_mode: bool = False

    @property
    def in_query_mode(self) -> bool:
        return self._in_query_mode

    @in_query_mode.setter
    def in_query_mode(self, value: bool) -> None:
        if self._in_query_mode and not value:

            # edge trigger "off" hide all labels
            self.contents_labels.hide()

        elif not self._in_query_mode and value:
            # edge trigger "on" hide all labels
            self.contents_labels.update_labels(self._datum_xy[0])

        self._in_query_mode = value

    def add_hovered(
        self,
        item: pg.GraphicsObject,
    ) -> None:
        assert getattr(item, 'delete'), f"{item} must define a ``.delete()``"
        self._hovered.add(item)

    def add_plot(
        self,
        plot: ChartPlotWidget,  # noqa
        digits: int = 0,

    ) -> None:
        '''
        Add chart to tracked set such that a cross-hair and possibly
        curve tracking cursor can be drawn on the plot.

        '''
        # add ``pg.graphicsItems.InfiniteLine``s
        # vertical and horizonal lines and a y-axis label

        vl = plot.addLine(x=0, pen=self.lines_pen, movable=False)
        vl.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        hl = plot.addLine(y=0, pen=self.lines_pen, movable=False)
        hl.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        hl.hide()

        yl = YAxisLabel(
            pi=plot.plotItem,
            # parent=plot.getAxis('right'),
            parent=plot.pi_overlay.get_axis(plot.plotItem, 'right'),
            digits=digits or self.digits,
            opacity=_ch_label_opac,
            bg_color=self.label_color,
        )
        yl.hide()  # on startup if mouse is off screen

        # TODO: checkout what ``.sigDelayed`` can be used for
        # (emitted once a sufficient delay occurs in mouse movement)
        px_moved = pg.SignalProxy(
            plot.scene().sigMouseMoved,
            rateLimit=_mouse_rate_limit,
            slot=self.mouseMoved,
            delay=_debounce_delay,
        )

        px_enter = pg.SignalProxy(
            plot.sig_mouse_enter,
            rateLimit=_mouse_rate_limit,
            slot=lambda: self.mouseAction('Enter', plot),
            delay=_debounce_delay,
        )
        px_leave = pg.SignalProxy(
            plot.sig_mouse_leave,
            rateLimit=_mouse_rate_limit,
            slot=lambda: self.mouseAction('Leave', plot),
            delay=_debounce_delay,
        )
        self.graphics[plot] = {
            'vl': vl,
            'hl': hl,
            'yl': yl,
            'px': (px_moved, px_enter, px_leave),
        }
        self.plots.append(plot)

        # Determine where to place x-axis label.
        # Place below the last plot by default, ow
        # keep x-axis right below main chart
        plot_index = -1 if _xaxis_at == 'bottom' else 0

        # ONLY create an x-axis label for the cursor
        # if this plot owns the 'bottom' axis.
        # if 'bottom' in plot.plotItem.axes:
        if plot.linked.xaxis_chart is plot:
            xlabel = self.xaxis_label = XAxisLabel(
                parent=self.plots[plot_index].getAxis('bottom'),
                # parent=self.plots[plot_index].pi_overlay.get_axis(
                #     plot.plotItem, 'bottom'
                # ),

                opacity=_ch_label_opac,
                bg_color=self.label_color,
            )
            # place label off-screen during startup
            xlabel.setPos(
                self.plots[0].mapFromView(QPointF(0, 0))
            )
            xlabel.show()

    def add_curve_cursor(
        self,
        chart: ChartPlotWidget,  # noqa
        curve: 'PlotCurveItem',  # noqa

    ) -> LineDot:
        # if this chart contains curves add line dot "cursors" to denote
        # the current sample under the mouse
        main_viz = chart.get_viz(chart.name)

        # read out last index
        i = main_viz.shm.array[-1]['index']
        cursor = LineDot(
            curve,
            index=i,
            plot=chart
        )
        chart.addItem(cursor)
        self.graphics[chart].setdefault('cursors', []).append(cursor)
        return cursor

    def mouseAction(
        self,
        action: str,
        plot: ChartPlotWidget,

    ) -> None:  # noqa

        log.debug(f"{(action, plot.name)}")
        if action == 'Enter':
            self.active_plot = plot
            plot.linked.godwidget._active_cursor = self

            # show horiz line and y-label
            self.graphics[plot]['hl'].show()
            self.graphics[plot]['yl'].show()

            if (
                not self.always_show_xlabel
                and not self.xaxis_label.isVisible()
            ):
                self.xaxis_label.show()

        # Leave: hide horiz line and y-label
        else:
            self.graphics[plot]['hl'].hide()
            self.graphics[plot]['yl'].hide()

            if (
                not self.always_show_xlabel
                and self.xaxis_label.isVisible()
            ):
                self.xaxis_label.hide()

    def mouseMoved(
        self,
        coords: tuple[QPointF],  # noqa

    ) -> None:
        '''
        Update horizonal and vertical lines when mouse moves inside
        either the main chart or any indicator subplot.

        '''
        pos = coords[0]

        # find position inside active plot
        try:
            # map to view coordinate system
            mouse_point = self.active_plot.mapToView(pos)
        except AttributeError:
            # mouse was not on active plot
            return

        x, y = mouse_point.x(), mouse_point.y()
        plot = self.active_plot

        # Update x if cursor changed after discretization calc
        # (this saves draw cycles on small mouse moves)
        last_ix, last_iy = self._datum_xy

        ix = round(x)  # since bars are centered around index

        # px perfect...
        line_offset = self._lw / 2

        # round y value to nearest tick step
        m = self._y_incr_mult
        iy = round(y * m) / m
        vl_y = iy - line_offset

        # update y-range items
        if iy != last_iy:

            if self._y_label_update:
                self.graphics[self.active_plot]['yl'].update_label(
                    # abs_pos=plot.mapFromView(QPointF(ix, iy)),
                    abs_pos=plot.mapFromView(QPointF(ix, vl_y)),
                    value=iy
                )

                # only update horizontal xhair line if label is enabled
                # self.graphics[plot]['hl'].setY(iy)
                self.graphics[plot]['hl'].setY(vl_y)

            # update all trackers
            for item in self._trackers:
                item.on_tracked_source(ix, iy)

        if ix != last_ix:

            if self.in_query_mode:
                # show contents labels on all linked charts and update
                # with cursor movement
                self.contents_labels.update_labels(ix)

            vl_x = ix + line_offset
            for plot, opts in self.graphics.items():

                # move the vertical line to the current "center of bar"
                opts['vl'].setX(vl_x)

                # update all subscribed curve dots
                for cursor in opts.get('cursors', ()):
                    cursor.setIndex(ix)

                # Update the label on the bottom of the crosshair.
                # TODO: make this an up-front calc that we update
                # on axis-widget resize events instead of on every mouse
                # update cylce.

                # left axis offset width for calcuating
                # absolute x-axis label placement.
                left_axis_width = 0
                if len(plot.pi_overlay.overlays):
                    # breakpoint()
                    lefts = plot.pi_overlay.get_axes('left')
                    if lefts:
                        for left in lefts:
                            left_axis_width += left.width()

                # map back to abs (label-local) coordinates
                if (
                    self.always_show_xlabel
                    or self.xaxis_label.isVisible()
                ):
                    self.xaxis_label.update_label(
                        abs_pos=(
                            plot.mapFromView(QPointF(vl_x, iy)) -
                            QPointF(left_axis_width, 0)
                        ),
                        value=ix,
                    )

        self._datum_xy = ix, iy

    def boundingRect(self) -> QRectF:
        try:
            return self.active_plot.boundingRect()
        except AttributeError:
            return self.plots[0].boundingRect()

    def show_xhair(
        self,
        y_label_level: float = None,
    ) -> None:

        plot = self.active_plot
        if not plot:
            return

        g = self.graphics[plot]
        # show horiz line and y-label
        g['hl'].show()
        g['vl'].show()

        self._y_label_update = True
        yl = g['yl']
        # yl.fg_color = pg.mkColor(hcolor('black'))
        # yl.bg_color = pg.mkColor(hcolor(self.label_color))
        if y_label_level:
            yl.update_from_data(0, y_label_level, _save_last=False)

        yl.show()

    def hide_xhair(
        self,
        hide_label: bool = False,
        y_label_level: float = None,
        just_vertical: bool = False,
        fg_color: str = None,
        # bg_color: str = 'papas_special',
    ) -> None:
        g = self.graphics[self.active_plot]

        hl = g['hl']
        if not just_vertical:
            hl.hide()

        g['vl'].hide()

        # only disable cursor y-label updates
        # if we're highlighting a line
        yl = g['yl']

        if hide_label:
            yl.hide()

        elif y_label_level:
            yl.update_from_data(0, y_label_level, _save_last=False)
            hl.setY(y_label_level)

        if fg_color is not None:
            yl.fg_color = pg.mkColor(hcolor(fg_color))
            yl.bg_color = pg.mkColor(hcolor('papas_special'))
