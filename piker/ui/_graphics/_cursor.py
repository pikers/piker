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
import math
from typing import Optional, Tuple, Set, Dict

import inspect
import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QPointF, QRectF

from .._style import (
    _xaxis_at,
    hcolor,
    _font,
)
from .._axes import YAxisLabel, XAxisLabel
from ...log import get_logger


log = get_logger(__name__)

# XXX: these settings seem to result in really decent mouse scroll
# latency (in terms of perceived lag in cross hair) so really be sure
# there's an improvement if you want to change it!
_mouse_rate_limit = 60  # TODO; should we calc current screen refresh rate?
_debounce_delay = 1 / 2e3
_ch_label_opac = 1


# TODO: we need to handle the case where index is outside
# the underlying datums range
class LineDot(pg.CurvePoint):

    def __init__(
        self,
        curve: pg.PlotCurveItem,
        index: int,
        plot: 'ChartPlotWidget',  # type: ingore # noqa
        pos=None,
        size: int = 6,  # in pxs
        color: str = 'default_light',
    ) -> None:
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
        dot = self.dot = QtGui.QGraphicsEllipseItem(
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
    ) -> None:
        if not isinstance(
            ev, QtCore.QDynamicPropertyChangeEvent
        ) or self.curve() is None:
            return False

        (x, y) = self.curve().getData()
        index = self.property('index')
        # first = self._plot._ohlc[0]['index']
        # first = x[0]
        # i = index - first
        i = index - x[0]
        if i > 0 and i < len(y):
            newPos = (index, y[i])
            QtGui.QGraphicsItem.setPos(self, *newPos)
            return True

        return False


# TODO: change this into our own ``Label``
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
        chart: 'ChartPlotWidget',  # noqa
        anchor_at: str = ('top', 'right'),
        justify_text: str = 'left',
        font_size: Optional[int] = None,
    ) -> None:
        font_size = font_size or _font.font.pixelSize()

        super().__init__(
            justify=justify_text,
            size=f'{str(font_size)}px'
        )

        if _font._physical_dpi >= 97:
            # ad-hoc scale it based on boundingRect
            # TODO: need proper fix for this?
            typical_br = _font._qfm.boundingRect('Qyp')
            anchor_font_size = math.ceil(typical_br.height() * 1.25)

        else:
            anchor_font_size = font_size

        # anchor to viewbox
        self.setParentItem(chart._vb)
        chart.scene().addItem(self)
        self.chart = chart

        v, h = anchor_at
        index = (self._corner_anchors[h], self._corner_anchors[v])
        margins = self._corner_margins[(v, h)]

        ydim = margins[1]
        if inspect.isfunction(margins[1]):
            margins = margins[0], ydim(anchor_font_size)

        self.anchor(itemPos=index, parentPos=index, offset=margins)

    def update_from_ohlc(
        self,
        name: str,
        index: int,
        array: np.ndarray,
    ) -> None:
        # this being "html" is the dumbest shit :eyeroll:
        first = array[0]['index']

        self.setText(
            "<b>i</b>:{index}<br/>"
            "<b>O</b>:{}<br/>"
            "<b>H</b>:{}<br/>"
            "<b>L</b>:{}<br/>"
            "<b>C</b>:{}<br/>"
            "<b>V</b>:{}<br/>"
            "<b>wap</b>:{}".format(
                *array[index - first][
                    ['open', 'high', 'low', 'close', 'volume', 'bar_wap']
                ],
                name=name,
                index=index,
            )
        )

    def update_from_value(
        self,
        name: str,
        index: int,
        array: np.ndarray,
    ) -> None:
        first = array[0]['index']
        if index < array[-1]['index'] and index > first:
            data = array[index - first][name]
            self.setText(f"{name}: {data:.2f}")


class Cursor(pg.GraphicsObject):

    def __init__(
        self,
        linkedsplitcharts: 'LinkedSplitCharts',  # noqa
        digits: int = 0
    ) -> None:
        super().__init__()
        # XXX: not sure why these are instance variables?
        # It's not like we can change them on the fly..?
        self.pen = pg.mkPen(
            color=hcolor('default'),
            style=QtCore.Qt.DashLine,
        )
        self.lines_pen = pg.mkPen(
            color=hcolor('davies'),
            style=QtCore.Qt.DashLine,
        )
        self.lsc = linkedsplitcharts
        self.graphics: Dict[str, pg.GraphicsObject] = {}
        self.plots: List['PlotChartWidget'] = []  # type: ignore # noqa
        self.active_plot = None
        self.digits: int = digits
        self._datum_xy: Tuple[int, float] = (0, 0)

        self._hovered: Set[pg.GraphicsObject] = set()
        self._trackers: Set[pg.GraphicsObject] = set()

        # value used for rounding y-axis discreet tick steps
        # computing once, up front, here cuz why not
        self._y_incr_mult = 1 / self.lsc._symbol.tick_size

        # line width in view coordinates
        self._lw = self.pixelWidth() * self.lines_pen.width()

        # xhair label's color name
        self.label_color: str = 'default'

        self._y_label_update: bool = True

    def add_hovered(
        self,
        item: pg.GraphicsObject,
    ) -> None:
        assert getattr(item, 'delete'), f"{item} must define a ``.delete()``"
        self._hovered.add(item)

    def add_plot(
        self,
        plot: 'ChartPlotWidget',  # noqa
        digits: int = 0,
    ) -> None:
        # add ``pg.graphicsItems.InfiniteLine``s
        # vertical and horizonal lines and a y-axis label

        vl = plot.addLine(x=0, pen=self.lines_pen, movable=False)
        vl.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

        hl = plot.addLine(y=0, pen=self.lines_pen, movable=False)
        hl.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)
        hl.hide()

        yl = YAxisLabel(
            chart=plot,
            parent=plot.getAxis('right'),
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

        self.xaxis_label = XAxisLabel(
            parent=self.plots[plot_index].getAxis('bottom'),
            opacity=_ch_label_opac,
            bg_color=self.label_color,
        )
        # place label off-screen during startup
        self.xaxis_label.setPos(self.plots[0].mapFromView(QPointF(0, 0)))

    def add_curve_cursor(
        self,
        plot: 'ChartPlotWidget',  # noqa
        curve: 'PlotCurveItem',  # noqa
    ) -> LineDot:
        # if this plot contains curves add line dot "cursors" to denote
        # the current sample under the mouse
        cursor = LineDot(
            curve,
            index=plot._ohlc[-1]['index'],
            plot=plot
        )
        plot.addItem(cursor)
        self.graphics[plot].setdefault('cursors', []).append(cursor)
        return cursor

    def mouseAction(self, action, plot):  # noqa
        log.debug(f"{(action, plot.name)}")
        if action == 'Enter':
            self.active_plot = plot

            # show horiz line and y-label
            self.graphics[plot]['hl'].show()
            self.graphics[plot]['yl'].show()

        else:  # Leave

            # hide horiz line and y-label
            self.graphics[plot]['hl'].hide()
            self.graphics[plot]['yl'].hide()

    def mouseMoved(
        self,
        evt: 'Tuple[QMouseEvent]',  # noqa
    ) -> None:  # noqa
        """Update horizonal and vertical lines when mouse moves inside
        either the main chart or any indicator subplot.
        """
        pos = evt[0]

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

        # round y value to nearest tick step
        m = self._y_incr_mult
        iy = round(y * m) / m

        # px perfect...
        line_offset = self._lw / 2

        # update y-range items
        if iy != last_iy:

            if self._y_label_update:
                self.graphics[self.active_plot]['yl'].update_label(
                    abs_pos=plot.mapFromView(QPointF(ix, iy)),
                    value=iy
                )

                # only update horizontal xhair line if label is enabled
                self.graphics[plot]['hl'].setY(iy)

            # update all trackers
            for item in self._trackers:
                item.on_tracked_source(ix, iy)

        if ix != last_ix:
            for plot, opts in self.graphics.items():

                # update the chart's "contents" label
                plot.update_contents_labels(ix)

                # move the vertical line to the current "center of bar"
                opts['vl'].setX(ix + line_offset)

                # update all subscribed curve dots
                for cursor in opts.get('cursors', ()):
                    cursor.setIndex(ix)

            # update the label on the bottom of the crosshair
            self.xaxis_label.update_label(

                # XXX: requires:
                # https://github.com/pyqtgraph/pyqtgraph/pull/1418
                # otherwise gobbles tons of CPU..

                # map back to abs (label-local) coordinates
                abs_pos=plot.mapFromView(QPointF(ix + line_offset, iy)),
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
        g = self.graphics[self.active_plot]
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
