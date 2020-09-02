"""
Chart axes graphics and behavior.
"""
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QPointF


# from .quantdom.base import Quotes
from .quantdom.utils import fromtimestamp
from ._style import _font, hcolor


class PriceAxis(pg.AxisItem):

    def __init__(
        self,
        # chart: 'ChartPlotWidget',
    ) -> None:
        super().__init__(orientation='right')
        self.setStyle(**{
            'textFillLimits': [(0, 0.5)],
            # 'tickTextWidth': 10,
            # 'tickTextHeight': 25,
            # 'autoExpandTextSpace': True,
            # 'maxTickLength': -20,
            # 'stopAxisAtTick': (True, True),
        })
        self.setLabel(**{'font-size': '10pt'})
        self.setTickFont(_font)
        self.setWidth(150)
        # self.chart = chart
        # accesed normally via
        # .getAxis('right')

    # XXX: drop for now since it just eats up h space

    # def tickStrings(self, vals, scale, spacing):
    #     digts = max(0, np.ceil(-np.log10(spacing * scale)))
    #     return [
    #         ('{:<8,.%df}' % digts).format(v).replace(',', ' ') for v in vals
    #     ]


class DynamicDateAxis(pg.AxisItem):
    tick_tpl = {'D1': '%Y-%b-%d'}

    def __init__(self, linked_charts, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.linked_charts = linked_charts
        self.setTickFont(_font)

        # default styling
        self.setStyle(
            tickTextOffset=7,
            textFillLimits=[(0, 0.70)],
            # TODO: doesn't seem to work -> bug in pyqtgraph?
            # tickTextHeight=2,
        )
        # self.setHeight(35)

    def tickStrings(self, values, scale, spacing):
        # if len(values) > 1 or not values:
        #     values = Quotes.time

        # strings = super().tickStrings(values, scale, spacing)
        s_period = 'D1'
        strings = []
        bars = self.linked_charts.chart._array
        quotes_count = len(bars) - 1

        for ibar in values:
            if ibar > quotes_count:
                return strings
            dt_tick = fromtimestamp(bars[int(ibar)]['time'])
            strings.append(
                dt_tick.strftime(self.tick_tpl[s_period])
            )
        return strings


class AxisLabel(pg.GraphicsObject):

    # bg_color = pg.mkColor('#a9a9a9')
    bg_color = pg.mkColor(hcolor('gray'))
    fg_color = pg.mkColor(hcolor('black'))

    def __init__(
        self,
        parent=None,
        digits=1,
        color=None,
        opacity=1,
        **kwargs
    ):
        super().__init__(parent)
        self.parent = parent
        self.opacity = opacity
        self.label_str = ''
        self.digits = digits

        # some weird color convertion logic?
        if isinstance(color, QtGui.QPen):
            self.bg_color = color.color()
            self.fg_color = pg.mkColor(hcolor('black'))
        elif isinstance(color, list):
            self.bg_color = {'>0': color[0].color(), '<0': color[1].color()}
            self.fg_color = pg.mkColor(hcolor('white'))

        self.setFlag(self.ItemIgnoresTransformations)

    def paint(self, p, option, widget):
        p.setRenderHint(p.TextAntialiasing, True)
        p.setPen(self.fg_color)
        if self.label_str:
            if not isinstance(self.bg_color, dict):
                bg_color = self.bg_color
            else:
                if int(self.label_str.replace(' ', '')) > 0:
                    bg_color = self.bg_color['>0']
                else:
                    bg_color = self.bg_color['<0']
            p.setOpacity(self.opacity)
            p.fillRect(option.rect, bg_color)
            p.setOpacity(1)
            p.setFont(_font)

        p.drawText(option.rect, self.text_flags, self.label_str)

    # uggggghhhh
    def tick_to_string(self, tick_pos):
        raise NotImplementedError()

    def boundingRect(self):  # noqa
        raise NotImplementedError()

    def update_label(self, evt_post, point_view):
        raise NotImplementedError()

    # end uggggghhhh


# _common_text_flags = (
#     QtCore.Qt.TextDontClip |
#     QtCore.Qt.AlignCenter |
#     QtCore.Qt.AlignTop |
#     QtCore.Qt.AlignHCenter |
#     QtCore.Qt.AlignVCenter
# )


class XAxisLabel(AxisLabel):

    text_flags = (
        QtCore.Qt.TextDontClip
        | QtCore.Qt.AlignCenter
        # | QtCore.Qt.AlignTop
        | QtCore.Qt.AlignVCenter
        # | QtCore.Qt.AlignHCenter
    )
    # text_flags = _common_text_flags

    def tick_to_string(self, tick_pos):
        # TODO: change to actual period
        tpl = self.parent.tick_tpl['D1']
        bars = self.parent.linked_charts.chart._array
        if tick_pos > len(bars):
            return 'Unknown Time'
        return fromtimestamp(bars[round(tick_pos)]['time']).strftime(tpl)

    def boundingRect(self):  # noqa
        return QtCore.QRectF(0, 0, 145, 40)

    def update_label(self, abs_pos, data):
        # ibar = view_pos.x()
        # if ibar > self.quotes_count:
        #     return
        self.label_str = self.tick_to_string(data)
        width = self.boundingRect().width()
        offset = 0  # if have margins
        new_pos = QPointF(abs_pos.x() - width / 2 - offset, 0)
        self.setPos(new_pos)


class YAxisLabel(AxisLabel):

    # text_flags = _common_text_flags
    text_flags = (
        QtCore.Qt.AlignLeft
        | QtCore.Qt.TextDontClip
        | QtCore.Qt.AlignVCenter
    )

    def tick_to_string(self, tick_pos):
        # WTF IS THIS FORMAT?
        return ('{: ,.%df}' % self.digits).format(tick_pos).replace(',', ' ')

    def boundingRect(self):  # noqa
        return QtCore.QRectF(0, 0, 120, 30)

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        data: float,  # data for text
    ) -> None:
        self.label_str = self.tick_to_string(data)
        height = self.boundingRect().height()
        offset = 0  # if have margins
        new_pos = QPointF(0, abs_pos.y() - height / 2 - offset)
        self.setPos(new_pos)


class YSticky(YAxisLabel):
    """Y-axis label that sticks to where it's placed despite chart resizing.
    """
    def __init__(
        self,
        chart,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._chart = chart

        # XXX: not sure why this wouldn't work with a proxy?
        # pg.SignalProxy(
        #     delay=0,
        #     rateLimit=60,
        #     slot=last.update_on_resize,
        # )
        chart.sigRangeChanged.connect(self.update_on_resize)

    def update_on_resize(self, vr, r):
        # TODO: figure out how to generalize across data schema
        self.update_from_data(*self._chart._array[-1][['index', 'close']])

    def update_from_data(
        self,
        index: int,
        last: float,
    ) -> None:
        self.update_label(
            self._chart.mapFromView(QPointF(index, last)),
            last
        )
