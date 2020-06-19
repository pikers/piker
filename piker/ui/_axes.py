"""
Chart axes graphics and behavior.
"""
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui


# from .quantdom.base import Quotes
from .quantdom.utils import fromtimestamp
from ._style import _font


class PriceAxis(pg.AxisItem):

    def __init__(self):
        super().__init__(orientation='right')
        self.setStyle(**{
            'textFillLimits': [(0, 0.8)],
            # 'tickTextWidth': 5,
            # 'tickTextHeight': 5,
            # 'autoExpandTextSpace': True,
            # 'maxTickLength': -20,
        })
        self.setLabel(**{'font-size': '10pt'})
        self.setTickFont(_font)

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
            textFillLimits=[(0, 0.90)],
            # TODO: doesn't seem to work -> bug in pyqtgraph?
            # tickTextHeight=2,
        )

    def tickStrings(self, values, scale, spacing):
        # if len(values) > 1 or not values:
        #     values = Quotes.time

        # strings = super().tickStrings(values, scale, spacing)
        s_period = 'D1'
        strings = []
        bars = self.linked_charts._array
        quotes_count = len(bars) - 1

        for ibar in values:
            if ibar > quotes_count:
                return strings
            dt_tick = fromtimestamp(bars[int(ibar)].time)
            strings.append(
                dt_tick.strftime(self.tick_tpl[s_period])
            )
        return strings


class AxisLabel(pg.GraphicsObject):

    # bg_color = pg.mkColor('#a9a9a9')
    bg_color = pg.mkColor('#808080')
    fg_color = pg.mkColor('#000000')

    def __init__(
        self,
        parent=None,
        digits=0,
        color=None,
        opacity=1,
        **kwargs
    ):
        super().__init__(parent)
        self.parent = parent
        self.opacity = opacity
        self.label_str = ''
        self.digits = digits
        # self.quotes_count = len(Quotes) - 1

        if isinstance(color, QtGui.QPen):
            self.bg_color = color.color()
            self.fg_color = pg.mkColor('#ffffff')
        elif isinstance(color, list):
            self.bg_color = {'>0': color[0].color(), '<0': color[1].color()}
            self.fg_color = pg.mkColor('#ffffff')

        self.setFlag(self.ItemIgnoresTransformations)

    def tick_to_string(self, tick_pos):
        raise NotImplementedError()

    def boundingRect(self):  # noqa
        raise NotImplementedError()

    def update_label(self, evt_post, point_view):
        raise NotImplementedError()

    def update_label_test(self, ypos=0, ydata=0):
        self.label_str = self.tick_to_string(ydata)
        height = self.boundingRect().height()
        offset = 0  # if have margins
        new_pos = QtCore.QPointF(0, ypos - height / 2 - offset)
        self.setPos(new_pos)

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


class XAxisLabel(AxisLabel):

    text_flags = (
        QtCore.Qt.TextDontClip | QtCore.Qt.AlignCenter | QtCore.Qt.AlignTop
    )

    def tick_to_string(self, tick_pos):
        # TODO: change to actual period
        tpl = self.parent.tick_tpl['D1']
        bars = self.parent.linked_charts._array
        if tick_pos > len(bars):
            return 'Unknown Time'
        return fromtimestamp(bars[round(tick_pos)].time).strftime(tpl)

    def boundingRect(self):  # noqa
        return QtCore.QRectF(0, 0, 145, 50)

    def update_label(self, evt_post, point_view):
        ibar = point_view.x()
        # if ibar > self.quotes_count:
        #     return
        self.label_str = self.tick_to_string(ibar)
        width = self.boundingRect().width()
        offset = 0  # if have margins
        new_pos = QtCore.QPointF(evt_post.x() - width / 2 - offset, 0)
        self.setPos(new_pos)


class YAxisLabel(AxisLabel):

    text_flags = (
        QtCore.Qt.TextDontClip | QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter
    )

    def tick_to_string(self, tick_pos):
        return ('{: ,.%df}' % self.digits).format(tick_pos).replace(',', ' ')

    def boundingRect(self):  # noqa
        return QtCore.QRectF(0, 0, 100, 40)

    def update_label(self, evt_post, point_view):
        self.label_str = self.tick_to_string(point_view.y())
        height = self.boundingRect().height()
        offset = 0  # if have margins
        new_pos = QtCore.QPointF(0, evt_post.y() - height / 2 - offset)
        self.setPos(new_pos)
