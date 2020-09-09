"""
Chart axes graphics and behavior.
"""
import time
from functools import partial
from typing import List


# import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QPointF

from .quantdom.utils import fromtimestamp
from ._style import _font, hcolor


class PriceAxis(pg.AxisItem):

    def __init__(
        self,
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
        self.setWidth(125)

    # XXX: drop for now since it just eats up h space

    # def tickStrings(self, vals, scale, spacing):
    #     digts = max(0, np.ceil(-np.log10(spacing * scale)))
    #     return [
    #         ('{:<8,.%df}' % digts).format(v).replace(',', ' ') for v in vals
    #     ]


class DynamicDateAxis(pg.AxisItem):
    # time formats mapped by seconds between bars
    tick_tpl = {
        60*60*24: '%Y-%b-%d',
        60: '%H:%M',
        30: '%H:%M:%S',
        5: '%H:%M:%S',
    }

    def __init__(
        self,
        linked_charts,
        *args,
        **kwargs
    ) -> None:
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

    def _indexes_to_timestrs(
        self,
        indexes: List[int],
    ) -> List[str]:
        bars = self.linked_charts.chart._array
        times = bars['time']
        bars_len = len(bars)
        delay = times[-1] - times[times != times[-1]][-1]

        epochs = times[list(
            map(int, filter(lambda i: i < bars_len, indexes))
        )]
        # TODO: **don't** have this hard coded shift to EST
        dts = pd.to_datetime(epochs, unit='s') - 4*pd.offsets.Hour()
        return dts.strftime(self.tick_tpl[delay])


    def tickStrings(self, values: List[float], scale, spacing):
        return self._indexes_to_timestrs(values)


class AxisLabel(pg.GraphicsObject):

    # bg_color = pg.mkColor('#a9a9a9')
    bg_color = pg.mkColor(hcolor('gray'))
    fg_color = pg.mkColor(hcolor('black'))

    def __init__(
        self,
        parent=None,
        digits=2,
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

    def boundingRect(self):  # noqa
        # TODO: we need to get the parent axe's dimensions transformed
        # to abs coords to be 100% correct here:
        # self.parent.boundingRect()
        return QtCore.QRectF(0, 0, 100, 31)

    def update_label(
        self,
        abs_pos: QPointF,  # scene coords
        data: float,  # data for text
        offset: int = 0  # if have margins, k?
    ) -> None:
        timestrs = self.parent._indexes_to_timestrs([int(data)])
        if not timestrs.any():
            return
        self.label_str = timestrs[0]
        width = self.boundingRect().width()
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
        offset: int = 0  # if have margins, k?
    ) -> None:
        self.label_str = self.tick_to_string(data)
        height = self.boundingRect().height()
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
        # TODO: add an `.index` to the array data-buffer layer
        # and make this way less shitty...
        a = self._chart._array
        fields = a.dtype.fields
        if fields and 'index' in fields:
            index, last = a[-1][['index', 'close']]
        else:
            # non-ohlc case
            index = len(a) - 1
            last = a[-1]
        self.update_from_data(
            index,
            last,
        )

    def update_from_data(
        self,
        index: int,
        last: float,
    ) -> None:
        self.update_label(
            self._chart.mapFromView(QPointF(index, last)),
            last
        )
