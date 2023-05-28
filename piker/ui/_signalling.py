# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
Signalling graphics and APIs.

WARNING: this code likely doesn't work at all (yet)
         since it was copied from another class that shouldn't
         have had it.
"""
import numpy as np
import pyqtgraph as pg
from PyQt5 import (
    QtCore,
    QtWidgets,
)

from .quantdom.charts import CenteredTextItem
from .quantdom.base import Quotes
from .quantdom.portfolio import Order, Portfolio


class SignallingApi(object):
    def __init__(self, plotgroup):
        self.plotgroup = plotgroup
        self.chart = plotgroup.chart

    def _show_text_signals(self, lbar, rbar):
        signals = [
            sig
            for sig in self.signals_text_items[lbar:rbar]
            if isinstance(sig, CenteredTextItem)
        ]
        if len(signals) <= 50:
            for sig in signals:
                sig.show()
        else:
            for sig in signals:
                sig.hide()

    def _remove_signals(self):
        self.chart.removeItem(self.signals_group_arrow)
        self.chart.removeItem(self.signals_group_text)
        del self.signals_text_items
        del self.signals_group_arrow
        del self.signals_group_text
        self.signals_visible = False

    def add_signals(self):
        self.signals_group_text = QtWidgets.QGraphicsItemGroup()
        self.signals_group_arrow = QtWidgets.QGraphicsItemGroup()
        self.signals_text_items = np.empty(len(Quotes), dtype=object)

        for p in Portfolio.positions:
            x, price = p.id_bar_open, p.open_price
            if p.type == Order.BUY:
                y = Quotes[x].low * 0.99
                pg.ArrowItem(
                    parent=self.signals_group_arrow,
                    pos=(x, y),
                    pen=self.plotgroup.long_pen,
                    brush=self.plotgroup.long_brush,
                    angle=90,
                    headLen=12,
                    tipAngle=50,
                )
                text_sig = CenteredTextItem(
                    parent=self.signals_group_text,
                    pos=(x, y),
                    pen=self.plotgroup.long_pen,
                    brush=self.plotgroup.long_brush,
                    text=(
                        'Buy at {:.%df}' % self.plotgroup.digits).format(
                        price),
                    valign=QtCore.Qt.AlignBottom,
                )
                text_sig.hide()
            else:
                y = Quotes[x].high * 1.01
                pg.ArrowItem(
                    parent=self.signals_group_arrow,
                    pos=(x, y),
                    pen=self.plotgroup.short_pen,
                    brush=self.plotgroup.short_brush,
                    angle=-90,
                    headLen=12,
                    tipAngle=50,
                )
                text_sig = CenteredTextItem(
                    parent=self.signals_group_text,
                    pos=(x, y),
                    pen=self.plotgroup.short_pen,
                    brush=self.plotgroup.short_brush,
                    text=('Sell at {:.%df}' % self.plotgroup.digits).format(
                        price),
                    valign=QtCore.Qt.AlignTop,
                )
                text_sig.hide()

            self.signals_text_items[x] = text_sig

        self.chart.addItem(self.signals_group_arrow)
        self.chart.addItem(self.signals_group_text)
        self.signals_visible = True
