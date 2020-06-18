"""
Signalling graphics and APIs.

WARNING: this code likely doesn't work at all (yet)
         since it was copied from another class that shouldn't
         have had it.
"""
import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui

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
        self.signals_group_text = QtGui.QGraphicsItemGroup()
        self.signals_group_arrow = QtGui.QGraphicsItemGroup()
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
