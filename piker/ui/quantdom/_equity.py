"""
Strategy and performance charting
"""
import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui

from .base import Quotes
from .portfolio import Portfolio
from .utils import timeit
from .charts import (
    PriceAxis,
    CHART_MARGINS,
    SampleLegendItem,
    YAxisLabel,
    CrossHairItem,
)


class EquityChart(QtGui.QWidget):

    eq_pen_pos_color = pg.mkColor('#00cc00')
    eq_pen_neg_color = pg.mkColor('#cc0000')
    eq_brush_pos_color = pg.mkColor('#40ee40')
    eq_brush_neg_color = pg.mkColor('#ee4040')
    long_pen_color = pg.mkColor('#008000')
    short_pen_color = pg.mkColor('#800000')
    buy_and_hold_pen_color = pg.mkColor('#4444ff')

    def __init__(self):
        super().__init__()
        self.xaxis = pg.DateAxisItem()
        self.xaxis.setStyle(tickTextOffset=7, textFillLimits=[(0, 0.80)])
        self.yaxis = PriceAxis()

        self.layout = QtGui.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)

        self.chart = pg.PlotWidget(
            axisItems={'bottom': self.xaxis, 'right': self.yaxis},
            enableMenu=False,
        )
        self.chart.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Plain)
        self.chart.getPlotItem().setContentsMargins(*CHART_MARGINS)
        self.chart.showGrid(x=True, y=True)
        self.chart.hideAxis('left')
        self.chart.showAxis('right')

        self.chart.setCursor(QtCore.Qt.BlankCursor)
        self.chart.sigXRangeChanged.connect(self._update_yrange_limits)

        self.layout.addWidget(self.chart)

    def _add_legend(self):
        legend = pg.LegendItem((140, 100), offset=(10, 10))
        legend.setParentItem(self.chart.getPlotItem())

        for arr, item in self.curves:
            legend.addItem(
                SampleLegendItem(item),
                item.opts['name']
                if not isinstance(item, tuple)
                else item[0].opts['name'],
            )

    def _add_ylabels(self):
        self.ylabels = []
        for arr, item in self.curves:
            color = (
                item.opts['pen']
                if not isinstance(item, tuple)
                else [i.opts['pen'] for i in item]
            )
            label = YAxisLabel(parent=self.yaxis, color=color)
            self.ylabels.append(label)

    def _update_ylabels(self, vb, rbar):
        for i, curve in enumerate(self.curves):
            arr, item = curve
            ylast = arr[rbar]
            ypos = vb.mapFromView(QtCore.QPointF(0, ylast)).y()
            axlabel = self.ylabels[i]
            axlabel.update_label_test(ypos=ypos, ydata=ylast)

    def _update_yrange_limits(self, vb=None):
        if not hasattr(self, 'min_curve'):
            return
        vr = self.chart.viewRect()
        lbar, rbar = int(vr.left()), int(vr.right())
        ylow = self.min_curve[lbar:rbar].min() * 1.1
        yhigh = self.max_curve[lbar:rbar].max() * 1.1

        std = np.std(self.max_curve[lbar:rbar]) * 4
        self.chart.setLimits(yMin=ylow, yMax=yhigh, minYRange=std)
        self.chart.setYRange(ylow, yhigh)
        self._update_ylabels(vb, rbar)

    @timeit
    def plot(self):
        equity_curve = Portfolio.equity_curve
        eq_pos = np.zeros_like(equity_curve)
        eq_neg = np.zeros_like(equity_curve)
        eq_pos[equity_curve >= 0] = equity_curve[equity_curve >= 0]
        eq_neg[equity_curve <= 0] = equity_curve[equity_curve <= 0]

        # Equity
        self.eq_pos_curve = pg.PlotCurveItem(
            eq_pos,
            name='Equity',
            fillLevel=0,
            antialias=True,
            pen=self.eq_pen_pos_color,
            brush=self.eq_brush_pos_color,
        )
        self.eq_neg_curve = pg.PlotCurveItem(
            eq_neg,
            name='Equity',
            fillLevel=0,
            antialias=True,
            pen=self.eq_pen_neg_color,
            brush=self.eq_brush_neg_color,
        )
        self.chart.addItem(self.eq_pos_curve)
        self.chart.addItem(self.eq_neg_curve)

        # Only Long
        self.long_curve = pg.PlotCurveItem(
            Portfolio.long_curve,
            name='Only Long',
            pen=self.long_pen_color,
            antialias=True,
        )
        self.chart.addItem(self.long_curve)

        # Only Short
        self.short_curve = pg.PlotCurveItem(
            Portfolio.short_curve,
            name='Only Short',
            pen=self.short_pen_color,
            antialias=True,
        )
        self.chart.addItem(self.short_curve)

        # Buy and Hold
        self.buy_and_hold_curve = pg.PlotCurveItem(
            Portfolio.buy_and_hold_curve,
            name='Buy and Hold',
            pen=self.buy_and_hold_pen_color,
            antialias=True,
        )
        self.chart.addItem(self.buy_and_hold_curve)

        self.curves = [
            (Portfolio.equity_curve, (self.eq_pos_curve, self.eq_neg_curve)),
            (Portfolio.long_curve, self.long_curve),
            (Portfolio.short_curve, self.short_curve),
            (Portfolio.buy_and_hold_curve, self.buy_and_hold_curve),
        ]

        self._add_legend()
        self._add_ylabels()

        ch = CrossHairItem(self.chart)
        self.chart.addItem(ch)

        arrs = (
            Portfolio.equity_curve,
            Portfolio.buy_and_hold_curve,
            Portfolio.long_curve,
            Portfolio.short_curve,
        )
        np_arrs = np.concatenate(arrs)
        _min = abs(np_arrs.min()) * -1.1
        _max = np_arrs.max() * 1.1

        self.chart.setLimits(
            xMin=Quotes[0].id,
            xMax=Quotes[-1].id,
            yMin=_min,
            yMax=_max,
            minXRange=60,
        )

        self.min_curve = arrs[0].copy()
        self.max_curve = arrs[0].copy()
        for arr in arrs[1:]:
            self.min_curve = np.minimum(self.min_curve, arr)
            self.max_curve = np.maximum(self.max_curve, arr)
