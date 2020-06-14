"""
High level Qt chart wrapping widgets.
"""
from PyQt5 import QtGui

from .quantdom.charts import SplitterChart


class QuotesTabWidget(QtGui.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QtGui.QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.toolbar_layout = QtGui.QHBoxLayout()
        self.toolbar_layout.setContentsMargins(10, 10, 15, 0)
        self.chart_layout = QtGui.QHBoxLayout()

        # self.init_timeframes_ui()
        # self.init_strategy_ui()

        self.layout.addLayout(self.toolbar_layout)
        self.layout.addLayout(self.chart_layout)

    def init_timeframes_ui(self):
        self.tf_layout = QtGui.QHBoxLayout()
        self.tf_layout.setSpacing(0)
        self.tf_layout.setContentsMargins(0, 12, 0, 0)
        time_frames = ('1M', '5M', '15M', '30M', '1H', '1D', '1W', 'MN')
        btn_prefix = 'TF'
        for tf in time_frames:
            btn_name = ''.join([btn_prefix, tf])
            btn = QtGui.QPushButton(tf)
            # TODO:
            btn.setEnabled(False)
            setattr(self, btn_name, btn)
            self.tf_layout.addWidget(btn)
        self.toolbar_layout.addLayout(self.tf_layout)

    # XXX: strat loader/saver that we don't need yet.
    # def init_strategy_ui(self):
    #     self.strategy_box = StrategyBoxWidget(self)
    #     self.toolbar_layout.addWidget(self.strategy_box)

    # TODO: this needs to be changed to ``load_symbol()``
    # which will not only load historical data but also a real-time
    # stream and schedule the redraw events on new quotes
    def update_chart(self, symbol):
        if not self.chart_layout.isEmpty():
            self.chart_layout.removeWidget(self.chart)
        self.chart = SplitterChart()
        self.chart.plot(symbol)
        self.chart_layout.addWidget(self.chart)

    def add_signals(self):
        self.chart.add_signals()
