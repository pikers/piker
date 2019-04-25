import sys

from PySide2.QtCharts import QtCharts
from PySide2.QtWidgets import QApplication, QMainWindow
from PySide2.QtCore import Qt, QPointF
from PySide2 import QtGui
import qdarkstyle

data = ((1, 7380, 7520, 7380, 7510, 7324),
    (2, 7520, 7580, 7410, 7440, 7372),
    (3, 7440, 7650, 7310, 7520, 7434),
    (4, 7450, 7640, 7450, 7550, 7480),
    (5, 7510, 7590, 7460, 7490, 7502),
    (6, 7500, 7590, 7480, 7560, 7512),
    (7, 7560, 7830, 7540, 7800, 7584))


app = QApplication([])
# set dark stylesheet
# import pdb; pdb.set_trace()
app.setStyleSheet(qdarkstyle.load_stylesheet_pyside())

series = QtCharts.QCandlestickSeries()
series.setDecreasingColor(Qt.darkRed)
series.setIncreasingColor(Qt.darkGreen)

ma5 = QtCharts.QLineSeries()  # 5-days average data line
tm = []  # stores str type data

# in a loop,  series and ma5 append corresponding data
for num, o, h, l, c, m in data:
    candle = QtCharts.QCandlestickSet(o, h, l, c)
    series.append(candle)
    ma5.append(QPointF(num, m))
    tm.append(str(num))

pen = candle.pen()
# import pdb; pdb.set_trace()

chart = QtCharts.QChart()

# import pdb; pdb.set_trace()
series.setBodyOutlineVisible(False)
series.setCapsVisible(False)
# brush = QtGui.QBrush()
# brush.setColor(Qt.green)
# series.setBrush(brush)
chart.addSeries(series)  # candle
chart.addSeries(ma5)  # ma5 line

chart.setAnimationOptions(QtCharts.QChart.SeriesAnimations)
chart.createDefaultAxes()
chart.legend().hide()

chart.axisX(series).setCategories(tm)
chart.axisX(ma5).setVisible(False)

view = QtCharts.QChartView(chart)
view.chart().setTheme(QtCharts.QChart.ChartTheme.ChartThemeDark)
view.setRubberBand(QtCharts.QChartView.HorizontalRubberBand)
# chartview.chart().setTheme(QtCharts.QChart.ChartTheme.ChartThemeBlueCerulean)

ui = QMainWindow()
# ui.setGeometry(50, 50, 500, 300)
ui.setCentralWidget(view)
ui.show()
sys.exit(app.exec_())
