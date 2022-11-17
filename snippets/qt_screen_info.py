"""
DPI and info helper script for display metrics.
"""

# Resource list for mucking with DPIs on multiple screens:
# https://stackoverflow.com/questions/42141354/convert-pixel-size-to-point-size-for-fonts-on-multiple-platforms
# https://stackoverflow.com/questions/25761556/qt5-font-rendering-different-on-various-platforms/25929628#25929628
# https://doc.qt.io/qt-5/highdpi.html
# https://stackoverflow.com/questions/20464814/changing-dpi-scaling-size-of-display-make-qt-applications-font-size-get-rendere
# https://stackoverflow.com/a/20465247
# https://doc.qt.io/archives/qt-4.8/qfontmetrics.html#width
# https://forum.qt.io/topic/54136/how-do-i-get-the-qscreen-my-widget-is-on-qapplication-desktop-screen-returns-a-qwidget-and-qobject_cast-qscreen-returns-null/3
# https://forum.qt.io/topic/43625/point-sizes-are-they-reliable/4
# https://stackoverflow.com/questions/16561879/what-is-the-difference-between-logicaldpix-and-physicaldpix-in-qt
# https://doc.qt.io/qt-5/qguiapplication.html#screenAt

from pyqtgraph import (
    QtGui,
    QtWidgets,
)
from PyQt5.QtCore import (
     Qt,
     QCoreApplication,
)

# Proper high DPI scaling is available in Qt >= 5.6.0. This attibute
# must be set before creating the application
if hasattr(Qt, 'AA_EnableHighDpiScaling'):
    QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)

if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
    QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)


app = QtWidgets.QApplication([])
window = QtWidgets.QMainWindow()
main_widget = QtWidgets.QWidget()
window.setCentralWidget(main_widget)
window.show()

# TODO: move widget through multiple displays and auto-detect the pixel
# ratio? (probably is gonna require calls to i3ipc on linux)..
pxr = main_widget.devicePixelRatioF()

# TODO: how to detect list of displays from API?
# screen = app.screens()[screen_num]


def ppscreeninfo(screen: 'QScreen') -> None:
    # screen_num = app.desktop().screenNumber()
    name = screen.name()
    size = screen.size()
    geo = screen.availableGeometry()
    phydpi = screen.physicalDotsPerInch()
    logdpi = screen.logicalDotsPerInch()
    rr = screen.refreshRate()

    print(
        # f'screen number: {screen_num}\n',
        f'screen: {name}\n'
        f'  size: {size}\n'
        f'  geometry: {geo}\n'
        f'  logical dpi: {logdpi}\n'
        f'  devicePixelRationF(): {pxr}\n'
        f'  physical dpi: {phydpi}\n'
        f'  refresh rate: {rr}\n'
    )

    print('-'*50 + '\n')

screen = app.screenAt(main_widget.geometry().center())
ppscreeninfo(screen)

screen = app.primaryScreen()
ppscreeninfo(screen)

# app-wide font
font = QtGui.QFont("Hack")
# use pixel size to be cross-resolution compatible?
font.setPixelSize(6)


fm = QtGui.QFontMetrics(font)
fontdpi = fm.fontDpi()
font_h = fm.height()

string = '10000'
str_br = fm.boundingRect(string)
str_w = str_br.width()


print(
    # f'screen number: {screen_num}\n',
    f'font dpi: {fontdpi}\n'
    f'font height: {font_h}\n'
    f'string bounding rect: {str_br}\n'
    f'string width : {str_w}\n'
)
