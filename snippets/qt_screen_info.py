from pyqtgraph import QtGui
from PyQt5 import QtCore
from PyQt5.QtCore import (
     Qt, QCoreApplication
)

# Proper high DPI scaling is available in Qt >= 5.6.0. This attibute
# must be set before creating the application
if hasattr(Qt, 'AA_EnableHighDpiScaling'):
    QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)

if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
    QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)


app = QtGui.QApplication([])
window = QtGui.QMainWindow()
main_widget = QtGui.QWidget()
window.setCentralWidget(main_widget)
window.show()

pxr = main_widget.devicePixelRatioF()

# screen_num = app.desktop().screenNumber()
# screen = app.screens()[screen_num]

screen = app.screenAt(main_widget.geometry().center())

name = screen.name()
size = screen.size()
geo = screen.availableGeometry()
phydpi = screen.physicalDotsPerInch()
logdpi = screen.logicalDotsPerInch()

print(
    # f'screen number: {screen_num}\n',
    f'screen name: {name}\n'
    f'screen size: {size}\n'
    f'screen geometry: {geo}\n\n'
    f'devicePixelRationF(): {pxr}\n'
    f'physical dpi: {phydpi}\n'
    f'logical dpi: {logdpi}\n'
)

print('-'*50)

screen = app.primaryScreen()

name = screen.name()
size = screen.size()
geo = screen.availableGeometry()
phydpi = screen.physicalDotsPerInch()
logdpi = screen.logicalDotsPerInch()

print(
    # f'screen number: {screen_num}\n',
    f'screen name: {name}\n'
    f'screen size: {size}\n'
    f'screen geometry: {geo}\n\n'
    f'devicePixelRationF(): {pxr}\n'
    f'physical dpi: {phydpi}\n'
    f'logical dpi: {logdpi}\n'
)


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
