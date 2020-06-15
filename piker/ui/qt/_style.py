"""
Qt styling.
"""
from PyQt5 import QtGui


# TODO: add "tina mode" to make everything look "conventional"
# white background (for tinas like our pal xb)
# pg.setConfigOption('background', 'w')


# chart-wide font
_font = QtGui.QFont("Hack", 4)
_i3_rgba = QtGui.QColor.fromRgbF(*[0.14]*3 + [1])


# splitter widget config
_xaxis_at = 'bottom'
