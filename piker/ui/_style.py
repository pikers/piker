"""
Qt UI styling.
"""
import pyqtgraph as pg
from PyQt5 import QtGui


# chart-wide font
_font = QtGui.QFont("Hack", 4)
_i3_rgba = QtGui.QColor.fromRgbF(*[0.14]*3 + [1])


# splitter widget config
_xaxis_at = 'bottom'


# charting config
_min_points_to_show = 3


_tina_mode = False


def enable_tina_mode() -> None:
    """Enable "tina mode" to make everything look "conventional"
    like your pet hedgehog always wanted.
    """
    # white background (for tinas like our pal xb)
    pg.setConfigOption('background', 'w')


def hcolor(name: str):
    """Hex color codes by hipster speak.
    """
    return '#' + {
        'black': '000000',  # lives matter
        'white': 'ffffff',  # for tinas and sunbathers
        'gray': '808080',  # like the kick
        'dad_blue': '326693',  # like his shirt
        'vwap_blue': '0582fb',

        # traditional
        'tina_green': '00cc00',
        'tina_red': 'fa0000',
    }[name]
