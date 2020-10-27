"""
Qt UI styling.
"""
import pyqtgraph as pg
from PyQt5 import QtGui
from qdarkstyle.palette import DarkPalette

from ..log import get_logger

log = get_logger(__name__)

# chart-wide font
# font size 6px / 53 dpi (3x scaled down on 4k hidpi)
_font_inches_we_like = 6 / 53

# use pixel size to be cross-resolution compatible?
_font = QtGui.QFont("Hack")
_font.setPixelSize(6)  # default

# _physical_font_height_in = 1/6  # inches
_font._fm = QtGui.QFontMetrics(_font)

# TODO: re-compute font size when main widget switches screens?
# https://forum.qt.io/topic/54136/how-do-i-get-the-qscreen-my-widget-is-on-qapplication-desktop-screen-returns-a-qwidget-and-qobject_cast-qscreen-returns-null/3


def configure_font_to_dpi(screen: QtGui.QScreen):
    """Set an appropriately sized font size depending on the screen DPI.

    If we end up needing to generalize this more here are some resources:

    - https://stackoverflow.com/questions/42141354/convert-pixel-size-to-point-size-for-fonts-on-multiple-platforms
    - https://stackoverflow.com/questions/25761556/qt5-font-rendering-different-on-various-platforms/25929628#25929628
    - https://doc.qt.io/qt-5/highdpi.html
    - https://stackoverflow.com/questions/20464814/changing-dpi-scaling-size-of-display-make-qt-applications-font-size-get-rendere
    - https://stackoverflow.com/a/20465247
    - https://doc.qt.io/archives/qt-4.8/qfontmetrics.html#width
    - https://forum.qt.io/topic/54136/how-do-i-get-the-qscreen-my-widget-is-on-qapplication-desktop-screen-returns-a-qwidget-and-qobject_cast-qscreen-returns-null/3
    - https://forum.qt.io/topic/43625/point-sizes-are-they-reliable/4

    Also, see the script in ``snippets/qt_screen_info.py``.

    """
    dpi = screen.physicalDotsPerInch()
    font_size = round(_font_inches_we_like * dpi)
    log.info(
        f"\nscreen:{screen.name()} with DPI: {dpi}"
        f"\nbest font size is {font_size}\n"
    )
    global _font
    _font.setPixelSize(font_size)
    return _font


# _i3_rgba = QtGui.QColor.fromRgbF(*[0.14]*3 + [1])

# splitter widget config
_xaxis_at = 'bottom'

# charting config
CHART_MARGINS = (0, 0, 2, 2)
_min_points_to_show = 6
_bars_from_right_in_follow_mode = int(6**2)
_bars_to_left_in_follow_mode = int(6**3)


_tina_mode = False


def enable_tina_mode() -> None:
    """Enable "tina mode" to make everything look "conventional"
    like your pet hedgehog always wanted.
    """
    # white background (for tinas like our pal xb)
    pg.setConfigOption('background', 'w')


def hcolor(name: str) -> str:
    """Hex color codes by hipster speak.
    """
    return {
        # lives matter
        'black': '#000000',
        'erie_black': '#1B1B1B',
        'licorice': '#1A1110',
        'papas_special': '#06070c',
        'svags': '#0a0e14',

        # fifty shades
        'gray': '#808080',  # like the kick
        'jet': '#343434',
        'cadet': '#91A3B0',
        'marengo': '#91A3B0',
        'charcoal': '#36454F',
        'gunmetal': '#91A3B0',
        'battleship': '#848482',
        'davies': '#555555',
        'bracket': '#666666',  # like the logo
        'original': '#a9a9a9',

        # palette
        'default': DarkPalette.COLOR_BACKGROUND_NORMAL,
        'default_light': DarkPalette.COLOR_BACKGROUND_LIGHT,

        'white': '#ffffff',  # for tinas and sunbathers

        # blue zone
        'dad_blue': '#326693',  # like his shirt
        'vwap_blue': '#0582fb',
        'dodger_blue': '#1e90ff',  # like the team?
        'panasonic_blue': '#0040be',  # from japan

        # traditional
        'tina_green': '#00cc00',
        'tina_red': '#fa0000',

    }[name]
