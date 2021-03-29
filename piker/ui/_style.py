# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
Qt UI styling.
"""
from typing import Optional, Dict
import math

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from qdarkstyle.palette import DarkPalette

from ..log import get_logger
from ._exec import current_screen

log = get_logger(__name__)

# chart-wide fonts specified in inches
_font_sizes: Dict[str, Dict[str, float]] = {
    'hi': {
        'default':  0.0616,
        'small':  0.055,
    },
    'lo': {
        'default':  6.5 / 64,
        'small':  6 / 64,
    },
}


class DpiAwareFont:
    def __init__(
        self,
        # TODO: move to config
        name: str = 'Hack',
        font_size: str = 'default',
        # size_in_inches: Optional[float] = None,
    ) -> None:
        self.name = name
        self._qfont = QtGui.QFont(name)
        # self._iwl = size_in_inches or _default_font_inches_we_like
        self._font_size: str = font_size
        self._qfm = QtGui.QFontMetrics(self._qfont)
        self._physical_dpi = None
        self._screen = None

    def _set_qfont_px_size(self, px_size: int) -> None:
        self._qfont.setPixelSize(px_size)
        self._qfm = QtGui.QFontMetrics(self._qfont)

    @property
    def screen(self) -> QtGui.QScreen:
        if self._screen is not None:
            try:
                self._screen.refreshRate()
            except RuntimeError:
                self._screen = current_screen()
        else:
            self._screen = current_screen()

        return self._screen

    @property
    def font(self):
        return self._qfont

    @property
    def px_size(self):
        return self._qfont.pixelSize()

    def configure_to_dpi(self, screen: Optional[QtGui.QScreen] = None):
        """Set an appropriately sized font size depending on the screen DPI.

        If we end up needing to generalize this more here there are resources
        listed in the script in ``snippets/qt_screen_info.py``.

        """
        if screen is None:
            screen = self.screen

        # take the max since scaling can make things ugly in some cases
        pdpi = screen.physicalDotsPerInch()
        ldpi = screen.logicalDotsPerInch()
        dpi = max(pdpi, ldpi)

        # for low dpi scale everything down
        if dpi <= 97:
            inches = _font_sizes['lo'][self._font_size]
        else:
            inches = _font_sizes['hi'][self._font_size]

        font_size = math.floor(inches * dpi)
        log.info(
            f"\nscreen:{screen.name()} with DPI: {dpi}"
            f"\nbest font size is {font_size}\n"
        )

        self._set_qfont_px_size(font_size)
        self._physical_dpi = dpi

    def boundingRect(self, value: str) -> QtCore.QRectF:

        screen = self.screen
        if screen is None:
            raise RuntimeError("You must call .configure_to_dpi() first!")

        unscaled_br = self._qfm.boundingRect(value)

        return QtCore.QRectF(
            0,
            0,
            unscaled_br.width(),
            unscaled_br.height(),
        )


# use inches size to be cross-resolution compatible?
_font = DpiAwareFont()

# TODO: re-compute font size when main widget switches screens?
# https://forum.qt.io/topic/54136/how-do-i-get-the-qscreen-my-widget-is-on-qapplication-desktop-screen-returns-a-qwidget-and-qobject_cast-qscreen-returns-null/3

# _i3_rgba = QtGui.QColor.fromRgbF(*[0.14]*3 + [1])

# splitter widget config
_xaxis_at = 'bottom'

# charting config
CHART_MARGINS = (0, 0, 2, 2)
_min_points_to_show = 6
_bars_from_right_in_follow_mode = int(130)
_bars_to_left_in_follow_mode = int(616)
_tina_mode = False


def enable_tina_mode() -> None:
    """Enable "tina mode" to make everything look "conventional"
    like your pet hedgehog always wanted.
    """
    # white background (for tinas like our pal xb)
    pg.setConfigOption('background', 'w')


def hcolor(name: str) -> str:
    """Hex color codes by hipster speak.

    This is an internal set of color codes hand picked
    for certain purposes.

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
        'grayer': '#4c4c4c',
        'grayest': '#3f3f3f',
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
        # 'bid_blue': '#0077ea',  # like the L1
        'bid_blue': '#3094d9',  # like the L1
        'aquaman': '#39abd0',

        # traditional
        'tina_green': '#00cc00',
        'tina_red': '#fa0000',

        'cucumber': '#006400',
        'cool_green': '#33b864',
        'dull_green': '#74a662',
        'hedge_green': '#518360',

        # orders and alerts
        'alert_yellow': '#e2d083',
        'alert_yellow_light': '#ffe366',

        # buys
        # 'hedge': '#768a75',
        # 'hedge': '#41694d',
        # 'hedge': '#558964',
        # 'hedge_light': '#5e9870',

        '80s_neon_green': '#00b677',
        # 'buy_green': '#41694d',
        'buy_green': '#558964',
        'buy_green_light': '#558964',

        # sells
        # techincally "raspberry"
        # 'sell_red': '#990036',
        # 'sell_red': '#9A0036',

        # brighter then above
        # 'sell_red': '#8c0030',

        'sell_red': '#b6003f',
        # 'sell_red': '#d00048',
        'sell_red_light': '#f85462',

        # 'sell_red': '#f85462',
        # 'sell_red_light': '#ff4d5c',

    }[name]
