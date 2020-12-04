# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
from typing import Optional
import math

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from qdarkstyle.palette import DarkPalette

from ..log import get_logger

log = get_logger(__name__)

# chart-wide fonts specified in inches
_default_font_inches_we_like = 0.0666
_down_2_font_inches_we_like = 6 / 96


class DpiAwareFont:
    def __init__(
        self,
        name: str = 'Hack',
        size_in_inches: Optional[float] = None,
    ) -> None:
        self.name = name
        self._qfont = QtGui.QFont(name)
        self._iwl = size_in_inches or _default_font_inches_we_like
        self._qfm = QtGui.QFontMetrics(self._qfont)
        self._physical_dpi = None
        self._screen = None
        self._dpi_scalar = 1.

    def _set_qfont_px_size(self, px_size: int) -> None:
        self._qfont.setPixelSize(px_size)
        self._qfm = QtGui.QFontMetrics(self._qfont)

    @property
    def font(self):
        return self._qfont

    @property
    def px_size(self):
        return self._qfont.pixelSize()

    def configure_to_dpi(self, screen: QtGui.QScreen):
        """Set an appropriately sized font size depending on the screen DPI.

        If we end up needing to generalize this more here there are resources
        listed in the script in ``snippets/qt_screen_info.py``.

        """
        # take the max since scaling can make things ugly in some cases
        pdpi = screen.physicalDotsPerInch()
        ldpi = screen.logicalDotsPerInch()
        dpi = max(pdpi, ldpi)

        font_size = math.floor(self._iwl * dpi)
        log.info(
            f"\nscreen:{screen.name()} with DPI: {dpi}"
            f"\nbest font size is {font_size}\n"
        )
        self._set_qfont_px_size(font_size)
        self._physical_dpi = dpi
        self._screen = screen

    def boundingRect(self, value: str) -> QtCore.QRectF:

        screen = self._screen
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
        # 'bid_blue': '#0077ea',  # like the L1
        'bid_blue': '#3094d9',  # like the L1
        'aquaman': '#39abd0',

        # traditional
        'tina_green': '#00cc00',
        'tina_red': '#fa0000',

    }[name]
