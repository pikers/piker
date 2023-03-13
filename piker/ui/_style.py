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

'''
Qt UI styling.

'''
from typing import Dict
import math

import pyqtgraph as pg
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import Qt, QCoreApplication
from qdarkstyle import DarkPalette

from ..log import get_logger

log = get_logger(__name__)

_magic_inches = 0.0666 * (1 + 6/16)

# chart-wide fonts specified in inches
_font_sizes: Dict[str, Dict[str, float]] = {
    'hi': {
        'default':  _magic_inches,
        'small': 0.9 * _magic_inches,
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

    ) -> None:
        self.name = name
        self._qfont = QtGui.QFont(name)
        self._font_size: str = font_size
        self._qfm = QtGui.QFontMetrics(self._qfont)
        self._font_inches: float = None
        self._screen = None

    def _set_qfont_px_size(self, px_size: int) -> None:
        self._qfont.setPixelSize(px_size)
        self._qfm = QtGui.QFontMetrics(self._qfont)

    @property
    def screen(self) -> QtGui.QScreen:
        from ._window import main_window

        if self._screen is not None:
            try:
                self._screen.refreshRate()
            except RuntimeError:
                self._screen = main_window().current_screen()
        else:
            self._screen = main_window().current_screen()

        return self._screen

    @property
    def font(self):
        return self._qfont

    def scale(self) -> float:
        screen = self.screen
        return screen.logicalDotsPerInch() / screen.physicalDotsPerInch()

    @property
    def px_size(self) -> int:
        return self._qfont.pixelSize()

    def configure_to_dpi(self, screen: QtGui.QScreen | None = None):
        '''
        Set an appropriately sized font size depending on the screen DPI.

        If we end up needing to generalize this more here there are resources
        listed in the script in ``snippets/qt_screen_info.py``.

        '''
        if screen is None:
            screen = self.screen

        # take the max since scaling can make things ugly in some cases
        pdpi = screen.physicalDotsPerInch()
        ldpi = screen.logicalDotsPerInch()

        # XXX: this is needed on sway/wayland where you set
        # ``QT_WAYLAND_FORCE_DPI=physical``
        if ldpi == 0:
            ldpi = pdpi

        mx_dpi = max(pdpi, ldpi)
        mn_dpi = min(pdpi, ldpi)
        scale = round(ldpi/pdpi, ndigits=2)

        if mx_dpi <= 97:  # for low dpi use larger font sizes
            inches = _font_sizes['lo'][self._font_size]

        else:  # hidpi use smaller font sizes
            inches = _font_sizes['hi'][self._font_size]

        dpi = mn_dpi

        mult = 1.0

        # No implicit DPI scaling was done by the DE so let's engage
        # some hackery ad-hoc scaling shiat.
        # dpi is likely somewhat scaled down so use slightly larger font size
        if scale >= 1.1 and self._font_size:

            # no idea why
            if 1.2 <= scale:
                mult = 1.0375

            if scale >= 1.5:
                mult = 1.375

        # TODO: this multiplier should probably be determined from
        # relative aspect ratios or something?
        inches *= mult

        # XXX: if additionally we detect a known DE scaling factor we
        # also scale *up* our font size on top of the existing
        # heuristical (aka no clue why it works) scaling from the block
        # above XD
        if (
            hasattr(Qt, 'AA_EnableHighDpiScaling')
            and QCoreApplication.testAttribute(Qt.AA_EnableHighDpiScaling)
        ):
            inches *= round(scale)

        # TODO: we might want to fiddle with incrementing font size by
        # +1 for the edge cases above. it seems doing it via scaling is
        # always going to hit that error in range mapping from inches:
        # float to px size: int.
        self._font_inches = inches
        font_size = math.floor(inches * dpi)

        log.debug(
            f"screen:{screen.name()}\n"
            f"pDPI: {pdpi}, lDPI: {ldpi}, scale: {scale}\n"
            f"\nOur best guess font size is {font_size}\n"
        )
        # apply the size
        self._set_qfont_px_size(font_size)

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
_font_small = DpiAwareFont(font_size='small')


def _config_fonts_to_screen() -> None:
    'configure global DPI aware font sizes'

    global _font, _font_small
    _font.configure_to_dpi()
    _font_small.configure_to_dpi()


# TODO: re-compute font size when main widget switches screens?
# https://forum.qt.io/topic/54136/how-do-i-get-the-qscreen-my-widget-is-on-qapplication-desktop-screen-returns-a-qwidget-and-qobject_cast-qscreen-returns-null/3

# splitter widget config
_xaxis_at = 'bottom'

# charting config
CHART_MARGINS = (0, 0, 2, 2)
_min_points_to_show = 6
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
        'original': '#a9a9a9',
        'gray': '#808080',  # like the kick
        'grayer': '#4c4c4c',
        'grayest': '#3f3f3f',
        'cadet': '#91A3B0',
        'marengo': '#91A3B0',
        'gunmetal': '#91A3B0',
        'battleship': '#848482',

        # default ohlc-bars/curve gray
        'bracket': '#666666',  # like the logo

        # bluish
        'charcoal': '#36454F',

        # work well for filled polygons which want a 'bracket' feel
        # going light to dark
        'davies': '#555555',
        'i3': '#494D4F',
        'jet': '#343434',

        # from ``qdarkstyle`` palette
        'default_darkest': DarkPalette.COLOR_BACKGROUND_1,
        'default_dark': DarkPalette.COLOR_BACKGROUND_2,
        'default': DarkPalette.COLOR_BACKGROUND_3,
        'default_light': DarkPalette.COLOR_BACKGROUND_4,
        'default_lightest': DarkPalette.COLOR_BACKGROUND_5,
        'default_spotlight': DarkPalette.COLOR_BACKGROUND_6,

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
