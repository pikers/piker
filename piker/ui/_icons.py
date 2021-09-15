# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
``QIcon`` hackery.

'''
from PyQt5.QtWidgets import QStyle
from PyQt5.QtGui import (
    QIcon, QPixmap, QColor
)
from PyQt5.QtCore import QSize

from ._style import hcolor

# https://www.pythonguis.com/faq/built-in-qicons-pyqt/
# account status icons taken from built-in set
_icon_names: dict[str, str] = {
    # these two seem to work for our mask hack done below to
    # change the coloring.
    'long_pp': 'SP_TitleBarShadeButton',
    'short_pp': 'SP_TitleBarUnshadeButton',
    'ready': 'SP_MediaPlay',
}
_icons: dict[str, QIcon] = {}


def mk_icons(

    style: QStyle,
    size: QSize,

) -> dict[str, QIcon]:
    '''This helper is indempotent.

    '''
    global _icons, _icon_names
    if _icons:
        return _icons

    _icons[None] = QIcon()  # the "null" icon

    # load account selection using current style
    for name, icon_name in _icon_names.items():

        stdpixmap = getattr(QStyle, icon_name)
        stdicon = style.standardIcon(stdpixmap)
        pixmap = stdicon.pixmap(size)

        # fill hack from SO to change icon color:
        # https://stackoverflow.com/a/38369468
        out_pixmap = QPixmap(size)
        out_pixmap.fill(QColor(hcolor('default_spotlight')))
        out_pixmap.setMask(pixmap.createHeuristicMask())

        # TODO: not idea why this doesn't work / looks like
        # trash.  Sure would be nice to just generate our own
        # pixmaps on the fly..
        # p = QPainter(out_pixmap)
        # p.setOpacity(1)
        # p.setBrush(QColor(hcolor('papas_special')))
        # p.setPen(QColor(hcolor('default_lightest')))
        # path = mk_marker_path(style='|<')
        # p.scale(6, 6)
        # # p.translate(0, 0)
        # p.drawPath(path)
        # p.save()
        # p.end()
        # del p
        # icon = QIcon(out_pixmap)

        icon = QIcon()
        icon.addPixmap(out_pixmap)

        _icons[name] = icon

    return _icons
