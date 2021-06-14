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
Qt main window singletons and stuff.

"""
import os
import signal
import time
from typing import Callable

from pyqtgraph import QtGui
from PyQt5 import QtCore
from PyQt5.QtGui import QLabel, QStatusBar

from ..log import get_logger
from ._style import _font_small, hcolor


log = get_logger(__name__)


class MultiStatus:

    bar: QStatusBar
    statuses: list[str]

    def __init__(self, bar, statuses) -> None:
        self.bar = bar
        self.statuses = statuses

    def open_status(
        self,
        msg: str,
    ) -> Callable[..., None]:
        '''Add a status to the status bar and return a close callback which
        when called will remove the status ``msg``.

        '''
        self.statuses.append(msg)

        def remove_msg() -> None:
            self.statuses.remove(msg)
            self.render()

        self.render()
        return remove_msg

    def render(self) -> None:
        if self.statuses:
            self.bar.showMessage(f'{" ".join(self.statuses)}')
        else:
            self.bar.clearMessage()


class MainWindow(QtGui.QMainWindow):

    size = (800, 500)
    title = 'piker chart (ur symbol is loading bby)'

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(*self.size)
        self.setWindowTitle(self.title)

        self._status_bar: QStatusBar = None
        self._status_label: QLabel = None

    @property
    def mode_label(self) -> QtGui.QLabel:

        # init mode label
        if not self._status_label:

            self._status_label = label = QtGui.QLabel()
            label.setStyleSheet(
                f"QLabel {{ color : {hcolor('gunmetal')}; }}"
            )
            label.setTextFormat(3)  # markdown
            label.setFont(_font_small.font)
            label.setMargin(2)
            label.setAlignment(
                QtCore.Qt.AlignVCenter
                | QtCore.Qt.AlignRight
            )
            self.statusBar().addPermanentWidget(label)
            label.show()

        return self._status_label

    def closeEvent(
        self,
        event: QtGui.QCloseEvent,
    ) -> None:
        """Cancel the root actor asap.

        """
        # raising KBI seems to get intercepted by by Qt so just use the system.
        os.kill(os.getpid(), signal.SIGINT)

    @property
    def status_bar(self) -> QStatusBar:

        # style and cached the status bar on first access
        if not self._status_bar:

            sb = self.statusBar()
            sb.setStyleSheet((
                f"color : {hcolor('gunmetal')};"
                f"background : {hcolor('default_dark')};"
                f"font-size : {_font_small.px_size}px;"
                "padding : 0px;"
                # "min-height : 19px;"
                # "qproperty-alignment: AlignVCenter;"
            ))
            self.setStatusBar(sb)
            self._status_bar = MultiStatus(sb, [])

        return self._status_bar

    def on_focus_change(
        self,
        old: QtGui.QWidget,
        new: QtGui.QWidget,
    ) -> None:

        log.debug(f'widget focus changed from {old} -> {new}')

        if new is not None:
            # cursor left window?
            name = getattr(new, 'mode_name', '')
            self.mode_label.setText(name)

    def current_screen(self) -> QtGui.QScreen:
        """Get a frickin screen (if we can, gawd).

        """
        app = QtGui.QApplication.instance()

        for _ in range(3):
            screen = app.screenAt(self.pos())
            print('trying to access QScreen...')
            if screen is None:
                time.sleep(0.5)
                continue

            break
        else:
            if screen is None:
                # try for the first one we can find
                screen = app.screens()[0]

        assert screen, "Wow Qt is dumb as shit and has no screen..."
        return screen


# singleton app per actor
_qt_win: QtGui.QMainWindow = None


def main_window() -> MainWindow:
    'Return the actor-global Qt window.'

    global _qt_win
    assert _qt_win
    return _qt_win
