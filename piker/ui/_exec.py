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
Trio - Qt integration

Run ``trio`` in guest mode on top of the Qt event loop.
All global Qt runtime settings are mostly defined here.
"""
from typing import Tuple, Callable, Dict, Any
import os
import platform
import signal
import time
import traceback

# Qt specific
import PyQt5  # noqa
import pyqtgraph as pg
from pyqtgraph import QtGui
from PyQt5 import QtCore
from PyQt5.QtGui import QLabel, QStatusBar
from PyQt5.QtCore import (
    pyqtRemoveInputHook,
    Qt,
    QCoreApplication,
)
import qdarkstyle
from qdarkstyle import DarkPalette
# import qdarkgraystyle
import trio
from outcome import Error

from .._daemon import maybe_open_pikerd, _tractor_kwargs
from ..log import get_logger
from ._pg_overrides import _do_overrides

log = get_logger(__name__)

# pyqtgraph global config
# might as well enable this for now?
pg.useOpenGL = True
pg.enableExperimental = True

# engage core tweaks that give us better response
# latency then the average pg user
_do_overrides()


# singleton app per actor
_qt_app: QtGui.QApplication = None
_qt_win: QtGui.QMainWindow = None


def current_screen() -> QtGui.QScreen:
    """Get a frickin screen (if we can, gawd).

    """
    global _qt_win, _qt_app

    for _ in range(3):
        screen = _qt_app.screenAt(_qt_win.pos())
        print('trying to access QScreen...')
        if screen is None:
            time.sleep(0.5)
            continue

        break
    else:
        if screen is None:
            # try for the first one we can find
            screen = _qt_app.screens()[0]

    assert screen, "Wow Qt is dumb as shit and has no screen..."
    return screen


# XXX: pretty sure none of this shit works on linux as per:
# https://bugreports.qt.io/browse/QTBUG-53022
# it seems to work on windows.. no idea wtf is up.
if platform.system() == "Windows":

    # Proper high DPI scaling is available in Qt >= 5.6.0. This attibute
    # must be set before creating the application
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)

    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)


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
            # TODO: i guess refactor stuff to avoid having to import here?
            from ._style import _font_small, hcolor
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
            # TODO: i guess refactor stuff to avoid having to import here?
            from ._style import _font_small, hcolor
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


def run_qtractor(
    func: Callable,
    args: Tuple,
    main_widget: QtGui.QWidget,
    tractor_kwargs: Dict[str, Any] = {},
    window_type: QtGui.QMainWindow = MainWindow,
) -> None:
    # avoids annoying message when entering debugger from qt loop
    pyqtRemoveInputHook()

    app = QtGui.QApplication.instance()
    if app is None:
        app = PyQt5.QtWidgets.QApplication([])

    # TODO: we might not need this if it's desired
    # to cancel the tractor machinery on Qt loop
    # close, however the details of doing that correctly
    # currently seem tricky..
    app.setQuitOnLastWindowClosed(False)

    # XXX: lmfao, this is how you disable text edit cursor blinking..smh
    app.setCursorFlashTime(0)

    # set global app singleton
    global _qt_app
    _qt_app = app

    # This code is from Nathaniel, and I quote:
    # "This is substantially faster than using a signal... for some
    # reason Qt signal dispatch is really slow (and relies on events
    # underneath anyway, so this is strictly less work)."
    REENTER_EVENT = QtCore.QEvent.Type(QtCore.QEvent.registerEventType())

    class ReenterEvent(QtCore.QEvent):
        pass

    class Reenter(QtCore.QObject):
        def event(self, event):
            event.fn()
            return False

    reenter = Reenter()

    def run_sync_soon_threadsafe(fn):
        event = ReenterEvent(REENTER_EVENT)
        event.fn = fn
        app.postEvent(reenter, event)

    def done_callback(outcome):

        if isinstance(outcome, Error):
            exc = outcome.error

            if isinstance(outcome.error, KeyboardInterrupt):
                # make it kinda look like ``trio``
                print("Terminated!")

            else:
                traceback.print_exception(type(exc), exc, exc.__traceback__)

        app.quit()

    # load dark theme
    stylesheet = qdarkstyle.load_stylesheet(
        qt_api='pyqt5',
        palette=DarkPalette,
    )
    app.setStyleSheet(stylesheet)

    # make window and exec
    window = window_type()

    # hook into app focus change events
    app.focusChanged.connect(window.on_focus_change)

    instance = main_widget()
    instance.window = window

    # override tractor's defaults
    tractor_kwargs.update(_tractor_kwargs)

    # define tractor entrypoint
    async def main():

        async with maybe_open_pikerd(
            **tractor_kwargs,
        ):
            await func(*((instance,) + args))

    # guest mode entry
    trio.lowlevel.start_guest_run(
        main,
        run_sync_soon_threadsafe=run_sync_soon_threadsafe,
        done_callback=done_callback,
        # restrict_keyboard_interrupt_to_checkpoints=True,
    )

    window.main_widget = main_widget
    window.setCentralWidget(instance)

    # store global ref
    # set global app singleton
    global _qt_win
    _qt_win = window

    # actually render to screen
    window.show()
    app.exec_()
