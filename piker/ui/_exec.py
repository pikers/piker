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
from PyQt5.QtCore import (
    pyqtRemoveInputHook,
    Qt,
    QCoreApplication,
)
import qdarkstyle
import trio
import tractor
from outcome import Error

from .._daemon import maybe_open_pikerd
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

    start = time.time()

    tries = 3
    for _ in range(3):
        screen = _qt_app.screenAt(_qt_win.pos())
        print(f'trying to get screen....')
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


class MainWindow(QtGui.QMainWindow):

    size = (800, 500)
    title = 'piker chart (ur symbol is loading bby)'

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(*self.size)
        self.setWindowTitle(self.title)

    def closeEvent(
        self,
        event: QtGui.QCloseEvent,
    ) -> None:
        """Cancel the root actor asap.

        """
        # raising KBI seems to get intercepted by by Qt so just use the system.
        os.kill(os.getpid(), signal.SIGINT)


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
    app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt5'))

    # make window and exec
    window = window_type()
    instance = main_widget()
    instance.window = window

    widgets = {
        'window': window,
        'main': instance,
    }

    # define tractor entrypoint
    async def main():

        async with maybe_open_pikerd(
            **tractor_kwargs,
        ):
            await func(*((widgets,) + args))

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
