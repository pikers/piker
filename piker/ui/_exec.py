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
import platform
import traceback

# Qt specific
import PyQt5  # noqa
import pyqtgraph as pg
from pyqtgraph import QtGui
from PyQt5 import QtCore
# from PyQt5.QtGui import QLabel, QStatusBar
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
from . import _style

log = get_logger(__name__)

# pyqtgraph global config
# might as well enable this for now?
pg.useOpenGL = True
pg.enableExperimental = True

# engage core tweaks that give us better response
# latency then the average pg user
_do_overrides()


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


def run_qtractor(
    func: Callable,
    args: Tuple,
    main_widget: QtGui.QWidget,
    tractor_kwargs: Dict[str, Any] = {},
    window_type: QtGui.QMainWindow = None,
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

    # This code is from Nathaniel, and I quote:
    # "This is substantially faster than using a signal... for some
    # reason Qt signal dispatch is really slow (and relies on events
    # underneath anyway, so this is strictly less work)."

    # source gist and credit to njs:
    # https://gist.github.com/njsmith/d996e80b700a339e0623f97f48bcf0cb
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
    from . import _window

    if window_type is None:
        window_type = _window.MainWindow

    window = window_type()

    # set global app's main window singleton
    _window._qt_win = window

    # configure global DPI aware font sizes now that a screen
    # should be active from which we can read a DPI.
    _style._config_fonts_to_screen()

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

    # actually render to screen
    window.show()
    app.exec_()
