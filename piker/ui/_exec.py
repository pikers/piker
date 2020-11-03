"""
Trio - Qt integration

Run ``trio`` in guest mode on top of the Qt event loop.
All global Qt runtime settings are mostly defined here.
"""
from functools import partial
import traceback
from typing import Tuple, Callable, Dict, Any

# Qt specific
import PyQt5  # noqa
from pyqtgraph import QtGui
from PyQt5 import QtCore
from PyQt5.QtCore import (
    pyqtRemoveInputHook, Qt, QCoreApplication
)
import qdarkstyle
import trio
import tractor
from outcome import Error


# singleton app per actor
_qt_app: QtGui.QApplication = None
_qt_win: QtGui.QMainWindow = None


def current_screen() -> QtGui.QScreen:

    global _qt_win, _qt_app
    return _qt_app.screenAt(_qt_win.centralWidget().geometry().center())


# Proper high DPI scaling is available in Qt >= 5.6.0. This attibute
# must be set before creating the application
if hasattr(Qt, 'AA_EnableHighDpiScaling'):
    QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)

if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
    QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)


class MainWindow(QtGui.QMainWindow):

    size = (800, 500)
    title = 'piker chart (bby)'

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(*self.size)
        self.setWindowTitle(self.title)


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

        print(f"Outcome: {outcome}")

        if isinstance(outcome, Error):
            exc = outcome.error
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

    # setup tractor entry point args
    main = partial(
        tractor._main,
        async_fn=func,
        args=args + (widgets,),
        arbiter_addr=(
            tractor._default_arbiter_host,
            tractor._default_arbiter_port,
        ),
        name='qtractor',
        **tractor_kwargs,
    )

    # guest mode
    trio.lowlevel.start_guest_run(
        main,
        run_sync_soon_threadsafe=run_sync_soon_threadsafe,
        done_callback=done_callback,
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
