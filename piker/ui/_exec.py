"""
Trio - Qt integration

Run ``trio`` in guest mode on top of the Qt event loop.
All global Qt runtime settings are mostly defined here.
"""
import traceback

import PyQt5  # noqa
from pyqtgraph import QtGui
from PyQt5 import QtCore
from PyQt5.QtCore import pyqtRemoveInputHook
import qdarkstyle
import trio
from outcome import Error


# Taken from Quantdom
class MainWindow(QtGui.QMainWindow):

    size = (800, 500)
    title = 'piker: chart'

    def __init__(self, parent=None):
        super().__init__(parent)
        # self.setMinimumSize(*self.size)
        self.setWindowTitle(self.title)


def run_qtrio(
    func,
    args,
    main_widget,
) -> None:
    # avoids annoying message when entering debugger from qt loop
    pyqtRemoveInputHook()

    app = QtGui.QApplication.instance()
    if app is None:
        app = PyQt5.QtWidgets.QApplication([])

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
    window = MainWindow()
    instance = main_widget()

    widgets = {
        'window': window,
        'main': instance,
    }

    # guest mode
    trio.lowlevel.start_guest_run(
        func,
        widgets,
        run_sync_soon_threadsafe=run_sync_soon_threadsafe,
        done_callback=done_callback,
    )

    window.main_widget = main_widget
    window.setCentralWidget(instance)
    # actually render to screen
    window.show()
    app.exec_()
