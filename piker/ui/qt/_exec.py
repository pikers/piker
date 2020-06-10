"""
Trio - Qt integration

Run ``trio`` in guest mode on top of the Qt event loop.
All global Qt runtime settings are mostly defined here.
"""
import traceback
from datetime import datetime

import PyQt5  # noqa
from pyqtgraph import QtGui
from PyQt5 import QtCore
from PyQt5.QtCore import pyqtRemoveInputHook
import qdarkstyle
import trio
from outcome import Error

from _chart import QuotesTabWidget
from quantdom.base import Symbol
from quantdom.loaders import get_quotes


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

    # This code is from Nathaniel:

    # This is substantially faster than using a signal... for some
    # reason Qt signal dispatch is really slow (and relies on events
    # underneath anyway, so this is strictly less work)
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
    window.show()
    app.exec_()


async def plot_aapl(widgets):
    qtw = widgets['main']
    s = Symbol(ticker='AAPL', mode=Symbol.SHARES)
    get_quotes(
        symbol=s.ticker,
        date_from=datetime(1900, 1, 1),
        date_to=datetime(2030, 12, 31),
    )
    # spawn chart
    qtw.update_chart(s)
    await trio.sleep_forever()


if __name__ == '__main__':
    run_qtrio(plot_aapl, (), QuotesTabWidget)
