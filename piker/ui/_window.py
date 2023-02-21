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

"""
Qt main window singletons and stuff.

"""
import os
import signal
import time
from typing import (
    Callable,
    Union,
)
import uuid

from PyQt5 import QtCore
from PyQt5.QtWidgets import (
    QWidget,
    QMainWindow,
    QApplication,
    QLabel,
    QStatusBar,
)

from PyQt5.QtGui import (
    QScreen,
    QCloseEvent,
)
from ..log import get_logger
from ._style import _font_small, hcolor
from ._chart import GodWidget


log = get_logger(__name__)


class MultiStatus:

    bar: QStatusBar
    statuses: list[str]

    def __init__(self, bar, statuses) -> None:
        self.bar = bar
        self.statuses = statuses
        self._to_clear: set = set()
        self._status_groups: dict[str, (set, Callable)] = {}

    def open_status(

        self,
        msg: str,
        final_msg: str | None = None,
        clear_on_next: bool = False,
        group_key: Union[bool, str] | None = False,

    ) -> Union[Callable[..., None], str]:
        '''
        Add a status to the status bar and return a close callback which
        when called will remove the status ``msg``.

        '''
        for old_msg in self._to_clear:
            try:
                self.statuses.remove(old_msg)
            except ValueError:
                pass

        self.statuses.append(msg)

        def remove_msg() -> None:
            try:
                self.statuses.remove(msg)
            except ValueError:
                pass

            self.render()

            if final_msg is not None:
                self.statuses.append(final_msg)
                self.render()
                self._to_clear.add(final_msg)

        ret = remove_msg

        # create a "status group" such that new `.open_status()`
        # calls can be made passing in the returned group key.
        # once all clear callbacks have been called from all statuses
        # in the group the final status msg to be removed will be the one
        # the one provided when `group_key=True`, this way you can
        # create a long living status that completes once all
        # sub-statuses have finished.
        if group_key is True:
            if clear_on_next:
                ValueError("Can't create group status and clear it on next?")

            # generate a key for a new "status group"
            new_group_key = str(uuid.uuid4())

            def pop_group_and_clear():

                subs, final_clear = self._status_groups.pop(new_group_key)
                assert not subs
                return remove_msg()

            self._status_groups[new_group_key] = (set(), pop_group_and_clear)
            ret = new_group_key

        elif group_key:

            def pop_from_group_and_maybe_clear_group():
                # remove the message for this sub-status
                remove_msg()

                # check to see if all other substatuses have cleared
                group_tup = self._status_groups.get(group_key)

                if group_tup:
                    subs, group_clear = group_tup
                    try:
                        subs.remove(msg)
                    except KeyError:
                        raise KeyError(f'no msg {msg} for group {group_key}!?')

                    if not subs:
                        group_clear()

            group = self._status_groups.get(group_key)
            if group:
                group[0].add(msg)
                ret = pop_from_group_and_maybe_clear_group

        self.render()

        if clear_on_next:
            self._to_clear.add(msg)

        return ret

    def render(self) -> None:
        '''
        Display all open statuses to bar.

        '''
        if self.statuses:
            self.bar.showMessage(f'{" ".join(self.statuses)}')
        else:
            self.bar.clearMessage()


class MainWindow(QMainWindow):

    # XXX: for tiling wms this should scale
    # with the alloted window size.
    # TODO: detect for tiling and if untrue set some size?
    # size = (300, 500)
    godwidget: GodWidget

    title = 'piker chart (ur symbol is loading bby)'

    def __init__(self, parent=None):
        super().__init__(parent)
        # self.setMinimumSize(*self.size)
        self.setWindowTitle(self.title)

        # set by runtime after `trio` is engaged.
        self.godwidget: GodWidget | None = None

        self._status_bar: QStatusBar = None
        self._status_label: QLabel = None
        self._size: tuple[int, int] | None = None

    @property
    def mode_label(self) -> QLabel:

        # init mode label
        if not self._status_label:

            self._status_label = label = QLabel()
            label.setStyleSheet(
                f"""QLabel {{
                    color : {hcolor('gunmetal')};
                }}
                """
                # font-size : {font_size}px;
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
        event: QCloseEvent,

    ) -> None:
        '''Cancel the root actor asap.

        '''
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

    def set_mode_name(
        self,
        name: str,

    ) -> None:

        self.mode_label.setText(f'mode:{name}')

    def on_focus_change(
        self,

        last: QWidget,
        current: QWidget,

    ) -> None:

        log.info(f'widget focus changed from {last} -> {current}')

        if current is not None:
            # cursor left window?
            name = getattr(current, 'mode_name', '')
            self.set_mode_name(name)

    def current_screen(self) -> QScreen:
        '''
        Get a frickin screen (if we can, gawd).

        '''
        app = QApplication.instance()

        for _ in range(3):
            screen = app.screenAt(self.pos())
            log.debug('trying to access QScreen...')
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

    def configure_to_desktop(
        self,
        size: tuple[int, int] | None = None,

    ) -> None:
        '''
        Explicitly size the window dimensions (for stacked window
        managers).

        For tina systems (like windoze) try to do a sane window size on
        startup.

        '''
        # https://stackoverflow.com/a/18975846
        if not size and not self._size:
            # app = QApplication.instance()
            geo = self.current_screen().geometry()
            h, w = geo.height(), geo.width()
            # use approx 1/3 of the area of the screen by default
            self._size = round(w * .666), round(h * .666)

        self.resize(*size or self._size)

    def resizeEvent(self, event: QtCore.QEvent) -> None:
        if (
            # event.spontaneous()
            event.oldSize().height == event.size().height
        ):
            event.ignore()
            return

        # XXX: uncomment for debugging..
        # attrs = {}
        # for key in dir(event):
        #     if key == '__dir__':
        #         continue
        #     attr = getattr(event, key)
        #     try:
        #         attrs[key] = attr()
        #     except TypeError:
        #         attrs[key] = attr

        # from pprint import pformat
        # print(
        #     f'{pformat(attrs)}\n'
        #     f'WINDOW RESIZE: {self.size()}\n\n'
        # )
        self.godwidget.on_win_resize(event)
        event.accept()


# singleton app per actor
_qt_win: QMainWindow = None


def main_window() -> MainWindow:
    'Return the actor-global Qt window.'

    global _qt_win
    assert _qt_win
    return _qt_win
