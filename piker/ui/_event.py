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
Qt event proxying and processing using ``trio`` mem chans.

"""
from contextlib import asynccontextmanager
from typing import Callable

from PyQt5 import QtCore
from PyQt5.QtCore import QEvent
from PyQt5.QtGui import QWidget
import trio


class EventRelay(QtCore.QObject):
    '''
    Relay Qt events over a trio memory channel for async processing.

    '''
    _event_types: set[QEvent] = set()
    _send_chan: trio.abc.SendChannel = None
    _filter_auto_repeats: bool = True

    def eventFilter(
        self,
        source: QWidget,
        ev: QEvent,
    ) -> None:
        '''
        Qt global event filter: return `False` to pass through and `True`
        to filter event out.

        https://doc.qt.io/qt-5/qobject.html#eventFilter
        https://doc.qt.io/qtforpython/overviews/eventsandfilters.html#event-filters

        '''
        etype = ev.type()
        # print(f'etype: {etype}')

        if etype in self._event_types:
            # ev.accept()

            # TODO: what's the right way to allow this?
            # if ev.isAutoRepeat():
            #     ev.ignore()

            # XXX: we unpack here because apparently doing it
            # after pop from the mem chan isn't showing the same
            # event object? no clue wtf is going on there, likely
            # something to do with Qt internals and calling the
            # parent handler?

            if etype in {QEvent.KeyPress, QEvent.KeyRelease}:

                # TODO: is there a global setting for this?
                if ev.isAutoRepeat() and self._filter_auto_repeats:
                    ev.ignore()
                    return True

                key = ev.key()
                mods = ev.modifiers()
                txt = ev.text()

                # NOTE: the event object instance coming out
                # the other side is mutated since Qt resumes event
                # processing **before** running a ``trio`` guest mode
                # tick, thus special handling or copying must be done.

                # send elements to async handler
                self._send_chan.send_nowait((ev, etype, key, mods, txt))

            else:
                # send event to async handler
                self._send_chan.send_nowait(ev)

            # **do not** filter out this event
            # and instead forward to the source widget
            return False

        # filter out this event
        # https://doc.qt.io/qt-5/qobject.html#installEventFilter
        return False


@asynccontextmanager
async def open_event_stream(

    source_widget: QWidget,
    event_types: set[QEvent] = {QEvent.KeyPress},
    filter_auto_repeats: bool = True,

) -> trio.abc.ReceiveChannel:

    # 1 to force eager sending
    send, recv = trio.open_memory_channel(16)

    kc = EventRelay()
    kc._send_chan = send
    kc._event_types = event_types
    kc._filter_auto_repeats = filter_auto_repeats

    source_widget.installEventFilter(kc)

    try:
        yield recv

    finally:
        await send.aclose()
        source_widget.removeEventFilter(kc)


@asynccontextmanager
async def open_handler(

    source_widget: QWidget,
    event_types: set[QEvent],
    async_handler: Callable[[QWidget, trio.abc.ReceiveChannel], None],
    **kwargs,

) -> None:

    async with (
        trio.open_nursery() as n,
        open_event_stream(source_widget, event_types, **kwargs) as event_recv_stream,
    ):
        n.start_soon(async_handler, source_widget, event_recv_stream)
        yield
        n.cancel_scope.cancel()
