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

from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QEvent
import trio


class KeyCloner(QtCore.QObject):
    """Clone and forward keyboard events over a trio memory channel
    for later async processing.

    """

    _send_chan: trio.abc.SendChannel = None

    def eventFilter(
        self,
        source: QtGui.QWidget,
        ev: QEvent,
    ) -> None:

        if ev.type() == QEvent.KeyPress:

            # XXX: we unpack here because apparently doing it
            # after pop from the mem chan isn't showing the same
            # event object? no clue wtf is going on there, likely
            # something to do with Qt internals and calling the
            # parent handler?
            key = ev.key()
            mods = ev.modifiers()
            txt = ev.text()

            # run async processing
            self._send_chan.send_nowait((key, mods, txt))

        # never intercept the event
        return False


@asynccontextmanager
async def open_key_stream(

    source_widget: QtGui.QWidget,

) -> trio.abc.ReceiveChannel:

    # 1 to force eager sending
    send, recv = trio.open_memory_channel(16)

    kc = KeyCloner()
    kc._send_chan = send
    source_widget.installEventFilter(kc)

    try:
        yield recv
    finally:
        await send.aclose()
        source_widget.removeEventFilter(kc)
