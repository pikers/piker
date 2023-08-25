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
from contextlib import asynccontextmanager as acm
from typing import Callable

import trio
from tractor.trionics import gather_contexts
from PyQt5 import QtCore
from PyQt5.QtCore import QEvent, pyqtBoundSignal
from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import (
    QGraphicsSceneMouseEvent as gs_mouse,
)

from piker.types import Struct


MOUSE_EVENTS = {
    gs_mouse.GraphicsSceneMousePress,
    gs_mouse.GraphicsSceneMouseRelease,
    QEvent.MouseButtonPress,
    QEvent.MouseButtonRelease,
    # QtGui.QMouseEvent,
}


# TODO: maybe consider some constrained ints down the road?
# https://pydantic-docs.helpmanual.io/usage/types/#constrained-types

class KeyboardMsg(Struct):
    '''Unpacked Qt keyboard event data.

    '''
    event: QEvent
    etype: int
    key: int
    mods: int
    txt: str

    def to_tuple(self) -> tuple:
        return tuple(self.to_dict().values())


class MouseMsg(Struct):
    '''Unpacked Qt keyboard event data.

    '''
    event: QEvent
    etype: int
    button: int


# TODO: maybe add some methods to detect key combos? Or is that gonna be
# better with pattern matching?
# # ctl + alt as combo
# ctlalt = False
# if (QtCore.Qt.AltModifier | QtCore.Qt.ControlModifier) == mods:
#     ctlalt = True


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
        # TODO: turn this on and see what we can filter by default (such
        # as mouseWheelEvent).
        # print(f'ev: {ev}')

        if etype not in self._event_types:
            return False

        # XXX: we unpack here because apparently doing it
        # after pop from the mem chan isn't showing the same
        # event object? no clue wtf is going on there, likely
        # something to do with Qt internals and calling the
        # parent handler?

        if etype in {QEvent.KeyPress, QEvent.KeyRelease}:

            msg = KeyboardMsg(
                event=ev,
                etype=etype,
                key=ev.key(),
                mods=ev.modifiers(),
                txt=ev.text(),
            )

            # TODO: is there a global setting for this?
            if ev.isAutoRepeat() and self._filter_auto_repeats:
                ev.ignore()
                # filter out this event and stop it's processing
                # https://doc.qt.io/qt-5/qobject.html#installEventFilter
                return True

            # NOTE: the event object instance coming out
            # the other side is mutated since Qt resumes event
            # processing **before** running a ``trio`` guest mode
            # tick, thus special handling or copying must be done.

        elif etype in MOUSE_EVENTS:
            # print('f mouse event: {ev}')
            msg = MouseMsg(
                event=ev,
                etype=etype,
                button=ev.button(),
            )

        else:
            msg = ev

        # send event-msg to async handler
        self._send_chan.send_nowait(msg)

        # **do not** filter out this event
        # and instead forward to the source widget
        # https://doc.qt.io/qt-5/qobject.html#installEventFilter
        return False


@acm
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
        async with send:
            yield recv
    finally:
        source_widget.removeEventFilter(kc)


@acm
async def open_signal_handler(

    signal: pyqtBoundSignal,
    async_handler: Callable,

) -> trio.abc.ReceiveChannel:

    send, recv = trio.open_memory_channel(0)

    def proxy_args_to_chan(*args):
        send.send_nowait(args)

    signal.connect(proxy_args_to_chan)

    async def proxy_to_handler():
        async for args in recv:
            await async_handler(*args)

    async with trio.open_nursery() as n:
        n.start_soon(proxy_to_handler)
        async with send:
            yield


@acm
async def open_handlers(

    source_widgets: list[QWidget],
    event_types: set[QEvent],
    async_handler: Callable[[QWidget, trio.abc.ReceiveChannel], None],
    **kwargs,

) -> None:
    async with (
        trio.open_nursery() as n,
        gather_contexts([
            open_event_stream(widget, event_types, **kwargs)
            for widget in source_widgets
        ]) as streams,
    ):
        for widget, event_recv_stream in zip(source_widgets, streams):
            n.start_soon(async_handler, widget, event_recv_stream)

        yield
