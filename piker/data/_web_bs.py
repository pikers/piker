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
ToOlS fOr CoPInG wITh "tHE wEB" protocols.

"""
from __future__ import annotations
from contextlib import (
    asynccontextmanager as acm,
)
from itertools import count
from functools import partial
from types import ModuleType
from typing import (
    Any,
    Optional,
    Callable,
    AsyncContextManager,
    AsyncGenerator,
    Iterable,
)
import json

import trio
from trio_typing import TaskStatus
from trio_websocket import (
    WebSocketConnection,
    open_websocket_url,
)
from wsproto.utilities import LocalProtocolError
from trio_websocket._impl import (
    ConnectionClosed,
    DisconnectionTimeout,
    ConnectionRejected,
    HandshakeError,
    ConnectionTimeout,
)

from piker.types import Struct
from ._util import log


class NoBsWs:
    '''
    Make ``trio_websocket`` sockets stay up no matter the bs.

    A shim interface that allows client code to stream from some
    ``WebSocketConnection`` but where any connectivy bs is handled
    automatcially and entirely in the background.

    NOTE: this type should never be created directly but instead is
    provided via the ``open_autorecon_ws()`` factor below.

    '''
    # apparently we can QoS for all sorts of reasons..so catch em.
    recon_errors = (
        ConnectionClosed,
        DisconnectionTimeout,
        ConnectionRejected,
        HandshakeError,
        ConnectionTimeout,
        LocalProtocolError,
    )

    def __init__(
        self,
        url: str,
        rxchan: trio.MemoryReceiveChannel,
        msg_recv_timeout: float,

        serializer: ModuleType = json
    ):
        self.url = url
        self._rx = rxchan
        self._timeout = msg_recv_timeout

        # signaling between caller and relay task which determines when
        # socket is connected (and subscribed).
        self._connected: trio.Event = trio.Event()

        # dynamically reset by the bg relay task
        self._ws: WebSocketConnection | None = None
        self._cs: trio.CancelScope | None = None

        # interchange codec methods
        # TODO: obviously the method API here may be different
        # for another interchange format..
        self._dumps: Callable = serializer.dumps
        self._loads: Callable = serializer.loads

    def connected(self) -> bool:
        return self._connected.is_set()

    async def reset(self) -> None:
        '''
        Reset the underlying ws connection by cancelling
        the bg relay task and waiting for it to signal
        a new connection.

        '''
        self._connected = trio.Event()
        self._cs.cancel()
        await self._connected.wait()

    async def send_msg(
        self,
        data: Any,
    ) -> None:
        while True:
            try:
                msg: Any = self._dumps(data)
                return await self._ws.send_message(msg)
            except self.recon_errors:
                await self.reset()

    async def recv_msg(self) -> Any:
        msg: Any = await self._rx.receive()
        data = self._loads(msg)
        return data

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.recv_msg()

    def set_recv_timeout(
        self,
        timeout: float,
    ) -> None:
        self._timeout = timeout


async def _reconnect_forever(
    url: str,
    snd: trio.MemorySendChannel,
    nobsws: NoBsWs,
    reset_after: int,  # msg recv timeout before reset attempt

    fixture: AsyncContextManager | None = None,
    task_status: TaskStatus = trio.TASK_STATUS_IGNORED,

) -> None:

    # TODO: can we just report "where" in the call stack
    # the client code is using the ws stream?
    # Maybe we can just drop this since it's already in the log msg
    # orefix?
    if fixture is not None:
        src_mod: str = fixture.__module__
    else:
        src_mod: str = 'unknown'

    async def proxy_msgs(
        ws: WebSocketConnection,
        pcs: trio.CancelScope,  # parent cancel scope
    ):
        '''
        Receive (under `timeout` deadline) all msgs from from underlying
        websocket and relay them to (calling) parent task via ``trio``
        mem chan.

        '''
        # after so many msg recv timeouts, reset the connection
        timeouts: int = 0

        while True:
            with trio.move_on_after(
                # can be dynamically changed by user code
                nobsws._timeout,
            ) as cs:
                try:
                    msg: Any = await ws.get_message()
                    await snd.send(msg)
                except nobsws.recon_errors:
                    log.exception(
                        f'{src_mod}\n'
                        f'{url} connection bail with:'
                    )
                    await trio.sleep(0.5)
                    pcs.cancel()

                    # go back to reonnect loop in parent task
                    return

            if cs.cancelled_caught:
                timeouts += 1
                if timeouts > reset_after:
                    log.error(
                        f'{src_mod}\n'
                        'WS feed seems down and slow af.. reconnecting\n'
                    )
                    pcs.cancel()

                    # go back to reonnect loop in parent task
                    return

    async def open_fixture(
        fixture: AsyncContextManager,
        nobsws: NoBsWs,
        task_status: TaskStatus = trio.TASK_STATUS_IGNORED,
    ):
        '''
        Open user provided `@acm` and sleep until any connection
        reset occurs.

        '''
        async with fixture(nobsws) as ret:
            assert ret is None
            task_status.started()
            await trio.sleep_forever()

    # last_err = None
    nobsws._connected = trio.Event()
    task_status.started()

    while not snd._closed:
        log.info(
            f'{src_mod}\n'
            f'{url} trying (RE)CONNECT'
        )

        ws: WebSocketConnection
        try:
            async with (
                trio.open_nursery() as n,
                open_websocket_url(url) as ws,
            ):
                cs = nobsws._cs = n.cancel_scope
                nobsws._ws = ws
                log.info(
                    f'{src_mod}\n'
                    f'Connection success: {url}'
                )

                # begin relay loop to forward msgs
                n.start_soon(
                    proxy_msgs,
                    ws,
                    cs,
                )

                if fixture is not None:
                    log.info(
                        f'{src_mod}\n'
                        f'Entering fixture: {fixture}'
                    )

                    # TODO: should we return an explicit sub-cs
                    # from this fixture task?
                    await n.start(
                        open_fixture,
                        fixture,
                        nobsws,
                    )

                # indicate to wrapper / opener that we are up and block
                # to let tasks run **inside** the ws open block above.
                nobsws._connected.set()
                await trio.sleep_forever()
        except HandshakeError:
            log.exception(f'Retrying connection')

        # ws & nursery block ends

        nobsws._connected = trio.Event()
        if cs.cancelled_caught:
            log.cancel(
                f'{url} connection cancelled!'
            )
            # if wrapper cancelled us, we expect it to also
            # have re-assigned a new event
            assert (
                nobsws._connected
                and not nobsws._connected.is_set()
            )

        # -> from here, move to next reconnect attempt iteration
        # in the while loop above Bp

    else:
        log.exception(
            f'{src_mod}\n'
            'ws connection closed by client...'
        )


@acm
async def open_autorecon_ws(
    url: str,

    fixture: AsyncContextManager | None = None,

    # time in sec between msgs received before
    # we presume connection might need a reset.
    msg_recv_timeout: float = 16,

    # count of the number of above timeouts before connection reset
    reset_after: int = 3,

) -> AsyncGenerator[tuple[...],  NoBsWs]:
    '''
    An auto-reconnect websocket (wrapper API) around
    ``trio_websocket.open_websocket_url()`` providing automatic
    re-connection on network errors, msg latency and thus roaming.

    Here we implement a re-connect websocket interface where a bg
    nursery runs ``WebSocketConnection.receive_message()``s in a loop
    and restarts the full http(s) handshake on catches of certain
    connetivity errors, or some user defined recv timeout.

    You can provide a ``fixture`` async-context-manager which will be
    entered/exitted around each connection reset; eg. for (re)requesting
    subscriptions without requiring streaming setup code to rerun.

    '''
    snd: trio.MemorySendChannel
    rcv: trio.MemoryReceiveChannel
    snd, rcv = trio.open_memory_channel(616)

    async with trio.open_nursery() as n:
        nobsws = NoBsWs(
            url,
            rcv,
            msg_recv_timeout=msg_recv_timeout,
        )
        await n.start(
            partial(
                _reconnect_forever,
                url,
                snd,
                nobsws,
                fixture=fixture,
                reset_after=reset_after,
            )
        )
        await nobsws._connected.wait()
        assert nobsws._cs
        assert nobsws.connected()

        try:
            yield nobsws
        finally:
            n.cancel_scope.cancel()


'''
JSONRPC response-request style machinery for transparent multiplexing of msgs
over a NoBsWs.

'''


class JSONRPCResult(Struct):
    id: int
    jsonrpc: str = '2.0'
    result: Optional[dict] = None
    error: Optional[dict] = None


@acm
async def open_jsonrpc_session(
    url: str,
    start_id: int = 0,
    response_type: type = JSONRPCResult,
    request_type: Optional[type] = None,
    request_hook: Optional[Callable] = None,
    error_hook: Optional[Callable] = None,
) -> Callable[[str, dict], dict]:

    async with (
        trio.open_nursery() as n,
        open_autorecon_ws(url) as ws
    ):
        rpc_id: Iterable = count(start_id)
        rpc_results: dict[int, dict] = {}

        async def json_rpc(method: str, params: dict) -> dict:
            '''
            perform a json rpc call and wait for the result, raise exception in
            case of error field present on response
            '''
            msg = {
                'jsonrpc': '2.0',
                'id': next(rpc_id),
                'method': method,
                'params': params
            }
            _id = msg['id']

            rpc_results[_id] = {
                'result': None,
                'event': trio.Event()
            }

            await ws.send_msg(msg)

            await rpc_results[_id]['event'].wait()

            ret = rpc_results[_id]['result']

            del rpc_results[_id]

            if ret.error is not None:
                raise Exception(json.dumps(ret.error, indent=4))

            return ret

        async def recv_task():
            '''
            receives every ws message and stores it in its corresponding
            result field, then sets the event to wakeup original sender
            tasks. also recieves responses to requests originated from
            the server side.

            '''
            async for msg in ws:
                match msg:
                    case {
                        'result': _,
                        'id': mid,
                    } if res_entry := rpc_results.get(mid):

                        res_entry['result'] = response_type(**msg)
                        res_entry['event'].set()

                    case {
                        'result': _,
                        'id': mid,
                    } if not rpc_results.get(mid):
                        log.warning(
                            f'Unexpected ws msg: {json.dumps(msg, indent=4)}'
                        )

                    case {
                        'method': _,
                        'params': _,
                    }:
                        log.debug(f'Recieved\n{msg}')
                        if request_hook:
                            await request_hook(request_type(**msg))

                    case {
                        'error': error
                    }:
                        log.warning(f'Recieved\n{error}')
                        if error_hook:
                            await error_hook(response_type(**msg))

                    case _:
                        log.warning(f'Unhandled JSON-RPC msg!?\n{msg}')

        n.start_soon(recv_task)
        yield json_rpc
        n.cancel_scope.cancel()
