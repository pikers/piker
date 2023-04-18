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
ToOlS fOr CoPInG wITh "tHE wEB" protocols.

"""
from contextlib import (
    asynccontextmanager,
    AsyncExitStack,
)
from itertools import count
from types import ModuleType
from typing import (
    Any,
    Optional,
    Callable,
    AsyncGenerator,
    Iterable,
)
import json

import trio
import trio_websocket
from wsproto.utilities import LocalProtocolError
from trio_websocket._impl import (
    ConnectionClosed,
    DisconnectionTimeout,
    ConnectionRejected,
    HandshakeError,
    ConnectionTimeout,
)

from ._util import log
from .types import Struct


class NoBsWs:
    '''
    Make ``trio_websocket`` sockets stay up no matter the bs.

    You can provide a ``fixture`` async-context-manager which will be
    enter/exitted around each reconnect operation.
    '''
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
        stack: AsyncExitStack,
        fixture: Optional[Callable] = None,
        serializer: ModuleType = json
    ):
        self.url = url
        self.fixture = fixture
        self._stack = stack
        self._ws: 'WebSocketConnection' = None  # noqa

        # TODO: is there some method we can call
        # on the underlying `._ws` to get this?
        self._connected: bool = False

    async def _connect(
        self,
        tries: int = 1000,
    ) -> None:

        self._connected = False
        while True:
            try:
                await self._stack.aclose()
            except self.recon_errors:
                await trio.sleep(0.5)
            else:
                break

        last_err = None
        for i in range(tries):
            try:
                self._ws = await self._stack.enter_async_context(
                    trio_websocket.open_websocket_url(self.url)
                )

                if self.fixture is not None:
                    # rerun user code fixture
                    ret = await self._stack.enter_async_context(
                        self.fixture(self)
                    )

                    assert ret is None

                log.info(f'Connection success: {self.url}')

                self._connected = True
                return self._ws

            except self.recon_errors as err:
                last_err = err
                log.error(
                    f'{self} connection bail with '
                    f'{type(err)}...retry attempt {i}'
                )
                await trio.sleep(0.5)
                self._connected = False
                continue
        else:
            log.exception('ws connection fail...')
            raise last_err

    def connected(self) -> bool:
        return self._connected

    async def send_msg(
        self,
        data: Any,
    ) -> None:
        while True:
            try:
                return await self._ws.send_message(json.dumps(data))
            except self.recon_errors:
                await self._connect()

    async def recv_msg(
        self,
    ) -> Any:
        while True:
            try:
                return json.loads(await self._ws.get_message())
            except self.recon_errors:
                await self._connect()

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.recv_msg()


@asynccontextmanager
async def open_autorecon_ws(
    url: str,

    # TODO: proper type cannot smh
    fixture: Optional[Callable] = None,

) -> AsyncGenerator[tuple[...], NoBsWs]:
    """Apparently we can QoS for all sorts of reasons..so catch em.

    """
    async with AsyncExitStack() as stack:
        ws = NoBsWs(url, stack, fixture=fixture)
        await ws._connect()

        try:
            yield ws

        finally:
            await stack.aclose()


'''
JSONRPC response-request style machinery for transparent multiplexing of msgs
over a NoBsWs.

'''


class JSONRPCResult(Struct):
    id: int
    jsonrpc: str = '2.0'
    result: Optional[dict] = None
    error: Optional[dict] = None


@asynccontextmanager
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
