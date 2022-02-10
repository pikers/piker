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
from contextlib import asynccontextmanager, AsyncExitStack
from types import ModuleType
from typing import Any, Callable, AsyncGenerator
import json

import trio
import trio_websocket
from trio_websocket._impl import (
    ConnectionClosed,
    DisconnectionTimeout,
    ConnectionRejected,
    HandshakeError,
    ConnectionTimeout,
)

from ..log import get_logger

log = get_logger(__name__)


class NoBsWs:
    """Make ``trio_websocket`` sockets stay up no matter the bs.

    """
    recon_errors = (
        ConnectionClosed,
        DisconnectionTimeout,
        ConnectionRejected,
        HandshakeError,
        ConnectionTimeout,
    )

    def __init__(
        self,
        url: str,
        token: str,
        stack: AsyncExitStack,
        fixture: Callable,
        serializer: ModuleType = json,
    ):
        self.url = url
        self.token = token
        self.fixture = fixture
        self._stack = stack
        self._ws: 'WebSocketConnection' = None  # noqa

    async def _connect(
        self,
        tries: int = 1000,
    ) -> None:
        while True:
            try:
                await self._stack.aclose()
            except (DisconnectionTimeout, RuntimeError):
                await trio.sleep(0.5)
            else:
                break

        last_err = None
        for i in range(tries):
            try:
                self._ws = await self._stack.enter_async_context(
                    trio_websocket.open_websocket_url(self.url)
                )
                # rerun user code fixture
                if self.token == '':
                    ret = await self._stack.enter_async_context(
                        self.fixture(self)
                    )
                else:
                    ret = await self._stack.enter_async_context(
                        self.fixture(self, self.token)
                    )

                assert ret is None

                log.info(f'Connection success: {self.url}')
                return self._ws

            except self.recon_errors as err:
                last_err = err
                log.error(
                    f'{self} connection bail with '
                    f'{type(err)}...retry attempt {i}'
                )
                await trio.sleep(0.5)
                continue
        else:
            log.exception('ws connection fail...')
            raise last_err

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


@asynccontextmanager
async def open_autorecon_ws(
    url: str,

    # TODO: proper type annot smh
    fixture: Callable,
    # used for authenticated websockets
    token: str = '',
) -> AsyncGenerator[tuple[...],  NoBsWs]:
    """Apparently we can QoS for all sorts of reasons..so catch em.

    """
    async with AsyncExitStack() as stack:
        ws = NoBsWs(url, token, stack, fixture=fixture)
        await ws._connect()

        try:
            yield ws

        finally:
            await stack.aclose()
