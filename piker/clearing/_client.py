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
Orders and execution client API.

"""
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from pprint import pformat
from typing import TYPE_CHECKING

import trio
import tractor
from tractor.trionics import broadcast_receiver

from ._util import (
    log,  # sub-sys logger
)
from ..data.types import Struct
from ..service import maybe_open_emsd
from ._messages import (
    Order,
    Cancel,
    BrokerdPosition,
)

if TYPE_CHECKING:
    from ._messages import (
        Status,
    )


class OrderClient(Struct):
    '''
    EMS-client-side order book ctl and tracking.

    (A)sync API for submitting orders and alerts to the `emsd` service;
    this is the main control for execution management from client code.

    '''
    # IPC stream to `emsd` actor
    _ems_stream: tractor.MsgStream

    # mem channels used to relay order requests to the EMS daemon
    _to_relay_task: trio.abc.SendChannel
    _from_sync_order_client: trio.abc.ReceiveChannel

    # history table
    _sent_orders: dict[str, Order] = {}

    def send_nowait(
        self,
        msg: Order | dict,

    ) -> dict | Order:
        '''
        Sync version of ``.send()``.

        '''
        self._sent_orders[msg.oid] = msg
        self._to_relay_task.send_nowait(msg)
        return msg

    async def send(
        self,
        msg: Order | dict,

    ) -> dict | Order:
        '''
        Send a new order msg async to the `emsd` service.

        '''
        self._sent_orders[msg.oid] = msg
        await self._ems_stream.send(msg)
        return msg

    def update_nowait(
        self,
        uuid: str,
        **data: dict,

    ) -> dict:
        '''
        Sync version of ``.update()``.

        '''
        cmd = self._sent_orders[uuid]
        msg = cmd.copy(update=data)
        self._sent_orders[uuid] = msg
        self._to_relay_task.send_nowait(msg)
        return msg

    async def update(
        self,
        uuid: str,
        **data: dict,
    ) -> dict:
        '''
        Update an existing order dialog with a msg updated from
        ``update`` kwargs.

        '''
        cmd = self._sent_orders[uuid]
        msg = cmd.copy(update=data)
        self._sent_orders[uuid] = msg
        await self._ems_stream.send(msg)
        return msg

    def _mk_cancel_msg(
        self,
        uuid: str,
    ) -> Cancel:
        cmd = self._sent_orders.get(uuid)
        if not cmd:
            log.error(
                f'Unknown order {uuid}!?\n'
                f'Maybe there is a stale entry or line?\n'
                f'You should report this as a bug!'
            )
            return

        fqme = str(cmd.symbol)
        return Cancel(
            oid=uuid,
            symbol=fqme,
        )

    def cancel_nowait(
        self,
        uuid: str,

    ) -> None:
        '''
        Sync version of ``.cancel()``.

        '''
        self._to_relay_task.send_nowait(
            self._mk_cancel_msg(uuid)
        )

    async def cancel(
        self,
        uuid: str,

    ) -> bool:
        '''
        Cancel an already existintg order (or alert) dialog.

        '''
        await self._ems_stream.send(
            self._mk_cancel_msg(uuid)
        )



async def relay_orders_from_sync_code(

    client: OrderClient,
    symbol_key: str,
    to_ems_stream: tractor.MsgStream,

) -> None:
    '''
    Order submission relay task: deliver orders sent from synchronous (UI)
    code to the EMS via ``OrderClient._from_sync_order_client``.

    This is run in the UI actor (usually the one running Qt but could be
    any other client service code). This process simply delivers order
    messages to the above ``_to_relay_task`` send channel (from sync code using
    ``.send_nowait()``), these values are pulled from the channel here
    and relayed to any consumer(s) that called this function using
    a ``tractor`` portal.

    This effectively makes order messages look like they're being
    "pushed" from the parent to the EMS where local sync code is likely
    doing the pushing from some non-async UI handler.

    '''
    async with (
        client._from_sync_order_client.subscribe() as sync_order_cmds
    ):
        async for cmd in sync_order_cmds:
            sym = cmd.symbol
            msg = pformat(cmd.to_dict())

            if sym == symbol_key:
                log.info(f'Send order cmd:\n{msg}')
                # send msg over IPC / wire
                await to_ems_stream.send(cmd)

            else:
                log.warning(
                    f'Ignoring unmatched order cmd for {sym} != {symbol_key}:'
                    f'\n{msg}'
                )


@acm
async def open_ems(
    fqme: str,
    mode: str = 'live',
    loglevel: str = 'error',

) -> tuple[
    OrderClient,
    tractor.MsgStream,
    dict[
        # brokername, acctid
        tuple[str, str],
        dict[str, BrokerdPosition],
    ],
    list[str],
    dict[str, Status],
]:
    '''
    (Maybe) spawn an EMS-daemon (emsd), deliver an `OrderClient` for
    requesting orders/alerts and a `trades_stream` which delivers all
    response-msgs.

    This is a "client side" entrypoint which may spawn the `emsd` service
    if it can't be discovered and generally speaking is the lowest level
    broker control client-API.

    '''
    # TODO: prolly hand in the `MktPair` instance directly here as well!
    from piker.accounting import unpack_fqme
    broker, mktep, venue, suffix = unpack_fqme(fqme)

    async with maybe_open_emsd(
        broker,
        loglevel=loglevel,
    ) as portal:

        from ._ems import _emsd_main
        async with (
            # connect to emsd
            portal.open_context(
                _emsd_main,
                fqme=fqme,
                exec_mode=mode,
                loglevel=loglevel,

            ) as (
                ctx,
                (
                    positions,
                    accounts,
                    dialogs,
                )
            ),

            # open 2-way trade command stream
            ctx.open_stream() as trades_stream,
        ):
            size: int = 100  # what should this be?
            tx, rx = trio.open_memory_channel(size)
            brx = broadcast_receiver(rx, size)

            # setup local ui event streaming channels for request/resp
            # streamging with EMS daemon
            client = OrderClient(
                _ems_stream=trades_stream,
                _to_relay_task=tx,
                _from_sync_order_client=brx,
            )

            client._ems_stream = trades_stream

            # start sync code order msg delivery task
            async with trio.open_nursery() as n:
                n.start_soon(
                    relay_orders_from_sync_code,
                    client,
                    fqme,
                    trades_stream
                )

                yield (
                    client,
                    trades_stream,
                    positions,
                    accounts,
                    dialogs,
                )

                # stop the sync-msg-relay task on exit.
                n.cancel_scope.cancel()
