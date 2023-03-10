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
Orders and execution client API.

"""
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from pprint import pformat
from typing import TYPE_CHECKING

import trio
import tractor
from tractor.trionics import broadcast_receiver

from ..log import get_logger
from ..data.types import Struct
from ..service import maybe_open_emsd
from ._messages import (
    Order,
    Cancel,
)
from ..brokers import get_brokermod

if TYPE_CHECKING:
    from ._messages import (
        BrokerdPosition,
        Status,
    )


log = get_logger(__name__)


class OrderBook(Struct):
    '''EMS-client-side order book ctl and tracking.

    A style similar to "model-view" is used here where this api is
    provided as a supervised control for an EMS actor which does all the
    hard/fast work of talking to brokers/exchanges to conduct
    executions.

    Currently, this is mostly for keeping local state to match the EMS
    and use received events to trigger graphics updates.

    '''
    # mem channels used to relay order requests to the EMS daemon
    _to_ems: trio.abc.SendChannel
    _from_order_book: trio.abc.ReceiveChannel
    _sent_orders: dict[str, Order] = {}

    def send(
        self,
        msg: Order | dict,

    ) -> dict:
        self._sent_orders[msg.oid] = msg
        self._to_ems.send_nowait(msg)
        return msg

    def send_update(
        self,

        uuid: str,
        **data: dict,

    ) -> dict:
        cmd = self._sent_orders[uuid]
        msg = cmd.copy(update=data)
        self._sent_orders[uuid] = msg
        self._to_ems.send_nowait(msg)
        return cmd

    def cancel(self, uuid: str) -> bool:
        """Cancel an order (or alert) in the EMS.

        """
        cmd = self._sent_orders.get(uuid)
        if not cmd:
            log.error(
                f'Unknown order {uuid}!?\n'
                f'Maybe there is a stale entry or line?\n'
                f'You should report this as a bug!'
            )
        msg = Cancel(
            oid=uuid,
            symbol=cmd.symbol,
        )
        self._to_ems.send_nowait(msg)


_orders: OrderBook = None


def get_orders(
    emsd_uid: tuple[str, str] = None
) -> OrderBook:
    """"
    OrderBook singleton factory per actor.

    """
    if emsd_uid is not None:
        # TODO: read in target emsd's active book on startup
        pass

    global _orders

    if _orders is None:
        size = 100
        tx, rx = trio.open_memory_channel(size)
        brx = broadcast_receiver(rx, size)

        # setup local ui event streaming channels for request/resp
        # streamging with EMS daemon
        _orders = OrderBook(
            _to_ems=tx,
            _from_order_book=brx,
        )

    return _orders


# TODO: we can get rid of this relay loop once we move
# order_mode inputs to async code!
async def relay_order_cmds_from_sync_code(

    symbol_key: str,
    to_ems_stream: tractor.MsgStream,

) -> None:
    """
    Order streaming task: deliver orders transmitted from UI
    to downstream consumers.

    This is run in the UI actor (usually the one running Qt but could be
    any other client service code). This process simply delivers order
    messages to the above ``_to_ems`` send channel (from sync code using
    ``.send_nowait()``), these values are pulled from the channel here
    and relayed to any consumer(s) that called this function using
    a ``tractor`` portal.

    This effectively makes order messages look like they're being
    "pushed" from the parent to the EMS where local sync code is likely
    doing the pushing from some UI.

    """
    book = get_orders()
    async with book._from_order_book.subscribe() as orders_stream:
        async for cmd in orders_stream:
            sym = cmd.symbol
            msg = pformat(cmd)
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
    fqsn: str,
    mode: str = 'live',
    loglevel: str = 'error',

) -> tuple[
    OrderBook,
    tractor.MsgStream,
    dict[
        # brokername, acctid
        tuple[str, str],
        list[BrokerdPosition],
    ],
    list[str],
    dict[str, Status],
]:
    '''
    Spawn an EMS daemon and begin sending orders and receiving
    alerts.

    This EMS tries to reduce most broker's terrible order entry apis to
    a very simple protocol built on a few easy to grok and/or
    "rantsy" premises:

    - most users will prefer "dark mode" where orders are not submitted
      to a broker until and execution condition is triggered
      (aka client-side "hidden orders")

    - Brokers over-complicate their apis and generally speaking hire
      poor designers to create them. We're better off using creating a super
      minimal, schema-simple, request-event-stream protocol to unify all the
      existing piles of shit (and shocker, it'll probably just end up
      looking like a decent crypto exchange's api)

    - all order types can be implemented with client-side limit orders

    - we aren't reinventing a wheel in this case since none of these
      brokers are exposing FIX protocol; it is they doing the re-invention.


    TODO: make some fancy diagrams using mermaid.io

    the possible set of responses from the stream  is currently:
    - 'dark_submitted', 'broker_submitted'
    - 'dark_cancelled', 'broker_cancelled'
    - 'dark_executed', 'broker_executed'
    - 'broker_filled'

    '''
    # wait for service to connect back to us signalling
    # ready for order commands
    book = get_orders()

    from ..data._source import unpack_fqsn
    broker, symbol, suffix = unpack_fqsn(fqsn)

    async with maybe_open_emsd(broker) as portal:

        mod = get_brokermod(broker)
        if (
            not getattr(mod, 'trades_dialogue', None)
            or mode == 'paper'
        ):
            mode = 'paper'

        from ._ems import _emsd_main
        async with (
            # connect to emsd
            portal.open_context(

                _emsd_main,
                fqsn=fqsn,
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
            # start sync code order msg delivery task
            async with trio.open_nursery() as n:
                n.start_soon(
                    relay_order_cmds_from_sync_code,
                    fqsn,
                    trades_stream
                )

                yield (
                    book,
                    trades_stream,
                    positions,
                    accounts,
                    dialogs,
                )
