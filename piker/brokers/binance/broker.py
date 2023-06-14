# piker: trading gear for hackers
# Copyright (C)
#   Guillermo Rodriguez (aka ze jefe)
#   Tyler Goodlet
#   (in stewardship for pikers)

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

'''
Live order control B)

'''
from __future__ import annotations
from typing import (
    Any,
    AsyncIterator,
)
import time

import tractor
import trio

from piker.brokers._util import (
    get_logger,
)
from piker.data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)
from piker.brokers import (
    open_cached_client,
)
from piker.clearing._messages import (
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdStatus,
    BrokerdPosition,
    BrokerdFill,
    BrokerdCancel,
    # BrokerdError,
)

log = get_logger('piker.brokers.binance')


async def handle_order_requests(
    ems_order_stream: tractor.MsgStream
) -> None:
    async with open_cached_client('binance') as client:

        async for request_msg in ems_order_stream:
            log.info(f'Received order request {request_msg}')

            action = request_msg['action']

            if action in {'buy', 'sell'}:
                # validate
                order = BrokerdOrder(**request_msg)

                # call our client api to submit the order
                reqid = await client.submit_limit(
                    order.symbol,
                    order.action,
                    order.size,
                    order.price,
                    oid=order.oid
                )

                # deliver ack that order has been submitted to broker routing
                await ems_order_stream.send(
                    BrokerdOrderAck(

                        # ems order request id
                        oid=order.oid,

                        # broker specific request id
                        reqid=reqid,
                        time_ns=time.time_ns(),

                    ).dict()
                )

            elif action == 'cancel':
                msg = BrokerdCancel(**request_msg) 

                await client.submit_cancel(msg.symbol, msg.reqid)

            else:
                log.error(f'Unknown order command: {request_msg}')


@tractor.context
async def open_trade_dialog(
    ctx: tractor.Context,
    loglevel: str = None

) -> AsyncIterator[dict[str, Any]]:

    async with open_cached_client('binance') as client:
        if not client.api_key:
            await ctx.started('paper')
            return

    # table: PpTable
    # ledger: TransactionLedger

    # TODO: load pps and accounts using accounting apis!
    positions: list[BrokerdPosition] = []
    accounts: list[str] = ['binance.default']
    await ctx.started((positions, accounts))

    async with (
        ctx.open_stream() as ems_stream,
        trio.open_nursery() as n,
        open_cached_client('binance') as client,
        client.manage_listen_key() as listen_key,
    ):
        n.start_soon(handle_order_requests, ems_stream)

        ws: NoBsWs
        async with open_autorecon_ws(
            f'wss://stream.binance.com:9443/ws/{listen_key}',
        ) as ws:
            event = await ws.recv_msg()

            # https://binance-docs.github.io/apidocs/spot/en/#payload-balance-update
            if event.get('e') == 'executionReport':

                oid: str = event.get('c')
                side: str = event.get('S').lower()
                status: str = event.get('X')
                order_qty: float = float(event.get('q'))
                filled_qty: float = float(event.get('z'))
                cum_transacted_qty: float = float(event.get('Z'))
                price_avg: float = cum_transacted_qty / filled_qty
                broker_time: float = float(event.get('T'))
                commission_amount: float = float(event.get('n'))
                commission_asset: float = event.get('N')

                if status == 'TRADE':
                    if order_qty == filled_qty:
                        msg = BrokerdFill(
                            reqid=oid,
                            time_ns=time.time_ns(),
                            action=side,
                            price=price_avg,
                            broker_details={
                                'name': 'binance',
                                'commissions': {
                                    'amount': commission_amount,
                                    'asset': commission_asset
                                },
                                'broker_time': broker_time
                            },
                            broker_time=broker_time
                        )

                else:
                    if status == 'NEW':
                        status = 'submitted'

                    elif status == 'CANCELED':
                        status = 'cancelled'

                    msg = BrokerdStatus(
                        reqid=oid,
                        time_ns=time.time_ns(),
                        status=status,
                        filled=filled_qty,
                        remaining=order_qty - filled_qty,
                        broker_details={'name': 'binance'}
                    )

            else:
                # XXX: temporary, to catch unhandled msgs
                breakpoint()

            await ems_stream.send(msg.dict())
