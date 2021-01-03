# piker: trading gear for hackers
# Copyright (C) 2018-present Tyler Goodlet (in stewardship for piker0)

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
In suit parlance: "Execution management systems"

"""
from typing import AsyncIterator

import trio
from trio_typing import TaskStatus
import tractor

from . import data
from .log import get_logger


log = get_logger(__name__)

_to_router: trio.abc.SendChannel = None
_from_ui: trio.abc.ReceiveChannel = None


# TODO: make this a ``tractor.msg.pub``
async def stream_orders():
    """Order streaming task: deliver orders transmitted from UI
    to downstream consumers.

    This is run in the UI actor (usually the one running Qt).
    The UI simply delivers order messages to the above ``_to_router``
    send channel (from sync code using ``.send_nowait()``), these values
    are pulled from the channel here and send to any consumer(s).

    """
    global _from_ui

    async for order in _from_ui:
        yield order


async def exec_orders(
    ctx: tractor.Context,
    broker: str,
    symbol: str,
    exec_price: float,
) -> AsyncIterator[dict]:

    async with data.open_feed(
        broker,
        [symbol],
        loglevel='info',
    ) as feed:

        # TODO: get initial price

        quote = await feed.receive()

        # we automatically figure out what the alert check condition
        # should be based on the current first price received from the
        # feed, instead of being like every other shitty tina platform
        # that makes the user choose the predicate operator.
        last = quote[symbol]['close']

        if exec_price > last:

            def check(price: float) -> bool:
                if price >= exec_price:
                    return True
                else:
                    return False

        elif exec_price < last:

            def check(price: float) -> bool:
                if price <= exec_price:
                    return True
                else:
                    return False

        async for quotes in feed.stream:

            for sym, quote in quotes.items():

                for tick in quote.get('ticks', ()):
                    price = tick.get('price')

                    # push trigger msg back to parent as an "alert"
                    # (mocking for eg. a "fill")
                    if price and check(price):
                        await ctx.send_yield({
                            'type': 'alert',
                        })
                        return
    # feed teardown


@tractor.stream
async def stream_and_route(ctx, ui_name):
    """Order router (sub)actor entrypoint.

    """
    actor = tractor.current_actor()

    # new router entry point
    async with tractor.wait_for_actor(ui_name) as portal:

        async with trio.open_nursery() as n:

            async for order in await portal.run(stream_orders):

                tp = order['type']
                price = order['price']
                sym = order['symbol']
                brokers = order['brokers']

                if tp == 'alert':
                    log.info(f'Alert {order} received in {actor.uid}')

                    n.start_soon(
                        exec_orders,
                        ctx,
                        # TODO: eventually support N-brokers
                        brokers[0],
                        sym,
                        price,
                    )

                # begin wait on next order


async def spawn_router_stream_alerts(
    ident: str,
    task_status: TaskStatus[str] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Spawn an EMS daemon and begin sending orders and receiving
    alerts.

    """
    # setup local ui event streaming channels
    global _from_ui, _to_router
    _to_router, _from_ui = trio.open_memory_channel(100)

    actor = tractor.current_actor()
    subactor_name = ident + '.router'

    async with tractor.open_nursery() as n:

        portal = await n.start_actor(
            subactor_name,
            rpc_module_paths=[__name__],
        )
        stream = await portal.run(
            stream_and_route,
            ui_name=actor.name
        )
        async with tractor.wait_for_actor(subactor_name):
            # let parent task continue
            task_status.started(_to_router)

        async for alert in stream:

            # TODO: this in another task?
            # not sure if this will ever be a bottleneck,
            # we probably could do graphics stuff first tho?
            result = await trio.run_process(
                [
                    'notify-send',
                    'piker',
                    f'Alert: {alert}',
                    '-u', 'normal',
                    '-t', '10000',
                ],
            )
            log.runtime(result)
