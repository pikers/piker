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
from dataclasses import dataclass, field
from typing import (
    AsyncIterator, List, Dict, Callable, Tuple,
    Any,
)
# import uuid

import pyqtgraph as pg
import trio
from trio_typing import TaskStatus
import tractor

from . import data
from .log import get_logger
from .data._source import Symbol
from .ui._style import hcolor


log = get_logger(__name__)

_to_router: trio.abc.SendChannel = None
_from_ui: trio.abc.ReceiveChannel = None
_lines = {}


_local_book = {}


@dataclass
class OrderBook:
    """Send (client?) side order book tracking.

    Mostly for keeping local state to match the EMS and use
    events to trigger graphics updates.

    """
    orders: Dict[str, dict] = field(default_factory=dict)
    _cmds_from_ui: trio.abc.ReceiveChannel = _from_ui

    async def alert(self, price: float) -> str:
        ...

    async def buy(self, price: float) -> str:
        ...

    async def sell(self, price: float) -> str:
        ...

    async def modify(self, oid: str, price) -> bool:
        ...

    async def cancel(self, oid: str) -> bool:
        ...


_orders: OrderBook = None


def get_orders() -> OrderBook:
    global _orders

    if _orders is None:
        _orders = OrderBook

    return _orders


# TODO: make this a ``tractor.msg.pub``
async def send_order_cmds():
    """Order streaming task: deliver orders transmitted from UI
    to downstream consumers.

    This is run in the UI actor (usually the one running Qt).
    The UI simply delivers order messages to the above ``_to_router``
    send channel (from sync code using ``.send_nowait()``), these values
    are pulled from the channel here and send to any consumer(s).

    """
    global _from_ui

    async for order in _from_ui:

        lc = order['chart']
        symbol = lc.symbol
        tp = order['type']
        price = order['price']
        oid = order['oid']

        print(f'oid: {oid}')
        # TODO
        # oid = str(uuid.uuid4())

        cmd = {
            'price': price,
            'action': 'alert',
            'symbol': symbol.key,
            'brokers': symbol.brokers,
            'type': tp,
            'price': price,
            'oid': oid,
        }

        _local_book[oid] = cmd

        yield cmd


# streaming tasks which check for conditions per symbol per broker
_scan_tasks: Dict[str, List] = {}

# levels which have an executable action (eg. alert, order, signal)
_levels: Dict[str, list] = {}

# up to date last values from target streams
_last_values: Dict[str, float] = {}


# TODO: numba all of this
def mk_check(trigger_price, known_last) -> Callable[[float, float], bool]:
    """Create a predicate for given ``exec_price`` based on last known
    price, ``known_last``.

    This is an automatic alert level thunk generator based on where the
    current last known value is and where the specified value of
    interest is; pick an appropriate comparison operator based on
    avoiding the case where the a predicate returns true immediately.

    """

    if trigger_price >= known_last:

        def check_gt(price: float) -> bool:
            if price >= trigger_price:
                return True
            else:
                return False

        return check_gt, 'gt'

    elif trigger_price <= known_last:

        def check_lt(price: float) -> bool:
            if price <= trigger_price:
                return True
            else:
                return False

        return check_lt, 'lt'


@dataclass
class _ExecBook:
    """EMS-side execution book.

    Contains conditions for executions (aka "orders").
    A singleton instance is created per EMS actor.

    """
    orders: Dict[
        Tuple[str, str],
        Tuple[
            # predicates
            Callable[[float], bool],

            # actions
            Callable[[float], Dict[str, Any]],

        ]
    ] = field(default_factory=dict)

    # most recent values
    lasts: Dict[
        Tuple[str, str],
        float
    ] = field(default_factory=dict)


_book = None


def get_book() -> _ExecBook:
    global _book

    if _book is None:
        _book = _ExecBook()

    return _book


async def exec_orders(
    ctx: tractor.Context,
    broker: str,
    symbol: str,
    exec_price: float,
    task_status: TaskStatus[dict] = trio.TASK_STATUS_IGNORED,
) -> AsyncIterator[dict]:

    async with data.open_feed(
        broker,
        [symbol],
        loglevel='info',
    ) as feed:

        # TODO: get initial price

        first_quote = await feed.receive()

        book = get_book()
        book.lasts[(broker, symbol)] = first_quote[symbol]['last']

        task_status.started(first_quote)

        # shield this field so the remote brokerd does not get cancelled
        stream = feed.stream

        with stream.shield():
            async for quotes in stream:

                for sym, quote in quotes.items():

                    execs = book.orders.get((broker, sym))

                    for tick in quote.get('ticks', ()):
                        price = tick.get('price')
                        if price < 0:
                            # lel, fuck you ib
                            continue

                        # update to keep new cmds informed
                        book.lasts[(broker, symbol)] = price

                        # begin price actions sequence

                        if not execs:
                            continue

                        for oid, pred, action in tuple(execs):
                            # push trigger msg back to parent as an "alert"
                            # (mocking for eg. a "fill")
                            if pred(price):
                                name = action(price)
                                await ctx.send_yield({
                                    'type': 'alert',
                                    'price': price,
                                    # current shm array index
                                    'index': feed.shm._last.value - 1,
                                    'name': name,
                                    'oid': oid,
                                })
                                execs.remove((oid, pred, action))
                                print(
                                    f"GOT ALERT FOR {exec_price} @ \n{tick}\n")
                                print(f'execs are {execs}')

        # feed teardown


@tractor.stream
async def stream_and_route(ctx, ui_name):
    """Order router (sub)actor entrypoint.

    """
    actor = tractor.current_actor()
    book = get_book()

    # new router entry point
    async with tractor.wait_for_actor(ui_name) as portal:

        # spawn one task per broker feed
        async with trio.open_nursery() as n:

            async for cmd in await portal.run(send_order_cmds):

                action = cmd.pop('action')

                if action == 'cancel':
                    pass

                tp = cmd.pop('type')

                trigger_price = cmd['price']
                sym = cmd['symbol']
                brokers = cmd['brokers']
                oid = cmd['oid']

                if tp == 'alert':
                    log.info(f'Alert {cmd} received in {actor.uid}')

                broker = brokers[0]
                last = book.lasts.get((broker, sym))

                if last is None:  # spawn new brokerd feed task

                    quote = await n.start(
                        exec_orders,
                        ctx,
                        # TODO: eventually support N-brokers
                        broker,
                        sym,
                        trigger_price,
                    )
                    print(f"received first quote {quote}")

                last = book.lasts[(broker, sym)]
                print(f'Known last is {last}')

                # Auto-gen scanner predicate:
                # we automatically figure out what the alert check condition
                # should be based on the current first price received from the
                # feed, instead of being like every other shitty tina platform
                # that makes the user choose the predicate operator.
                pred, name = mk_check(trigger_price, last)

                # create list of executions on first entry
                book.orders.setdefault((broker, sym), []).append(
                    (oid, pred, lambda p: name)
                )

            # continue and wait on next order cmd


async def spawn_router_stream_alerts(
    chart,
    symbol: Symbol,
    # lines: 'LinesEditor',
    task_status: TaskStatus[str] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Spawn an EMS daemon and begin sending orders and receiving
    alerts.

    """
    # setup local ui event streaming channels
    global _from_ui, _to_router, _lines
    _to_router, _from_ui = trio.open_memory_channel(100)

    actor = tractor.current_actor()
    subactor_name = 'piker.ems'

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

            yb = pg.mkBrush(hcolor('alert_yellow'))

            angle = 90 if alert['name'] == 'lt' else -90

            arrow = pg.ArrowItem(
                angle=angle,
                baseAngle=0,
                headLen=5,
                headWidth=2,
                tailLen=None,
                brush=yb,
            )
            arrow.setPos(alert['index'], alert['price'])
            chart.plotItem.addItem(arrow)

            # delete the line from view
            oid = alert['oid']
            print(f'_lines: {_lines}')
            print(f'deleting line with oid: {oid}')
            _lines.pop(oid).delete()

            # TODO: this in another task?
            # not sure if this will ever be a bottleneck,
            # we probably could do graphics stuff first tho?

            # XXX: linux only for now
            result = await trio.run_process(
                [
                    'notify-send',
                    '-u', 'normal',
                    '-t', '10000',
                    'piker',
                    f'alert: {alert}',
                ],
            )
            log.runtime(result)
