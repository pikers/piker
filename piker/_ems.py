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
In suit parlance: "Execution management systems"

"""
from pprint import pformat
import time
from dataclasses import dataclass, field
from typing import (
    AsyncIterator, Dict, Callable, Tuple,
)

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor

from . import data
from .log import get_logger
from .data._source import Symbol


log = get_logger(__name__)

# setup local ui event streaming channels for request/resp
# streamging with EMS daemon
_to_ems, _from_order_book = trio.open_memory_channel(100)


@dataclass
class OrderBook:
    """Buy-side (client-side ?) order book ctl and tracking.

    A style similar to "model-view" is used here where this api is
    provided as a supervised control for an EMS actor which does all the
    hard/fast work of talking to brokers/exchanges to conduct
    executions.

    Currently, mostly for keeping local state to match the EMS and use
    received events to trigger graphics updates.

    """
    _sent_orders: Dict[str, dict] = field(default_factory=dict)
    # _confirmed_orders: Dict[str, dict] = field(default_factory=dict)

    _to_ems: trio.abc.SendChannel = _to_ems
    _from_order_book: trio.abc.ReceiveChannel = _from_order_book

    # def on_fill(self, uuid: str) -> None:
    #     cmd = self._sent_orders[uuid]
    #     log.info(f"Order executed: {cmd}")
    #     self._confirmed_orders[uuid] = cmd

    def send(
        self,
        uuid: str,
        symbol: 'Symbol',
        price: float,
        action: str,
    ) -> str:
        cmd = {
            'action': action,
            'price': price,
            'symbol': symbol.key,
            'brokers': symbol.brokers,
            'oid': uuid,
        }
        self._sent_orders[uuid] = cmd
        self._to_ems.send_nowait(cmd)

    async def modify(self, oid: str, price) -> bool:
        ...

    def cancel(self, uuid: str) -> bool:
        """Cancel an order (or alert) from the EMS.

        """
        cmd = self._sent_orders[uuid]
        msg = {
            'action': 'cancel',
            'oid': uuid,
            'symbol': cmd['symbol'],
        }
        self._to_ems.send_nowait(msg)


_orders: OrderBook = None


def get_orders(emsd_uid: Tuple[str, str] = None) -> OrderBook:

    if emsd_uid is not None:
        # TODO: read in target emsd's active book on startup
        pass

    global _orders

    if _orders is None:
        _orders = OrderBook()

    return _orders


# TODO: make this a ``tractor.msg.pub``
async def send_order_cmds():
    """Order streaming task: deliver orders transmitted from UI
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
    global _from_order_book

    async for cmd in _from_order_book:

        # send msg over IPC / wire
        log.info(f'sending order cmd: {cmd}')
        yield cmd


# TODO: numba all of this
def mk_check(trigger_price, known_last) -> Callable[[float, float], bool]:
    """Create a predicate for given ``exec_price`` based on last known
    price, ``known_last``.

    This is an automatic alert level thunk generator based on where the
    current last known value is and where the specified value of
    interest is; pick an appropriate comparison operator based on
    avoiding the case where the a predicate returns true immediately.

    """
    # str compares:
    # https://stackoverflow.com/questions/46708708/compare-strings-in-numba-compiled-function

    if trigger_price >= known_last:

        def check_gt(price: float) -> bool:
            return price >= trigger_price

        return check_gt, 'down'

    elif trigger_price <= known_last:

        def check_lt(price: float) -> bool:
            return price <= trigger_price

        return check_lt, 'up'

    else:
        return None, None


@dataclass
class _ExecBook:
    """EMS-side execution book.

    Contains conditions for executions (aka "orders").
    A singleton instance is created per EMS actor (for now).

    """
    broker: str

    # levels which have an executable action (eg. alert, order, signal)
    orders: Dict[
        # Tuple[str, str],
        str,  # symbol
        Dict[
            str,  # uuid
            Tuple[
                Callable[[float], bool],  # predicate
                str,  # name
                dict,  # cmd / msg type
            ]
        ]
    ] = field(default_factory=dict)

    # tracks most recent values per symbol each from data feed
    lasts: Dict[
        Tuple[str, str],
        float
    ] = field(default_factory=dict)

    # mapping of broker order ids to piker ems ids
    _broker2ems_ids: Dict[str, str] = field(default_factory=bidict)


_books: Dict[str, _ExecBook] = {}


def get_book(broker: str) -> _ExecBook:

    global _books
    return _books.setdefault(broker, _ExecBook(broker))


# def scan_quotes(
#     quotes: dict,


async def exec_loop(
    ctx: tractor.Context,
    broker: str,
    symbol: str,
    task_status: TaskStatus[dict] = trio.TASK_STATUS_IGNORED,
) -> AsyncIterator[dict]:

    async with data.open_feed(
        broker,
        [symbol],
        loglevel='info',
    ) as feed:

        # TODO: get initial price quote from target broker
        first_quote = await feed.receive()
        book = get_book(broker)
        book.lasts[(broker, symbol)] = first_quote[symbol]['last']

        # TODO: wrap this in a more re-usable general api
        client = feed.mod.get_client_proxy(feed._brokerd_portal)

        # return control to parent task
        task_status.started((first_quote, feed, client))

        ##############################
        # begin price actions sequence
        # XXX: optimize this for speed
        ##############################

        # shield this field so the remote brokerd does not get cancelled
        stream = feed.stream
        with stream.shield():

            # this stream may eventually contain multiple
            # symbols
            async for quotes in stream:

                # TODO: numba all this!

                # start = time.time()
                for sym, quote in quotes.items():

                    execs = book.orders.get((broker, sym))
                    if not execs:
                        continue

                    for tick in quote.get('ticks', ()):
                        price = tick.get('price')
                        if price < 0:
                            # lel, fuck you ib
                            continue

                        # update to keep new cmds informed
                        book.lasts[(broker, symbol)] = price

                        for oid, (pred, name, cmd) in tuple(execs.items()):

                            # push trigger msg back to parent as an "alert"
                            # (mocking for eg. a "fill")
                            if pred(price):

                                # register broker id for ems id
                                order_id = await client.submit_limit(
                                    oid=oid,
                                    symbol=sym,
                                    action=cmd['action'],
                                    price=round(price, 2),
                                )
                                # resp = book._broker2ems_ids.setdefault(
                                book._broker2ems_ids[order_id] = oid

                                resp = {
                                    'resp': 'submitted',
                                    'name': name,
                                    'ems_trigger_time_ns': time.time_ns(),
                                    # current shm array index
                                    'index': feed.shm._last.value - 1,
                                    'trigger_price': price,
                                }

                                await ctx.send_yield(resp)

                                log.info(f'removing pred for {oid}')
                                pred, name, cmd = execs.pop(oid)

                                log.debug(f'execs are {execs}')

                # print(f'execs scan took: {time.time() - start}')
        # feed teardown


# XXX: right now this is very very ad-hoc to IB
# TODO: lots of cases still to handle
# - short-sale but securities haven't been located, in this case we
#    should probably keep the order in some kind of weird state or cancel
#    it outright?
# status='PendingSubmit', message=''),
# status='Cancelled', message='Error 404, reqId 1550: Order held while securities are located.'),
# status='PreSubmitted', message='')],

async def receive_trade_updates(
    ctx: tractor.Context,
    feed: 'Feed',  # noqa
    book: _ExecBook,
    task_status: TaskStatus[dict] = trio.TASK_STATUS_IGNORED,
) -> AsyncIterator[dict]:
    """Trades update loop - receive updates from broker, convert
    to EMS responses, transmit to ordering client(s).

    This is where trade confirmations from the broker are processed
    and appropriate responses relayed back to the original EMS client
    actor. There is a messaging translation layer throughout.

    """
    trades_stream = await feed.recv_trades_data()
    first = await trades_stream.__anext__()

    # startup msg
    assert first['trade_events'] == 'started'
    task_status.started()

    async for trade_event in trades_stream:
        event = trade_event['trade_events']

        try:
            order = event['order']
        except KeyError:

            # Relay broker error messages
            err = event['error']

            # broker request id - must be normalized
            # into error transmission by broker backend.
            reqid = err['brid']

            # TODO: handle updates!
            oid = book._broker2ems_ids.get(reqid)

            # XXX should we make one when it's blank?
            log.error(pformat(err['message']))

        else:
            log.info(f'Received broker trade event:\n{pformat(event)}')

            status = event['orderStatus']['status']
            reqid = order['orderId']

            # TODO: handle updates!
            oid = book._broker2ems_ids.get(reqid)

            if status in {'Cancelled'}:
                resp = {'resp': 'cancelled'}

            elif status in {'Submitted'}:
                # ack-response that order is live/submitted
                # to the broker
                resp = {'resp': 'submitted'}

            # elif status in {'Executed', 'Filled'}:
            elif status in {'Filled'}:

                # order was filled by broker
                fills = []
                for fill in event['fills']:
                    e = fill['execution']
                    fills.append(
                        (e.time, e.price, e.shares, e.side)
                    )

                resp = {
                    'resp': 'executed',
                    'fills': fills,
                }

            else:  # active in EMS
                # ack-response that order is live in EMS
                # (aka as a client side limit)
                resp = {'resp': 'active'}

            # send response packet to EMS client(s)
            resp['oid'] = oid

            await ctx.send_yield(resp)


@tractor.stream
async def stream_and_route(
    ctx: tractor.Context,
    client_actor_name: str,
    broker: str,
    symbol: str,
    mode: str = 'live',  # ('paper', 'dark', 'live')
) -> None:
    """EMS (sub)actor entrypoint.

    This is the daemon (child) side routine which starts an EMS
    runtime per broker/feed and and begins streaming back alerts
    from executions to order clients.

    """
    actor = tractor.current_actor()
    book = get_book(broker)

    # new router entry point
    async with tractor.wait_for_actor(client_actor_name) as portal:

        # spawn one task per broker feed
        async with trio.open_nursery() as n:

            # TODO: eventually support N-brokers
            quote, feed, client = await n.start(
                exec_loop,
                ctx,
                broker,
                symbol,
            )

            # for paper mode we need to mock this trades response feed
            await n.start(
                receive_trade_updates,
                ctx,
                feed,
                book,
            )

            async for cmd in await portal.run(send_order_cmds):

                log.info(f'{cmd} received in {actor.uid}')

                action = cmd['action']
                oid = cmd['oid']
                sym = cmd['symbol']

                if action == 'cancel':

                    # check for live-broker order
                    brid = book._broker2ems_ids.inverse[oid]
                    if brid:
                        log.info("Submitting cancel for live order")
                        await client.submit_cancel(oid=brid)

                    # check for EMS active exec
                    else:
                        book.orders[symbol].pop(oid, None)
                        await ctx.send_yield(
                            {'action': 'cancelled',
                             'oid': oid}
                        )

                elif action in ('alert', 'buy', 'sell',):

                    trigger_price = cmd['price']
                    brokers = cmd['brokers']
                    broker = brokers[0]

                    last = book.lasts[(broker, sym)]
                    # print(f'Known last is {last}')

                    if action in ('buy', 'sell',):

                        # if the predicate resolves immediately send the
                        # execution to the broker asap
                        # if pred(last):
                        if mode == 'live':
                            # send order
                            log.warning("ORDER FILLED IMMEDIATELY!?!?!?!")
                            # IF SEND ORDER RIGHT AWAY CONDITION

                            # register broker id for ems id
                            order_id = await client.submit_limit(
                                oid=oid,
                                symbol=sym,
                                action=action,
                                price=round(trigger_price, 2),
                            )
                            book._broker2ems_ids[order_id] = oid

                            # book.orders[symbol][oid] = None

                            # XXX: the trades data broker response loop
                            # (``receive_trade_updates()`` above) will
                            # handle sending the ems side acks back to
                            # the cmd sender from here

                        elif mode in {'dark', 'paper'}:

                            # Auto-gen scanner predicate:
                            # we automatically figure out what the alert check
                            # condition should be based on the current first
                            # price received from the feed, instead of being
                            # like every other shitty tina platform that makes
                            # the user choose the predicate operator.
                            pred, name = mk_check(trigger_price, last)

                            # submit execution/order to EMS scanner loop
                            # create list of executions on first entry
                            book.orders.setdefault(
                                (broker, sym), {}
                            )[oid] = (pred, name, cmd)

                            # ack-response that order is live here
                            await ctx.send_yield({
                                'resp': 'ems_active',
                                'oid': oid
                            })

            # continue and wait on next order cmd


async def _ems_main(
    order_mode,
    broker: str,
    symbol: Symbol,
    # lines: 'LinesEditor',
    task_status: TaskStatus[str] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Spawn an EMS daemon and begin sending orders and receiving
    alerts.

    """

    actor = tractor.current_actor()
    subactor_name = 'emsd'

    # TODO: add ``maybe_spawn_emsd()`` for this
    async with tractor.open_nursery() as n:

        portal = await n.start_actor(
            subactor_name,
            enable_modules=[__name__],
        )
        stream = await portal.run(
            stream_and_route,
            client_actor_name=actor.name,
            broker=broker,
            symbol=symbol.key,

        )

        async with tractor.wait_for_actor(subactor_name):
            # let parent task continue
            task_status.started(_to_ems)

        # begin the trigger-alert stream
        # this is where we receive **back** messages
        # about executions **from** the EMS actor
        async for msg in stream:

            # delete the line from view
            oid = msg['oid']
            resp = msg['resp']

            # response to 'action' request (buy/sell)
            if resp in ('ems_active', 'submitted'):
                log.info(f"order accepted: {msg}")

                # show line label once order is live
                order_mode.on_submit(oid)

            # response to 'cancel' request
            elif resp in ('cancelled',):

                # delete level from view
                order_mode.on_cancel(oid)
                log.info(f'deleting line with oid: {oid}')

            # response to 'action' request (buy/sell)
            elif resp in ('executed',):
                await order_mode.on_exec(oid, msg)
