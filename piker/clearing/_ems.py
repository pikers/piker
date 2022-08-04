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
In da suit parlances: "Execution management systems"

"""
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from math import isnan
from pprint import pformat
import time
from typing import AsyncIterator, Callable

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor

from ..log import get_logger
from ..data._normalize import iterticks
from ..data.feed import Feed, maybe_open_feed
from ..data.types import Struct
from .._daemon import maybe_spawn_brokerd
from . import _paper_engine as paper
from ._messages import (
    Status, Order,
    BrokerdCancel, BrokerdOrder, BrokerdOrderAck, BrokerdStatus,
    BrokerdFill, BrokerdError, BrokerdPosition,
)


log = get_logger(__name__)


# TODO: numba all of this
def mk_check(

    trigger_price: float,
    known_last: float,
    action: str,

) -> Callable[[float, float], bool]:
    '''
    Create a predicate for given ``exec_price`` based on last known
    price, ``known_last``.

    This is an automatic alert level thunk generator based on where the
    current last known value is and where the specified value of
    interest is; pick an appropriate comparison operator based on
    avoiding the case where the a predicate returns true immediately.

    '''
    # str compares:
    # https://stackoverflow.com/questions/46708708/compare-strings-in-numba-compiled-function

    if trigger_price >= known_last:

        def check_gt(price: float) -> bool:
            return price >= trigger_price

        return check_gt

    elif trigger_price <= known_last:

        def check_lt(price: float) -> bool:
            return price <= trigger_price

        return check_lt

    raise ValueError(
        f'trigger: {trigger_price}, last: {known_last}'
    )


@dataclass
class _DarkBook:
    '''
    EMS-trigger execution book.

    Contains conditions for executions (aka "orders" or "triggers")
    which are not exposed to brokers and thus the market; i.e. these are
    privacy focussed "client side" orders which are submitted in real-time
    based on specified trigger conditions.

    An instance per `brokerd` is created per EMS actor (for now).

    '''
    broker: str

    # levels which have an executable action (eg. alert, order, signal)
    orders: dict[
        str,  # symbol
        dict[
            str,  # uuid
            tuple[
                Callable[[float], bool],  # predicate
                str,  # name
                dict,  # cmd / msg type
            ]
        ]
    ] = field(default_factory=dict)

    # tracks most recent values per symbol each from data feed
    lasts: dict[
        str,
        float,
    ] = field(default_factory=dict)

    # mapping of piker ems order ids to current brokerd order flow message
    _ems_entries: dict[str, str] = field(default_factory=dict)
    _ems2brokerd_ids: dict[str, str] = field(default_factory=bidict)


# XXX: this is in place to prevent accidental positions that are too
# big. Now obviously this won't make sense for crypto like BTC, but
# for most traditional brokers it should be fine unless you start
# slinging NQ futes or something; check ur margin.
_DEFAULT_SIZE: float = 1.0


async def clear_dark_triggers(

    brokerd_orders_stream: tractor.MsgStream,
    ems_client_order_stream: tractor.MsgStream,
    quote_stream: tractor.ReceiveMsgStream,  # noqa
    broker: str,
    fqsn: str,

    book: _DarkBook,

) -> None:
    '''
    Core dark order trigger loop.

    Scan the (price) data feed and submit triggered orders
    to broker.

    '''
    # XXX: optimize this for speed!
    # TODO:
    # - numba all this!
    # - this stream may eventually contain multiple symbols
    async for quotes in quote_stream:
        # start = time.time()
        for sym, quote in quotes.items():
            execs = book.orders.get(sym, {})
            for tick in iterticks(
                quote,
                # dark order price filter(s)
                types=(
                    'ask',
                    'bid',
                    'trade',
                    'last',
                    # 'dark_trade',  # TODO: should allow via config?
                )
            ):
                price = tick.get('price')
                ttype = tick['type']

                # update to keep new cmds informed
                book.lasts[sym] = price

                for oid, (
                    pred,
                    tf,
                    cmd,
                    percent_away,
                    abs_diff_away
                ) in (
                    tuple(execs.items())
                ):
                    if (
                        not pred or
                        ttype not in tf or
                        not pred(price)
                    ):
                        log.debug(
                            f'skipping quote for {sym} '
                            f'{pred}, {ttype} not in {tf}?, {pred(price)}'
                        )
                        # majority of iterations will be non-matches
                        continue

                    action: str = cmd['action']
                    symbol: str = cmd['symbol']
                    bfqsn: str = symbol.replace(f'.{broker}', '')

                    if action == 'alert':
                        # nothing to do but relay a status
                        # message back to the requesting ems client
                        resp = 'alert_triggered'

                    else:  # executable order submission

                        # submit_price = price + price*percent_away
                        submit_price = price + abs_diff_away

                        log.info(
                            f'Dark order triggered for price {price}\n'
                            f'Submitting order @ price {submit_price}')

                        msg = BrokerdOrder(
                            action=cmd['action'],
                            oid=oid,
                            account=cmd['account'],
                            time_ns=time.time_ns(),

                            # this **creates** new order request for the
                            # underlying broker so we set a "broker
                            # request id" (``reqid`` kwarg) to ``None``
                            # so that the broker client knows that we
                            # aren't trying to modify an existing
                            # order-request and instead create a new one.
                            reqid=None,

                            symbol=bfqsn,
                            price=submit_price,
                            size=cmd['size'],
                        )
                        await brokerd_orders_stream.send(msg)

                        # mark this entry as having sent an order
                        # request.  the entry will be replaced once the
                        # target broker replies back with
                        # a ``BrokerdOrderAck`` msg including the
                        # allocated unique ``BrokerdOrderAck.reqid`` key
                        # generated by the broker's own systems.
                        book._ems_entries[oid] = msg

                        # our internal status value for client-side
                        # triggered "dark orders"
                        resp = 'dark_triggered'

                    msg = Status(
                        oid=oid,  # ems order id
                        time_ns=time.time_ns(),
                        resp=resp,
                        trigger_price=price,
                        brokerd_msg=cmd,
                    )

                    # remove exec-condition from set
                    log.info(f'removing pred for {oid}')
                    pred = execs.pop(oid, None)
                    if not pred:
                        log.warning(
                            f'pred for {oid} was already removed!?'
                        )

                    try:
                        await ems_client_order_stream.send(msg)
                    except (
                        trio.ClosedResourceError,
                    ):
                        log.warning(
                            f'client {ems_client_order_stream} stream is broke'
                        )
                        break

                else:  # condition scan loop complete
                    log.debug(f'execs are {execs}')
                    if execs:
                        book.orders[fqsn] = execs

        # print(f'execs scan took: {time.time() - start}')


@dataclass
class TradesRelay:

    # for now we keep only a single connection open with
    # each ``brokerd`` for simplicity.
    brokerd_dialogue: tractor.MsgStream

    # map of symbols to dicts of accounts to pp msgs
    positions: dict[
        # brokername, acctid
        tuple[str, str],
        list[BrokerdPosition],
    ]

    # allowed account names
    accounts: tuple[str]

    # count of connected ems clients for this ``brokerd``
    consumers: int = 0


class Router(Struct):
    '''
    Order router which manages and tracks per-broker dark book,
    alerts, clearing and related data feed management.

    A singleton per ``emsd`` actor.

    '''
    # setup at actor spawn time
    nursery: trio.Nursery

    # broker to book map
    books: dict[str, _DarkBook] = {}

    # order id to client stream map
    clients: set[tractor.MsgStream] = set()
    dialogues: dict[str, list[tractor.MsgStream]] = {}

    # brokername to trades-dialogues streams with ``brokerd`` actors
    relays: dict[str, TradesRelay] = {}

    def get_dark_book(
        self,
        brokername: str,

    ) -> _DarkBook:

        return self.books.setdefault(brokername, _DarkBook(brokername))

    @asynccontextmanager
    async def maybe_open_brokerd_trades_dialogue(
        self,
        feed: Feed,
        symbol: str,
        dark_book: _DarkBook,
        exec_mode: str,
        loglevel: str,

    ) -> tuple[dict, tractor.MsgStream]:
        '''Open and yield ``brokerd`` trades dialogue context-stream if none
        already exists.

        '''
        relay = self.relays.get(feed.mod.name)

        if (
            relay is None

            # We always want to spawn a new relay for the paper engine
            # per symbol since we need a new tractor context to be
            # opened for every every symbol such that a new data feed
            # and ``PaperBoi`` client will be created and then used to
            # simulate clearing events.
            or exec_mode == 'paper'
        ):

            relay = await self.nursery.start(
                open_brokerd_trades_dialogue,
                self,
                feed,
                symbol,
                exec_mode,
                loglevel,
            )

        relay.consumers += 1

        # TODO: get updated positions here?
        assert relay.brokerd_dialogue
        try:
            yield relay

        finally:

            # TODO: what exactly needs to be torn down here or
            # are we just consumer tracking?

            relay.consumers -= 1


_router: Router = None


async def open_brokerd_trades_dialogue(

    router: Router,
    feed: Feed,
    symbol: str,
    exec_mode: str,
    loglevel: str,

    task_status: TaskStatus[TradesRelay] = trio.TASK_STATUS_IGNORED,

) -> tuple[dict, tractor.MsgStream]:
    '''
    Open and yield ``brokerd`` trades dialogue context-stream if none
    already exists.

    '''
    trades_endpoint = getattr(feed.mod, 'trades_dialogue', None)

    broker = feed.mod.name

    # TODO: make a `tractor` bug/test for this!
    # if only i could member what the problem was..
    # probably some GC of the portal thing?
    # portal = feed.portal

    # XXX: we must have our own portal + channel otherwise
    # when the data feed closes it may result in a half-closed
    # channel that the brokerd side thinks is still open somehow!?
    async with maybe_spawn_brokerd(
        broker,
        loglevel=loglevel,

    ) as portal:
        if (
            trades_endpoint is None
            or exec_mode == 'paper'
        ):
            # for paper mode we need to mock this trades response feed
            # so we load bidir stream to a new sub-actor running
            # a paper-simulator clearing engine.

            # load the paper trading engine
            exec_mode = 'paper'
            log.warning(f'Entering paper trading mode for {broker}')

            # load the paper trading engine as a subactor of this emsd
            # actor to simulate the real IPC load it'll have when also
            # pulling data from feeds
            open_trades_endpoint = paper.open_paperboi(
                fqsn='.'.join([symbol, broker]),
                loglevel=loglevel,
            )

        else:
            # open live brokerd trades endpoint
            open_trades_endpoint = portal.open_context(
                trades_endpoint,
                loglevel=loglevel,
            )

        try:
            positions: list[BrokerdPosition]
            accounts: tuple[str]

            async with (
                open_trades_endpoint as (brokerd_ctx, (positions, accounts,)),
                brokerd_ctx.open_stream() as brokerd_trades_stream,

            ):
                # XXX: really we only want one stream per `emsd` actor
                # to relay global `brokerd` order events unless we're
                # doing to expect each backend to relay only orders
                # affiliated with a particular ``trades_dialogue()``
                # session (seems annoying for implementers). So, here
                # we cache the relay task and instead of running multiple
                # tasks (which will result in multiples of the same msg being
                # relayed for each EMS client) we just register each client
                # stream to this single relay loop using _router.dialogues

                # begin processing order events from the target brokerd backend
                # by receiving order submission response messages,
                # normalizing them to EMS messages and relaying back to
                # the piker order client set.

                # locally cache and track positions per account with
                # a table of (brokername, acctid) -> `BrokerdPosition`
                # msgs.
                pps = {}
                for msg in positions:
                    log.info(f'loading pp: {msg}')

                    account = msg['account']

                    # TODO: better value error for this which
                    # dumps the account and message and states the
                    # mismatch..
                    assert account in accounts

                    pps.setdefault(
                        (broker, account),
                        [],
                    ).append(msg)

                relay = TradesRelay(
                    brokerd_dialogue=brokerd_trades_stream,
                    positions=pps,
                    accounts=accounts,
                    consumers=1,
                )

                _router.relays[broker] = relay

                # the ems scan loop may be cancelled by the client but we
                # want to keep the ``brokerd`` dialogue up regardless

                task_status.started(relay)

                await translate_and_relay_brokerd_events(
                    broker,
                    brokerd_trades_stream,
                    _router,
                )

                # this context should block here indefinitely until
                # the ``brokerd`` task either dies or is cancelled

        finally:
            # parent context must have been closed
            # remove from cache so next client will respawn if needed
            relay = _router.relays.pop(broker, None)
            if not relay:
                log.warning(f'Relay for {broker} was already removed!?')


@tractor.context
async def _setup_persistent_emsd(

    ctx: tractor.Context,

) -> None:

    global _router

    # open a root "service nursery" for the ``emsd`` actor
    async with trio.open_nursery() as service_nursery:

        _router = Router(nursery=service_nursery)

        # TODO: send back the full set of persistent
        # orders/execs?
        await ctx.started()

        # allow service tasks to run until cancelled
        await trio.sleep_forever()


async def translate_and_relay_brokerd_events(

    broker: str,
    brokerd_trades_stream: tractor.MsgStream,
    router: Router,

) -> AsyncIterator[dict]:
    '''
    Trades update loop - receive updates from ``brokerd`` trades
    endpoint, convert to EMS response msgs, transmit **only** to
    ordering client(s).

    This is where trade confirmations from the broker are processed and
    appropriate responses relayed **only** back to the original EMS
    client actor. There is a messaging translation layer throughout.

    Expected message translation(s):

        broker       ems
        'error'  ->  log it locally (for now)
        'status' ->  relabel as 'broker_<status>', if complete send 'executed'
        'fill'   ->  'broker_filled'

    Currently handled status values from IB:
        {'presubmitted', 'submitted', 'cancelled', 'inactive'}

    '''
    book = router.get_dark_book(broker)
    relay = router.relays[broker]

    assert relay.brokerd_dialogue == brokerd_trades_stream

    async for brokerd_msg in brokerd_trades_stream:

        name = brokerd_msg['name']

        log.info(
            f'Received broker trade event:\n'
            f'{pformat(brokerd_msg)}'
        )

        if name == 'position':

            pos_msg = BrokerdPosition(**brokerd_msg)

            # XXX: this will be useful for automatic strats yah?
            # keep pps per account up to date locally in ``emsd`` mem
            sym, broker = pos_msg.symbol, pos_msg.broker

            relay.positions.setdefault(
                # NOTE: translate to a FQSN!
                (broker, sym),
                []
            ).append(pos_msg)

            # fan-out-relay position msgs immediately by
            # broadcasting updates on all client streams
            for client_stream in router.clients.copy():
                try:
                    await client_stream.send(pos_msg)
                except(
                    trio.ClosedResourceError,
                    trio.BrokenResourceError,
                ):
                    router.clients.remove(client_stream)
                    log.warning(
                        f'client for {client_stream} was already closed?')

            continue

        # Get the broker (order) request id, this **must** be normalized
        # into messaging provided by the broker backend
        reqid = brokerd_msg['reqid']

        # all piker originated requests will have an ems generated oid field
        oid = brokerd_msg.get(
            'oid',
            book._ems2brokerd_ids.inverse.get(reqid)
        )

        if oid is None:

            # XXX: paper clearing special cases
            # paper engine race case: ``Client.submit_limit()`` hasn't
            # returned yet and provided an output reqid to register
            # locally, so we need to retreive the oid that was already
            # packed at submission since we already know it ahead of
            # time
            paper = brokerd_msg['broker_details'].get('paper_info')
            ext = brokerd_msg['broker_details'].get('external')
            if paper:
                # paperboi keeps the ems id up front
                oid = paper['oid']

            elif ext:
                # may be an order msg specified as "external" to the
                # piker ems flow (i.e. generated by some other
                # external broker backend client (like tws for ib)
                log.error(f"External trade event {ext}")

                continue

            else:
                # something is out of order, we don't have an oid for
                # this broker-side message.
                log.error(
                    f'Unknown oid: {oid} for msg:\n'
                    f'{pformat(brokerd_msg)}\n'
                    'Unable to relay message to client side!?'
                )

        else:
            # check for existing live flow entry
            entry = book._ems_entries.get(oid)
            old_reqid = entry.reqid

            if old_reqid and old_reqid != reqid:
                log.warning(
                    f'Brokerd order id change for {oid}:\n'
                    f'{old_reqid} -> {reqid}'
                )

            # initial response to brokerd order request
            if name == 'ack':

                # register the brokerd request id (that was generated
                # / created internally by the broker backend) with our
                # local ems order id for reverse lookup later.
                # a ``BrokerdOrderAck`` **must** be sent after an order
                # request in order to establish this id mapping.
                book._ems2brokerd_ids[oid] = reqid
                log.info(
                    'Rx ACK for order\n'
                    f'oid: {oid} -> reqid: {reqid}'
                )

                # new order which has not yet be registered into the
                # local ems book, insert it now and handle 2 cases:

                # - the order has previously been requested to be
                # cancelled by the ems controlling client before we
                # received this ack, in which case we relay that cancel
                # signal **asap** to the backend broker
                action = getattr(entry, 'action', None)
                if action and action == 'cancel':
                    # assign newly providerd broker backend request id
                    entry.reqid = reqid

                    # tell broker to cancel immediately
                    await brokerd_trades_stream.send(entry)

                # - the order is now active and will be mirrored in
                # our book -> registered as live flow
                else:
                    # update the flow with the ack msg
                    book._ems_entries[oid] = BrokerdOrderAck(**brokerd_msg)

                continue

            # a live flow now exists
            oid = entry.oid

        # TODO: instead this should be our status set.
        # ack, open, fill, closed, cancelled'

        resp = None
        broker_details = {}

        if name in (
            'error',
        ):
            # TODO: figure out how this will interact with EMS clients
            # for ex. on an error do we react with a dark orders
            # management response, like cancelling all dark orders?

            # This looks like a supervision policy for pending orders on
            # some unexpected failure - something we need to think more
            # about.  In most default situations, with composed orders
            # (ex.  brackets), most brokers seem to use a oca policy.

            msg = BrokerdError(**brokerd_msg)

            # XXX should we make one when it's blank?
            log.error(pformat(msg))

            # TODO: getting this bs, prolly need to handle status messages
            # 'Market data farm connection is OK:usfarm.nj'

            # another stupid ib error to handle
            # if 10147 in message: cancel

            resp = 'broker_errored'
            broker_details = msg

            # don't relay message to order requester client
            # continue

        elif name in (
            'status',
        ):
            msg = BrokerdStatus(**brokerd_msg)

            if msg.status == 'cancelled':

                log.info(f'Cancellation for {oid} is complete!')

            if msg.status == 'filled':

                # conditional execution is fully complete, no more
                # fills for the noted order
                if not msg.remaining:

                    resp = 'broker_executed'

                    # be sure to pop this stream from our dialogue set
                    # since the order dialogue should be done.
                    log.info(f'Execution for {oid} is complete!')

                # just log it
                else:
                    log.info(f'{broker} filled {msg}')

            else:
                # one of {submitted, cancelled}
                resp = 'broker_' + msg.status

            # pass the BrokerdStatus msg inside the broker details field
            broker_details = msg

        elif name in (
            'fill',
        ):
            msg = BrokerdFill(**brokerd_msg)

            # proxy through the "fill" result(s)
            resp = 'broker_filled'
            broker_details = msg

            log.info(f'\nFill for {oid} cleared with:\n{pformat(resp)}')

        else:
            raise ValueError(f'Brokerd message {brokerd_msg} is invalid')

        # Create and relay response status message
        # to requesting EMS client
        try:
            ems_client_order_stream = router.dialogues[oid]
            await ems_client_order_stream.send(
                Status(
                    oid=oid,
                    resp=resp,
                    time_ns=time.time_ns(),
                    broker_reqid=reqid,
                    brokerd_msg=broker_details,
                )
            )
        except KeyError:
            log.error(
                f'Received `brokerd` msg for unknown client with oid: {oid}')

    # TODO: do we want this to keep things cleaned up?
    # it might require a special status from brokerd to affirm the
    # flow is complete?
    # router.dialogues.pop(oid)


async def process_client_order_cmds(

    client_order_stream: tractor.MsgStream,  # noqa
    brokerd_order_stream: tractor.MsgStream,

    symbol: str,
    feed: Feed,  # noqa
    dark_book: _DarkBook,
    router: Router,

) -> None:

    client_dialogues = router.dialogues

    # cmd: dict
    async for cmd in client_order_stream:
        log.info(f'Received order cmd:\n{pformat(cmd)}')

        oid = cmd['oid']
        # register this stream as an active dialogue for this order id
        # such that translated message from the brokerd backend can be
        # routed (relayed) to **just** that client stream (and in theory
        # others who are registered for such order affiliated msgs).
        client_dialogues[oid] = client_order_stream
        reqid = dark_book._ems2brokerd_ids.inverse.get(oid)
        live_entry = dark_book._ems_entries.get(oid)

        match cmd:

            # existing live-broker order cancel
            case {
                'action': 'cancel',
                'oid': oid,
            } if live_entry:
                reqid = live_entry.reqid
                msg = BrokerdCancel(
                    oid=oid,
                    reqid=reqid,
                    time_ns=time.time_ns(),
                    account=live_entry.account,
                )

                # NOTE: cancel response will be relayed back in messages
                # from corresponding broker
                if reqid is not None:
                    # send cancel to brokerd immediately!
                    log.info(
                        f'Submitting cancel for live order {reqid}'
                    )
                    await brokerd_order_stream.send(msg)

                else:
                    # this might be a cancel for an order that hasn't been
                    # acked yet by a brokerd, so register a cancel for when
                    # the order ack does show up later such that the brokerd
                    # order request can be cancelled at that time.
                    dark_book._ems_entries[oid] = msg

            # dark trigger cancel
            case {
                'action': 'cancel',
                'oid': oid,
            } if not live_entry:
                try:
                    # remove from dark book clearing
                    dark_book.orders[symbol].pop(oid, None)

                    # tell client side that we've cancelled the
                    # dark-trigger order
                    await client_order_stream.send(
                        Status(
                            resp='dark_cancelled',
                            oid=oid,
                            time_ns=time.time_ns(),
                        )
                    )
                    # de-register this client dialogue
                    router.dialogues.pop(oid)

                except KeyError:
                    log.exception(f'No dark order for {symbol}?')

            # live order submission
            case {
                'oid': oid,
                'symbol': fqsn,
                'price': trigger_price,
                'size': size,
                'action': ('buy' | 'sell') as action,
                'exec_mode': 'live',
            }:
                # TODO: eventually we should be receiving
                # this struct on the wire unpacked in a scoped protocol
                # setup with ``tractor``.
                msg = Order(**cmd)
                broker = msg.brokers[0]

                # remove the broker part before creating a message
                # to send to the specific broker since they probably
                # aren't expectig their own name, but should they?
                sym = fqsn.replace(f'.{broker}', '')

                if live_entry is not None:
                    # sanity check on emsd id
                    assert live_entry.oid == oid
                    reqid = live_entry.reqid
                    # if we already had a broker order id then
                    # this is likely an order update commmand.
                    log.info(f"Modifying live {broker} order: {reqid}")

                msg = BrokerdOrder(
                    oid=oid,  # no ib support for oids...
                    time_ns=time.time_ns(),

                    # if this is None, creates a new order
                    # otherwise will modify any existing one
                    reqid=reqid,

                    symbol=sym,
                    action=action,
                    price=trigger_price,
                    size=size,
                    account=msg.account,
                )

                # send request to backend
                # XXX: the trades data broker response loop
                # (``translate_and_relay_brokerd_events()`` above) will
                # handle relaying the ems side responses back to
                # the client/cmd sender from this request
                log.info(f'Sending live order to {broker}:\n{pformat(msg)}')
                await brokerd_order_stream.send(msg)

                # an immediate response should be ``BrokerdOrderAck``
                # with ems order id from the ``trades_dialogue()``
                # endpoint, but we register our request as part of the
                # flow so that if a cancel comes from the requesting
                # client, before that ack, when the ack does arrive we
                # immediately take the reqid from the broker and cancel
                # that live order asap.
                dark_book._ems_entries[oid] = msg

            # dark-order / alert submission
            case {
                'oid': oid,
                'symbol': fqsn,
                'price': trigger_price,
                'size': size,
                'exec_mode': exec_mode,
                'action': action,
            } if (
                    # "DARK" triggers
                    # submit order to local EMS book and scan loop,
                    # effectively a local clearing engine, which
                    # scans for conditions and triggers matching executions
                    exec_mode in ('dark', 'paper')
                    or action == 'alert'
            ):
                # Auto-gen scanner predicate:
                # we automatically figure out what the alert check
                # condition should be based on the current first
                # price received from the feed, instead of being
                # like every other shitty tina platform that makes
                # the user choose the predicate operator.
                last = dark_book.lasts[fqsn]

                # sometimes the real-time feed hasn't come up
                # so just pull from the latest history.
                if isnan(last):
                    last = feed.shm.array[-1]['close']

                pred = mk_check(trigger_price, last, action)

                spread_slap: float = 5
                min_tick = feed.symbols[sym].tick_size

                if action == 'buy':
                    tickfilter = ('ask', 'last', 'trade')
                    percent_away = 0.005

                    # TODO: we probably need to scale this based
                    # on some near term historical spread
                    # measure?
                    abs_diff_away = spread_slap * min_tick

                elif action == 'sell':
                    tickfilter = ('bid', 'last', 'trade')
                    percent_away = -0.005
                    abs_diff_away = -spread_slap * min_tick

                else:  # alert
                    tickfilter = ('trade', 'utrade', 'last')
                    percent_away = 0
                    abs_diff_away = 0

                # submit execution/order to EMS scan loop
                # NOTE: this may result in an override of an existing
                # dark book entry if the order id already exists
                dark_book.orders.setdefault(
                    fqsn, {}
                )[oid] = (
                    pred,
                    tickfilter,
                    cmd,
                    percent_away,
                    abs_diff_away
                )
                resp = 'dark_submitted'

                # alerts have special msgs to distinguish
                if action == 'alert':
                    resp = 'alert_submitted'

                await client_order_stream.send(
                    Status(
                        resp=resp,
                        oid=oid,
                        time_ns=time.time_ns(),
                    )
                )


@tractor.context
async def _emsd_main(

    ctx: tractor.Context,
    fqsn: str,
    exec_mode: str,  # ('paper', 'live')

    loglevel: str = 'info',

) -> None:
    '''EMS (sub)actor entrypoint providing the
    execution management (micro)service which conducts broker
    order clearing control on behalf of clients.

    This is the daemon (child) side routine which starts an EMS runtime
    task (one per broker-feed) and and begins streaming back alerts from
    each broker's executions/fills.

    ``send_order_cmds()`` is called here to execute in a task back in
    the actor which started this service (spawned this actor), presuming
    capabilities allow it, such that requests for EMS executions are
    received in a stream from that client actor and then responses are
    streamed back up to the original calling task in the same client.

    The primary ``emsd`` task tree is:

    - ``_emsd_main()``:
      sets up brokerd feed, order feed with ems client, trades dialogue with
      brokderd trading api.
       |
        - ``clear_dark_triggers()``:
          run (dark order) conditions on inputs and trigger brokerd "live"
          order submissions.
       |
        - (maybe) ``translate_and_relay_brokerd_events()``:
          accept normalized trades responses from brokerd, process and
          relay to ems client(s); this is a effectively a "trade event
          reponse" proxy-broker.
       |
        - ``process_client_order_cmds()``:
          accepts order cmds from requesting clients, registers dark orders and
          alerts with clearing loop.

    '''
    global _router
    assert _router

    from ..data._source import unpack_fqsn
    broker, symbol, suffix = unpack_fqsn(fqsn)
    dark_book = _router.get_dark_book(broker)

    # TODO: would be nice if in tractor we can require either a ctx arg,
    # or a named arg with ctx in it and a type annotation of
    # tractor.Context instead of strictly requiring a ctx arg.
    ems_ctx = ctx

    feed: Feed

    # spawn one task per broker feed
    async with (
        maybe_open_feed(
            [fqsn],
            loglevel=loglevel,
        ) as (feed, quote_stream),
    ):

        # XXX: this should be initial price quote from target provider
        first_quote = feed.first_quotes[fqsn]

        book = _router.get_dark_book(broker)
        book.lasts[fqsn] = first_quote['last']

        # open a stream with the brokerd backend for order
        # flow dialogue
        async with (

            # only open if one isn't already up: we try to keep
            # as few duplicate streams as necessary
            _router.maybe_open_brokerd_trades_dialogue(
                feed,
                symbol,
                dark_book,
                exec_mode,
                loglevel,

            ) as relay,

            trio.open_nursery() as n,
        ):

            brokerd_stream = relay.brokerd_dialogue  # .clone()

            # signal to client that we're started and deliver
            # all known pps and accounts for this ``brokerd``.
            await ems_ctx.started((
                relay.positions,
                list(relay.accounts),
            ))

            # establish 2-way stream with requesting order-client and
            # begin handling inbound order requests and updates
            async with ems_ctx.open_stream() as ems_client_order_stream:

                # trigger scan and exec loop
                n.start_soon(
                    clear_dark_triggers,

                    brokerd_stream,
                    ems_client_order_stream,
                    quote_stream,
                    broker,
                    fqsn,  # form: <name>.<venue>.<suffix>.<broker>
                    book
                )

                # start inbound (from attached client) order request processing
                try:
                    _router.clients.add(ems_client_order_stream)

                    # main entrypoint, run here until cancelled.
                    await process_client_order_cmds(

                        ems_client_order_stream,

                        # relay.brokerd_dialogue,
                        brokerd_stream,

                        fqsn,
                        feed,
                        dark_book,
                        _router,
                    )

                finally:
                    # try to remove client from "registry"
                    try:
                        _router.clients.remove(ems_client_order_stream)
                    except KeyError:
                        log.warning(
                            f'Stream {ems_client_order_stream._ctx.chan.uid}'
                            ' was already dropped?'
                        )

                    dialogues = _router.dialogues

                    for oid, client_stream in dialogues.copy().items():

                        if client_stream == ems_client_order_stream:

                            log.warning(
                                f'client dialogue is being abandoned:\n'
                                f'{oid} ->\n{client_stream._ctx.chan.uid}'
                            )
                            dialogues.pop(oid)

                            # TODO: for order dialogues left "alive" in
                            # the ems this is where we should allow some
                            # system to take over management. Likely we
                            # want to allow the user to choose what kind
                            # of policy to use (eg. cancel all orders
                            # from client, run some algo, etc.)
