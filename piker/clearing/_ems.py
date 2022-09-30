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
from __future__ import annotations
from collections import (
    defaultdict,
    ChainMap,
)
from contextlib import asynccontextmanager
from math import isnan
from pprint import pformat
import time
from typing import (
    AsyncIterator,
    Any,
    Callable,
    Optional,
)

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
    Order,
    Status,
    # Cancel,
    BrokerdCancel,
    BrokerdOrder,
    # BrokerdOrderAck,
    BrokerdStatus,
    BrokerdFill,
    BrokerdError,
    BrokerdPosition,
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


class _DarkBook(Struct):
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
    ] = {}

    # tracks most recent values per symbol each from data feed
    lasts: dict[
        str,
        float,
    ] = {}

    # _ems_entries: dict[str, str] = {}
    _active: dict = {}

    _ems2brokerd_ids: dict[str, str] = bidict()


# XXX: this is in place to prevent accidental positions that are too
# big. Now obviously this won't make sense for crypto like BTC, but
# for most traditional brokers it should be fine unless you start
# slinging NQ futes or something; check ur margin.
_DEFAULT_SIZE: float = 1.0


async def clear_dark_triggers(

    router: Router,
    brokerd_orders_stream: tractor.MsgStream,
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
                # update to keep new cmds informed
                book.lasts[sym] = price
                ttype = tick['type']

                for oid, (
                    pred,
                    tf,
                    # TODO: send this msg instead?
                    cmd,
                    percent_away,
                    abs_diff_away
                ) in (
                    tuple(execs.items())
                ):
                    if (
                        not pred
                        or ttype not in tf
                        or not pred(price)
                    ):
                        # log.runtime(
                        #     f'skipping quote for {sym} '
                        #     f'{pred} -> {pred(price)}\n'
                        #     f'{ttype} not in {tf}?'
                        # )
                        # majority of iterations will be non-matches
                        continue

                    brokerd_msg: Optional[BrokerdOrder] = None
                    match cmd:
                        # alert: nothing to do but relay a status
                        # back to the requesting ems client
                        case Order(action='alert'):
                            resp = 'triggered'

                        # executable order submission
                        case Order(
                            action=action,
                            symbol=symbol,
                            account=account,
                            size=size,
                        ):
                            bfqsn: str = symbol.replace(f'.{broker}', '')
                            submit_price = price + abs_diff_away
                            resp = 'triggered'  # hidden on client-side

                            log.info(
                                f'Dark order triggered for price {price}\n'
                                f'Submitting order @ price {submit_price}')

                            brokerd_msg = BrokerdOrder(
                                action=action,
                                oid=oid,
                                account=account,
                                time_ns=time.time_ns(),
                                symbol=bfqsn,
                                price=submit_price,
                                size=size,
                            )

                            await brokerd_orders_stream.send(brokerd_msg)

                            # book._ems_entries[oid] = live_req
                            # book._msgflows[oid].maps.insert(0, live_req)

                        case _:
                            raise ValueError(f'Invalid dark book entry: {cmd}')

                    # fallthrough logic
                    status = Status(
                        oid=oid,  # ems dialog id
                        time_ns=time.time_ns(),
                        resp=resp,
                        req=cmd,
                        brokerd_msg=brokerd_msg,
                    )

                    # remove exec-condition from set
                    log.info(f'removing pred for {oid}')
                    pred = execs.pop(oid, None)
                    if not pred:
                        log.warning(
                            f'pred for {oid} was already removed!?'
                        )

                    # update actives
                    # mark this entry as having sent an order
                    # request.  the entry will be replaced once the
                    # target broker replies back with
                    # a ``BrokerdOrderAck`` msg including the
                    # allocated unique ``BrokerdOrderAck.reqid`` key
                    # generated by the broker's own systems.
                    if cmd.action == 'alert':
                        # don't register the alert status (so it won't
                        # be reloaded by clients) since it's now
                        # complete / closed.
                        book._active.pop(oid)
                    else:
                        book._active[oid] = status

                    # send response to client-side
                    for client_stream in router.dialogs[oid]:
                        try:
                            await client_stream.send(status)
                        except (
                            trio.ClosedResourceError,
                        ):
                            log.warning(
                                f'{client_stream} stream broke?'
                            )
                            break

                else:  # condition scan loop complete
                    log.debug(f'execs are {execs}')
                    if execs:
                        book.orders[fqsn] = execs

        # print(f'execs scan took: {time.time() - start}')


class TradesRelay(Struct):

    # for now we keep only a single connection open with
    # each ``brokerd`` for simplicity.
    brokerd_stream: tractor.MsgStream

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

    fqsn2dialogs: defaultdict[
        str,  # fqsn
        list[str],  # oids
    ] = defaultdict(list)

    dialogs: defaultdict[
        str,  # ems uuid (oid)
        list[tractor.MsgStream]  # client side msg stream
    ] = defaultdict(list)

    # mapping of ems dialog ids to msg flow history
    msgflows: defaultdict[
        str,
        ChainMap[dict[str, dict]],
    ] = defaultdict(ChainMap)

    # brokername to trades-dialogs streams with ``brokerd`` actors
    relays: dict[
        str,  # broker name
        TradesRelay,
    ] = {}

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
        '''
        Open and yield ``brokerd`` trades dialogue context-stream if
        none already exists.

        '''
        broker = feed.mod.name
        relay: TradesRelay = self.relays.get(broker)

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
                open_brokerd_trades_dialog,
                self,
                feed,
                symbol,
                exec_mode,
                loglevel,
            )

            self.nursery.start_soon(
                translate_and_relay_brokerd_events,
                broker,
                relay.brokerd_stream,
                self,
            )

        relay.consumers += 1

        # TODO: get updated positions here?
        assert relay.brokerd_stream
        try:
            yield relay
        finally:

            # TODO: what exactly needs to be torn down here or
            # are we just consumer tracking?
            relay.consumers -= 1

    async def client_broadcast(
        self,
        msg: dict,

    ) -> None:
        for client_stream in self.clients.copy():
            try:
                await client_stream.send(msg)
            except (
                trio.ClosedResourceError,
                trio.BrokenResourceError,
            ):
                self.clients.remove(client_stream)
                log.warning(
                    f'client for {client_stream} was already closed?')


_router: Router = None


async def open_brokerd_trades_dialog(

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
                # going to expect each backend to relay only orders
                # affiliated with a particular ``trades_dialogue()``
                # session (seems annoying for implementers). So, here
                # we cache the relay task and instead of running multiple
                # tasks (which will result in multiples of the same msg being
                # relayed for each EMS client) we just register each client
                # stream to this single relay loop in the dialog table.

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
                    brokerd_stream=brokerd_trades_stream,
                    positions=pps,
                    accounts=accounts,
                    consumers=1,
                )

                _router.relays[broker] = relay

                # the ems scan loop may be cancelled by the client but we
                # want to keep the ``brokerd`` dialogue up regardless

                task_status.started(relay)

                # this context should block here indefinitely until
                # the ``brokerd`` task either dies or is cancelled
                await trio.sleep_forever()

        finally:
            # parent context must have been closed remove from cache so
            # next client will respawn if needed
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
        ('status' | 'fill'} ->  relayed through see ``Status`` msg type.

    Currently handled status values from IB:
        {'presubmitted', 'submitted', 'cancelled', 'inactive'}

    '''
    book: _DarkBook = router.get_dark_book(broker)
    relay: TradesRelay = router.relays[broker]

    assert relay.brokerd_stream == brokerd_trades_stream

    brokerd_msg: dict[str, Any]
    async for brokerd_msg in brokerd_trades_stream:
        fmsg = pformat(brokerd_msg)
        log.info(
            f'Received broker trade event:\n'
            f'{fmsg}'
        )
        status_msg: Optional[Status] = None

        match brokerd_msg:
            # BrokerdPosition
            case {
                'name': 'position',
                'symbol': sym,
                'broker': broker,
            }:
                pos_msg = BrokerdPosition(**brokerd_msg)

                # XXX: this will be useful for automatic strats yah?
                # keep pps per account up to date locally in ``emsd`` mem
                # sym, broker = pos_msg.symbol, pos_msg.broker

                relay.positions.setdefault(
                    # NOTE: translate to a FQSN!
                    (broker, sym),
                    []
                ).append(pos_msg)

                # fan-out-relay position msgs immediately by
                # broadcasting updates on all client streams
                await router.client_broadcast(pos_msg)
                continue

            # BrokerdOrderAck
            # initial response to brokerd order request
            case {
                'name': 'ack',
                'reqid': reqid,  # brokerd generated order-request id
                'oid': oid,  # ems order-dialog id
            }:
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

                # 1. the order has previously been requested to be
                # cancelled by the ems controlling client before we
                # received this ack, in which case we relay that cancel
                # signal **asap** to the backend broker
                status_msg = book._active.get(oid)
                if not status_msg:
                    log.warning(f'Rx Ack for closed/unknown order?: {oid}')
                    continue

                req = status_msg.req
                if req and req.action == 'cancel':
                    # assign newly providerd broker backend request id
                    # and tell broker to cancel immediately
                    status_msg.reqid = reqid
                    await brokerd_trades_stream.send(req)

                # 2. the order is now active and will be mirrored in
                # our book -> registered as live flow
                else:
                    # TODO: should we relay this ack state?
                    status_msg.resp = 'pending'

                # no msg to client necessary
                continue

            # BrokerdOrderError
            case {
                'name': 'error',
                'oid': oid,  # ems order-dialog id
                'reqid': reqid,  # brokerd generated order-request id
                'symbol': sym,
            } if status_msg := book._active.get(oid):

                msg = BrokerdError(**brokerd_msg)
                log.error(fmsg)  # XXX make one when it's blank?

                # TODO: figure out how this will interact with EMS clients
                # for ex. on an error do we react with a dark orders
                # management response, like cancelling all dark orders?
                # This looks like a supervision policy for pending orders on
                # some unexpected failure - something we need to think more
                # about.  In most default situations, with composed orders
                # (ex.  brackets), most brokers seem to use a oca policy.
                ems_client_order_streams = router.dialogs[oid]

                status_msg.resp = 'error'
                status_msg.brokerd_msg = msg
                book._active[oid] = status_msg

                for stream in ems_client_order_streams:
                    await stream.send(status_msg)

            # BrokerdStatus
            case {
                'name': 'status',
                'status': status,
                'reqid': reqid,  # brokerd generated order-request id

            } if (
                (oid := book._ems2brokerd_ids.inverse.get(reqid))
                and status in (
                    'canceled',
                    'open',
                    'closed',
                )
            ):
                msg = BrokerdStatus(**brokerd_msg)

                # TODO: maybe pack this into a composite type that
                # contains both the IPC stream as well the
                # msg-chain/dialog.
                ems_client_order_streams = router.dialogs[oid]
                status_msg = book._active.get(oid)

                if (
                    not ems_client_order_streams
                    or not status_msg
                ):
                    log.warning(
                        f'Received status for untracked dialog {oid}:\n'
                        f'{fmsg}'
                    )
                    continue

                status_msg.resp = status

                # retrieve existing live flow
                old_reqid = status_msg.reqid
                if old_reqid and old_reqid != reqid:
                    log.warning(
                        f'Brokerd order id change for {oid}:\n'
                        f'{old_reqid}:{type(old_reqid)} ->'
                        f' {reqid}{type(reqid)}'
                    )

                status_msg.reqid = reqid  # THIS LINE IS CRITICAL!
                status_msg.brokerd_msg = msg
                status_msg.src = msg.broker_details['name']

                for stream in ems_client_order_streams:
                    await stream.send(status_msg)

                if status == 'closed':
                    log.info(f'Execution for {oid} is complete!')
                    status_msg = book._active.pop(oid)

                elif status == 'canceled':
                    log.cancel(f'Cancellation for {oid} is complete!')
                    status_msg = book._active.pop(oid)

                else:  # open
                    # relayed from backend but probably not handled so
                    # just log it
                    log.info(f'{broker} opened order {msg}')

            # BrokerdFill
            case {
                'name': 'fill',
                'reqid': reqid,  # brokerd generated order-request id
                # 'symbol': sym,  # paper engine doesn't have this, nbd?
            }:
                oid = book._ems2brokerd_ids.inverse.get(reqid)
                if not oid:
                    # TODO: maybe we could optionally check for an
                    # ``.oid`` in the msg since we're planning to
                    # maybe-kinda offer that via using ``Status``
                    # in the longer run anyway?
                    log.warning(f'Unkown fill for {fmsg}')
                    continue

                # proxy through the "fill" result(s)
                msg = BrokerdFill(**brokerd_msg)
                log.info(f'Fill for {oid} cleared with:\n{fmsg}')

                ems_client_order_streams = router.dialogs[oid]

                # XXX: bleh, a fill can come after 'closed' from `ib`?
                # only send a late fill event we haven't already closed
                # out the dialog status locally.
                status_msg = book._active.get(oid)
                if status_msg:
                    status_msg.resp = 'fill'
                    status_msg.reqid = reqid
                    status_msg.brokerd_msg = msg

                    for stream in ems_client_order_streams:
                        await stream.send(status_msg)
                    # await ems_client_order_stream.send(status_msg)

            # ``Status`` containing an embedded order msg which
            # should be loaded as a "pre-existing open order" from the
            # brokerd backend.
            case {
                'name': 'status',
                'resp': status,
                'reqid': reqid,  # brokerd generated order-request id
            }:
                if (
                    status != 'open'
                ):
                    # TODO: check for an oid we might know since it was
                    # registered from a previous order/status load?
                    log.error(
                        f'Unknown/transient status msg:\n'
                        f'{fmsg}\n'
                        'Unable to relay message to client side!?'
                    )

                # TODO: we probably want some kind of "tagging" system
                # for external order submissions like this eventually
                # to be able to more formally handle multi-player
                # trading...
                else:
                    # existing open backend order which we broadcast to
                    # all currently connected clients.
                    log.info(
                        f'Relaying existing open order:\n {brokerd_msg}'
                    )

                    # use backend request id as our ems id though this
                    # may end up with collisions?
                    status_msg = Status(**brokerd_msg)
                    order = Order(**status_msg.req)
                    assert order.price and order.size
                    status_msg.req = order

                    assert status_msg.src  # source tag?
                    oid = str(status_msg.reqid)

                    # attempt to avoid collisions
                    status_msg.reqid = oid
                    assert status_msg.resp == 'open'

                    # register this existing broker-side dialog
                    book._ems2brokerd_ids[oid] = reqid
                    book._active[oid] = status_msg

                    # fan-out-relay position msgs immediately by
                    # broadcasting updates on all client streams
                    await router.client_broadcast(status_msg)

                # don't fall through
                continue

            # brokerd error
            case {
                'name': 'status',
                'status': 'error',
            }:
                log.error(f'Broker error:\n{fmsg}')
                # XXX: we presume the brokerd cancels its own order
                continue

            # TOO FAST ``BrokerdStatus`` that arrives
            # before the ``BrokerdAck``.
            case {
                # XXX: sometimes there is a race with the backend (like
                # `ib` where the pending stauts will be related before
                # the ack, in which case we just ignore the faster
                # pending msg and wait for our expected ack to arrive
                # later (i.e. the first block below should enter).
                'name': 'status',
                'status': status,
                'reqid': reqid,
            }:
                oid = book._ems2brokerd_ids.inverse.get(reqid)
                msg = f'Unhandled broker status for dialog {reqid}:\n'
                if oid:
                    status_msg = book._active.get(oid)
                    # status msg may not have been set yet or popped?
                    # NOTE: have seen a key error here on kraken
                    # clearable limits..
                    if status_msg:
                        msg += (
                            f'last status msg: {pformat(status_msg)}\n\n'
                            f'this msg:{fmsg}\n'
                        )

                log.warning(msg)

            case _:
                raise ValueError(f'Brokerd message {brokerd_msg} is invalid')

        # XXX: ugh sometimes we don't access it?
        # if status_msg is not None:
        #     del status_msg


async def process_client_order_cmds(

    client_order_stream: tractor.MsgStream,
    brokerd_order_stream: tractor.MsgStream,

    symbol: str,
    feed: Feed,
    dark_book: _DarkBook,
    router: Router,

) -> None:

    client_dialogs = router.dialogs

    # cmd: dict
    async for cmd in client_order_stream:
        log.info(f'Received order cmd:\n{pformat(cmd)}')

        # CAWT DAMN we need struct support!
        oid = str(cmd['oid'])

        # register this stream as an active dialogue for this order id
        # such that translated message from the brokerd backend can be
        # routed (relayed) to **just** that client stream (and in theory
        # others who are registered for such order affiliated msgs).
        subs = client_dialogs[oid]
        if client_order_stream not in subs:
            subs.append(client_order_stream)

        router.fqsn2dialogs[symbol].append(oid)
        reqid = dark_book._ems2brokerd_ids.inverse.get(oid)

        # any dark/live status which is current
        status = dark_book._active.get(oid)

        match cmd:
            # existing live-broker order cancel
            case {
                'action': 'cancel',
                'oid': oid,
            } if (
                (status := dark_book._active.get(oid))
                and status.resp in ('open', 'pending')
            ):
                reqid = status.reqid
                order = status.req
                to_brokerd_msg = BrokerdCancel(
                    oid=oid,
                    reqid=reqid,
                    time_ns=time.time_ns(),
                    # account=live_entry.account,
                    account=order.account,
                )

                # NOTE: cancel response will be relayed back in messages
                # from corresponding broker
                if reqid is not None:
                    # send cancel to brokerd immediately!
                    log.info(
                        f'Submitting cancel for live order {reqid}'
                    )
                    await brokerd_order_stream.send(to_brokerd_msg)

                else:
                    # this might be a cancel for an order that hasn't been
                    # acked yet by a brokerd, so register a cancel for when
                    # the order ack does show up later such that the brokerd
                    # order request can be cancelled at that time.
                    # dark_book._ems_entries[oid] = msg
                    # special case for now..
                    status.req = to_brokerd_msg

            # dark trigger cancel
            case {
                'action': 'cancel',
                'oid': oid,
            } if (
                status and status.resp == 'dark_open'
                # or status and status.req
            ):
                # remove from dark book clearing
                entry = dark_book.orders[symbol].pop(oid, None)
                if entry:
                    (
                        pred,
                        tickfilter,
                        cmd,
                        percent_away,
                        abs_diff_away
                    ) = entry

                    # tell client side that we've cancelled the
                    # dark-trigger order
                    status.resp = 'canceled'
                    status.req = cmd

                    await client_order_stream.send(status)
                    # de-register this order dialogue from all clients
                    router.dialogs[oid].clear()
                    router.dialogs.pop(oid)
                    dark_book._active.pop(oid)

                else:
                    log.exception(f'No dark order for {symbol}?')

            # TODO: eventually we should be receiving
            # this struct on the wire unpacked in a scoped protocol
            # setup with ``tractor``.

            # live order submission
            case {
                'oid': oid,
                'symbol': fqsn,
                'price': trigger_price,
                'size': size,
                'action': ('buy' | 'sell') as action,
                'exec_mode': ('live' | 'paper'),
            }:
                # TODO: relay this order msg directly?
                req = Order(**cmd)
                broker = req.brokers[0]

                # remove the broker part before creating a message
                # to send to the specific broker since they probably
                # aren't expectig their own name, but should they?
                sym = fqsn.replace(f'.{broker}', '')

                if status is not None:
                    # if we already had a broker order id then
                    # this is likely an order update commmand.
                    reqid = status.reqid
                    log.info(f"Modifying live {broker} order: {reqid}")
                    status.req = req
                    status.resp = 'pending'

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
                    account=req.account,
                )

                if status is None:
                    status = Status(
                        oid=oid,
                        reqid=reqid,
                        resp='pending',
                        time_ns=time.time_ns(),
                        brokerd_msg=msg,
                        req=req,
                    )

                dark_book._active[oid] = status

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
                # dark_book._msgflows[oid].maps.insert(0, msg.to_dict())

            # dark-order / alert submission
            case {
                'oid': oid,
                'symbol': fqsn,
                'price': trigger_price,
                'size': size,
                'exec_mode': exec_mode,
                'action': action,
                'brokers': brokers,  # list
            } if (
                    # "DARK" triggers
                    # submit order to local EMS book and scan loop,
                    # effectively a local clearing engine, which
                    # scans for conditions and triggers matching executions
                    exec_mode in ('dark',)
                    or action == 'alert'
            ):
                req = Order(**cmd)

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
                    last = feed.rt_shm.array[-1]['close']

                pred = mk_check(trigger_price, last, action)

                spread_slap: float = 5
                sym = fqsn.replace(f'.{brokers[0]}', '')
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
                    req,
                    percent_away,
                    abs_diff_away
                )
                resp = 'dark_open'

                # alerts have special msgs to distinguish
                # if action == 'alert':
                #     resp = 'open'

                status = Status(
                    resp=resp,
                    oid=oid,
                    time_ns=time.time_ns(),
                    req=req,
                    src='dark',
                )
                dark_book._active[oid] = status
                await client_order_stream.send(status)


@tractor.context
async def _emsd_main(
    ctx: tractor.Context,
    fqsn: str,
    exec_mode: str,  # ('paper', 'live')
    loglevel: str = 'info',

) -> None:
    '''
    EMS (sub)actor entrypoint providing the execution management
    (micro)service which conducts broker order clearing control on
    behalf of clients.

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

    # spawn one task per broker feed
    feed: Feed
    async with (
        maybe_open_feed(
            [fqsn],
            loglevel=loglevel,
        ) as (feed, quote_stream),
    ):

        # XXX: this should be initial price quote from target provider
        first_quote: dict = feed.first_quotes[fqsn]
        book: _DarkBook = _router.get_dark_book(broker)
        book.lasts[fqsn]: float = first_quote['last']

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

            brokerd_stream = relay.brokerd_stream

            # signal to client that we're started and deliver
            # all known pps and accounts for this ``brokerd``.
            await ems_ctx.started((
                relay.positions,
                list(relay.accounts),
                book._active,
            ))

            # establish 2-way stream with requesting order-client and
            # begin handling inbound order requests and updates
            async with ems_ctx.open_stream() as client_stream:

                # register the client side before starting the
                # brokerd-side relay task to ensure the client is
                # delivered all exisiting open orders on startup.
                _router.clients.add(client_stream)
                for oid in _router.fqsn2dialogs[fqsn]:
                    _router.dialogs[oid].append(client_stream)

                # trigger scan and exec loop
                n.start_soon(
                    clear_dark_triggers,
                    _router,
                    brokerd_stream,
                    quote_stream,
                    broker,
                    fqsn,  # form: <name>.<venue>.<suffix>.<broker>
                    book
                )

                # start inbound (from attached client) order request processing
                # main entrypoint, run here until cancelled.
                try:
                    await process_client_order_cmds(
                        client_stream,
                        brokerd_stream,
                        fqsn,
                        feed,
                        dark_book,
                        _router,
                    )

                finally:
                    # try to remove client from "registry"
                    try:
                        _router.clients.remove(client_stream)
                    except KeyError:
                        log.warning(
                            f'Stream {client_stream._ctx.chan.uid}'
                            ' was already dropped?'
                        )

                    dialogs = _router.dialogs
                    for oid, client_streams in dialogs.items():
                        if client_stream in client_streams:
                            client_streams.remove(client_stream)

                            # TODO: for order dialogs left "alive" in
                            # the ems this is where we should allow some
                            # system to take over management. Likely we
                            # want to allow the user to choose what kind
                            # of policy to use (eg. cancel all orders
                            # from client, run some algo, etc.)
                            if not client_streams:
                                log.warning(
                                    f'Order dialog is being unmonitored:\n'
                                    f'{oid} ->\n{client_stream._ctx.chan.uid}'
                                )
