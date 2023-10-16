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
In da suit parlances: "Execution management systems"

"""
from __future__ import annotations
from collections import (
    defaultdict,
    # ChainMap,
)
from contextlib import asynccontextmanager as acm
from decimal import Decimal
from math import isnan
from pprint import pformat
from time import time_ns
from types import ModuleType
from typing import (
    AsyncIterator,
    Any,
    Callable,
    Hashable,
    Optional,
    TYPE_CHECKING,
)

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor

from ._util import (
    log,  # sub-sys logger
    get_console_log,
)
from ..accounting._mktinfo import (
    unpack_fqme,
    dec_digits,
)
from piker.types import Struct
from ..ui._notify import notify_from_ems_status_msg
from ..data import iterticks
from ._messages import (
    Order,
    Status,
    Error,
    BrokerdCancel,
    BrokerdOrder,
    # BrokerdOrderAck,
    BrokerdStatus,
    BrokerdFill,
    BrokerdError,
    BrokerdPosition,
)

if TYPE_CHECKING:
    from ..data import (
        Feed,
        Flume,
    )


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


class DarkBook(Struct):
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
    triggers: dict[
        str,  # symbol
        dict[
            str,  # uuid for triggerable execution
            tuple[
                Callable[[float], bool],  # predicate
                tuple[str, ...],  # tickfilter
                dict | Order,  # cmd / msg type

                # live submission constraint parameters
                float,  # percent_away max price diff
                float,  # abs_diff_away max price diff
                int,  # min_tick_digits to round the clearable price
            ]
        ]
    ] = {}

    lasts: dict[str, float] = {}  # quote prices
    _active: dict[str, Status] = {}  # active order dialogs
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
    fqme: str,

    book: DarkBook,

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
    quote_stream._raise_on_lag = False
    async for quotes in quote_stream:
        # start = time.time()
        for sym, quote in quotes.items():
            # TODO: make this a msg-compat struct
            execs: tuple = book.triggers.get(sym, {})
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
                    abs_diff_away,
                    price_tick_digits,
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
                            bfqme: str = symbol.replace(f'.{broker}', '')
                            submit_price: float = round(
                                price + abs_diff_away,
                                ndigits=price_tick_digits,
                            )
                            resp: str = 'triggered'  # hidden on client-side

                            log.info(
                                f'Dark order triggered for price {price}\n'
                                f'Submitting order @ price {submit_price}')

                            brokerd_msg = BrokerdOrder(
                                action=action,
                                oid=oid,
                                account=account,
                                time_ns=time_ns(),
                                symbol=bfqme,
                                price=submit_price,
                                size=size,
                            )
                            await brokerd_orders_stream.send(brokerd_msg)

                        case _:
                            raise ValueError(f'Invalid dark book entry: {cmd}')

                    # fallthrough logic
                    status = Status(
                        oid=oid,  # ems dialog id
                        time_ns=time_ns(),
                        resp=resp,
                        req=cmd,
                        brokerd_msg=brokerd_msg,
                    )

                    # remove exec-condition from set
                    log.info(f'Removing trigger for {oid}')
                    trigger: tuple | None = execs.pop(oid, None)
                    if not trigger:
                        log.warning(
                            f'trigger for {oid} was already removed!?'
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
                    await router.client_broadcast(
                        fqme,
                        status,
                    )

                else:  # condition scan loop complete
                    log.debug(f'execs are {execs}')
                    if execs:
                        book.triggers[fqme] = execs

        # print(f'execs scan took: {time.time() - start}')


class TradesRelay(Struct):

    # for now we keep only a single connection open with
    # each ``brokerd`` for simplicity.
    brokerd_stream: tractor.MsgStream

    # map of symbols to dicts of accounts to pp msgs
    positions: dict[
        # brokername, acctid ->
        tuple[str, str],
        # fqme -> msg
        dict[str, BrokerdPosition],
    ]

    # allowed account names
    accounts: tuple[str]


@acm
async def open_brokerd_dialog(
    brokermod: ModuleType,
    portal: tractor.Portal,
    exec_mode: str,
    fqme: str | None = None,
    loglevel: str | None = None,

) -> tuple[
    tractor.MsgStream,
    # {(brokername, accountname) -> {fqme -> msg}}
    dict[(str, str), dict[str, BrokerdPosition]],
    list[str],
]:
    '''
    Open either a live trades control dialog or a dialog with a new
    paper engine instance depending on live trading support for the
    broker backend, configuration, or client code usage.

    '''
    broker: str = brokermod.name

    def mk_paper_ep():
        from . import _paper_engine as paper_mod

        nonlocal brokermod, exec_mode

        # for logging purposes
        brokermod = paper_mod

        # for paper mode we need to mock this trades response feed
        # so we load bidir stream to a new sub-actor running
        # a paper-simulator clearing engine.

        # load the paper trading engine
        log.info(f'{broker}: Entering `paper` trading mode')

        # load the paper trading engine as a subactor of this emsd
        # actor to simulate the real IPC load it'll have when also
        # pulling data from feeds
        if not fqme:
            log.warning(
                f'Paper engine activate for {broker} but no fqme provided?'
            )

        return paper_mod.open_paperboi(
            fqme=fqme,
            broker=broker,
            loglevel=loglevel,
        )

    # take the first supported ep we detect
    # on the backend mod.
    trades_endpoint: Callable
    for ep_name in [
        'open_trade_dialog',  # probably final name?
        'trades_dialogue',  # legacy
    ]:
        trades_endpoint = getattr(
            brokermod,
            ep_name,
            None,
        )
        if trades_endpoint:
            break
    else:
        log.warning(
            f'No live trading EP found: {brokermod.name}?'
        )
        exec_mode: str = 'paper'

    if (
        trades_endpoint is not None
        or exec_mode != 'paper'
    ):
        # open live brokerd trades endpoint
        open_trades_endpoint = portal.open_context(
            trades_endpoint,
        )

    @acm
    async def maybe_open_paper_ep():
        if exec_mode == 'paper':
            async with mk_paper_ep() as msg:
                yield msg
                return

        # open trades-dialog endpoint with backend broker
        async with open_trades_endpoint as msg:
            ctx, first = msg

            # runtime indication that the backend can't support live
            # order ctrl yet, so boot the paperboi B0
            if first == 'paper':
                async with mk_paper_ep() as msg:
                    yield msg
                    return
            else:
                # working live ep case B)
                yield msg
                return

    pps_by_broker_account: dict[(str, str), BrokerdPosition] = {}

    async with (
        maybe_open_paper_ep() as (
            brokerd_ctx,
            (position_msgs, accounts),
        ),
        brokerd_ctx.open_stream() as brokerd_trades_stream,
    ):
        # XXX: really we only want one stream per `emsd`
        # actor to relay global `brokerd` order events
        # unless we're going to expect each backend to
        # relay only orders affiliated with a particular
        # ``trades_dialogue()`` session (seems annoying
        # for implementers). So, here we cache the relay
        # task and instead of running multiple tasks
        # (which will result in multiples of the same
        # msg being relayed for each EMS client) we just
        # register each client stream to this single
        # relay loop in the dialog table.

        # begin processing order events from the target
        # brokerd backend by receiving order submission
        # response messages, normalizing them to EMS
        # messages and relaying back to the piker order
        # client set.

        # locally cache and track positions per account with
        # a nested table of msgs:
        #  tuple(brokername, acctid) ->
        #      (fqme: str ->
        #           `BrokerdPosition`)
        for msg in position_msgs:

            msg = BrokerdPosition(**msg)
            log.info(
                f'loading pp for {brokermod.__name__}:\n'
                f'{pformat(msg.to_dict())}',
            )

            # TODO: state any mismatch here?
            account: str = msg.account
            assert account in accounts

            pps_by_broker_account.setdefault(
                (broker, account),
                {},
            )[msg.symbol] = msg

        # should be unique entries, verdad!
        assert len(set(accounts)) == len(accounts)

        yield (
            brokerd_trades_stream,
            pps_by_broker_account,
            accounts,
        )


class Router(Struct):
    '''
    Order router which manages and tracks per-broker dark book,
    alerts, clearing and related data feed management.

    A singleton per ``emsd`` actor.

    '''
    # setup at actor spawn time
    nursery: trio.Nursery

    # broker to book map
    books: dict[str, DarkBook] = {}

    # NOTE: disable for since stupid "dunst"
    notify_on_order_loads: bool = False

    # sets of clients mapped from subscription keys
    subscribers: defaultdict[
        str,  # sub key, default fqme
        set[tractor.MsgStream],  # unique client streams
    ] = defaultdict(set)

    # sets of clients dynamically registered for specific
    # order flows based on subscription config.
    dialogs: defaultdict[
        str,  # ems uuid (oid)
        set[tractor.MsgStream]  # client side msg stream
    ] = defaultdict(set)

    # TODO: mapping of ems dialog ids to msg flow history
    # - use the new ._util.OrderDialogs?
    # msgflows: defaultdict[
    #     str,
    #     ChainMap[dict[str, dict]],
    # ] = defaultdict(ChainMap)

    # brokername to trades-dialogs streams with ``brokerd`` actors
    relays: dict[
        str,  # broker name
        TradesRelay,
    ] = {}

    def get_dark_book(
        self,
        brokername: str,

    ) -> DarkBook:

        return self.books.setdefault(brokername, DarkBook(brokername))

    def get_subs(
        self,
        oid: str,

    ) -> set[tractor.MsgStream]:
        '''
        Deliver list of non-closed subscriber client msg streams.

        '''
        return set(
            stream for stream in self.dialogs[oid]
            if not stream._closed
        )

    @acm
    async def maybe_open_brokerd_dialog(
        self,
        brokermod: ModuleType,
        portal: tractor.Portal,
        exec_mode: str,
        fqme: str,
        loglevel: str,

    ) -> None:
        broker = brokermod.name

        relay: TradesRelay = self.relays.get(broker)
        if (
            relay

            # We always want to spawn a new relay for the paper
            # engine per symbol since we need a new tractor context
            # to be opened for every every symbol such that a new
            # data feed and ``PaperBoi`` client will be created and
            # then used to simulate clearing events.
            and exec_mode != 'paper'
        ):
            # deliver already cached instance
            yield relay
            return

        async with open_brokerd_dialog(
            brokermod=brokermod,
            portal=portal,
            exec_mode=exec_mode,
            fqme=fqme,
            loglevel=loglevel,

        ) as (
            brokerd_stream,
            pp_msg_table,
            accounts,
        ):
            # create a new relay and sync it's state according
            # to brokerd-backend reported position msgs.
            relay = TradesRelay(
                brokerd_stream=brokerd_stream,
                positions=pp_msg_table,
                accounts=tuple(accounts),
            )
            self.relays[broker] = relay

            # this context should block here indefinitely until
            # the ``brokerd`` task either dies or is cancelled
            try:
                yield relay
            finally:
                # parent context must have been closed remove from cache so
                # next client will respawn if needed
                relay = self.relays.pop(broker, None)
                if not relay:
                    log.warning(
                        f'Relay for {broker} was already removed!?')

    async def open_trade_relays(
        self,
        fqme: str,
        exec_mode: str,
        loglevel: str,

        task_status: TaskStatus[
            tuple[TradesRelay, Feed]
        ] = trio.TASK_STATUS_IGNORED,

    ) -> tuple[TradesRelay, Feed]:
        '''
        Maybe open a live feed to the target fqme, start `brokerd` order
        msg relay and dark clearing tasks to run in the background
        indefinitely.

        '''
        from ..data.feed import maybe_open_feed

        async with (
            maybe_open_feed(
                [fqme],
                loglevel=loglevel,
            ) as feed,
        ):
            # extract expanded fqme in case input was of a less
            # qualified form, eg. xbteur.kraken -> xbteur.spot.kraken
            fqme: str = list(feed.flumes.keys())[0]
            brokername, _, _, _ = unpack_fqme(fqme)
            brokermod = feed.mods[brokername]
            broker = brokermod.name
            portal = feed.portals[brokermod]

            # XXX: this should be initial price quote from target provider
            flume = feed.flumes[fqme]
            first_quote: dict = flume.first_quote
            book: DarkBook = self.get_dark_book(broker)
            book.lasts[fqme]: float = float(first_quote['last'])

            async with self.maybe_open_brokerd_dialog(
                brokermod=brokermod,
                portal=portal,
                exec_mode=exec_mode,
                fqme=fqme,
                loglevel=loglevel,
            ) as relay:

                # dark book clearing loop, also lives with parent
                # daemon to allow dark order clearing while no
                # client is connected.
                self.nursery.start_soon(
                    clear_dark_triggers,
                    self,
                    relay.brokerd_stream,
                    flume.stream,
                    broker,
                    fqme,  # form: <name>.<venue>.<suffix>.<broker>
                    book
                )

                client_ready = trio.Event()
                task_status.started(
                    (fqme, relay, feed, client_ready)
                )

                # sync to the client side by waiting for the stream
                # connection setup before relaying any existing live
                # orders from the brokerd.
                await client_ready.wait()
                assert self.subscribers

                # spawn a ``brokerd`` order control dialog stream
                # that syncs lifetime with the parent `emsd` daemon.
                self.nursery.start_soon(
                    translate_and_relay_brokerd_events,
                    broker,
                    relay.brokerd_stream,
                    self,
                )

                await trio.sleep_forever()

    async def client_broadcast(
        self,
        sub_key: str,
        msg: dict,
        notify_on_headless: bool = True,

    ) -> bool:
        # print(f'SUBSCRIBERS: {self.subscribers}')
        to_remove: set[tractor.MsgStream] = set()

        if sub_key == 'all':
            subs = set()
            for s in self.subscribers.values():
                subs |= s
        else:
            subs = self.subscribers[sub_key]

        sent_some: bool = False
        for client_stream in subs:
            try:
                await client_stream.send(msg)
                sent_some = True
            except (
                trio.ClosedResourceError,
                trio.BrokenResourceError,
            ):
                to_remove.add(client_stream)
                log.warning(
                    f'client for {client_stream} was already closed?')

        if to_remove:
            subs.difference_update(to_remove)

        if (
            not sent_some
            and self.notify_on_order_loads
            and notify_on_headless
        ):
            log.info(
                'No clients attached, '
                f'firing notification for {sub_key} msg:\n'
                f'{msg}'
            )
            await notify_from_ems_status_msg(
                msg,
                is_subproc=True,
            )
        return sent_some


_router: Router = None


@tractor.context
async def _setup_persistent_emsd(
    ctx: tractor.Context,
    loglevel: str | None = None,

) -> None:

    if loglevel:
        get_console_log(loglevel)

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
    book: DarkBook = router.get_dark_book(broker)
    relay: TradesRelay = router.relays[broker]
    assert relay.brokerd_stream == brokerd_trades_stream

    brokerd_msg: dict[str, Any]
    async for brokerd_msg in brokerd_trades_stream:
        fmsg = pformat(brokerd_msg)
        log.info(
            f'Rx brokerd trade msg:\n'
            f'{fmsg}'
        )
        status_msg: Status | None = None

        match brokerd_msg:
            # BrokerdPosition
            case {
                'name': 'position',
                'broker': broker,
            }:
                pos_msg = BrokerdPosition(**brokerd_msg)

                # XXX: this will be useful for automatic strats yah?
                # keep pps per account up to date locally in ``emsd`` mem
                # sym, broker = pos_msg.symbol, pos_msg.broker

                # NOTE: translate to a FQME!
                relay.positions.setdefault(
                    (broker, pos_msg.account),
                    {}
                )[pos_msg.symbol] = pos_msg

                # fan-out-relay position msgs immediately by
                # broadcasting updates on all client streams
                # TODO: this should be subscription based for privacy
                # eventually!
                await router.client_broadcast('all', pos_msg)
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

                if status_msg.cancel_called:
                    # assign newly providerd broker backend request id
                    # and tell broker to cancel immediately
                    status_msg.reqid = reqid

                    # NOTE: as per comment in cancel-request-block
                    # above: This is an ack to
                    # a client-already-cancelled order request so we
                    # must immediately send a cancel to the brokerd upon
                    # rx of this ACK.
                    await brokerd_trades_stream.send(
                        BrokerdCancel(
                            oid=oid,
                            reqid=reqid,
                            time_ns=time_ns(),
                            account=status_msg.req.account,
                        )
                    )

                # 2. the order is now active and will be mirrored in
                # our book -> registered as live flow
                else:
                    # TODO: should we relay this ack state?
                    status_msg.resp = 'pending'

                # no msg to client necessary
                continue

            # BrokerdError
            # TODO: figure out how this will interact with EMS clients
            # for ex. on an error do we react with a dark orders
            # management response, like cancelling all dark orders?
            # This looks like a supervision policy for pending orders on
            # some unexpected failure - something we need to think more
            # about.  In most default situations, with composed orders
            # (ex.  brackets), most brokers seem to use a oca policy.
            case {
                'name': 'error',
                'oid': oid,  # ems order-dialog id
                'reqid': reqid,  # brokerd generated order-request id
            }:
                if (
                    not oid
                    # try to lookup any order dialog by
                    # brokerd-side id..
                    and not (
                        oid := book._ems2brokerd_ids.inverse.get(reqid)
                    )
                ):
                    log.warning(
                        f'Rxed unusable error-msg:\n'
                        f'{brokerd_msg}'
                    )
                    continue

                msg = BrokerdError(**brokerd_msg)

                # NOTE: retreive the last client-side response
                # OR create an error when we have no last msg /dialog
                # on record
                status_msg: Status
                if not (status_msg := book._active.get(oid)):
                    status_msg = Error(
                        time_ns=time_ns(),
                        oid=oid,
                        reqid=reqid,
                        brokerd_msg=msg,
                    )
                else:
                    # only modify last status if we have an active
                    # ongoing dialog..
                    status_msg.resp = 'error'
                    status_msg.brokerd_msg = msg

                book._active[oid] = status_msg

                log.error(
                    'Translating brokerd error to status:\n'
                    f'{fmsg}'
                    f'{status_msg.to_dict()}'
                )
                if req := status_msg.req:
                    fqme: str = req.symbol
                else:
                    bdmsg: Struct = status_msg.brokerd_msg
                    fqme: str = (
                        bdmsg.symbol  # might be None
                        or
                        bdmsg.broker_details['flow']
                        # NOTE: what happens in empty case in the
                        # broadcast below? it's a problem?
                        .get('symbol', '')
                    )

                await router.client_broadcast(
                    fqme,
                    status_msg,
                )

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
                status_msg = book._active.get(oid)
                if not status_msg:
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

                await router.client_broadcast(
                    status_msg.req.symbol,
                    status_msg,
                )

                if status == 'closed':
                    log.info(f'Execution for {oid} is complete!')
                    status_msg = book._active.pop(oid)

                elif status == 'canceled':
                    log.cancel(f'Cancellation for {oid} is complete!')
                    status_msg = book._active.pop(oid, None)

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
                    log.warning(f'Unknown fill for {fmsg}')
                    continue

                # proxy through the "fill" result(s)
                msg = BrokerdFill(**brokerd_msg)
                log.info(f'Fill for {oid} cleared with:\n{fmsg}')

                # XXX: bleh, a fill can come after 'closed' from `ib`?
                # only send a late fill event we haven't already closed
                # out the dialog status locally.
                status_msg = book._active.get(oid)
                if status_msg:
                    status_msg.resp = 'fill'
                    status_msg.reqid = reqid
                    status_msg.brokerd_msg = msg

                    # TODO: if no client is connected (aka we're
                    # headless) we should record the fill in the
                    # ``.msg_flow`` chain and re-transmit on client
                    # connect so that fills can be displayed in a
                    # chart?
                    await router.client_broadcast(
                        status_msg.req.symbol,
                        status_msg,
                    )

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

                    # NOTE: be sure to pack an fqme for the client side!
                    order = Order(**status_msg.req)
                    order.symbol = f'{order.symbol}.{broker}'

                    assert order.price and order.size
                    status_msg.req = order

                    assert status_msg.src  # source tag?
                    oid: str = str(status_msg.reqid)

                    # attempt to avoid collisions
                    status_msg.reqid = oid
                    assert status_msg.resp == 'open'

                    # register this existing broker-side dialog
                    book._ems2brokerd_ids[oid] = reqid
                    book._active[oid] = status_msg

                    # fan-out-relay position msgs immediately by
                    # broadcasting updates on all client streams
                    await router.client_broadcast(
                        order.symbol,
                        status_msg,
                    )

            # TOO FAST ``BrokerdStatus`` that arrives
            # before the ``BrokerdAck``.
            # NOTE XXX: sometimes there is a race with the backend (like
            # `ib` where the pending status will be relayed *before*
            # the ack msg, in which case we just ignore the faster
            # pending msg and wait for our expected ack to arrive
            # later (i.e. the first block below should enter).
            case {
                'name': 'status',
                'status': status,
                'reqid': reqid,
            }:
                msg = (
                    f'Unhandled broker status for dialog {reqid}:\n'
                    f'{pformat(brokerd_msg)}'
                )
                if (
                    oid := book._ems2brokerd_ids.inverse.get(reqid)
                ):
                    # NOTE: have seen a key error here on kraken
                    # clearable limits..
                    if status_msg := book._active.get(oid):
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

    fqme: str,
    flume: Flume,
    dark_book: DarkBook,
    router: Router,

) -> None:
    '''
    Client-dialog request loop: accept order requests and deliver
    initial status msg responses to subscribed clients.

    This task-loop handles both management of dark triggered orders and
    alerts by inserting them into the "dark book"-table as well as
    submitting live orders immediately if requested by the client.

    '''
    # cmd: dict
    async for cmd in client_order_stream:
        log.info(f'Received order cmd:\n{pformat(cmd)}')

        # CAWT DAMN we need struct support!
        oid = str(cmd['oid'])

        # register this stream as an active order dialog (msg flow) for
        # this order id such that translated message from the brokerd
        # backend can be routed and relayed to subscribed clients.
        subs = router.dialogs[oid]

        # add all subscribed clients for this fqme (should eventually be
        # a more generalize subscription system) to received order msg
        # updates (and thus show stuff in the UI).
        subs.add(client_order_stream)
        subs.update(router.subscribers[fqme])

        reqid = dark_book._ems2brokerd_ids.inverse.get(oid)

        # any dark/live status which is current
        status = dark_book._active.get(oid)

        match cmd:
            # existing LIVE CANCEL
            case {
                'action': 'cancel',
                'oid': oid,
            } if (
                status
                and status.resp in (
                    'open',
                    'pending',
                )
            ):
                reqid = status.reqid
                order = status.req

                # XXX: cancelled-before-ack race case.
                # This might be a cancel for an order that hasn't been
                # acked yet by a brokerd (so it's in the midst of being
                # ``BrokerdAck``ed for submission but we don't have that
                # confirmation response back yet). Set this client-side
                # msg state so when the ack does show up (later)
                # logic in ``translate_and_relay_brokerd_events()`` can
                # forward the cancel request to the `brokerd` side of
                # the order flow ASAP.
                status.cancel_called = True

                # NOTE: cancel response will be relayed back in messages
                # from corresponding broker
                if reqid is not None:
                    # send cancel to brokerd immediately!
                    log.info(
                        f'Submitting cancel for live order {reqid}'
                    )
                    await brokerd_order_stream.send(
                        BrokerdCancel(
                            oid=oid,
                            reqid=reqid,
                            time_ns=time_ns(),
                            account=order.account,
                        )
                    )

            # DARK trigger CANCEL
            case {
                'action': 'cancel',
                'oid': oid,
            } if (
                status
                and status.resp == 'dark_open'
            ):
                # remove from dark book clearing
                entry: tuple | None = dark_book.triggers[fqme].pop(oid, None)
                if entry:
                    (
                        pred,
                        tickfilter,
                        cmd,
                        percent_away,
                        abs_diff_away,
                        min_tick_digits,
                    ) = entry

                    # tell client side that we've cancelled the
                    # dark-trigger order
                    status.resp = 'canceled'
                    status.req = cmd

                    await router.client_broadcast(
                        fqme,
                        status,
                    )

                    # de-register this order dialogue from all clients
                    router.dialogs[oid].clear()
                    router.dialogs.pop(oid)
                    dark_book._active.pop(oid)

                else:
                    log.exception(f'No dark order for {fqme}?')

            # TODO: eventually we should be receiving
            # this struct on the wire unpacked in a scoped protocol
            # setup with ``tractor`` using ``msgspec``.

            # LIVE order REQUEST
            case {
                'oid': oid,
                'symbol': fqme,
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
                sym = fqme.replace(f'.{broker}', '')

                if status is not None:
                    # if we already had a broker order id then
                    # this is likely an order update commmand.
                    reqid = status.reqid
                    log.info(f"Modifying live {broker} order: {reqid}")
                    status.req = req
                    status.resp = 'pending'

                msg = BrokerdOrder(
                    oid=oid,  # no ib support for oids...
                    time_ns=time_ns(),

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
                        time_ns=time_ns(),
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

            # DARK-order / alert REQUEST
            case {
                'oid': oid,
                'symbol': fqme,
                'price': trigger_price,
                'size': size,
                'exec_mode': exec_mode,
                'action': action,
                'brokers': _,  # list
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
                last = dark_book.lasts[fqme]

                # sometimes the real-time feed hasn't come up
                # so just pull from the latest history.
                if isnan(last):
                    last = flume.rt_shm.array[-1]['close']

                pred = mk_check(trigger_price, last, action)

                # NOTE: for dark orders currently we submit
                # the triggered live order at a price 5 ticks
                # above/below the L1 prices.
                # TODO: make this configurable from our top level
                # config, prolly in a .clearing` section?
                spread_slap: float = 5
                min_tick = Decimal(flume.mkt.price_tick)
                min_tick_digits: int = dec_digits(min_tick)

                tickfilter: tuple[str, ...]
                percent_away: float

                if action == 'buy':
                    tickfilter = ('ask', 'last', 'trade')
                    percent_away: float = 0.005

                    # TODO: we probably need to scale this based
                    # on some near term historical spread
                    # measure?
                    abs_diff_away = float(round(
                        spread_slap * min_tick,
                        ndigits=min_tick_digits,
                    ))

                elif action == 'sell':
                    tickfilter = ('bid', 'last', 'trade')
                    percent_away: float = -0.005
                    abs_diff_away: float = float(round(
                        -spread_slap * min_tick,
                        ndigits=min_tick_digits,
                    ))

                else:  # alert
                    tickfilter = ('trade', 'utrade', 'last')
                    percent_away: float = 0
                    abs_diff_away: float = 0

                # submit execution/order to EMS scan loop
                # NOTE: this may result in an override of an existing
                # dark book entry if the order id already exists
                dark_book.triggers.setdefault(
                    fqme, {}
                )[oid] = (
                    pred,
                    tickfilter,
                    req,
                    percent_away,
                    abs_diff_away,
                    min_tick_digits,
                )
                resp = 'dark_open'

                # alerts have special msgs to distinguish
                # if action == 'alert':
                #     resp = 'open'

                status = Status(
                    resp=resp,
                    oid=oid,
                    time_ns=time_ns(),
                    req=req,
                    src='dark',
                )
                dark_book._active[oid] = status

                # broadcast status to all subscribed clients
                await router.client_broadcast(
                    fqme,
                    status,
                )

            case _:
                log.warning(f'Rx UNHANDLED order request {cmd}')


@acm
async def maybe_open_trade_relays(
    router: Router,
    fqme: str,
    exec_mode: str,  # ('paper', 'live')
    loglevel: str = 'info',

) -> tuple:

    def cache_on_fqme_unless_paper(
        router: Router,
        fqme: str,
        exec_mode: str,  # ('paper', 'live')
        loglevel: str = 'info',
    ) -> Hashable:
        if exec_mode == 'paper':
            return f'paper_{fqme}'
        else:
            return fqme

    # XXX: closure to enable below use of
    # ``tractor.trionics.maybe_open_context()``
    @acm
    async def cached_mngr(
        router: Router,
        fqme: str,
        exec_mode: str,  # ('paper', 'live')
        loglevel: str = 'info',
    ):

        fqme, relay, feed, client_ready = await _router.nursery.start(
            _router.open_trade_relays,
            fqme,
            exec_mode,
            loglevel,
        )
        yield fqme, relay, feed, client_ready

    async with tractor.trionics.maybe_open_context(
        acm_func=cached_mngr,
        kwargs={
            'router': _router,
            'fqme': fqme,
            'exec_mode': exec_mode,
            'loglevel': loglevel,
        },
        key=cache_on_fqme_unless_paper,
    ) as (
        cache_hit,
        (fqme, relay, feed, client_ready)
    ):
        if cache_hit:
            log.info(f'Reusing existing trades relay for {fqme}:\n'
                     f'{relay}\n')

        yield fqme, relay, feed, client_ready


@tractor.context
async def _emsd_main(
    ctx: tractor.Context,
    fqme: str,
    exec_mode: str,  # ('paper', 'live')
    loglevel: str | None = None,

) -> tuple[
    dict[
        # brokername, acctid
        tuple[str, str],
        list[BrokerdPosition],
    ],
    list[str],
    dict[str, Status],
]:
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

    - ``_setup_persistent_emsd()``:
      is the ``emsd`` actor's primary *service-fixture* task which
      is opened by the `pikerd` service manager and sets up
      a process-global (actor-local) ``Router`` instance and opens
      a service nursery which lives until the backend broker is
      shutdown or the ems is terminated; all tasks are
      *dynamically* started (and persisted) within this service
      nursery when the below endpoint context is opened:
        |
        - ``_emsd_main()``:
          attaches a real-time quote feed and trades dialogue with
          a `brokerd` actor which connects to the backend broker's
          trading api for every connecting client.
           |
            - ``clear_dark_triggers()``:
              run (dark order) conditions on inputs and trigger brokerd
              "live" order submissions.
           |
            - ``process_client_order_cmds()``:
              accepts order cmds from requesting clients, registers
              dark orders and alerts with above (dark) clearing loop.
           |
            - (maybe) ``translate_and_relay_brokerd_events()``:
              accept normalized trades responses from brokerd, process and
              relay to ems client(s); this is a effectively a "trade event
              reponse" proxy-broker.

    '''
    global _router
    assert _router

    broker, _, _, _ = unpack_fqme(fqme)

    # TODO: would be nice if in tractor we can require either a ctx
    # arg, or a named arg with ctx in it and a type annotation of
    # `tractor.Context` instead of strictly requiring a ctx arg.
    ems_ctx = ctx

    # spawn one task per broker feed
    relay: TradesRelay
    feed: Feed
    client_ready: trio.Event

    # NOTE: open a stream with the brokerd backend for order flow
    # dialogue and dark clearing but only open one: we try to keep as
    # few duplicate streams as necessary per ems actor.
    async with maybe_open_trade_relays(
        _router,
        fqme,
        exec_mode,
        loglevel,
    ) as (fqme, relay, feed, client_ready):

        brokerd_stream = relay.brokerd_stream
        dark_book = _router.get_dark_book(broker)

        # signal to client that we're started and deliver
        # all known pps and accounts for this ``brokerd``.
        await ems_ctx.started((
            relay.positions,
            list(relay.accounts),
            dark_book._active,
        ))

        # establish 2-way stream with requesting order-client and
        # begin handling inbound order requests and updates
        async with ems_ctx.open_stream() as client_stream:

            # register the client side before starting the
            # brokerd-side relay task to ensure the client is
            # delivered all exisiting open orders on startup.
            # TODO: instead of by fqme we need a subscription
            # system/schema here to limit what each new client is
            # allowed to see in terms of broadcasted order flow
            # updates per dialog.
            _router.subscribers[fqme].add(client_stream)
            client_ready.set()

            # start inbound (from attached client) order request processing
            # main entrypoint, run here until cancelled.
            try:
                flume = feed.flumes[fqme]
                await process_client_order_cmds(
                    client_stream,
                    brokerd_stream,
                    fqme,
                    flume,
                    dark_book,
                    _router,
                )
            finally:
                # try to remove client from subscription registry
                _router.subscribers[fqme].remove(client_stream)

                for oid, client_streams in _router.dialogs.items():
                    client_streams.discard(client_stream)

                    # TODO: for order dialogs left "alive" in
                    # the ems this is where we should allow some
                    # system to take over management. Likely we
                    # want to allow the user to choose what kind
                    # of policy to use (eg. cancel all orders
                    # from client, run some algo, etc.)
                    if not client_streams:
                        log.warning(
                            f'Order dialog is not being monitored:\n'
                            f'{oid} ->\n{client_stream._ctx.chan.uid}'
                        )
