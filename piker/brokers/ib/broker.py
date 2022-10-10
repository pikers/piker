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
Order and trades endpoints for use with ``piker``'s EMS.

"""
from __future__ import annotations
from bisect import insort
from contextlib import ExitStack
from dataclasses import asdict
from functools import partial
from pprint import pformat
import time
from typing import (
    Any,
    Optional,
    AsyncIterator,
    Union,
)

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor
from ib_insync.contract import (
    Contract,
)
from ib_insync.order import (
    Trade,
    OrderStatus,
)
from ib_insync.objects import (
    Fill,
    Execution,
    CommissionReport,
)
from ib_insync.objects import Position as IbPosition
import pendulum

from piker import config
from piker.pp import (
    Position,
    Transaction,
    open_trade_ledger,
    open_pps,
    PpTable,
)
from piker.log import get_console_log
from piker.clearing._messages import (
    Order,
    Status,
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdStatus,
    BrokerdPosition,
    BrokerdCancel,
    BrokerdFill,
    BrokerdError,
)
from piker.data._source import Symbol
from .api import (
    _accounts2clients,
    con2fqsn,
    log,
    get_config,
    open_client_proxies,
    Client,
    MethodProxy,
)


def pack_position(
    pos: IbPosition

) -> dict[str, Any]:

    con = pos.contract
    fqsn, calc_price = con2fqsn(con)

    # TODO: options contracts into a sane format..
    return (
        con.conId,
        BrokerdPosition(
            broker='ib',
            account=pos.account,
            symbol=fqsn,
            currency=con.currency,
            size=float(pos.position),
            avg_price=float(pos.avgCost) / float(con.multiplier or 1.0),
        ),
    )


async def handle_order_requests(

    ems_order_stream: tractor.MsgStream,
    accounts_def: dict[str, str],

) -> None:

    request_msg: dict
    async for request_msg in ems_order_stream:
        log.info(f'Received order request {request_msg}')

        action = request_msg['action']
        account = request_msg['account']

        acct_number = accounts_def.get(account)
        if not acct_number:
            log.error(
                f'An IB account number for name {account} is not found?\n'
                'Make sure you have all TWS and GW instances running.'
            )
            await ems_order_stream.send(
                BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],
                    reason=f'No account found: `{account}` ?',
                )
            )
            continue

        client = _accounts2clients.get(account)
        if not client:
            log.error(
                f'An IB client for account name {account} is not found.\n'
                'Make sure you have all TWS and GW instances running.'
            )
            await ems_order_stream.send(BrokerdError(
                oid=request_msg['oid'],
                symbol=request_msg['symbol'],
                reason=f'No api client loaded for account: `{account}` ?',
            ))
            continue

        if action in {'buy', 'sell'}:
            # validate
            order = BrokerdOrder(**request_msg)

            # XXX: by default 0 tells ``ib_insync`` methods that
            # there is no existing order so ask the client to create
            # a new one (which it seems to do by allocating an int
            # counter - collision prone..)
            reqid = order.reqid
            if reqid is not None:
                reqid = int(reqid)

            # call our client api to submit the order
            reqid = client.submit_limit(
                oid=order.oid,
                symbol=order.symbol,
                price=order.price,
                action=order.action,
                size=order.size,
                account=acct_number,
                reqid=reqid,
            )
            if reqid is None:
                await ems_order_stream.send(BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],
                    reason='Order already active?',
                ))

            # deliver ack that order has been submitted to broker routing
            await ems_order_stream.send(
                BrokerdOrderAck(
                    # ems order request id
                    oid=order.oid,
                    # broker specific request id
                    reqid=reqid,
                    account=account,
                )
            )

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)
            client.submit_cancel(reqid=int(msg.reqid))

        else:
            log.error(f'Unknown order command: {request_msg}')


async def recv_trade_updates(

    client: Client,
    to_trio: trio.abc.SendChannel,

) -> None:
    """Stream a ticker using the std L1 api.
    """
    client.inline_errors(to_trio)

    # sync with trio task
    to_trio.send_nowait(None)

    def push_tradesies(
        eventkit_obj,
        obj,
        fill: Optional[Fill] = None,
        report: Optional[CommissionReport] = None,
    ):
        '''
        Push events to trio task.

        '''
        match eventkit_obj.name():

            case 'orderStatusEvent':
                item = ('status', obj)

            case 'commissionReportEvent':
                assert report
                item = ('cost', report)

            case 'execDetailsEvent':
                # execution details event
                item = ('fill', (obj, fill))

            case 'positionEvent':
                item = ('position', obj)

            case _:
                log.error(f'Error unknown event {obj}')
                return

        log.info(f'eventkit event ->\n{pformat(item)}')

        try:
            to_trio.send_nowait(item)
        except trio.BrokenResourceError:
            log.exception(f'Disconnected from {eventkit_obj} updates')
            eventkit_obj.disconnect(push_tradesies)

    # hook up to the weird eventkit object - event stream api
    for ev_name in [
        'orderStatusEvent',  # all order updates
        'execDetailsEvent',  # all "fill" updates
        'positionEvent',  # avg price updates per symbol per account

        # XXX: ugh, it is a separate event from IB and it's
        # emitted as follows:
        # self.ib.commissionReportEvent.emit(trade, fill, report)
        'commissionReportEvent',

        # XXX: not sure yet if we need these
        # 'updatePortfolioEvent',

        # XXX: these all seem to be weird ib_insync internal
        # events that we probably don't care that much about
        # given the internal design is wonky af..
        # 'newOrderEvent',
        # 'orderModifyEvent',
        # 'cancelOrderEvent',
        # 'openOrderEvent',
    ]:
        eventkit_obj = getattr(client.ib, ev_name)
        handler = partial(push_tradesies, eventkit_obj)
        eventkit_obj.connect(handler)

    # let the engine run and stream
    await client.ib.disconnectedEvent


async def update_ledger_from_api_trades(
    trade_entries: list[dict[str, Any]],
    client: Union[Client, MethodProxy],

) -> tuple[
    dict[str, Transaction],
    dict[str, dict],
]:

    # XXX; ERRGGG..
    # pack in the "primary/listing exchange" value from a
    # contract lookup since it seems this isn't available by
    # default from the `.fills()` method endpoint...
    for entry in trade_entries:
        condict = entry['contract']
        conid = condict['conId']
        pexch = condict['primaryExchange']

        if not pexch:
            cons = await client.get_con(conid=conid)
            if cons:
                con = cons[0]
                pexch = con.primaryExchange or con.exchange
            else:
                # for futes it seems like the primary is always empty?
                pexch = condict['exchange']

        entry['listingExchange'] = pexch

    conf = get_config()
    entries = api_trades_to_ledger_entries(
        conf['accounts'].inverse,
        trade_entries,
    )
    # normalize recent session's trades to the `Transaction` type
    trans_by_acct: dict[str, dict[str, Transaction]] = {}

    for acctid, trades_by_id in entries.items():
        # normalize to transaction form
        trans_by_acct[acctid] = norm_trade_records(trades_by_id)

    return trans_by_acct, entries


async def update_and_audit_msgs(
    acctid: str,  # no `ib.` prefix is required!
    pps: list[Position],
    cids2pps: dict[tuple[str, int], BrokerdPosition],
    validate: bool = False,

) -> list[BrokerdPosition]:

    msgs: list[BrokerdPosition] = []
    for p in pps:
        bsuid = p.bsuid

        # retreive equivalent ib reported position message
        # for comparison/audit versus the piker equivalent
        # breakeven pp calcs.
        ibppmsg = cids2pps.get((acctid, bsuid))

        if ibppmsg:
            msg = BrokerdPosition(
                broker='ib',

                # XXX: ok so this is annoying, we're relaying
                # an account name with the backend suffix prefixed
                # but when reading accounts from ledgers we don't
                # need it and/or it's prefixed in the section
                # table..
                account=ibppmsg.account,
                # XXX: the `.ib` is stripped..?
                symbol=ibppmsg.symbol,
                currency=ibppmsg.currency,
                size=p.size,
                avg_price=p.ppu,
            )
            msgs.append(msg)

            if validate:
                ibsize = ibppmsg.size
                pikersize = msg.size
                diff = pikersize - ibsize

                # if ib reports a lesser pp it's not as bad since we can
                # presume we're at least not more in the shit then we
                # thought.
                if diff and pikersize:
                    reverse_split_ratio = pikersize / ibsize
                    split_ratio = 1/reverse_split_ratio

                    if split_ratio >= reverse_split_ratio:
                        entry = f'split_ratio = {int(split_ratio)}'
                    else:
                        entry = f'split_ratio = 1/{int(reverse_split_ratio)}'

                    raise ValueError(
                    # log.error(
                        f'POSITION MISMATCH ib <-> piker ledger:\n'
                        f'ib: {ibppmsg}\n'
                        f'piker: {msg}\n'
                        f'reverse_split_ratio: {reverse_split_ratio}\n'
                        f'split_ratio: {split_ratio}\n\n'
                        'FIGURE OUT WHY TF YOUR LEDGER IS OFF!?!?\n\n'
                        'If you are expecting a (reverse) split in this '
                        'instrument you should probably put the following '
                        f'in the `pps.toml` section:\n{entry}'
                    )
                    msg.size = ibsize

            if ibppmsg.avg_price != msg.avg_price:

                # TODO: make this a "propoganda" log level?
                log.warning(
                    'The mega-cucks at IB want you to believe with their '
                    f'"FIFO" positioning for {msg.symbol}:\n'
                    f'"ib" mega-cucker avg price: {ibppmsg.avg_price}\n'
                    f'piker, LIFO breakeven PnL price: {msg.avg_price}'
                )

        else:
            # make brand new message
            msg = BrokerdPosition(
                broker='ib',

                # XXX: ok so this is annoying, we're relaying
                # an account name with the backend suffix prefixed
                # but when reading accounts from ledgers we don't
                # need it and/or it's prefixed in the section
                # table.. we should just strip this from the message
                # right since `.broker` is already included?
                account=f'ib.{acctid}',
                # XXX: the `.ib` is stripped..?
                symbol=p.symbol.front_fqsn(),
                # currency=ibppmsg.currency,
                size=p.size,
                avg_price=p.ppu,
            )
            if validate and p.size:
                # raise ValueError(
                log.error(
                    f'UNEXPECTED POSITION says ib:\n'
                    f'piker: {msg}\n'
                    'YOU SHOULD FIGURE OUT WHY TF YOUR LEDGER IS OFF!?\n'
                    'THEY LIQUIDATED YOU OR YOUR MISSING LEDGER RECORDS!?'
                )
            msgs.append(msg)

    return msgs


@tractor.context
async def trades_dialogue(

    ctx: tractor.Context,
    loglevel: str = None,

) -> AsyncIterator[dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    accounts_def = config.load_accounts(['ib'])

    global _client_cache

    # deliver positions to subscriber before anything else
    all_positions = []
    accounts = set()
    acctids = set()
    cids2pps: dict[str, BrokerdPosition] = {}

    # TODO: this causes a massive tractor bug when you run marketstored
    # with ``--tsdb``... you should get:
    # - first error the assertion
    # - chart should get that error and die
    # - pikerd goes to debugger again from trio nursery multi-error
    # - hitting final control-c to kill daemon will lead to hang
    # assert 0

    # TODO: just write on teardown?
    # we might also want to delegate a specific actor for
    # ledger writing / reading for speed?
    async with (
        open_client_proxies() as (proxies, aioclients),
    ):
        # Open a trade ledgers stack for appending trade records over
        # multiple accounts.
        # TODO: we probably want to generalize this into a "ledgers" api..
        ledgers: dict[str, dict] = {}
        tables: dict[str, PpTable] = {}
        order_msgs: list[Status] = []
        with (
            ExitStack() as lstack,
        ):
            # load ledgers and pps for all detected client-proxies
            for account, proxy in proxies.items():
                assert account in accounts_def
                accounts.add(account)
                acctid = account.strip('ib.')
                acctids.add(acctid)

                # open ledger and pptable wrapper for each
                # detected account.
                ledger = ledgers[acctid] = lstack.enter_context(
                    open_trade_ledger('ib', acctid)
                )
                table = tables[acctid] = lstack.enter_context(
                    open_pps('ib', acctid)
                )

            for account, proxy in proxies.items():
                client = aioclients[account]
                trades: list[Trade] = client.ib.openTrades()
                for trade in trades:
                    order = trade.order
                    quant = trade.order.totalQuantity
                    action = order.action.lower()
                    size = {
                        'sell': -1,
                        'buy': 1,
                    }[action] * quant
                    con = trade.contract

                    # TODO: in the case of the SMART venue (aka ib's
                    # router-clearing sys) we probably should handle
                    # showing such orders overtop of the fqsn for the
                    # primary exchange, how to map this easily is going
                    # to be a bit tricky though?
                    deats = await proxy.con_deats(contracts=[con])
                    fqsn = list(deats)[0]

                    reqid = order.orderId

                    # TODO: maybe embed a ``BrokerdOrder`` instead
                    # since then we can directly load it on the client
                    # side in the order mode loop?
                    msg = Status(
                        time_ns=time.time_ns(),
                        resp='open',
                        oid=str(reqid),
                        reqid=reqid,

                        # embedded order info
                        req=Order(
                            action=action,
                            exec_mode='live',
                            oid=str(reqid),
                            symbol=fqsn,
                            account=accounts_def.inverse[order.account],
                            price=order.lmtPrice,
                            size=size,
                        ),
                        src='ib',
                    )
                    order_msgs.append(msg)

                # process pp value reported from ib's system. we only use these
                # to cross-check sizing since average pricing on their end uses
                # the so called (bs) "FIFO" style which more or less results in
                # a price that's not useful for traders who want to not lose
                # money.. xb
                for pos in client.positions():

                    # collect all ib-pp reported positions so that we can be
                    # sure know which positions to update from the ledger if
                    # any are missing from the ``pps.toml``
                    bsuid, msg = pack_position(pos)

                    acctid = msg.account = accounts_def.inverse[msg.account]
                    acctid = acctid.strip('ib.')
                    cids2pps[(acctid, bsuid)] = msg
                    assert msg.account in accounts, (
                        f'Position for unknown account: {msg.account}')

                    ledger = ledgers[acctid]
                    table = tables[acctid]

                    pp = table.pps.get(bsuid)
                    if (
                        not pp
                        or pp.size != msg.size
                    ):
                        trans = norm_trade_records(ledger)
                        table.update_from_trans(trans)

                        # update trades ledgers for all accounts from connected
                        # api clients which report trades for **this session**.
                        trades = await proxy.trades()
                        (
                            trans_by_acct,
                            api_to_ledger_entries,
                        ) = await update_ledger_from_api_trades(
                            trades,
                            proxy,
                        )

                        # if new trades are detected from the API, prepare
                        # them for the ledger file and update the pptable.
                        if api_to_ledger_entries:
                            trade_entries = api_to_ledger_entries[acctid]

                            # write ledger with all new trades **AFTER**
                            # we've updated the `pps.toml` from the
                            # original ledger state! (i.e. this is
                            # currently done on exit)
                            ledger.update(trade_entries)

                            trans = trans_by_acct.get(acctid)
                            if trans:
                                table.update_from_trans(trans)

                        # XXX: not sure exactly why it wouldn't be in
                        # the updated output (maybe this is a bug?) but
                        # if you create a pos from TWS and then load it
                        # from the api trades it seems we get a key
                        # error from ``update[bsuid]`` ?
                        pp = table.pps.get(bsuid)
                        if not pp:
                            log.error(
                                f'The contract id for {msg} may have '
                                f'changed to {bsuid}\nYou may need to '
                                'adjust your ledger for this, skipping '
                                'for now.'
                            )
                            continue

                        # XXX: not sure exactly why it wouldn't be in
                        # the updated output (maybe this is a bug?) but
                        # if you create a pos from TWS and then load it
                        # from the api trades it seems we get a key
                        # error from ``update[bsuid]`` ?
                        pp = table.pps[bsuid]
                        if msg.size != pp.size:
                            log.error(
                                'Position mismatch {pp.symbol.front_fqsn()}:\n'
                                f'ib: {msg.size}\n'
                                f'piker: {pp.size}\n'
                            )

                active_pps, closed_pps = table.dump_active()

                # load all positions from `pps.toml`, cross check with
                # ib's positions data, and relay re-formatted pps as
                # msgs to the ems.
                # __2 cases__:
                # - new trades have taken place this session that we want to
                #   always reprocess indempotently,
                # - no new trades yet but we want to reload and audit any
                #   positions reported by ib's sys that may not yet be in
                #   piker's ``pps.toml`` state-file.
                for pps in [active_pps, closed_pps]:
                    msgs = await update_and_audit_msgs(
                        acctid,
                        pps.values(),
                        cids2pps,
                        validate=True,
                    )
                    all_positions.extend(msg for msg in msgs)

            if not all_positions and cids2pps:
                raise RuntimeError(
                    'Positions reported by ib but not found in `pps.toml`!?\n'
                    f'{pformat(cids2pps)}'
                )

            await ctx.started((
                all_positions,
                tuple(name for name in accounts_def if name in accounts),
            ))

            # proxy wrapper for starting trade event stream
            async def open_trade_event_stream(
                client: Client,
                task_status: TaskStatus[
                    trio.abc.ReceiveChannel
                ] = trio.TASK_STATUS_IGNORED,
            ):
                # each api client has a unique event stream
                async with tractor.to_asyncio.open_channel_from(
                    recv_trade_updates,
                    client=client,
                ) as (first, trade_event_stream):

                    task_status.started(trade_event_stream)
                    await trio.sleep_forever()

            async with (
                ctx.open_stream() as ems_stream,
                trio.open_nursery() as n,
            ):
                # relay existing open orders to ems
                for msg in order_msgs:
                    await ems_stream.send(msg)

                for client in set(aioclients.values()):
                    trade_event_stream = await n.start(
                        open_trade_event_stream,
                        client,
                    )

                    # start order request handler **before** local trades
                    # event loop
                    n.start_soon(
                        handle_order_requests,
                        ems_stream,
                        accounts_def,
                    )

                    # allocate event relay tasks for each client connection
                    n.start_soon(
                        deliver_trade_events,
                        n,
                        trade_event_stream,
                        ems_stream,
                        accounts_def,
                        cids2pps,
                        proxies,

                        ledgers,
                        tables,
                    )

                # TODO: make this thread-async!
                table.write_config()

                # block until cancelled
                await trio.sleep_forever()


async def emit_pp_update(
    ems_stream: tractor.MsgStream,
    trade_entry: dict,
    accounts_def: bidict,
    proxies: dict,
    cids2pps: dict,

    ledgers,
    tables,

) -> None:

    # compute and relay incrementally updated piker pp
    acctid = accounts_def.inverse[trade_entry['execution']['acctNumber']]
    proxy = proxies[acctid]

    acctid = acctid.strip('ib.')
    (
        records_by_acct,
        api_to_ledger_entries,
    ) = await update_ledger_from_api_trades(
        [trade_entry],
        proxy,
    )
    trans = records_by_acct[acctid]
    r = list(trans.values())[0]

    table = tables[acctid]
    table.update_from_trans(trans)
    active, closed = table.dump_active()

    # NOTE: update ledger with all new trades
    for acctid, trades_by_id in api_to_ledger_entries.items():
        ledger = ledgers[acctid]
        ledger.update(trades_by_id)

    # generate pp msgs and cross check with ib's positions data, relay
    # re-formatted pps as msgs to the ems.
    for pos in filter(
        bool,
        [active.get(r.bsuid), closed.get(r.bsuid)]
    ):
        msgs = await update_and_audit_msgs(
            acctid,
            [pos],
            cids2pps,

            # ib pp event might not have arrived yet
            validate=False,
        )
        if msgs:
            msg = msgs[0]
            log.info('Emitting pp msg: {msg}')
            break

    await ems_stream.send(msg)


_statuses: dict[str, str] = {
    'cancelled': 'canceled',
    'submitted': 'open',

    # XXX: just pass these through? it duplicates actual fill events other
    # then the case where you the `.remaining == 0` case which is our
    # 'closed'` case.
    # 'filled': 'pending',
    # 'pendingsubmit': 'pending',

    # TODO: see a current ``ib_insync`` issue around this:
    # https://github.com/erdewit/ib_insync/issues/363
    'inactive': 'pending',
}


async def deliver_trade_events(

    nurse: trio.Nursery,
    trade_event_stream: trio.MemoryReceiveChannel,
    ems_stream: tractor.MsgStream,
    accounts_def: dict[str, str],  # eg. `'ib.main'` -> `'DU999999'`
    cids2pps: dict[tuple[str, str], BrokerdPosition],
    proxies: dict[str, MethodProxy],

    ledgers,
    tables,

) -> None:
    '''
    Format and relay all trade events for a given client to emsd.

    '''
    action_map = {'BOT': 'buy', 'SLD': 'sell'}
    ids2fills: dict[str, dict] = {}

    # TODO: for some reason we can receive a ``None`` here when the
    # ib-gw goes down? Not sure exactly how that's happening looking
    # at the eventkit code above but we should probably handle it...
    async for event_name, item in trade_event_stream:
        log.info(f'ib sending {event_name}:\n{pformat(item)}')

        match event_name:
            # NOTE: we remap statuses to the ems set via the
            # ``_statuses: dict`` above.

            # https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#a17f2a02d6449710b6394d0266a353313
            # short list:
            # - PendingSubmit
            # - PendingCancel
            # - PreSubmitted (simulated orders)
            # - ApiCancelled (cancelled by client before submission
            #                 to routing)
            # - Cancelled
            # - Filled
            # - Inactive (reject or cancelled but not by trader)

            # XXX: here's some other sucky cases from the api
            # - short-sale but securities haven't been located, in this
            #   case we should probably keep the order in some kind of
            #   weird state or cancel it outright?

            # status='PendingSubmit', message=''),
            # status='Cancelled', message='Error 404,
            #   reqId 1550: Order held while securities are located.'),
            # status='PreSubmitted', message='')],

            case 'status':

                # XXX: begin normalization of nonsense ib_insync internal
                # object-state tracking representations...

                # unwrap needed data from ib_insync internal types
                trade: Trade = item
                status: OrderStatus = trade.orderStatus
                ib_status_key = status.status.lower()

                # TODO: try out cancelling inactive orders after delay:
                # https://github.com/erdewit/ib_insync/issues/363
                # acctid = accounts_def.inverse[trade.order.account]

                # # double check there is no error when
                # # cancelling.. gawwwd
                # if ib_status_key == 'cancelled':
                #     last_log = trade.log[-1]
                #     if (
                #         last_log.message
                #         and 'Error' not in last_log.message
                #     ):
                #         ib_status_key = trade.log[-2].status

                # elif ib_status_key == 'inactive':

                #     async def sched_cancel():
                #         log.warning(
                #             'OH GAWD an inactive order.scheduling a cancel\n'
                #             f'{pformat(item)}'
                #         )
                #         proxy = proxies[acctid]
                #         await proxy.submit_cancel(reqid=trade.order.orderId)
                #         await trio.sleep(1)
                #         nurse.start_soon(sched_cancel)

                #     nurse.start_soon(sched_cancel)

                status_key = (
                    _statuses.get(ib_status_key.lower())
                    or ib_status_key.lower()
                )

                remaining = status.remaining
                if (
                    status_key == 'filled'
                ):
                    fill: Fill = trade.fills[-1]
                    execu: Execution = fill.execution
                    # execdict = asdict(execu)
                    # execdict.pop('acctNumber')

                    fill_msg = BrokerdFill(
                        # should match the value returned from
                        # `.submit_limit()`
                        reqid=execu.orderId,
                        time_ns=time.time_ns(),  # cuz why not
                        action=action_map[execu.side],
                        size=execu.shares,
                        price=execu.price,
                        # broker_details=execdict,
                        # XXX: required by order mode currently
                        broker_time=execu.time,
                    )
                    await ems_stream.send(fill_msg)

                    if remaining == 0:
                        # emit a closed status on filled statuses where
                        # all units were cleared.
                        status_key = 'closed'

                # skip duplicate filled updates - we get the deats
                # from the execution details event
                msg = BrokerdStatus(
                    reqid=trade.order.orderId,
                    time_ns=time.time_ns(),  # cuz why not
                    account=accounts_def.inverse[trade.order.account],

                    # everyone doin camel case..
                    status=status_key,  # force lower case

                    filled=status.filled,
                    reason=status.whyHeld,

                    # this seems to not be necessarily up to date in the
                    # execDetails event.. so we have to send it here I guess?
                    remaining=remaining,

                    broker_details={'name': 'ib'},
                )
                await ems_stream.send(msg)
                continue

            case 'fill':

                # for wtv reason this is a separate event type
                # from IB, not sure why it's needed other then for extra
                # complexity and over-engineering :eyeroll:.
                # we may just end up dropping these events (or
                # translating them to ``Status`` msgs) if we can
                # show the equivalent status events are no more latent.

                # unpack ib_insync types
                # pep-0526 style:
                # https://www.python.org/dev/peps/pep-0526/#global-and-local-variable-annotations
                trade: Trade
                fill: Fill
                trade, fill = item
                execu: Execution = fill.execution
                execid = execu.execId

                # TODO:
                # - normalize out commissions details?
                # - this is the same as the unpacking loop above in
                # ``trades_to_ledger_entries()`` no?
                trade_entry = ids2fills.setdefault(execid, {})
                cost_already_rx = bool(trade_entry)

                # if the costs report was already received this
                # should be not empty right?
                comms = fill.commissionReport.commission
                if cost_already_rx:
                    assert comms

                trade_entry.update(
                    {
                        'contract': asdict(fill.contract),
                        'execution': asdict(fill.execution),
                        # 'commissionReport': asdict(fill.commissionReport),
                        # supposedly server fill time?
                        'broker_time': execu.time,
                        'name': 'ib',
                    }
                )

                # 2 cases:
                # - fill comes first or
                # - comms report comes first
                comms = fill.commissionReport.commission
                if comms:
                    # UGHHH since the commision report object might be
                    # filled in **after** we already serialized to dict..
                    # def need something better for all this.
                    trade_entry.update(
                        {'commissionReport': asdict(fill.commissionReport)}
                    )

                if comms or cost_already_rx:
                    # only send a pp update once we have a cost report
                    await emit_pp_update(
                        ems_stream,
                        trade_entry,
                        accounts_def,
                        proxies,
                        cids2pps,
                        ledgers,
                        tables,
                    )

            case 'cost':

                cr: CommissionReport = item
                execid = cr.execId

                trade_entry = ids2fills.setdefault(execid, {})
                fill_already_rx = bool(trade_entry)

                # only fire a pp msg update if,
                # - we haven't already
                # - the fill event has already arrived
                #   but it didn't yet have a commision report
                #   which we fill in now.
                if (
                    fill_already_rx
                    and 'commissionReport' not in trade_entry
                ):
                    # no fill msg has arrived yet so just fill out the
                    # cost report for now and when the fill arrives a pp
                    # msg can be emitted.
                    trade_entry.update(
                        {'commissionReport': asdict(cr)}
                    )

                    await emit_pp_update(
                        ems_stream,
                        trade_entry,
                        accounts_def,
                        proxies,
                        cids2pps,
                        ledgers,
                        tables,
                    )

            case 'error':
                err: dict = item

                # f$#$% gawd dammit insync..
                con = err['contract']
                if isinstance(con, Contract):
                    err['contract'] = asdict(con)

                if err['reqid'] == -1:
                    log.error(f'TWS external order error:\n{pformat(err)}')

                # TODO: we don't want to relay data feed / lookup errors
                # so we need some further filtering logic here..
                # for most cases the 'status' block above should take
                # care of this.
                # await ems_stream.send(BrokerdStatus(
                #     status='error',
                #     reqid=err['reqid'],
                #     reason=err['reason'],
                #     time_ns=time.time_ns(),
                #     account=accounts_def.inverse[trade.order.account],
                #     broker_details={'name': 'ib'},
                # ))

            case 'position':

                cid, msg = pack_position(item)
                log.info(f'New IB position msg: {msg}')
                # cuck ib and it's shitty fifo sys for pps!
                continue

            case 'event':

                # it's either a general system status event or an external
                # trade event?
                log.info(f"TWS system status: \n{pformat(item)}")

                # TODO: support this again but needs parsing at the callback
                # level...
                # reqid = item.get('reqid', 0)
                # if getattr(msg, 'reqid', 0) < -1:
                # log.info(f"TWS triggered trade\n{pformat(msg)}")

                # msg.reqid = 'tws-' + str(-1 * reqid)

                # mark msg as from "external system"
                # TODO: probably something better then this.. and start
                # considering multiplayer/group trades tracking
                # msg.broker_details['external_src'] = 'tws'

            case _:
                log.error(f'WTF: {event_name}: {item}')


def norm_trade_records(
    ledger: dict[str, Any],

) -> list[Transaction]:
    '''
    Normalize a flex report or API retrieved executions
    ledger into our standard record format.

    '''
    records: list[Transaction] = []
    for tid, record in ledger.items():

        conid = record.get('conId') or record['conid']
        comms = record.get('commission')
        if comms is None:
            comms = -1*record['ibCommission']
        price = record.get('price') or record['tradePrice']

        # the api doesn't do the -/+ on the quantity for you but flex
        # records do.. are you fucking serious ib...!?
        size = record.get('quantity') or record['shares'] * {
            'BOT': 1,
            'SLD': -1,
        }[record['side']]

        exch = record['exchange']
        lexch = record.get('listingExchange')

        suffix = lexch or exch
        symbol = record['symbol']

        # likely an opts contract record from a flex report..
        # TODO: no idea how to parse ^ the strike part from flex..
        # (00010000 any, or 00007500 tsla, ..)
        # we probably must do the contract lookup for this?
        if '   ' in symbol or '--' in exch:
            underlying, _, tail = symbol.partition('   ')
            suffix = exch = 'opt'
            expiry = tail[:6]
            # otype = tail[6]
            # strike = tail[7:]

            print(f'skipping opts contract {symbol}')
            continue

        # timestamping is way different in API records
        dtstr = record.get('datetime')
        date = record.get('date')
        flex_dtstr = record.get('dateTime')

        if dtstr or date:
            dt = pendulum.parse(dtstr or date)

        elif flex_dtstr:
            # probably a flex record with a wonky non-std timestamp..
            dt = parse_flex_dt(record['dateTime'])

        # special handling of symbol extraction from
        # flex records using some ad-hoc schema parsing.
        instr = record.get('assetCategory')
        if instr == 'FUT':
            symbol = record['description'][:3]

        # try to build out piker fqsn from record.
        expiry = record.get(
            'lastTradeDateOrContractMonth') or record.get('expiry')
        if expiry:
            expiry = str(expiry).strip(' ')
            suffix = f'{exch}.{expiry}'
            expiry = pendulum.parse(expiry)

        fqsn = Symbol.from_fqsn(
            fqsn=f'{symbol}.{suffix}.ib',
            info={},
        ).front_fqsn().rstrip('.ib')

        # NOTE: for flex records the normal fields for defining an fqsn
        # sometimes won't be available so we rely on two approaches for
        # the "reverse lookup" of piker style fqsn keys:
        # - when dealing with API trade records received from
        #   `IB.trades()` we do a contract lookup at he time of processing
        # - when dealing with flex records, it is assumed the record
        #   is at least a day old and thus the TWS position reporting system
        #   should already have entries if the pps are still open, in
        #   which case, we can pull the fqsn from that table (see
        #   `trades_dialogue()` above).
        insort(
            records,
            Transaction(
                fqsn=fqsn,
                tid=tid,
                size=size,
                price=price,
                cost=comms,
                dt=dt,
                expiry=expiry,
                bsuid=conid,
            ),
            key=lambda t: t.dt
        )

    return {r.tid: r for r in records}


def parse_flex_dt(
    record: str,
) -> pendulum.datetime:
    date, ts = record.split(';')
    dt = pendulum.parse(date)
    ts = f'{ts[:2]}:{ts[2:4]}:{ts[4:]}'
    tsdt = pendulum.parse(ts)
    return dt.set(hour=tsdt.hour, minute=tsdt.minute, second=tsdt.second)


def api_trades_to_ledger_entries(
    accounts: bidict,
    trade_entries: list[object],

) -> dict:
    '''
    Convert API execution objects entry objects into ``dict`` form,
    pretty much straight up without modification except add
    a `pydatetime` field from the parsed timestamp.

    '''
    trades_by_account = {}
    for t in trade_entries:
        # NOTE: example of schema we pull from the API client.
        # {
        #     'commissionReport': CommissionReport(...
        #     'contract': {...
        #     'execution': Execution(...
        #     'time': 1654801166.0
        # }

        # flatten all sub-dicts and values into one top level entry.
        entry = {}
        for section, val in t.items():
            match section:
                case 'contract' | 'execution' | 'commissionReport':
                    # sub-dict cases
                    entry.update(val)

                case 'time':
                    # ib has wack ns timestamps, or is that us?
                    continue

                case _:
                    entry[section] = val

        tid = str(entry['execId'])
        dt = pendulum.from_timestamp(entry['time'])
        # TODO: why isn't this showing seconds in the str?
        entry['pydatetime'] = dt
        entry['datetime'] = str(dt)
        acctid = accounts[entry['acctNumber']]

        if not tid:
            # this is likely some kind of internal adjustment
            # transaction, likely one of the following:
            # - an expiry event that will show a "book trade" indicating
            #   some adjustment to cash balances: zeroing or itm settle.
            # - a manual cash balance position adjustment likely done by
            #   the user from the accounts window in TWS where they can
            #   manually set the avg price and size:
            #   https://api.ibkr.com/lib/cstools/faq/web1/index.html#/tag/DTWS_ADJ_AVG_COST
            log.warning(f'Skipping ID-less ledger entry:\n{pformat(entry)}')
            continue

        trades_by_account.setdefault(
            acctid, {}
        )[tid] = entry

    # sort entries in output by python based datetime
    for acctid in trades_by_account:
        trades_by_account[acctid] = dict(sorted(
            trades_by_account[acctid].items(),
            key=lambda entry: entry[1].pop('pydatetime'),
        ))

    return trades_by_account


def flex_records_to_ledger_entries(
    accounts: bidict,
    trade_entries: list[object],

) -> dict:
    '''
    Convert flex report entry objects into ``dict`` form, pretty much
    straight up without modification except add a `pydatetime` field
    from the parsed timestamp.

    '''
    trades_by_account = {}
    for t in trade_entries:
        entry = t.__dict__

        # XXX: LOL apparently ``toml`` has a bug
        # where a section key error will show up in the write
        # if you leave a table key as an `int`? So i guess
        # cast to strs for all keys..

        # oddly for some so-called "BookTrade" entries
        # this field seems to be blank, no cuckin clue.
        # trade['ibExecID']
        tid = str(entry.get('ibExecID') or entry['tradeID'])
        # date = str(entry['tradeDate'])

        # XXX: is it going to cause problems if a account name
        # get's lost? The user should be able to find it based
        # on the actual exec history right?
        acctid = accounts[str(entry['accountId'])]

        # probably a flex record with a wonky non-std timestamp..
        dt = entry['pydatetime'] = parse_flex_dt(entry['dateTime'])
        entry['datetime'] = str(dt)

        if not tid:
            # this is likely some kind of internal adjustment
            # transaction, likely one of the following:
            # - an expiry event that will show a "book trade" indicating
            #   some adjustment to cash balances: zeroing or itm settle.
            # - a manual cash balance position adjustment likely done by
            #   the user from the accounts window in TWS where they can
            #   manually set the avg price and size:
            #   https://api.ibkr.com/lib/cstools/faq/web1/index.html#/tag/DTWS_ADJ_AVG_COST
            log.warning(f'Skipping ID-less ledger entry:\n{pformat(entry)}')
            continue

        trades_by_account.setdefault(
            acctid, {}
        )[tid] = entry

    for acctid in trades_by_account:
        trades_by_account[acctid] = dict(sorted(
            trades_by_account[acctid].items(),
            key=lambda entry: entry[1]['pydatetime'],
        ))

    return trades_by_account


def load_flex_trades(
    path: Optional[str] = None,

) -> dict[str, Any]:

    from ib_insync import flexreport, util

    conf = get_config()

    if not path:
        # load ``brokers.toml`` and try to get the flex
        # token and query id that must be previously defined
        # by the user.
        token = conf.get('flex_token')
        if not token:
            raise ValueError(
                'You must specify a ``flex_token`` field in your'
                '`brokers.toml` in order load your trade log, see our'
                'intructions for how to set this up here:\n'
                'PUT LINK HERE!'
            )

        qid = conf['flex_trades_query_id']

        # TODO: hack this into our logging
        # system like we do with the API client..
        util.logToConsole()

        # TODO: rewrite the query part of this with async..httpx?
        report = flexreport.FlexReport(
            token=token,
            queryId=qid,
        )

    else:
        # XXX: another project we could potentially look at,
        # https://pypi.org/project/ibflex/
        report = flexreport.FlexReport(path=path)

    trade_entries = report.extract('Trade')
    ln = len(trade_entries)
    log.info(f'Loaded {ln} trades from flex query')

    trades_by_account = flex_records_to_ledger_entries(
        conf['accounts'].inverse,  # reverse map to user account names
        trade_entries,
    )

    ledger_dict: Optional[dict] = None

    for acctid in trades_by_account:
        trades_by_id = trades_by_account[acctid]

        with open_trade_ledger('ib', acctid) as ledger_dict:
            tid_delta = set(trades_by_id) - set(ledger_dict)
            log.info(
                'New trades detected\n'
                f'{pformat(tid_delta)}'
            )
            if tid_delta:
                sorted_delta = dict(sorted(
                    {tid: trades_by_id[tid] for tid in tid_delta}.items(),
                    key=lambda entry: entry[1].pop('pydatetime'),
                ))
                ledger_dict.update(sorted_delta)

    return ledger_dict


if __name__ == '__main__':
    import sys
    import os

    args = sys.argv
    if len(args) > 1:
        args = args[1:]
        for arg in args:
            path = os.path.abspath(arg)
            load_flex_trades(path=path)
    else:
        # expect brokers.toml to have an entry and
        # pull from the web service.
        load_flex_trades()
