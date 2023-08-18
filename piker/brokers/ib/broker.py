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
from contextlib import ExitStack
from collections import ChainMap
from functools import partial
from pprint import pformat
import time
from typing import (
    Any,
    AsyncIterator,
)

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor
from tractor.to_asyncio import LinkedTaskChannel
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
    Position as IbPosition,
)

from piker import config
from piker.types import Struct
from piker.accounting import (
    Position,
    Transaction,
    open_trade_ledger,
    TransactionLedger,
    open_account,
    Account,
    Asset,
    MktPair,
)
from piker.data import (
    open_symcache,
    SymbologyCache,
)
from piker.clearing import OrderDialogs
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
from ._util import log
from .api import (
    _accounts2clients,
    get_config,
    open_client_proxies,
    Client,
    MethodProxy,
)
from .symbols import (
    con2fqme,
    # get_mkt_info,
)
from .ledger import (
    norm_trade_records,
    tx_sort,
    update_ledger_from_api_trades,
)


def pack_position(
    pos: IbPosition,
    accounts_def: bidict[str, str],

) -> tuple[
    str,
    dict[str, Any]
]:

    con: Contract = pos.contract
    fqme, calc_price = con2fqme(con)

    # TODO: options contracts into a sane format..
    return (
        str(con.conId),
        BrokerdPosition(
            broker='ib',
            account=accounts_def.inverse[pos.account],
            symbol=fqme,
            currency=con.currency,
            size=float(pos.position),
            avg_price=float(pos.avgCost) / float(con.multiplier or 1.0),
        ),
    )


async def handle_order_requests(

    ems_order_stream: tractor.MsgStream,
    accounts_def: dict[str, str],
    flows: OrderDialogs,

) -> None:

    request_msg: dict
    async for request_msg in ems_order_stream:
        log.info(f'Received order request {request_msg}')

        action: str = request_msg['action']
        account: str = request_msg['account']
        acct_number = accounts_def.get(account)
        oid: str = request_msg['oid']

        if not acct_number:
            log.error(
                f'An IB account number for name {account} is not found?\n'
                'Make sure you have all TWS and GW instances running.'
            )
            err_msg = BrokerdError(
                oid=oid,
                symbol=request_msg['symbol'],
                reason=f'No account found: `{account}` ?',
            )
            await ems_order_stream.send(err_msg)
            continue

        client = _accounts2clients.get(account)
        if not client:
            log.error(
                f'An IB client for account name {account} is not found.\n'
                'Make sure you have all TWS and GW instances running.'
            )
            err_msg = BrokerdError(
                oid=oid,
                symbol=request_msg['symbol'],
                reason=f'No api client loaded for account: `{account}` ?',
            )
            await ems_order_stream.send(err_msg)
            continue

        if action in {'buy', 'sell'}:
            # validate
            order = BrokerdOrder(**request_msg)

            # XXX: by default 0 tells ``ib_insync`` methods that
            # there is no existing order so ask the client to create
            # a new one (which it seems to do by allocating an int
            # counter - collision prone..)
            reqid: int | None = order.reqid
            if reqid is not None:
                log.error(f'TYPE .reqid: {reqid} -> {type(reqid)}')
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
            str_reqid: str = str(reqid)
            if reqid is None:
                err_msg = BrokerdError(
                    oid=oid,
                    symbol=request_msg['symbol'],
                    reason='Order already active?',
                )
                await ems_order_stream.send(err_msg)

            # deliver ack that order has been submitted to broker routing
            ack = BrokerdOrderAck(
                # ems order request id
                oid=order.oid,
                # broker specific request id
                reqid=str_reqid,
                account=account,
            )
            await ems_order_stream.send(ack)
            flows.add_msg(str_reqid, order.to_dict())
            flows.add_msg(str_reqid, ack.to_dict())

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)
            client.submit_cancel(reqid=int(msg.reqid))

        else:
            log.error(f'Unknown order command: {request_msg}')


async def recv_trade_updates(

    client: Client,
    to_trio: trio.abc.SendChannel,

) -> None:
    '''
    Receive and relay order control and positioning related events
    from `ib_insync`, pack as tuples and push over mem-chan to our
    trio relay task for processing and relay to EMS.

    '''
    client.inline_errors(to_trio)

    # sync with trio task
    to_trio.send_nowait(client.ib)

    def push_tradesies(
        eventkit_obj,
        obj,
        fill: Fill | None = None,
        report: CommissionReport | None = None,
    ):
        '''
        Push events to trio task.

        '''
        emit: tuple | object
        event_name: str = eventkit_obj.name()
        match event_name:

            case 'orderStatusEvent':
                emit: Trade = obj

            case 'commissionReportEvent':
                assert report
                emit: CommissionReport = report

            case 'execDetailsEvent':
                # execution details event
                emit: tuple[Trade, Fill] = (obj, fill)

            case 'positionEvent':
                emit: Position = obj

            case _:
                log.error(f'Error unknown event {obj}')
                return

        log.info(f'eventkit event ->\n{pformat(emit)}')

        try:
            # emit event name + relevant ibis internal objects
            to_trio.send_nowait((event_name, emit))
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
        # -> prolly not since the named tuple type doesn't offer
        # much more then a few more pnl fields..
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


async def update_and_audit_pos_msg(
    acctid: str,  # no `ib.` prefix is required!
    pikerpos: Position,
    ibpos: IbPosition,
    cons2mkts: dict[Contract, MktPair],
    validate: bool = True,

) -> BrokerdPosition:

    # NOTE: lookup the ideal `MktPair` value, since multi-venue
    # trade records can result in multiple MktpPairs (eg. qqq.arca.ib and
    # qqq.nasdaq.ib can map to the same bs_mktid but have obviously
    # different .fqme: str values..), instead allow caller to
    # provide a table with the desired output mkt-map values;
    # eventually this should probably come from a deterministically
    # generated symcache..
    # TODO: figure out how make this not be so frickin CRAP by
    # either allowing bs_mktid to be the position key or possibly
    # be extra pendantic with the `Client._mkts` table?
    con: Contract = ibpos.contract
    mkt: MktPair = cons2mkts.get(con, pikerpos.mkt)
    bs_fqme: str = mkt.bs_fqme

    msg = BrokerdPosition(
        broker='ib',

        # TODO: probably forget about this once we drop this msg
        # entirely from our set..
        # XXX: ok so this is annoying, we're relaying
        # an account name with the backend suffix prefixed
        # but when reading accounts from ledgers we don't
        # need it and/or it's prefixed in the section
        # table.. we should just strip this from the message
        # right since `.broker` is already included?
        account=f'ib.{acctid}',
        # account=account_def.inverse[ibpos.account],

        # XXX: the `.ib` is stripped..?
        symbol=bs_fqme,

        # remove..
        # currency=ibpos.currency,

        # NOTE: always take their size since it's usually the
        # true gospel.. this SHOULD be the same always as ours
        # tho..
        # size=pikerpos.size,
        size=ibpos.position,

        avg_price=pikerpos.ppu,
    )

    ibfmtmsg: str = pformat(ibpos._asdict())
    pikerfmtmsg: str = pformat(msg.to_dict())

    ibsize: float = ibpos.position
    pikersize: float = msg.size
    diff: float = pikersize - ibsize

    # NOTE: compare equivalent ib reported position message for
    # comparison/audit versus the piker equivalent breakeven pp
    # calcs. if ib reports a lesser pp it's not as bad since we can
    # presume we're at least not more in the shit then we thought.
    if (
        diff
        and (
            pikersize
            or ibsize
        )
    ):

        # reverse_split_ratio = pikersize / ibsize
        # split_ratio = 1/reverse_split_ratio
        # if split_ratio >= reverse_split_ratio:
        #     entry = f'split_ratio = {int(split_ratio)}'
        # else:
        #     entry = f'split_ratio = 1/{int(reverse_split_ratio)}'

        msg.size = ibsize
        logmsg: str = (
            f'Pos mismatch in ib vs. the piker ledger!\n'
            f'IB:\n{ibfmtmsg}\n\n'
            f'PIKER:\n{pikerfmtmsg}\n\n'

            # 'If you are expecting a (reverse) split in this '
            # 'instrument you should probably put the following'
            # 'in the `pps.toml` section:\n'
            # f'{entry}\n'
            # f'reverse_split_ratio: {reverse_split_ratio}\n'
            # f'split_ratio: {split_ratio}\n\n'
        )

        if validate:
            raise ValueError(logmsg)
        else:
            # await tractor.pause()
            log.error(logmsg)

    # TODO: make this a "propaganda" log level?
    if ibpos.avgCost != msg.avg_price:
        log.warning(
            f'IB "FIFO" avg price for {msg.symbol} is DIFF:\n'
            f'ib: {ibfmtmsg}\n'
            '---------------------------\n'
            f'piker: {pformat(msg.to_dict())}'
        )

    return msg


async def aggr_open_orders(
    order_msgs: list[Status],
    client: Client,
    proxy: MethodProxy,
    accounts_def: bidict[str, str],

) -> None:
    '''
    Collect all open orders from client and fill in `order_msgs: list`.

    '''
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
        # showing such orders overtop of the fqme for the
        # primary exchange, how to map this easily is going
        # to be a bit tricky though?
        deats = await proxy.con_deats(contracts=[con])
        fqme = list(deats)[0]

        reqid: str = str(order.orderId)

        # TODO: maybe embed a ``BrokerdOrder`` instead
        # since then we can directly load it on the client
        # side in the order mode loop?
        msg = Status(
            time_ns=time.time_ns(),
            resp='open',
            oid=reqid,
            reqid=reqid,

            # embedded order info
            req=Order(
                action=action,
                exec_mode='live',
                oid=str(reqid),
                symbol=fqme,
                account=accounts_def.inverse[order.account],
                price=order.lmtPrice,
                size=size,
            ),
            src='ib',
        )
        order_msgs.append(msg)

    return order_msgs


async def open_trade_event_stream(
    client: Client,
    task_status: TaskStatus[
        LinkedTaskChannel
    ] = trio.TASK_STATUS_IGNORED,
):
    '''
    Proxy wrapper for starting trade event stream from ib_insync
    which spawns an asyncio task that registers an internal closure
    (`push_tradies()`) which in turn relays trading events through
    a `tractor.to_asyncio.LinkedTaskChannel` which the parent
    (caller task) can use to process said events in trio-land.

    NOTE: each api client has a unique event stream.

    '''
    trade_event_stream: LinkedTaskChannel

    async with tractor.to_asyncio.open_channel_from(
        recv_trade_updates,
        client=client,
    ) as (
        _,  # first pushed val
        trade_event_stream,
    ):
        task_status.started(trade_event_stream)
        # block forever to keep session trio-asyncio session
        # up until cancelled or error on either side.
        await trio.sleep_forever()


class IbAcnt(Struct):
    '''
    Wrapper around the useful info for doing accounting (mostly for
    position tracking).

    '''
    key: str
    balances: dict[
        str,  # fiat or crypto name
        float  # current balance
    ]
    # TODO: do we need the asset instances?
        # (float, Asset),
    # ]
    positions: dict[str, IbPosition]


@tractor.context
async def open_trade_dialog(
    ctx: tractor.Context,

) -> AsyncIterator[dict[str, Any]]:

    # task local msg dialog tracking
    flows = OrderDialogs()
    accounts_def = config.load_accounts(['ib'])

    # deliver positions to subscriber before anything else
    all_positions: list[BrokerdPosition] = []
    accounts: set[str] = set()
    acctids: set[str] = set()

    symcache: SymbologyCache
    async with (
        open_client_proxies() as (
            proxies,
            aioclients,
        ),

        # TODO: do this as part of `open_account()`!?
        open_symcache('ib', only_from_memcache=True) as symcache,
    ):
        # Open a trade ledgers stack for appending trade records over
        # multiple accounts.
        # TODO: we probably want to generalize this into a "ledgers" api..
        ledgers: dict[str, TransactionLedger] = {}
        tables: dict[str, Account] = {}
        order_msgs: list[Status] = []
        conf = get_config()
        accounts_def_inv: bidict[str, str] = bidict(conf['accounts']).inverse

        with (
            ExitStack() as lstack,
        ):
            # load ledgers and pps for all detected client-proxies
            account: str
            proxy: MethodProxy
            for account, proxy in proxies.items():
                assert account in accounts_def
                accounts.add(account)
                acctid: str = account.strip('ib.')
                acctids.add(acctid)

                # open ledger and pptable wrapper for each
                # detected account.
                ledger: TransactionLedger
                ledger = ledgers[acctid] = lstack.enter_context(
                    open_trade_ledger(
                        'ib',
                        acctid,
                        tx_sort=tx_sort,
                        symcache=symcache,
                    )
                )

                # load all positions from `pps.toml`, cross check with
                # ib's positions data, and relay re-formatted pps as
                # msgs to the ems.
                # __2 cases__:
                # - new trades have taken place this session that we want to
                #   always reprocess indempotently,
                # - no new trades yet but we want to reload and audit any
                #   positions reported by ib's sys that may not yet be in
                #   piker's ``pps.toml`` state-file.
                tables[acctid] = lstack.enter_context(
                    open_account(
                        'ib',
                        acctid,
                        write_on_exit=True,
                    )
                )

            for account, proxy in proxies.items():
                client: Client = aioclients[account]

                # process pp value reported from ib's system. we only
                # use these to cross-check sizing since average pricing
                # on their end uses the so called (bs) "FIFO" style
                # which more or less results in a price that's not
                # useful for traders who want to not lose money.. xb
                # -> collect all ib-pp reported positions so that we can be
                # sure know which positions to update from the ledger if
                # any are missing from the ``pps.toml``
                # await tractor.pp()
                ib_positions: dict[str, IbPosition] = {}
                pos: IbPosition  # named tuple subtype
                for pos in client.positions():
                    bs_mktid: str = str(pos.contract.conId)
                    ib_positions[bs_mktid] = pos

                    bs_mktid, msg = pack_position(pos, accounts_def)
                    acctid: str = msg.account.strip('ib.')

                    assert msg.account in accounts, (
                        f'Position for unknown account: {msg.account}')

                balances: dict[str, tuple[float, Asset]] = {}
                for av in client.ib.accountValues():
                    match av.tag:
                        case 'CashBalance':
                            balances[av.currency] = float(av.value)

                        # TODO: if we want supposed forex pnls?
                        # case 'UnrealizedPnL':
                        #     ...

                ibacnt = IbAcnt(
                    key=acctid,
                    balances=balances,
                    positions=ib_positions,
                )
                # print(
                #     f'Current balances for {ibacnt.key}: {ibacnt.balances}'
                # )

                # order_msgs is filled in by this helper
                await aggr_open_orders(
                    order_msgs,
                    client,
                    proxy,
                    accounts_def,
                )
                acctid: str = account.strip('ib.')
                ledger: dict = ledgers[acctid]
                acnt: Account = tables[acctid]

                # update position table with latest ledger from all
                # gathered transactions: ledger file + api records.
                trans: dict[str, Transaction] = norm_trade_records(
                    ledger,
                    symcache=symcache,
                )

                # update trades ledgers for all accounts from connected
                # api clients which report trades for **this session**.
                api_fills: list[Fill] = await proxy.get_fills()
                if api_fills:
                    api_trans_by_acct: dict[str, Transaction]
                    api_to_ledger_entries: dict[str, dict]
                    (
                        api_trans_by_acct,
                        api_to_ledger_entries,
                    ) = await update_ledger_from_api_trades(
                        api_fills,
                        proxy,
                        accounts_def_inv,
                        symcache=symcache,
                    )

                    # if new api_fills are detected from the API, prepare
                    # them for the ledger file and update the pptable.
                    if (
                        api_to_ledger_entries
                        and (trade_entries := api_to_ledger_entries.get(acctid))
                    ):
                        # TODO: fix this `tractor` BUG!
                        # https://github.com/goodboy/tractor/issues/354
                        # await tractor.pp()

                        # write ledger with all new api_fills
                        # **AFTER** we've updated the `pps.toml`
                        # from the original ledger state! (i.e. this
                        # is currently done on exit)
                        for tid, entry in trade_entries.items():
                            ledger.setdefault(tid, {}).update(entry)

                        if api_trans := api_trans_by_acct.get(acctid):
                            trans.update(api_trans)

                # update account (and thus pps) from all gathered transactions
                acnt.update_from_ledger(
                    trans,
                    symcache=ledger.symcache,
                )

            # iterate all (newly) updated pps tables for every
            # client-account and build out position msgs to deliver to
            # EMS.
            for acctid, acnt in tables.items():
                active_pps, closed_pps = acnt.dump_active()

                for pps in [active_pps, closed_pps]:
                    piker_pps: list[Position] = list(pps.values())
                    for pikerpos in piker_pps:
                        # TODO: map from both the contract ID
                        # (bs_mktid) AND the piker-ified FQME ??
                        # - they might change the fqme when bbby get's
                        #   downlisted to pink XD
                        # - the bs_mktid can randomly change like in
                        #   gnln.nasdaq..
                        ibpos: IbPosition | None = ibacnt.positions.get(
                            pikerpos.bs_mktid
                        )
                        if ibpos:
                            bs_mktid: str = str(ibpos.contract.conId)
                            msg = await update_and_audit_pos_msg(
                                acctid,
                                pikerpos,
                                ibpos,
                                cons2mkts=client._cons2mkts,
                                validate=False,
                            )
                            if msg and msg.size != 0:
                                all_positions.append(msg)
                        elif (
                            not ibpos
                            and pikerpos.cumsize
                        ):
                            logmsg: str = (
                                f'UNEXPECTED POSITION says IB => {msg.symbol}\n'
                                'Maybe they LIQUIDATED YOU or your ledger is wrong?\n'
                            )
                            log.error(logmsg)

            await ctx.started((
                all_positions,
                tuple(name for name in accounts_def if name in accounts),
            ))

            async with (
                ctx.open_stream() as ems_stream,
                trio.open_nursery() as n,
            ):
                # relay existing open orders to ems
                for msg in order_msgs:
                    await ems_stream.send(msg)

                for client in set(aioclients.values()):
                    trade_event_stream: LinkedTaskChannel = await n.start(
                        open_trade_event_stream,
                        client,
                    )

                    # start order request handler **before** local trades
                    # event loop
                    n.start_soon(
                        handle_order_requests,
                        ems_stream,
                        accounts_def,
                        flows,
                    )

                    # allocate event relay tasks for each client connection
                    n.start_soon(
                        deliver_trade_events,

                        trade_event_stream,
                        ems_stream,
                        accounts_def,
                        proxies,
                        ledgers,
                        tables,
                        flows,
                    )

                # write account and ledger files immediately!
                # TODO: make this thread-async!
                for acctid, acnt in tables.items():
                    acnt.write_config()
                    ledgers[acctid].write_config()

                # block until cancelled
                await trio.sleep_forever()


async def emit_pp_update(
    ems_stream: tractor.MsgStream,

    accounts_def: bidict[str, str],
    proxies: dict,

    ledgers: dict[str, dict[str, Any]],
    acnts: dict[str, Account],

    ibpos: IbPosition,  # required!

    # NEED it before we actually update the trade ledger
    fill: Fill | None = None,

) -> None:
    '''
    Emit a position update to the EMS either directly from
    a `IbPosition` update (received from the API) or ideally from
    a `piker.accounting.Position` update (once it's entirely bug
    free xD) by extracting the trade record from the (optionally
    provided) `Fill` event, convert it into a `Transaction`, update
    the backing ledger and emit a msg for the account's `Position`
    entry.

    '''
    accounts_def_inv: bidict[str, str] = accounts_def.inverse
    accnum: str = ibpos.account
    fq_acctid: str = accounts_def_inv[accnum]
    proxy: MethodProxy = proxies[fq_acctid]
    client: Client = proxy._aio_ns

    # XXX FILL CASE:
    # compute and relay incrementally updated piker pos
    # after doing accounting calcs
    if fill:
        (
            records_by_acct,
            api_to_ledger_entries,
        ) = await update_ledger_from_api_trades(
            [fill],
            proxy,
            accounts_def_inv,
        )
        trans: dict[str, Transaction] = records_by_acct[fq_acctid]
        tx: Transaction = list(trans.values())[0]

        acctid: str = fq_acctid.strip('ib.')
        acnt: Account = acnts[acctid]
        ledger: TransactionLedger = ledgers[acctid]

        # write to disk/storage
        ledger.write_config()

        # con: Contract = fill.contract

        acnt.update_from_ledger(
            trans,

            # XXX: currently this is likely empty since we have no
            # support!
            symcache=ledger.symcache,

            # TODO: remove this hack by attempting to symcache an
            # incrementally updated table?
            _mktmap_table=client._contracts
        )

        # re-compute all positions that have changed state.
        # TODO: likely we should change the API to return the
        # position updates from `.update_from_ledger()`?
        active, closed = acnt.dump_active()

        # NOTE: update ledger with all new trades
        for fq_acctid, trades_by_id in api_to_ledger_entries.items():
            acctid: str = fq_acctid.strip('ib.')
            ledger: dict = ledgers[acctid]

            # NOTE: don't override flex/previous entries with new API
            # ones, just update with new fields or create new entry.
            for tid, tdict in trades_by_id.items():
                ledger.setdefault(tid, {}).update(tdict)

        # generate pp msgs and cross check with ib's positions data, relay
        # re-formatted pps as msgs to the ems.
        msg: dict | None = None
        for pos in filter(
            bool,
            [
                active.get(tx.bs_mktid),
                closed.get(tx.bs_mktid)
            ]
        ):
            msg = await update_and_audit_pos_msg(
                acctid,
                pos,
                ibpos,
                cons2mkts=client._cons2mkts,

                # ib pp event might not have arrived yet
                validate=False,
            )
            if msg:
                log.info(f'Emitting pp msg: {msg}')
                break

    # XXX NO FILL CASE:
    # if just handed an `IbPosition`, pack it and relay for now
    # since we always want the size to be up to date even if
    # the ppu is wrong..
    else:
        bs_mktid, msg = pack_position(ibpos, accounts_def)

    if msg:
        await ems_stream.send(msg)
    else:
        await tractor.pause()


# NOTE: See `OrderStatus` def for key schema;
# https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#a17f2a02d6449710b6394d0266a353313
# => we remap statuses to the ems set via the below table:
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
_statuses: dict[str, str] = {
    'Filled': 'filled',
    'Cancelled': 'canceled',
    'Submitted': 'open',

    'PendingSubmit': 'pending',
    'PendingCancel': 'pending',
    'PreSubmitted': 'pending',
    'ApiPending': 'pending',
    'ApiCancelled': 'pending',

    # TODO: see a current ``ib_insync`` issue around this:
    # https://github.com/erdewit/ib_insync/issues/363
    'Inactive': 'pending',
}

_action_map = {
    'BOT': 'buy',
    'SLD': 'sell',
}


# TODO: try out cancelling inactive orders after delay:
# https://github.com/erdewit/ib_insync/issues/363 (was originally
# inside `deliver_trade_events` status handler block.
# acctid = accounts_def.inverse[trade.order.account]
# double check there is no error when
# cancelling.. gawwwd
# if ib_status_key == 'cancelled':
#     last_log = trade.log[-1]
#     if (
#         last_log.message
#         and 'Error' not in last_log.message
#     ):
#         ib_status_key = trade.log[-2].status
#
# elif ib_status_key == 'inactive':
#
#     async def sched_cancel():
#         log.warning(
#             'OH GAWD an inactive order.scheduling a cancel\n'
#             f'{pformat(item)}'
#         )
#         proxy = proxies[acctid]
#         await proxy.submit_cancel(reqid=trade.order.orderId)
#         await trio.sleep(1)
#         nurse.start_soon(sched_cancel)
#
#     nurse.start_soon(sched_cancel)


# TODO: maybe just make this a flat func without an interal loop
# and call it *from* the `trade_event_stream` loop? Might look
# a lot nicer doing that from open_trade_dialog() instead of
# starting a separate task?
async def deliver_trade_events(

    # nurse: trio.Nursery,
    trade_event_stream: trio.MemoryReceiveChannel,
    ems_stream: tractor.MsgStream,
    accounts_def: dict[str, str],  # eg. `'ib.main'` -> `'DU999999'`
    proxies: dict[str, MethodProxy],

    ledgers,
    tables,

    flows: OrderDialogs,

) -> None:
    '''
    Format and relay all trade events for a given client to emsd.

    '''
    # task local msg dialog tracking
    clears: dict[
        Contract,
        list[
            IbPosition | None,  # filled by positionEvent
            Fill | None,  # filled by order status and exec details
        ]
    ] = {}
    execid2con: dict[str, Contract] = {}

    # TODO: for some reason we can receive a ``None`` here when the
    # ib-gw goes down? Not sure exactly how that's happening looking
    # at the eventkit code above but we should probably handle it...
    async for event_name, item in trade_event_stream:
        log.info(f'Relaying `{event_name}`:\n{pformat(item)}')
        match event_name:
            case 'orderStatusEvent':

                # XXX: begin normalization of nonsense ib_insync internal
                # object-state tracking representations...

                # unwrap needed data from ib_insync internal types
                trade: Trade = item
                reqid: str = str(trade.order.orderId)
                status: OrderStatus = trade.orderStatus
                status_str: str = _statuses[status.status]
                remaining: float = status.remaining
                if (
                    status_str == 'filled'
                ):
                    fill: Fill = trade.fills[-1]
                    execu: Execution = fill.execution

                    fill_msg = BrokerdFill(
                        time_ns=time.time_ns(),  # cuz why not

                        # NOTE: should match the value returned from
                        # `.submit_limit()`
                        reqid=reqid,

                        action=_action_map[execu.side],
                        size=execu.shares,
                        price=execu.price,

                        # DO we care? should this be in another
                        # msg like the position msg?
                        # broker_details=execdict,

                        # XXX: required by order mode currently
                        broker_time=execu.time,
                    )

                    await ems_stream.send(fill_msg)
                    flows.add_msg(reqid, fill_msg.to_dict())

                    if remaining == 0:
                        # emit a closed status on filled statuses where
                        # all units were cleared.
                        status_str = 'closed'

                # skip duplicate filled updates - we get the deats
                # from the execution details event
                msg = BrokerdStatus(
                    reqid=reqid,
                    time_ns=time.time_ns(),  # cuz why not
                    account=accounts_def.inverse[trade.order.account],

                    # everyone doin camel case..
                    status=status_str,

                    filled=status.filled,
                    reason=status.whyHeld,

                    # this seems to not be necessarily up to date in the
                    # execDetails event.. so we have to send it here I guess?
                    remaining=remaining,

                    broker_details={'name': 'ib'},
                )
                await ems_stream.send(msg)
                flows.add_msg(reqid, msg.to_dict())

            # XXX: for wtv reason this is a separate event type
            # from IB, not sure why it's needed other then for extra
            # complexity and over-engineering :eyeroll:.
            # we may just end up dropping these events (or
            # translating them to ``Status`` msgs) if we can
            # show the equivalent status events are no more latent.
            case 'execDetailsEvent':
                # unpack attrs pep-0526 style.
                trade: Trade
                con: Contract = trade.contract
                fill: Fill
                trade, fill = item
                execu: Execution = fill.execution
                execid: str = execu.execId
                report: CommissionReport = fill.commissionReport

                # always fill in id to con map so when commissions
                # arrive we can maybe fire the pos update..
                execid2con[execid] = con

                # TODO:
                # - normalize out commissions details?
                # - this is the same as the unpacking loop above in
                # ``trades_to_ledger_entries()`` no?

                # 2 cases:
                # - fill comes first or
                # - commission report comes first
                clear: tuple = clears.setdefault(
                    con,
                    [None, fill],
                )
                pos, _fill = clear

                # NOTE: we have to handle the case where a pos msg
                # has already been set (bc we already relayed rxed
                # one before both the exec-deats AND the
                # comms-report?) but the comms-report hasn't yet
                # arrived, so we fill in the fill (XD) and wait for
                # the cost to show up before relaying the pos msg
                # to the EMS..
                if _fill is None:
                    clear[1] = fill

                cost: float = report.commission
                if (
                    pos
                    and fill
                    and cost
                ):
                    await emit_pp_update(
                        ems_stream,
                        accounts_def,
                        proxies,
                        ledgers,
                        tables,

                        ibpos=pos,
                        fill=fill,
                    )
                    clears.pop(con)

            case 'commissionReportEvent':

                cr: CommissionReport = item
                execid: str = cr.execId

                # only fire a pp msg update if,
                # - we haven't already
                # - the fill event has already arrived
                #   but it didn't yet have a commision report
                #   which we fill in now.

                # placehold i guess until someone who know wtf
                # contract this is from can fill it in...
                con: Contract | None = execid2con.setdefault(execid, None)
                if (
                    con
                    and (clear := clears.get(con))
                ):
                    pos, fill = clear
                    if (
                        pos
                        and fill
                    ):
                        assert fill.commissionReport == cr
                        await emit_pp_update(
                            ems_stream,
                            accounts_def,
                            proxies,
                            ledgers,
                            tables,

                            ibpos=pos,
                            fill=fill,
                        )
                        clears.pop(con)
                        # TODO: should we clean this?
                        # execid2con.pop(execid)

            # always update with latest ib pos msg info since
            # we generally audit against it for sanity and
            # testing AND we require it to be updated to avoid
            # error msgs emitted from `update_and_audit_pos_msg()`
            case 'positionEvent':
                pos: IbPosition = item
                con: Contract = pos.contract

                bs_mktid, ppmsg = pack_position(pos, accounts_def)
                log.info(f'New IB position msg: {ppmsg}')

                _, fill = clears.setdefault(
                    con,
                    [pos, None],
                )
                # only send a pos update once we've actually rxed
                # the msg from IB since generally speaking we use
                # their 'cumsize' as gospel.
                await emit_pp_update(
                    ems_stream,
                    accounts_def,
                    proxies,
                    ledgers,
                    tables,

                    ibpos=pos,
                )

            case 'error':
                # NOTE: see impl deats in
                # `Client.inline_errors()::push_err()`
                err: dict = item

                # never relay errors for non-broker related issues
                # https://interactivebrokers.github.io/tws-api/message_codes.html
                code: int = err['error_code']
                if code in {
                    200,  # uhh

                    # hist pacing / connectivity
                    162,
                    165,

                    # WARNING codes:
                    # https://interactivebrokers.github.io/tws-api/message_codes.html#warning_codes
                    # Attribute 'Outside Regular Trading Hours' is
                    # " 'ignored based on the order type and
                    # destination. PlaceOrder is now ' 'being
                    # processed.',
                    2109,

                    # XXX: lol this isn't even documented..
                    # 'No market data during competing live session'
                    1669,
                }:
                    continue

                reqid: str = str(err['reqid'])
                reason: str = err['reason']

                if err['reqid'] == -1:
                    log.error(f'TWS external order error:\n{pformat(err)}')

                flow: dict = dict(flows.get(reqid)) or  {}

                # TODO: we don't want to relay data feed / lookup errors
                # so we need some further filtering logic here..
                # for most cases the 'status' block above should take
                # care of this.
                err_msg = BrokerdError(
                    reqid=reqid,
                    reason=reason,
                    broker_details={
                        'name': 'ib',
                        'flow': flow,
                    },
                )
                flows.add_msg(reqid, err_msg.to_dict())
                await ems_stream.send(err_msg)

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
