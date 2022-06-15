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
    Option,
)
from ib_insync.order import (
    Trade,
    OrderStatus,
)
from ib_insync.objects import (
    Fill,
    Execution,
)
from ib_insync.objects import Position
import pendulum

from piker import config
from piker import pp
from piker.log import get_console_log
from piker.clearing._messages import (
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
    _adhoc_futes_set,
    log,
    get_config,
    open_client_proxies,
    Client,
    MethodProxy,
)
# from .feed import open_data_client


def pack_position(
    pos: Position

) -> dict[str, Any]:
    con = pos.contract

    if isinstance(con, Option):
        # TODO: option symbol parsing and sane display:
        symbol = con.localSymbol.replace(' ', '')

    else:
        # TODO: lookup fqsn even for derivs.
        symbol = con.symbol.lower()

    exch = (con.primaryExchange or con.exchange).lower()
    fqsn = '.'.join((symbol, exch))
    if not exch:
        # attempt to lookup the symbol from our
        # hacked set..
        for sym in _adhoc_futes_set:
            if symbol in sym:
                fqsn = sym
                break

    expiry = con.lastTradeDateOrContractMonth
    if expiry:
        fqsn += f'.{expiry}'

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
            await ems_order_stream.send(BrokerdError(
                oid=request_msg['oid'],
                symbol=request_msg['symbol'],
                reason=f'No account found: `{account}` ?',
            ).dict())
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
            ).dict())
            continue

        if action in {'buy', 'sell'}:
            # validate
            order = BrokerdOrder(**request_msg)

            # call our client api to submit the order
            reqid = client.submit_limit(
                oid=order.oid,
                symbol=order.symbol,
                price=order.price,
                action=order.action,
                size=order.size,
                account=acct_number,

                # XXX: by default 0 tells ``ib_insync`` methods that
                # there is no existing order so ask the client to create
                # a new one (which it seems to do by allocating an int
                # counter - collision prone..)
                reqid=order.reqid,
            )
            if reqid is None:
                await ems_order_stream.send(BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],
                    reason='Order already active?',
                ).dict())

            # deliver ack that order has been submitted to broker routing
            await ems_order_stream.send(
                BrokerdOrderAck(
                    # ems order request id
                    oid=order.oid,
                    # broker specific request id
                    reqid=reqid,
                    time_ns=time.time_ns(),
                    account=account,
                ).dict()
            )

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)
            client.submit_cancel(reqid=msg.reqid)

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

    def push_tradesies(eventkit_obj, obj, fill=None):
        """Push events to trio task.

        """
        if fill is not None:
            # execution details event
            item = ('fill', (obj, fill))

        elif eventkit_obj.name() == 'positionEvent':
            item = ('position', obj)

        else:
            item = ('status', obj)

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

        # 'commissionReportEvent',
        # XXX: ugh, it is a separate event from IB and it's
        # emitted as follows:
        # self.ib.commissionReportEvent.emit(trade, fill, report)

        # XXX: not sure yet if we need these
        # 'updatePortfolioEvent',

        # XXX: these all seem to be weird ib_insync intrernal
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
    trade_entries: dict[str, Any],
    ib_pp_msgs: dict[int, BrokerdPosition],  # conid -> msg
    client: Union[Client, MethodProxy],

) -> dict[str, Any]:

    # construct piker pps from trade ledger, underneath using
    # LIFO style breakeven pricing calcs.
    conf = get_config()

    # retreive new trade executions from the last session
    # and/or day's worth of trading and convert into trade
    # records suitable for a local ledger file.
    # trades_by_account: dict = {}
    # for client in clients:

    # trade_entries = await client.trades()

    # XXX; ERRGGG..
    # pack in the "primary/listing exchange" value from a
    # contract lookup since it seems this isn't available by
    # default from the `.fills()` method endpoint...
    for entry in trade_entries:
        condict = entry['contract']
        conid = condict['conId']
        pexch = condict['primaryExchange']

        if not pexch:
            con = (await client.get_con(conid=conid))[0]
            pexch = con.primaryExchange

        entry['listingExchange'] = pexch

    records = trades_to_records(
        conf['accounts'].inverse,
        trade_entries,
    )
    # trades_by_account.update(records)

    # write recent session's trades to the user's (local) ledger file.
    for acctid, trades_by_id in records.items():

        with pp.open_trade_ledger('ib', acctid) as ledger:
            ledger.update(trades_by_id)

        # (incrementally) update the user's pps in mem and
        # in the `pps.toml`.
        records = norm_trade_records(trades_by_id)

        # remap stupid ledger fqsns (which are often
        # filled with lesser venue/exchange values) to
        # the ones we pull from the API via ib's reported
        # positioning messages.
        for r in records:
            normed_msg = ib_pp_msgs[r.bsuid]
            if normed_msg.symbol != r.fqsn:
                log.warning(
                    f'Remapping ledger fqsn: {r.fqsn} -> {normed_msg.symbol}'
                )
                r.fqsn = normed_msg.symbol

        active = pp.update_pps_conf('ib', acctid, records)

        return active


async def update_and_audit(
    by_fqsn: dict[str, pp.Position],
    cids2pps: dict[int, BrokerdPosition],

) -> list[BrokerdPosition]:

    msgs: list[BrokerdPosition] = []
    pps: dict[int, pp.Position] = {}

    for fqsn, p in by_fqsn.items():
        bsuid = p.bsuid

        # build trade-session-actor local table
        # of pps from unique symbol ids.
        pps[bsuid] = p

        # retreive equivalent ib reported position message
        # for comparison/audit versus the piker equivalent
        # breakeven pp calcs.
        ibppmsg = cids2pps[bsuid]

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
            avg_price=p.avg_price,
        )
        ibsize = ibppmsg.size
        pikersize = msg.size
        diff = pikersize - ibsize

        # if ib reports a lesser pp it's not as bad since we can
        # presume we're at least not more in the shit then we
        # thought.
        if diff:
            raise ValueError(
                f'POSITION MISMATCH ib <-> piker ledger:\n'
                f'ib: {msg}\n'
                f'piker: {ibppmsg}\n'
                'YOU SHOULD FIGURE OUT WHY TF YOUR LEDGER IS OFF!?!?'
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
    clients: list[tuple[Client, trio.MemoryReceiveChannel]] = []

    async with (
        trio.open_nursery() as nurse,
        open_client_proxies() as (proxies, aioclients),
    ):
        for account, proxy in proxies.items():

            client = aioclients[account]

            async def open_stream(
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

            trade_event_stream = await nurse.start(open_stream)

            clients.append((client, trade_event_stream))

            assert account in accounts_def
            accounts.add(account)

        cids2pps = {}
        used_accounts = set()

        # process pp value reported from ib's system. we only use these
        # to cross-check sizing since average pricing on their end uses
        # the so called (bs) "FIFO" style which more or less results in
        # a price that's not useful for traders who want to not lose
        # money.. xb
        for client in aioclients.values():
            for pos in client.positions():
                cid, msg = pack_position(pos)
                acctid = msg.account = accounts_def.inverse[msg.account]
                used_accounts.add(acctid)
                cids2pps[cid] = msg
                assert msg.account in accounts, (
                    f'Position for unknown account: {msg.account}')

        # update trades ledgers for all accounts from
        # connected api clients.
        for account, proxy in proxies.items():
            await update_ledger_from_api_trades(
                await proxy.trades(),
                cids2pps,  # pass these in to map to correct fqsns..
                proxy,
            )

        # load all positions from `pps.toml`, cross check with ib's
        # positions data, and relay re-formatted pps as msgs to the ems.
        for acctid, by_fqsn in pp.get_pps(
            'ib', acctids=used_accounts,
        ).items():

            msgs = await update_and_audit(by_fqsn, cids2pps)
            all_positions.extend(msg.dict() for msg in msgs)

        if not all_positions and cids2pps:
            raise RuntimeError(
                'Positions report by ib but not found in `pps.toml` !?')

        # log.info(f'Loaded {len(trades)} from this session')
        # TODO: write trades to local ``trades.toml``
        # - use above per-session trades data and write to local file
        # - get the "flex reports" working and pull historical data and
        # also save locally.

        await ctx.started((
            all_positions,
            tuple(name for name in accounts_def if name in accounts),
        ))

        async with (
            ctx.open_stream() as ems_stream,
            trio.open_nursery() as n,
        ):
            # start order request handler **before** local trades event loop
            n.start_soon(handle_order_requests, ems_stream, accounts_def)

            # allocate event relay tasks for each client connection
            for client, stream in clients:
                n.start_soon(
                    deliver_trade_events,
                    stream,
                    ems_stream,
                    accounts_def
                )

            # block until cancelled
            await trio.sleep_forever()


async def deliver_trade_events(

    trade_event_stream: trio.MemoryReceiveChannel,
    ems_stream: tractor.MsgStream,
    accounts_def: dict[str, str],

) -> None:
    '''Format and relay all trade events for a given client to the EMS.

    '''
    action_map = {'BOT': 'buy', 'SLD': 'sell'}

    # TODO: for some reason we can receive a ``None`` here when the
    # ib-gw goes down? Not sure exactly how that's happening looking
    # at the eventkit code above but we should probably handle it...
    async for event_name, item in trade_event_stream:

        log.info(f'ib sending {event_name}:\n{pformat(item)}')

        # TODO: templating the ib statuses in comparison with other
        # brokers is likely the way to go:
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

        if event_name == 'status':

            # XXX: begin normalization of nonsense ib_insync internal
            # object-state tracking representations...

            # unwrap needed data from ib_insync internal types
            trade: Trade = item
            status: OrderStatus = trade.orderStatus

            # skip duplicate filled updates - we get the deats
            # from the execution details event
            msg = BrokerdStatus(

                reqid=trade.order.orderId,
                time_ns=time.time_ns(),  # cuz why not
                account=accounts_def.inverse[trade.order.account],

                # everyone doin camel case..
                status=status.status.lower(),  # force lower case

                filled=status.filled,
                reason=status.whyHeld,

                # this seems to not be necessarily up to date in the
                # execDetails event.. so we have to send it here I guess?
                remaining=status.remaining,

                broker_details={'name': 'ib'},
            )

        elif event_name == 'fill':

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

            # TODO: normalize out commissions details?
            details = {
                'contract': asdict(fill.contract),
                'execution': asdict(fill.execution),
                'commissions': asdict(fill.commissionReport),
                'broker_time': execu.time,   # supposedly server fill time
                'name': 'ib',
            }

            msg = BrokerdFill(
                # should match the value returned from `.submit_limit()`
                reqid=execu.orderId,
                time_ns=time.time_ns(),  # cuz why not

                action=action_map[execu.side],
                size=execu.shares,
                price=execu.price,

                broker_details=details,
                # XXX: required by order mode currently
                broker_time=details['broker_time'],

            )

        elif event_name == 'error':

            err: dict = item

            # f$#$% gawd dammit insync..
            con = err['contract']
            if isinstance(con, Contract):
                err['contract'] = asdict(con)

            if err['reqid'] == -1:
                log.error(f'TWS external order error:\n{pformat(err)}')

            # TODO: what schema for this msg if we're going to make it
            # portable across all backends?
            # msg = BrokerdError(**err)
            continue

        elif event_name == 'position':

            cid, msg = pack_position(item)
            msg.account = accounts_def.inverse[msg.account]

        elif event_name == 'event':

            # it's either a general system status event or an external
            # trade event?
            log.info(f"TWS system status: \n{pformat(item)}")

            # TODO: support this again but needs parsing at the callback
            # level...
            # reqid = item.get('reqid', 0)
            # if getattr(msg, 'reqid', 0) < -1:
            # log.info(f"TWS triggered trade\n{pformat(msg.dict())}")

            continue

            # msg.reqid = 'tws-' + str(-1 * reqid)

            # mark msg as from "external system"
            # TODO: probably something better then this.. and start
            # considering multiplayer/group trades tracking
            # msg.broker_details['external_src'] = 'tws'

        # XXX: we always serialize to a dict for msgpack
        # translations, ideally we can move to an msgspec (or other)
        # encoder # that can be enabled in ``tractor`` ahead of
        # time so we can pass through the message types directly.
        await ems_stream.send(msg.dict())


def norm_trade_records(
    ledger: dict[str, Any],

) -> dict[str, list[pp.Transaction]]:
    '''
    Normalize a flex report or API retrieved executions
    ledger into our standard record format.

    '''
    records: list[pp.Transaction] = []

    for tid, record in ledger.items():
        # date, time = record['dateTime']
        # cost = record['cost']
        # action = record['buySell']
        conid = record.get('conId') or record['conid']
        comms = record.get('commission') or -1*record['ibCommission']
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

        fqsn = Symbol.from_fqsn(
            fqsn=f'{symbol}.{suffix}.ib',
            info={},
        ).front_fqsn().rstrip('.ib')

        # NOTE: for flex records the normal fields won't be available so
        # we have to do a lookup at some point to reverse map the conid
        # to a fqsn?

        # con = await proxy.get_con(conid)

        records.append(pp.Transaction(
            fqsn=fqsn,
            tid=tid,
            size=size,
            price=price,
            cost=comms,
            bsuid=conid,
        ))

    return records


def trades_to_records(
    accounts: bidict,
    trade_entries: list[object],
    source_type: str = 'api',

) -> dict:
    '''
    Convert either of API execution objects or flex report
    entry objects into ``dict`` form, pretty much straight up
    without modification.

    '''
    trades_by_account = {}

    for t in trade_entries:
        if source_type == 'flex':
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

        elif source_type == 'api':
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
                    case _:
                        entry[section] = val

            tid = str(entry['execId'])
            dt = pendulum.from_timestamp(entry['time'])
            # TODO: why isn't this showing seconds in the str?
            entry['date'] = str(dt)
            acctid = accounts[entry['acctNumber']]

        trades_by_account.setdefault(
            acctid, {}
        )[tid] = entry

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
    # log.info(f'Loaded {ln} trades from flex query')
    print(f'Loaded {ln} trades from flex query')

    trades_by_account = trades_to_records(
        # get reverse map to user account names
        conf['accounts'].inverse,
        trade_entries,
        source_type='flex',
    )

    ledgers = {}
    for acctid, trades_by_id in trades_by_account.items():
        with pp.open_trade_ledger('ib', acctid) as ledger:
            ledger.update(trades_by_id)

        ledgers[acctid] = ledger

    return ledgers


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
