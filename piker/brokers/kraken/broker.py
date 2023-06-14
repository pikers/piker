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

'''
Order api and machinery

'''
from collections import ChainMap, defaultdict
from contextlib import (
    asynccontextmanager as acm,
    aclosing,
)
from functools import partial
from itertools import count
import math
from pprint import pformat
import time
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    Union,
)

from bidict import bidict
import pendulum
import trio
import tractor

from piker.accounting import (
    Position,
    PpTable,
    Transaction,
    TransactionLedger,
    open_trade_ledger,
    open_pps,
    get_likely_pair,
)
from piker.accounting._mktinfo import (
    MktPair,
)
from piker.clearing._messages import (
    Order,
    Status,
    BrokerdCancel,
    BrokerdError,
    BrokerdFill,
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdPosition,
    BrokerdStatus,
)
from .api import (
    log,
    Client,
    BrokerError,
    get_client,
)
from .feed import (
    get_mkt_info,
    open_autorecon_ws,
    NoBsWs,
    stream_messages,
)

MsgUnion = Union[
    BrokerdCancel,
    BrokerdError,
    BrokerdFill,
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdPosition,
    BrokerdStatus,
]


class TooFastEdit(Exception):
    'Edit requests faster then api submissions'


# TODO: make this wrap the `Client` and `ws` instances
# and give it methods to submit cancel vs. add vs. edit
# requests?
class BrokerClient:
    '''
    Actor global, client-unique order manager API.

    For now provides unique ``brokerd`` defined "request ids"
    and "user reference" values to track ``kraken`` ws api order
    dialogs.

    '''
    counter: Iterable = count(1)
    _table: set[int] = set()

    @classmethod
    def new_reqid(cls) -> int:
        for reqid in cls.counter:
            if reqid not in cls._table:
                cls._table.add(reqid)
                return reqid

    @classmethod
    def add_reqid(cls, reqid: int) -> None:
        cls._table.add(reqid)


async def handle_order_requests(

    ws: NoBsWs,
    client: Client,
    ems_order_stream: tractor.MsgStream,
    token: str,
    apiflows: dict[int, ChainMap[dict[str, dict]]],
    ids: bidict[str, int],
    reqids2txids: dict[int, str],

) -> None:
    '''
    Process new order submission requests from the EMS
    and deliver acks or errors.

    '''
    # XXX: UGH, let's unify this.. with ``msgspec``.
    msg: dict[str, Any]
    order: BrokerdOrder

    async for msg in ems_order_stream:
        log.info(f'Rx order msg:\n{pformat(msg)}')
        match msg:
            case {
                'action': 'cancel',
            }:
                cancel = BrokerdCancel(**msg)
                reqid = ids[cancel.oid]

                try:
                    txid = reqids2txids[reqid]
                except KeyError:
                    # XXX: not sure if this block ever gets hit now?
                    log.error('TOO FAST CANCEL/EDIT')
                    reqids2txids[reqid] = TooFastEdit(reqid)
                    await ems_order_stream.send(
                        BrokerdError(
                            oid=msg['oid'],
                            symbol=msg['symbol'],
                            reason=(
                                f'Edit too fast:{reqid}, cancelling..'
                            ),

                        )
                    )
                else:
                    # call ws api to cancel:
                    # https://docs.kraken.com/websockets/#message-cancelOrder
                    await ws.send_msg({
                        'event': 'cancelOrder',
                        'token': token,
                        'reqid': reqid,
                        'txid': [txid],  # should be txid from submission
                    })

            case {
                'account': 'kraken.spot' as account,
                'action': action,
            } if action in {'buy', 'sell'}:

                # validate
                order = BrokerdOrder(**msg)

                # logic from old `Client.submit_limit()`
                if order.oid in ids:
                    ep = 'editOrder'
                    reqid = ids[order.oid]  # integer not txid
                    try:
                        txid = reqids2txids[reqid]
                    except KeyError:
                        # XXX: not sure if this block ever gets hit now?
                        log.error('TOO FAST EDIT')
                        reqids2txids[reqid] = TooFastEdit(reqid)
                        await ems_order_stream.send(
                            BrokerdError(
                                oid=msg['oid'],
                                symbol=msg['symbol'],
                                reason=(
                                    f'TooFastEdit reqid:{reqid}, cancelling..'
                                ),

                            )
                        )
                    else:
                        extra = {
                            'orderid': txid,  # txid
                            'newuserref': str(reqid),
                        }

                else:
                    ep = 'addOrder'

                    reqid = BrokerClient.new_reqid()
                    ids[order.oid] = reqid
                    log.debug(
                        f"Adding order {reqid}\n"
                        f'{ids}'
                    )
                    extra = {
                        'ordertype': 'limit',
                        'type': order.action,
                    }

                    psym = order.symbol.upper()
                    pair = f'{psym[:3]}/{psym[3:]}'

                # XXX: ACK the request **immediately** before sending
                # the api side request to ensure the ems maps the oid ->
                # reqid correctly!
                resp = BrokerdOrderAck(
                    oid=order.oid,  # ems order request id
                    reqid=reqid,  # our custom int mapping
                    account=account,  # piker account
                )
                await ems_order_stream.send(resp)

                # call ws api to submit the order:
                # https://docs.kraken.com/websockets/#message-addOrder
                req = {
                    'event': ep,
                    'token': token,

                    'reqid': reqid,  # remapped-to-int uid from ems
                    # XXX: we set these to the same value since for us
                    # a request dialog and an order's state-liftime are
                    # treated the same. Also this used to not work, the
                    # values used to be mutex for some odd reason until
                    # we dealt with support about it, and then they
                    # fixed it and pretended like we were crazy and the
                    # issue was never there lmao... coorps bro.
                    # 'userref': str(reqid),
                    'userref': str(reqid),
                    'pair': pair,
                    'price': str(order.price),
                    'volume': str(order.size),
                    # validate: 'true',  # validity check, nothing more
                } | extra

                log.info(f'Submitting WS order request:\n{pformat(req)}')
                await ws.send_msg(req)

                # placehold for sanity checking in relay loop
                apiflows[reqid].maps.append(msg)

            case _:
                account = msg.get('account')
                if account != 'kraken.spot':
                    log.error(
                        'This is a kraken account, \
                        only a `kraken.spot` selection is valid'
                    )

                await ems_order_stream.send(
                    BrokerdError(
                        oid=msg['oid'],
                        symbol=msg['symbol'],
                        reason=(
                            'Invalid request msg:\n{msg}'
                        ))
                )


@acm
async def subscribe(
    ws: NoBsWs,
    token: str,
    subs: list[tuple[str, dict]] = [
        ('ownTrades', {
            # don't send first 50 trades on startup,
            # we already pull this manually from the rest endpoint.
            'snapshot': False,
        },),
        ('openOrders', {
            # include rate limit counters
            'ratecounter': True,
        },),

    ],
):
    '''
    Setup ws api subscriptions:
    https://docs.kraken.com/websockets/#message-subscribe

    By default we sign up for trade and order update events.

    '''
    # more specific logic for this in kraken's sync client:
    # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
    assert token
    subnames: set[str] = set()

    for name, sub_opts in subs:
        msg = {
            'event': 'subscribe',
            'subscription': {
                'name': name,
                'token': token,
                **sub_opts,
            }
        }

        # TODO: we want to eventually allow unsubs which should
        # be completely fine to request from a separate task
        # since internally the ws methods appear to be FIFO
        # locked.
        await ws.send_msg(msg)
        subnames.add(name)

    # wait on subscriptionn acks
    with trio.move_on_after(5):
        while True:
            match (msg := await ws.recv_msg()):
                case {
                    'event': 'subscriptionStatus',
                    'status': 'subscribed',
                    'subscription': sub_opts,
                } as msg:
                    log.info(
                        f'Sucessful subscribe for {sub_opts}:\n'
                        f'{pformat(msg)}'
                    )
                    subnames.remove(sub_opts['name'])
                    if not subnames:
                        break

                case {
                    'event': 'subscriptionStatus',
                    'status': 'error',
                    'errorMessage': errmsg,
                } as msg:
                    raise RuntimeError(
                        f'{errmsg}\n\n'
                        f'{pformat(msg)}'
                    )
    yield

    for sub in subs:
        # unsub from all pairs on teardown
        await ws.send_msg({
            'event': 'unsubscribe',
            'subscription': [sub],
        })

    # XXX: do we need to ack the unsub?
    # await ws.recv_msg()


def trades2pps(
    table: PpTable,
    acctid: str,
    new_trans: dict[str, Transaction] = {},

    write_storage: bool = True,

) -> list[BrokerdPosition]:
    if new_trans:
        updated = table.update_from_trans(
            new_trans,
        )
        log.info(f'Updated pps:\n{pformat(updated)}')

    pp_entries, closed_pp_objs = table.dump_active()
    pp_objs: dict[Union[str, int], Position] = table.pps

    pps: dict[int, Position]
    position_msgs: list[dict] = []

    for pps in [pp_objs, closed_pp_objs]:
        for tid, p in pps.items():
            msg = BrokerdPosition(
                broker='kraken',
                # XXX: ok so this is annoying, we're
                # relaying an account name with the
                # backend suffix prefixed but when
                # reading accounts from ledgers we
                # don't need it and/or it's prefixed
                # in the section table.. we should
                # just strip this from the message
                # right since `.broker` is already
                # included?
                account='kraken.' + acctid,
                symbol=p.mkt.fqme,
                size=p.size,
                avg_price=p.ppu,
                currency='',
            )
            position_msgs.append(msg)

    if write_storage:
        # TODO: ideally this blocks the this task
        # as little as possible. we need to either do
        # these writes in another actor, or try out `trio`'s
        # async file IO api?
        table.write_config()

    return position_msgs


@tractor.context
async def trades_dialogue(
    ctx: tractor.Context,
    loglevel: str = None,

) -> AsyncIterator[dict[str, Any]]:

    async with get_client() as client:

        # make ems flip to paper mode when no creds setup in
        # `brokers.toml` B0
        if not client._api_key:
            await ctx.started('paper')
            return

        # NOTE: currently we expect the user to define a "source fiat"
        # (much like the web UI let's you set an "account currency")
        # such that all positions (nested or flat) will be translated to
        # this source currency's terms.
        src_fiat = client.conf['src_fiat']

        # auth required block
        acctid = client._name
        acc_name = 'kraken.' + acctid

        # task local msg dialog tracking
        apiflows: defaultdict[
            int,
            ChainMap[dict[str, dict]],
        ] = defaultdict(ChainMap)

        # 2way map for ems ids to kraken int reqids..
        ids: bidict[str, int] = bidict()
        reqids2txids: bidict[int, str] = bidict()

        # NOTE: testing code for making sure the rt incremental update
        # of positions, via newly generated msgs works. In order to test
        # this,
        # - delete the *ABSOLUTE LAST* entry from account's corresponding
        #   trade ledgers file (NOTE this MUST be the last record
        #   delivered from the api ledger),
        # - open you ``pps.toml`` and find that same tid and delete it
        #   from the pp's clears table,
        # - set this flag to `True`
        #
        # You should see an update come in after the order mode
        # boots up which shows your latest correct asset
        # balance size after the "previously unknown simulating a live
        # fill" update comes in on the relay loop even though the fill
        # will be ignored by the ems (no known reqid) the pp msg should
        # update things correctly.
        simulate_pp_update: bool = False

        table: PpTable
        ledger: TransactionLedger
        with (
            open_pps(
                'kraken',
                acctid,
                write_on_exit=True,
            ) as table,

            open_trade_ledger(
                'kraken',
                acctid,
            ) as ledger,
        ):
            # transaction-ify the ledger entries
            ledger_trans = await norm_trade_records(ledger)

            if not table.pps:
                # NOTE: we can't use this since it first needs
                # broker: str input support!
                # table.update_from_trans(ledger.to_trans())
                table.update_from_trans(ledger_trans)
                table.write_config()

            # TODO: eventually probably only load
            # as far back as it seems is not deliverd in the
            # most recent 50 trades and assume that by ordering we
            # already have those records in the ledger.
            tids2trades = await client.get_trades()
            ledger.update(tids2trades)
            if tids2trades:
                ledger.write_config()

            api_trans = await norm_trade_records(tids2trades)

            # retrieve kraken reported balances
            # and do diff with ledger to determine
            # what amount of trades-transactions need
            # to be reloaded.
            balances = await client.get_balances()

            for dst, size in balances.items():

                # we don't care about tracking positions
                # in the user's source fiat currency.
                if (
                    dst == src_fiat
                    or not any(
                        dst in bs_mktid for bs_mktid in table.pps
                    )
                ):
                    log.warning(
                        f'Skipping balance `{dst}`:{size} for position calcs!'
                    )
                    continue

                def has_pp(
                    dst: str,
                    size: float,

                ) -> Position | None:

                    src2dst: dict[str, str] = {}

                    for bs_mktid in table.pps:
                        likely_pair = get_likely_pair(
                            src_fiat,
                            dst,
                            bs_mktid,
                        )
                        if likely_pair:
                            src2dst[src_fiat] = dst

                    for src, dst in src2dst.items():
                        pair = f'{dst}{src_fiat}'
                        pp = table.pps.get(pair)
                        if (
                            pp
                            and math.isclose(pp.size, size)
                        ):
                            return pp

                        elif (
                            size == 0
                            and pp.size
                        ):
                            log.warning(
                                f'`kraken` account says you have  a ZERO '
                                f'balance for {bs_mktid}:{pair}\n'
                                f'but piker seems to think `{pp.size}`\n'
                                'This is likely a discrepancy in piker '
                                'accounting if the above number is'
                                "large,' though it's likely to due lack"
                                "f tracking xfers fees.."
                            )
                            return pp

                    return None  # signal no entry

                pos = has_pp(dst, size)
                if not pos:

                    # we have a balance for which there is no pp
                    # entry? so we have to likely update from the
                    # ledger.
                    updated = table.update_from_trans(ledger_trans)
                    log.info(f'Updated pps from ledger:\n{pformat(updated)}')
                    pos = has_pp(dst, size)

                    if (
                        not pos
                        and not simulate_pp_update
                    ):
                        # try reloading from API
                        table.update_from_trans(api_trans)
                        pos = has_pp(dst, size)
                        if not pos:

                            # get transfers to make sense of abs balances.
                            # NOTE: we do this after ledger and API
                            # loading since we might not have an entry
                            # in the ``pps.toml`` for the necessary pair
                            # yet and thus this likely pair grabber will
                            # likely fail.
                            for bs_mktid in table.pps:
                                likely_pair = get_likely_pair(
                                    src_fiat,
                                    dst,
                                    bs_mktid,
                                )
                                if likely_pair:
                                    break
                            else:
                                raise ValueError(
                                    'Could not find a position pair in '
                                    'ledger for likely widthdrawal '
                                    f'candidate: {dst}'
                                )

                            if likely_pair:
                                # this was likely pp that had a withdrawal
                                # from the dst asset out of the account.

                                xfer_trans = await client.get_xfers(
                                    dst,
                                    # TODO: not all src assets are
                                    # 3 chars long...
                                    src_asset=likely_pair[3:],
                                )
                                if xfer_trans:
                                    updated = table.update_from_trans(
                                        xfer_trans,
                                        cost_scalar=1,
                                    )
                                    log.info(
                                        f'Updated {dst} from transfers:\n'
                                        f'{pformat(updated)}'
                                    )

                        if has_pp(dst, size):
                            raise ValueError(
                                'Could not reproduce balance:\n'
                                f'dst: {dst}, {size}\n'
                            )

            # only for simulate-testing a "new fill" since
            # otherwise we have to actually conduct a live clear.
            if simulate_pp_update:
                tid = list(tids2trades)[0]
                last_trade_dict = tids2trades[tid]
                # stage a first reqid of `0`
                reqids2txids[0] = last_trade_dict['ordertxid']

            ppmsgs: list[BrokerdPosition] = trades2pps(
                table,
                acctid,
            )
            await ctx.started((ppmsgs, [acc_name]))

            # TODO: ideally this blocks the this task
            # as little as possible. we need to either do
            # these writes in another actor, or try out `trio`'s
            # async file IO api?
            table.write_config()

            # Get websocket token for authenticated data stream
            # Assert that a token was actually received.
            resp = await client.endpoint('GetWebSocketsToken', {})
            err = resp.get('error')
            if err:
                raise BrokerError(err)

            token = resp['result']['token']

            ws: NoBsWs
            async with (
                ctx.open_stream() as ems_stream,
                open_autorecon_ws(
                    'wss://ws-auth.kraken.com/',
                    fixture=partial(
                        subscribe,
                        token=token,
                    ),
                ) as ws,
                aclosing(stream_messages(ws)) as stream,
                trio.open_nursery() as nurse,
            ):
                # task for processing inbound requests from ems
                nurse.start_soon(
                    handle_order_requests,
                    ws,
                    client,
                    ems_stream,
                    token,
                    apiflows,
                    ids,
                    reqids2txids,
                )

                # enter relay loop
                await handle_order_updates(
                    ws,
                    stream,
                    ems_stream,
                    apiflows,
                    ids,
                    reqids2txids,
                    table,
                    api_trans,
                    acctid,
                    acc_name,
                    token,
                )


async def handle_order_updates(
    ws: NoBsWs,
    ws_stream: AsyncIterator,
    ems_stream: tractor.MsgStream,
    apiflows: dict[int, ChainMap[dict[str, dict]]],
    ids: bidict[str, int],
    reqids2txids: bidict[int, str],
    table: PpTable,

    # transaction records which will be updated
    # on new trade clearing events (aka order "fills")
    ledger_trans: dict[str, Transaction],
    acctid: str,
    acc_name: str,
    token: str,

) -> None:
    '''
    Main msg handling loop for all things order management.

    This code is broken out to make the context explicit and state
    variables defined in the signature clear to the reader.

    '''
    async for msg in ws_stream:
        match msg:

            # TODO: turns out you get the fill events from the
            # `openOrders` before you get this, so it might be better
            # to do all fill/status/pp updates in that sub and just use
            # this one for ledger syncs?

            # For eg. we could take the "last 50 trades" and do a diff
            # with the ledger and then only do a re-sync if something
            # seems amiss?

            # process and relay clearing trade events to ems
            # https://docs.kraken.com/websockets/#message-ownTrades
            # format as tid -> trade event map
            # eg. received msg format,
            # [{'TOKWHY-SMTUB-G5DOI6': {
            #   'cost': '95.29047',
            #   'fee': '0.24776',
            #   'margin': '0.00000',
            #   'ordertxid': 'OKSUXM-3OLSB-L7TN72',
            #   'ordertype': 'limit',
            #   'pair': 'XBT/EUR',
            #   'postxid': 'TKH2SE-M7IF5-CFI7LT',
            #   'price': '21268.20000',
            #   'time': '1657990947.640891',
            #   'type': 'buy',
            #   'vol': '0.00448042'
            # }}]
            case [
                trades_msgs,
                'ownTrades',
                {'sequence': seq},
            ]:
                log.info(
                    f'ownTrades update_{seq}:\n'
                    f'{pformat(trades_msgs)}'
                )
                trades = {
                    tid: trade
                    for entry in trades_msgs
                    for (tid, trade) in entry.items()

                    # don't re-process datums we've already seen
                    # if tid not in ledger_trans
                }
                for tid, trade in trades.items():
                    assert tid not in ledger_trans
                    txid = trade['ordertxid']
                    reqid = trade.get('userref')

                    if not reqid:
                        # NOTE: yet again, here we don't have any ref to the
                        # reqid that's generated by us (as the client) and
                        # sent in the order request, so we have to look it
                        # up from our own registry...
                        reqid = reqids2txids.inverse[txid]
                        if not reqid:
                            log.warning(f'Unknown trade dialog: {txid}')

                    action = trade['type']
                    price = float(trade['price'])
                    size = float(trade['vol'])
                    broker_time = float(trade['time'])

                    # TODO: we can emit this on the "closed" state in
                    # the `openOrders` sub-block below.
                    status_msg = BrokerdStatus(
                        reqid=reqid,
                        time_ns=time.time_ns(),
                        account=acc_name,
                        status='filled',
                        filled=size,
                        reason='Order filled by kraken',
                        broker_details={
                            'name': 'kraken',
                            'broker_time': broker_time
                        },

                        # TODO: figure out if kraken gives a count
                        # of how many units of underlying were
                        # filled. Alternatively we can decrement
                        # this value ourselves by associating and
                        # calcing from the diff with the original
                        # client-side request, see:
                        # https://github.com/pikers/piker/issues/296
                        remaining=0,
                    )
                    await ems_stream.send(status_msg)

                new_trans = await norm_trade_records(trades)
                ppmsgs = trades2pps(
                    table,
                    acctid,
                    new_trans,
                )
                for pp_msg in ppmsgs:
                    await ems_stream.send(pp_msg)

            # process and relay order state change events
            # https://docs.kraken.com/websockets/#message-openOrders
            case [
                order_msgs,
                'openOrders',
                {'sequence': seq},
            ]:
                for order_msg in order_msgs:
                    log.info(
                        f'`openOrders` msg update_{seq}:\n'
                        f'{pformat(order_msg)}'
                    )
                    txid, update_msg = list(order_msg.items())[0]

                    # XXX: eg. of full msg schema:
                    # {'avg_price': _,
                    # 'cost': _,
                    # 'descr': {
                    #     'close': None,
                    #     'leverage': None,
                    #     'order': descr,
                    #     'ordertype': 'limit',
                    #     'pair': 'XMR/EUR',
                    #     'price': '74.94000000',
                    #     'price2': '0.00000000',
                    #     'type': 'buy'
                    # },
                    # 'expiretm': None,
                    # 'fee': '0.00000000',
                    # 'limitprice': '0.00000000',
                    # 'misc': '',
                    # 'oflags': 'fciq',
                    # 'opentm': '1656966131.337344',
                    # 'refid': None,
                    # 'starttm': None,
                    # 'stopprice': '0.00000000',
                    # 'timeinforce': 'GTC',
                    # 'vol': submit_vlm,  # '13.34400854',
                    # 'vol_exec': exec_vlm}  # 0.0000
                    match update_msg:

                        # EMS-unknown LIVE order that needs to be
                        # delivered and loaded on the client-side.
                        case {
                            'userref': reqid,
                            'descr': {
                                'pair': pair,
                                'price': price,
                                'type': action,
                            },
                            'vol': vol,

                            # during a fill this field is **not**
                            # provided! but, it is always avail on
                            # actual status updates.. see case above.
                            'status': status,
                            **rest,
                        } if (
                            ids.inverse.get(reqid) is None
                        ):
                            # parse out existing live order
                            fqme = pair.replace('/', '').lower()
                            price = float(price)
                            size = float(vol)

                            # register the userref value from
                            # kraken (usually an `int` staring
                            # at 1?) as our reqid.
                            reqids2txids[reqid] = txid
                            oid = str(reqid)
                            ids[oid] = reqid  # NOTE!: str -> int

                            # ensure wtv reqid they give us we don't re-use on
                            # new order submissions to this actor's client.
                            BrokerClient.add_reqid(reqid)

                            # fill out ``Status`` + boxed ``Order``
                            status_msg = Status(
                                time_ns=time.time_ns(),
                                resp='open',
                                oid=oid,
                                reqid=reqid,

                                # embedded order info
                                req=Order(
                                    action=action,
                                    exec_mode='live',
                                    oid=oid,
                                    symbol=fqme,
                                    account=acc_name,
                                    price=price,
                                    size=size,
                                ),
                                src='kraken',
                            )
                            apiflows[reqid].maps.append(status_msg.to_dict())
                            await ems_stream.send(status_msg)
                            continue

                        case {
                            'userref': reqid,

                            # during a fill this field is **not**
                            # provided! but, it is always avail on
                            # actual status updates.. see case above.
                            'status': status,
                            **rest,
                        }:
                            # TODO: store this in a ChainMap instance
                            # per order dialog.
                            # submit_vlm = rest.get('vol', 0)
                            # fee = rest.get('fee', 0)
                            if status == 'closed':
                                vlm = 0
                            else:
                                vlm = rest.get('vol_exec', 0)

                            if status == 'canceled':
                                reqids2txids.pop(reqid)

                                # we specially ignore internal order
                                # updates triggered by kraken's "edit"
                                # endpoint.
                                if rest['cancel_reason'] == 'Order replaced':
                                    # TODO:
                                    # - put the edit order status update
                                    #   code here?
                                    # - send open order status msg.
                                    log.info(
                                        f'Order replaced: {txid}@reqid={reqid}'
                                    )

                                    # we don't do normal msg emission on
                                    # a replacement cancel since it's
                                    # the result of an "edited order"
                                    # and thus we mask the kraken
                                    # backend cancel then create details
                                    # from the ems side.
                                    continue
                            else:
                                # XXX: keep kraken engine's ``txid`` synced
                                # with the ems dialog's ``reqid``.
                                reqids2txids[reqid] = txid

                                ourreqid = reqids2txids.inverse.get(txid)
                                if ourreqid is None:
                                    log.info(
                                        'Mapping new txid to our reqid:\n'
                                        f'{reqid} -> {txid}'
                                    )

                            oid = ids.inverse.get(reqid)
                            # XXX: too fast edit handled by the
                            # request handler task: this
                            # scenario occurs when ems side
                            # requests are coming in too quickly
                            # such that there is no known txid
                            # yet established for the ems
                            # dialog's last reqid when the
                            # request handler task is already
                            # receceiving a new update for that
                            # reqid. In this case we simply mark
                            # the reqid as being "too fast" and
                            # then when we get the next txid
                            # update from kraken's backend, and
                            # thus the new txid, we simply
                            # cancel the order for now.

                            # TODO: Ideally we eventually
                            # instead make the client side of
                            # the ems block until a submission
                            # is confirmed by the backend
                            # instead of this hacky throttle
                            # style approach and avoid requests
                            # coming in too quickly on the other
                            # side of the ems, aka the client
                            # <-> ems dialog.
                            if (
                                status == 'open'
                                and isinstance(
                                    reqids2txids.get(reqid),
                                    TooFastEdit
                                )
                            ):
                                # TODO: don't even allow this case
                                # by not moving the client side line
                                # until an edit confirmation
                                # arrives...
                                log.cancel(
                                    f'Received too fast edit {txid}:\n'
                                    f'{update_msg}\n'
                                    'Cancelling order for now!..'
                                )
                                # call ws api to cancel:
                                # https://docs.kraken.com/websockets/#message-cancelOrder
                                await ws.send_msg({
                                    'event': 'cancelOrder',
                                    'token': token,
                                    'reqid': reqid or 0,
                                    'txid': [txid],
                                })
                                continue

                            # send BrokerdStatus messages for all
                            # order state updates
                            resp = BrokerdStatus(

                                reqid=reqid,
                                time_ns=time.time_ns(),  # cuz why not
                                account=f'kraken.{acctid}',

                                # everyone doin camel case..
                                status=status,  # force lower case

                                filled=vlm,
                                reason='',  # why held?
                                remaining=vlm,

                                # TODO: need to extract the submit vlm
                                # from a prior msg update..
                                # (
                                #     float(submit_vlm)
                                #     -
                                #     float(exec_vlm)
                                # ),

                                broker_details=dict(
                                    {'name': 'kraken'}, **update_msg
                                ),
                            )

                            apiflows[reqid].maps.append(update_msg)
                            await ems_stream.send(resp)

                        # fill msg.
                        # eg. contents (in total):
                        # {
                        #     'vol_exec': '0.84709869',
                        #     'cost': '101.25370642',
                        #     'fee': '0.26325964',
                        #     'avg_price': '119.53000001',
                        #     'userref': 0,
                        # }
                        # NOTE: there is no `status` field
                        case {
                            'vol_exec': vlm,
                            'avg_price': price,
                            'userref': reqid,
                            **rest,
                        } as msg:

                            ourreqid = reqids2txids.inverse[txid]
                            assert reqid == ourreqid
                            log.info(
                                f'openOrders vlm={vlm} Fill for {reqid}:\n'
                                f'{update_msg}'
                            )

                            fill_msg = BrokerdFill(
                                time_ns=time.time_ns(),
                                reqid=reqid,
                                # just use size value for now?
                                # action=action,
                                size=float(vlm),
                                price=float(price),

                                # TODO: maybe capture more msg data
                                # i.e fees?
                                broker_details={'name': 'kraken'} | order_msg,
                                broker_time=time.time(),
                            )
                            await ems_stream.send(fill_msg)

                        case _:
                            log.warning(
                                'Unknown orders msg:\n'
                                f'{txid}:{order_msg}'
                            )

            # order request status updates
            case {
                'event': etype,
                'status': status,
                'reqid': reqid,
                **rest,
            } as event if (
                etype in {
                    'addOrderStatus',
                    'editOrderStatus',
                    'cancelOrderStatus',
                }
            ):
                log.info(
                    f'{etype}:\n'
                    f'{pformat(msg)}'
                )

                txid = rest.get('txid')
                lasttxid = reqids2txids.get(reqid)

                # TODO: relay these to EMS once it supports
                # open order loading.
                oid = ids.inverse.get(reqid)
                if not oid:
                    log.warning(
                        'Unknown order status update?:\n'
                        f'{event}'
                    )
                    continue

                # update the msg chain
                chain = apiflows[reqid]
                chain.maps.append(event)

                if status == 'error':
                    # any of ``{'add', 'edit', 'cancel'}``
                    action = etype.removesuffix('OrderStatus')
                    errmsg = rest['errorMessage']
                    log.error(
                        f'Failed to {action} order {reqid}:\n'
                        f'{errmsg}'
                    )
                    await ems_stream.send(BrokerdError(
                        oid=oid,
                        # XXX: use old reqid in case it changed?
                        reqid=reqid,
                        symbol=chain.get('symbol', 'N/A'),

                        reason=f'Failed {action}:\n{errmsg}',
                        broker_details=event
                    ))

                    txid = txid or lasttxid
                    if (
                        txid

                        # we throttle too-fast-requests on the ems side
                        and not isinstance(txid, TooFastEdit)
                    ):
                        # client was editting too quickly
                        # so we instead cancel this order
                        log.cancel(
                            f'Cancelling {reqid}@{txid} due to:\n {event}')
                        await ws.send_msg({
                            'event': 'cancelOrder',
                            'token': token,
                            'reqid': reqid or 0,
                            'txid': [txid],
                        })
            case _:
                log.warning(f'Unhandled trades update msg: {msg}')


async def norm_trade_records(
    ledger: dict[str, Any],

) -> dict[str, Transaction]:

    records: dict[str, Transaction] = {}

    for tid, record in ledger.items():

        size = float(record.get('vol')) * {
            'buy': 1,
            'sell': -1,
        }[record['type']]

        # we normalize to kraken's `altname` always..
        bs_mktid: str = Client.normalize_symbol(record['pair'])
        fqme = f'{bs_mktid.lower()}.kraken'
        mkt: MktPair = (await get_mkt_info(fqme))[0]

        records[tid] = Transaction(
            fqme=fqme,
            sym=mkt,
            tid=tid,
            size=size,
            price=float(record['price']),
            cost=float(record['fee']),
            dt=pendulum.from_timestamp(float(record['time'])),
            bs_mktid=bs_mktid,
        )

    return records
