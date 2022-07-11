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
from contextlib import (
    asynccontextmanager as acm,
    contextmanager as cm,
)
from functools import partial
from itertools import chain, count
from pprint import pformat
import time
from typing import (
    Any,
    AsyncIterator,
    Union,
)

from async_generator import aclosing
from bidict import bidict
import pendulum
import trio
import tractor
import wsproto

from piker import pp
from piker.clearing._messages import (
    BrokerdCancel,
    BrokerdError,
    BrokerdFill,
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdPosition,
    BrokerdStatus,
)
from . import log
from .api import (
    Client,
    BrokerError,
    get_client,
)
from .feed import (
    get_console_log,
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


async def handle_order_requests(

    ws: NoBsWs,
    client: Client,
    ems_order_stream: tractor.MsgStream,
    token: str,
    emsflow: dict[str, list[MsgUnion]],
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
    counter = count()

    async for msg in ems_order_stream:
        log.info(f'Rx order msg:\n{pformat(msg)}')
        match msg:
            case {
                'action': 'cancel',
            }:
                cancel = BrokerdCancel(**msg)
                # last = emsflow[cancel.oid]
                reqid = ids[cancel.oid]
                txid = reqids2txids[reqid]

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
                    txid = reqids2txids[reqid]
                    extra = {
                        'orderid': txid,  # txid
                    }

                else:
                    ep = 'addOrder'
                    reqid = next(counter)
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
                    'pair': pair,
                    'price': str(order.price),
                    'volume': str(order.size),

                    # only ensures request is valid, nothing more
                    # validate: 'true',

                } | extra
                log.info(f'Submitting WS order request:\n{pformat(req)}')
                await ws.send_msg(req)

                # placehold for sanity checking in relay loop
                emsflow.setdefault(order.oid, []).append(order)

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
                        ),

                    )
                )


@acm
async def subscribe(
    ws: wsproto.WSConnection,
    token: str,
    subs: list[str] = ['ownTrades', 'openOrders'],
):
    '''
    Setup ws api subscriptions:
    https://docs.kraken.com/websockets/#message-subscribe

    By default we sign up for trade and order update events.

    '''
    # more specific logic for this in kraken's sync client:
    # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188

    assert token
    for sub in subs:
        msg = {
            'event': 'subscribe',
            'subscription': {
                'name': sub,
                'token': token,
            }
        }

        # TODO: we want to eventually allow unsubs which should
        # be completely fine to request from a separate task
        # since internally the ws methods appear to be FIFO
        # locked.
        await ws.send_msg(msg)

    yield

    for sub in subs:
        # unsub from all pairs on teardown
        await ws.send_msg({
            'event': 'unsubscribe',
            'subscription': [sub],
        })

    # XXX: do we need to ack the unsub?
    # await ws.recv_msg()


@tractor.context
async def trades_dialogue(
    ctx: tractor.Context,
    loglevel: str = None,
) -> AsyncIterator[dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    async with get_client() as client:

        # TODO: make ems flip to paper mode via
        # some returned signal if the user only wants to use
        # the data feed or we return this?
        # await ctx.started(({}, ['paper']))

        if not client._api_key:
            raise RuntimeError(
                'Missing Kraken API key in `brokers.toml`!?!?')

        # auth required block
        acctid = client._name
        acc_name = 'kraken.' + acctid

        # pull and deliver trades ledger
        trades = await client.get_trades()
        log.info(
            f'Loaded {len(trades)} trades from account `{acc_name}`'
        )
        with open_ledger(
            acctid,
            trades,
        ) as trans:
            active, closed = pp.update_pps_conf(
                'kraken',
                acctid,
                trade_records=trans,
                ledger_reload={}.fromkeys(t.bsuid for t in trans),
            )

        position_msgs: list[dict] = []
        pps: dict[int, pp.Position]
        for pps in [active, closed]:
            for tid, p in pps.items():
                msg = BrokerdPosition(
                    broker='kraken',
                    account=acc_name,
                    symbol=p.symbol.front_fqsn(),
                    size=p.size,
                    avg_price=p.be_price,
                    currency='',
                )
                position_msgs.append(msg)

        await ctx.started(
            (position_msgs, [acc_name])
        )

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
            trio.open_nursery() as n,
            aclosing(stream_messages(ws)) as stream,
        ):
            # task local msg dialog tracking
            emsflow: dict[
                str,
                list[MsgUnion],
            ] = {}

            # 2way map for ems ids to kraken int reqids..
            ids: bidict[str, int] = bidict()
            reqids2txids: bidict[int, str] = bidict()

            # task for processing inbound requests from ems
            n.start_soon(
                handle_order_requests,
                ws,
                client,
                ems_stream,
                token,
                emsflow,
                ids,
                reqids2txids,
            )

            # enter relay loop
            await handle_order_updates(
                ws,
                stream,
                ems_stream,
                emsflow,
                ids,
                reqids2txids,
                trans,
                acctid,
                acc_name,
                token,
            )


async def handle_order_updates(
    ws: NoBsWs,
    ws_stream: AsyncIterator,
    ems_stream: tractor.MsgStream,
    emsflow: dict[str, list[MsgUnion]],
    ids: bidict[str, int],
    reqids2txids: bidict[int, str],
    trans: set[pp.Transaction],
    acctid: str,
    acc_name: str,
    token: str,

) -> None:
    '''
    Main msg handling loop for all things order management.

    This code is broken out to make the context explicit and state variables
    defined in the signature clear to the reader.

    '''
    # transaction records which will be updated
    # on new trade clearing events (aka order "fills")
    trans: set[pp.Transaction]

    async for msg in ws_stream:
        match msg:
            # process and relay clearing trade events to ems
            # https://docs.kraken.com/websockets/#message-ownTrades
            # TODO: turns out you get the fill events from the
            # `openOrders` before you get this, so it might be better
            # to do all fill/status/pp updates in that sub and just use
            # this one for ledger syncs?
            # For eg. we could take the "last 50 trades" and do a diff
            # with the ledger and then only do a re-sync if something
            # seems amiss?
            case [
                trades_msgs,
                'ownTrades',
                {'sequence': seq},
            ]:
                log.info(
                    f'ownTrades update_{seq}:\n'
                    f'{pformat(trades_msgs)}'
                )
                # flatten msgs to an {id -> data} table for processing
                trades = {
                    tid: trade
                    for entry in trades_msgs
                    for (tid, trade) in entry.items()

                    # only emit entries which are already not-in-ledger
                    if tid not in {r.tid for r in trans}
                }
                for tid, trade in trades.items():

                    txid = trade['ordertxid']

                    # NOTE: yet again, here we don't have any ref to the
                    # reqid that's generated by us (as the client) and
                    # sent in the order request, so we have to look it
                    # up from our own registry...
                    reqid = reqids2txids.inverse[txid]

                    action = trade['type']
                    price = float(trade['price'])
                    size = float(trade['vol'])
                    broker_time = float(trade['time'])

                    # send a fill msg for gui update
                    fill_msg = BrokerdFill(
                        time_ns=time.time_ns(),
                        reqid=reqid,

                        action=action,
                        size=size,
                        price=price,

                        # TODO: maybe capture more msg data
                        # i.e fees?
                        broker_details={'name': 'kraken'} | trade,
                        broker_time=broker_time
                    )
                    await ems_stream.send(fill_msg)

                    filled_msg = BrokerdStatus(
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
                    await ems_stream.send(filled_msg)

                if not trades:
                    # skip pp emissions if we have already
                    # processed all trades in this msg.
                    continue

                # update ledger and position tracking
                await tractor.breakpoint()
                trans: set[pp.Transaction]
                with open_ledger(
                    acctid,
                    trades,

                ) as trans:
                    # TODO: ideally we can pass in an existing
                    # pps state to this right? such that we
                    # don't have to do a ledger reload all the
                    # time..
                    active, closed = pp.update_pps_conf(
                        'kraken',
                        acctid,
                        trade_records=list(trans),
                        ledger_reload={}.fromkeys(
                            t.bsuid for t in trans),
                    )

                # emit any new pp msgs to ems
                for pos in filter(
                    bool,
                    chain(active.values(), closed.values()),
                ):
                    pp_msg = BrokerdPosition(
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
                        account=f'kraken.{acctid}',
                        symbol=pos.symbol.front_fqsn(),
                        size=pos.size,
                        avg_price=pos.be_price,

                        # TODO
                        # currency=''
                    )
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
                    match update_msg:

                        # we ignore internal order updates triggered by
                        # kraken's "edit" endpoint.
                        case {
                            'cancel_reason': 'Order replaced',
                            'status': status,
                            # 'userref': reqid,  # XXX: always zero bug XD
                            # **rest,
                        }:
                            log.info(
                                f'Order {txid} was replaced'
                            )
                            continue

                        case {
                            # XXX: lol, ws bug, this is always 0!
                            'userref': _,

                            # during a fill this field is **not**
                            # provided! but, it is always avail on
                            # actual status updates.. see case above.
                            'status': status,
                            **rest,

                            # XXX: eg. of remaining msg schema:
                            # 'avg_price': _,
                            # 'cost': _,
                            # 'descr': {
                                # 'close': None,
                                # 'leverage': None,
                                # 'order': descr,
                                # 'ordertype': 'limit',
                                # 'pair': 'XMR/EUR',
                                # 'price': '74.94000000',
                                # 'price2': '0.00000000',
                                # 'type': 'buy'
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
                            # 'vol_exec': exec_vlm,  # 0.0000
                        }:
                            ems_status = {
                                'open': 'submitted',
                                'closed': 'filled',
                                'canceled': 'cancelled',
                                # do we even need to forward
                                # this state to the ems?
                                'pending': 'pending',
                            }[status]

                            # TODO: store this in a ChainMap instance
                            # per order dialog.
                            # submit_vlm = rest.get('vol', 0)
                            # fee = rest.get('fee', 0)
                            if status == 'closed':
                                vlm = 0
                            else:
                                vlm = rest.get('vol_exec', 0)

                            reqid = reqids2txids.inverse[txid]

                            oid = ids.inverse.get(reqid)
                            if not oid:
                                # TODO: handle these and relay them
                                # through the EMS to the client / UI
                                # side!
                                log.warning(
                                    f'Received active order {txid}:\n'
                                    f'{update_msg}\n'
                                    'Cancelling order for now!..'
                                )

                                # call ws api to cancel:
                                # https://docs.kraken.com/websockets/#message-cancelOrder
                                await ws.send_msg({
                                    'event': 'cancelOrder',
                                    'token': token,
                                    'reqid': reqid,
                                    'txid': [txid],
                                })
                                continue

                            msgs = emsflow[oid]

                            # send BrokerdStatus messages for all
                            # order state updates
                            resp = BrokerdStatus(

                                reqid=reqid,
                                time_ns=time.time_ns(),  # cuz why not
                                account=f'kraken.{acctid}',

                                # everyone doin camel case..
                                status=ems_status,  # force lower case

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
                            msgs.append(resp)
                            await ems_stream.send(resp)

                        # fill event.
                        # there is no `status` field
                        case {
                            'vol_exec': vlm,
                            **rest,
                        }:
                            # eg. fill msg contents (in total):
                            # {
                            #     'vol_exec': '0.84709869',
                            #     'cost': '101.25370642',
                            #     'fee': '0.26325964',
                            #     'avg_price': '119.53000001',
                            #     'userref': 0,
                            # }
                            # TODO: emit fill msg from here
                            reqid = reqids2txids.inverse[txid]
                            log.info(
                                f'openOrders vlm={vlm} Fill for {reqid}:\n'
                                f'{update_msg}'
                            )
                            continue

                        case _:
                            log.warning(
                                'Unknown orders msg:\n'
                                f'{txid}:{order_msg}'
                            )

            # TODO: given the 'openOrders' sub , pretty
            # much all the msgs we get for this sub are duplicate
            # of the (incremental) updates in that one though we still
            # need them because that sub seems to have a bug where the
            # `userref` field is always 0 instead of our generated reqid
            # value...
            # Not sure why kraken devs decided to repeat themselves but
            # it almost seems as though we could drop this entire sub
            # and get everything we need by just parsing msgs correctly
            # above? The only reason for this seems to be remapping
            # underlying `txid` values on order "edits" which the
            # `openOrders` sub doesn't seem to have any knowledge of.
            # I'd also like to ask them which event guarantees that the
            # the live order is now in the book, since these status ones
            # almost seem more like request-acks then state guarantees.
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
                oid = ids.inverse.get(reqid)
                # TODO: relay these to EMS once it supports
                # open order loading.
                if not oid:
                    log.warning(
                        'Unknown order status update?:\n'
                        f'{event}'
                    )
                    continue

                txid = rest.get('txid')
                if txid:
                    # XXX: we **must** do this mapping for edit order
                    # status updates since the `openOrders` sub above
                    # never relays back the correct client-side `reqid`
                    # that is put in the order request..
                    reqids2txids[reqid] = txid

                msgs = emsflow[oid]
                last = msgs[-1]
                resps, errored = process_status(
                    event,
                    oid,
                    token,
                    msgs,
                    last,
                )
                if resps:
                    msgs.extend(resps)
                    for resp in resps:
                        await ems_stream.send(resp)

            case _:
                log.warning(f'Unhandled trades update msg: {msg}')


def process_status(
    event: dict[str, str],
    oid: str,
    token: str,
    msgs: list[MsgUnion],
    last: MsgUnion,

) -> tuple[list[MsgUnion], bool]:
    '''
    Process `'[add/edit/cancel]OrderStatus'` events by translating to
    and returning the equivalent EMS-msg responses.

    '''
    match event:
        case {
            'event': etype,
            'status': 'error',
            'reqid': reqid,
            'errorMessage': errmsg,
        }:
            # any of ``{'add', 'edit', 'cancel'}``
            action = etype.rstrip('OrderStatus')
            log.error(
                f'Failed to {action} order {reqid}:\n'
                f'{errmsg}'
            )
            resp = BrokerdError(
                oid=oid,
                # XXX: use old reqid in case it changed?
                reqid=reqid,
                symbol=getattr(last, 'symbol', 'N/A'),

                reason=f'Failed {action}:\n{errmsg}',
                broker_details=event
            )
            return [resp], True

        # successful request cases
        case {
            'event': 'addOrderStatus',
            'status': "ok",
            'reqid': reqid,  # oid from ems side
            'txid': txid,
            'descr': descr,  # only on success?
        }:
            log.info(
                f'Submitted order: {descr}\n'
                f'ems oid: {oid}\n'
                f'brokerd reqid: {reqid}\n'
                f'txid: {txid}\n'
            )
            return [], False

        case {
            'event': 'editOrderStatus',
            'status': "ok",
            'reqid': reqid,  # oid from ems side
            'descr': descr,

            # NOTE: for edit request this is a new value
            'txid': txid,
            'originaltxid': origtxid,
        }:
            log.info(
                f'Editting order {oid}[requid={reqid}]:\n'
                f'brokerd reqid: {reqid}\n'
                f'txid: {origtxid} -> {txid}\n'
                f'{descr}'
            )
            # deliver another ack to update the ems-side `.reqid`.
            return [], False

        case {
            "event": "cancelOrderStatus",
            "status": "ok",
            'reqid': reqid,

            # XXX: sometimes this isn't provided!?
            # 'txid': txids,
            **rest,
        }:
            for txid in rest.get('txid', [last.reqid]):
                log.info(
                    f'Cancelling order {oid}[requid={reqid}]:\n'
                    f'brokerd reqid: {reqid}\n'
                )
            return [], False


def norm_trade_records(
    ledger: dict[str, Any],

) -> list[pp.Transaction]:

    records: list[pp.Transaction] = []
    for tid, record in ledger.items():

        size = float(record.get('vol')) * {
            'buy': 1,
            'sell': -1,
        }[record['type']]

        # we normalize to kraken's `altname` always..
        bsuid = norm_sym = Client.normalize_symbol(record['pair'])

        records.append(
            pp.Transaction(
                fqsn=f'{norm_sym}.kraken',
                tid=tid,
                size=size,
                price=float(record['price']),
                cost=float(record['fee']),
                dt=pendulum.from_timestamp(float(record['time'])),
                bsuid=bsuid,

                # XXX: there are no derivs on kraken right?
                # expiry=expiry,
            )
        )

    return records


@cm
def open_ledger(
    acctid: str,
    trade_entries: list[dict[str, Any]],

) -> set[pp.Transaction]:
    '''
    Write recent session's trades to the user's (local) ledger file.

    '''
    with pp.open_trade_ledger(
        'kraken',
        acctid,
    ) as ledger:

        # normalize to transaction form
        # TODO: cawt damn, we should probably delegate to cryptofeed for
        # this insteada of re-hacking kraken's total crap?
        records = norm_trade_records(trade_entries)
        yield set(records)

        # update on exit
        ledger.update(trade_entries)
