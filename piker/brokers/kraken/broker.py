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
from contextlib import asynccontextmanager as acm
from functools import partial
from itertools import chain, count
from pprint import pformat
import time
from typing import (
    Any,
    AsyncIterator,
    # Callable,
    # Optional,
    # Union,
)

from bidict import bidict
import pendulum
# from pydantic import BaseModel
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
    normalize_symbol,
)
from .feed import (
    get_console_log,
    open_autorecon_ws,
    NoBsWs,
    stream_messages,
)


async def handle_order_requests(

    ws: NoBsWs,
    client: Client,
    ems_order_stream: tractor.MsgStream,
    token: str,
    requests: dict[str, BrokerdOrder],
    ids: bidict[str, int],

) -> None:
    '''
    Process new order submission requests from the EMS
    and deliver acks or errors.

    '''
    # XXX: UGH, let's unify this.. with ``msgspec``.
    request_msg: dict
    order: BrokerdOrder
    counter = count()

    async for request_msg in ems_order_stream:
        log.info(
            'Received order request:\n'
            f'{pformat(request_msg)}'
        )

        account = request_msg['account']

        if account != 'kraken.spot':
            log.error(
                'This is a kraken account, \
                only a `kraken.spot` selection is valid'
            )
            await ems_order_stream.send(
                BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],
                    reason=(
                        'Kraken only, order mode disabled due to '
                        'https://github.com/pikers/piker/issues/299'
                    ),

                ).dict()
            )
            continue

        action = request_msg['action']
        if action in {'buy', 'sell'}:

            # validate
            msg = BrokerdOrder(**request_msg)

            # logic from old `Client.submit_limit()`
            if msg.oid in ids:
                ep = 'editOrder'
                reqid = ids[msg.oid]  # integer not txid
                order = requests[msg.oid]
                assert order.oid == msg.oid
                extra = {
                    'orderid': msg.reqid,  # txid
                }

                # XXX: TODO: get this working, but currently the EMS
                # doesn't support changing order `.reqid` (in this case
                # kraken changes them via a cancel and a new
                # submission). So for now cancel and report the error.
                await ws.send_msg({
                    'event': 'cancelOrder',
                    'token': token,
                    'reqid': reqid,
                    'txid': [msg.reqid],  # should be txid from submission
                })
                continue

            else:
                ep = 'addOrder'
                reqid = next(counter)
                ids[msg.oid] = reqid
                log.debug(
                    f"GENERATED ORDER {reqid}\n"
                    f'{ids}'
                )
                extra = {
                    'ordertype': 'limit',
                    'type': msg.action,
                }

                psym = msg.symbol.upper()
                pair = f'{psym[:3]}/{psym[3:]}'

                # call ws api to submit the order:
                # https://docs.kraken.com/websockets/#message-addOrder
                await ws.send_msg({
                    'event': ep,
                    'token': token,

                    'reqid': reqid,  # remapped-to-int uid from ems
                    'pair': pair,
                    'price': str(msg.price),
                    'volume': str(msg.size),

                    # only ensures request is valid, nothing more
                    # validate: 'true',

                } | extra)

        elif action == 'cancel':

            msg = BrokerdCancel(**request_msg)
            assert msg.oid in requests
            reqid = ids[msg.oid]

            # call ws api to cancel:
            # https://docs.kraken.com/websockets/#message-cancelOrder
            await ws.send_msg({
                'event': 'cancelOrder',
                'token': token,
                'reqid': reqid,
                'txid': [msg.reqid],  # should be txid from submission
            })

        else:
            log.error(f'Unknown order command: {request_msg}')

        # placehold for sanity checking in relay loop
        requests[msg.oid] = msg


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
        trans = await update_ledger(acctid, trades)
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
                position_msgs.append(msg.dict())

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
        ):
            reqmsgs: dict[str, BrokerdOrder] = {}

            # 2way map for ems ids to kraken int reqids..
            ids: bidict[str, int] = bidict()

            # task for processing inbound requests from ems
            n.start_soon(
                handle_order_requests,
                ws,
                client,
                ems_stream,
                token,
                reqmsgs,
                ids,
            )

            count: int = 0

            # process and relay trades events to ems
            # https://docs.kraken.com/websockets/#message-ownTrades
            async for msg in stream_messages(ws):
                match msg:
                    case [
                        trades_msgs,
                        'ownTrades',
                        {'sequence': seq},
                    ]:
                        # XXX: do we actually need this orrr?
                        # ensure that we are only processing new trades?
                        assert seq > count
                        count += 1

                        # flatten msgs for processing
                        trades = {
                            tid: trade
                            for entry in trades_msgs
                            for (tid, trade) in entry.items()

                            # only emit entries which are already not-in-ledger
                            if tid not in {r.tid for r in trans}
                        }
                        for tid, trade in trades.items():

                            # parse-cast
                            reqid = trade['ordertxid']
                            action = trade['type']
                            price = float(trade['price'])
                            size = float(trade['vol'])
                            broker_time = float(trade['time'])

                            # send a fill msg for gui update
                            fill_msg = BrokerdFill(
                                reqid=reqid,
                                time_ns=time.time_ns(),

                                action=action,
                                size=size,
                                price=price,
                                # TODO: maybe capture more msg data
                                # i.e fees?
                                broker_details={'name': 'kraken'},
                                broker_time=broker_time
                            )
                            await ems_stream.send(fill_msg.dict())

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
                            await ems_stream.send(filled_msg.dict())

                        # update ledger and position tracking
                        trans = await update_ledger(acctid, trades)
                        active, closed = pp.update_pps_conf(
                            'kraken',
                            acctid,
                            trade_records=trans,
                            ledger_reload={}.fromkeys(
                                t.bsuid for t in trans),
                        )

                        # emit pp msgs
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
                            await ems_stream.send(pp_msg.dict())

                    case [
                        order_msgs,
                        'openOrders',
                        {'sequence': seq},
                    ]:
                        # TODO: async order update handling which we
                        # should remove from `handle_order_requests()`
                        # above:
                        # https://github.com/pikers/piker/issues/293
                        # https://github.com/pikers/piker/issues/310
                        log.info(f'Orders update {seq}:{order_msgs}')

                        for order_msg in order_msgs:
                            log.info(
                                'Order msg update:\n'
                                f'{pformat(order_msg)}'
                            )
                            txid, update_msg = list(order_msg.items())[0]
                            match update_msg:
                                case {
                                    'status': status,
                                    'userref': reqid,
                                    **rest,

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
                                        'closed': 'cancelled',
                                        'canceled': 'cancelled',
                                        'pending': 'pending',
                                    }[status]

                                    submit_vlm = rest.get('vol', 0)
                                    exec_vlm = rest.get('vol_exec', 0)

                                    # send BrokerdStatus messages for all
                                    # order state updates
                                    msg = BrokerdStatus(

                                        reqid=txid,
                                        time_ns=time.time_ns(),  # cuz why not
                                        account=f'kraken.{acctid}',

                                        # everyone doin camel case..
                                        status=ems_status,  # force lower case

                                        filled=exec_vlm,
                                        reason='',  # why held?
                                        remaining=(
                                            float(submit_vlm)
                                            -
                                            float(exec_vlm)
                                        ),

                                        broker_details=dict(
                                            {'name': 'kraken'}, **update_msg
                                        ),
                                    )
                                    await ems_stream.send(msg.dict())

                                case _:
                                    log.warning(
                                        'Unknown orders msg:\n'
                                        f'{txid}:{order_msg}'
                                    )

                    case {
                        'event': etype,
                        'status': status,
                        'errorMessage': errmsg,
                        'reqid': reqid,
                    } if (
                        etype in {'addOrderStatus', 'editOrderStatus'}
                        and status == 'error'
                    ):
                        log.error(
                            f'Failed to submit order {reqid}:\n'
                            f'{errmsg}'
                        )
                        oid = ids.inverse[reqid]
                        order = reqmsgs[oid]
                        await ems_stream.send(
                            BrokerdError(
                                oid=oid,
                                # use old reqid in case it changed?
                                reqid=order.reqid,
                                symbol=order.symbol,
                                reason=f'Failed submit:\n{errmsg}',
                                broker_details=resp
                            ).dict()
                        )

                        # if we rx any error cancel the order again
                        await ws.send_msg({
                            'event': 'cancelOrder',
                            'token': token,
                            'reqid': reqid,
                            'txid': [order.reqid],  # txid from submission
                        })

                    case {
                        'event': 'addOrderStatus',
                        'status': status,
                        'reqid': reqid,  # oid from ems side

                        # NOTE: in the case of an edit request this is
                        # a new value!
                        'txid': txid,

                        'descr': descr,  # only on success?
                        # 'originaltxid': txid,  # only on edits
                        # **rest,
                    }:
                        oid = ids.inverse[reqid]
                        order = reqmsgs[oid]
                        log.info(
                            f'Submitting order {oid}[{reqid}]:\n'
                            f'txid: {txid}\n'
                            f'{descr}'
                        )

                        # deliver ack immediately
                        await ems_stream.send(
                            BrokerdOrderAck(
                                oid=oid,  # ems order request id
                                reqid=txid,  # kraken unique order id
                                account=order.account,  # piker account
                            ).dict()
                        )

                    case {
                        'event': 'editOrderStatus',
                        'status': status,
                        'errorMessage': errmsg,
                        'reqid': reqid,  # oid from ems side
                        'descr': descr,

                        # NOTE: for edit request this is a new value
                        'txid': txid,
                        'originaltxid': origtxid,
                        # **rest,
                    }:
                        log.info(
                            f'Editting order {oid}[{reqid}]:\n'
                            f'txid: {origtxid} -> {txid}\n'
                            f'{descr}'
                        )
                        # deliver another ack to update the ems-side
                        # `.reqid`.
                        oid = ids.inverse[reqid]
                        await ems_stream.send(
                            BrokerdOrderAck(
                                oid=oid,  # ems order request id
                                reqid=txid,  # kraken unique order id
                                account=order.account,  # piker account
                            ).dict()
                        )

                    # successful cancellation
                    case {
                        "event": "cancelOrderStatus",
                        "status": "ok",
                        'txid': txids,
                        'reqid': reqid,
                    }:
                        # TODO: should we support "batch" acking of
                        # multiple cancels thus avoiding the below loop?
                        oid = ids.inverse[reqid]
                        msg = reqmsgs[oid]

                        for txid in txids:
                            await ems_stream.send(
                                BrokerdStatus(
                                    reqid=txid,
                                    account=msg.account,
                                    time_ns=time.time_ns(),
                                    status='cancelled',
                                    reason='Cancel success: {oid}@{txid}',
                                    broker_details=resp,
                                ).dict()
                            )

                    # failed cancel
                    case {
                       "event": "cancelOrderStatus",
                       "status": "error",
                       "errorMessage": errmsg,
                       'reqid': reqid,
                    }:
                        oid = ids.inverse[reqid]
                        msg = reqmsgs[oid]

                        await ems_stream.send(
                            BrokerdError(
                                oid=oid,
                                reqid=msg.reqid,
                                symbol=msg.symbol,
                                reason=f'Failed order cancel {errmsg}',
                                broker_details=resp
                            ).dict()
                        )

                    case _:
                        log.warning(f'Unhandled trades msg: {msg}')


def norm_trade_records(
    ledger: dict[str, Any],

) -> list[pp.Transaction]:

    records: list[pp.Transaction] = []

    for tid, record in ledger.items():

        size = record.get('vol') * {
            'buy': 1,
            'sell': -1,
        }[record['type']]
        bsuid = record['pair']
        norm_sym = normalize_symbol(bsuid)

        records.append(
            pp.Transaction(
                fqsn=f'{norm_sym}.kraken',
                tid=tid,
                size=float(size),
                price=float(record['price']),
                cost=float(record['fee']),
                dt=pendulum.from_timestamp(float(record['time'])),
                bsuid=bsuid,

                # XXX: there are no derivs on kraken right?
                # expiry=expiry,
            )
        )

    return records


async def update_ledger(
    acctid: str,
    trade_entries: list[dict[str, Any]],

) -> list[pp.Transaction]:

    # write recent session's trades to the user's (local) ledger file.
    with pp.open_trade_ledger(
        'kraken',
        acctid,
    ) as ledger:
        ledger.update(trade_entries)

    # normalize to transaction form
    records = norm_trade_records(trade_entries)
    return records
