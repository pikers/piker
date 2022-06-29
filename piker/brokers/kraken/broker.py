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
from pprint import pformat
import time
from typing import (
    Any,
    AsyncIterator,
    # Callable,
    # Optional,
    # Union,
)

from pydantic import BaseModel
import trio
import tractor
import wsproto

from piker.clearing._paper_engine import PaperBoi
from piker.clearing._messages import (
    BrokerdPosition, BrokerdOrder, BrokerdStatus,
    BrokerdOrderAck, BrokerdError, BrokerdCancel,
    BrokerdFill,
)
from .api import (
    Client,
    BrokerError,
    get_client,
    log,
    normalize_symbol,
)
from .feed import (
    get_console_log,
    open_autorecon_ws,
    NoBsWs,
    stream_messages,
)


class Trade(BaseModel):
    '''
    Trade class that helps parse and validate ownTrades stream

    '''
    reqid: str  # kraken order transaction id
    action: str  # buy or sell
    price: str  # price of asset
    size: str  # vol of asset
    broker_time: str  # e.g GTC, GTD


def pack_positions(
    acc: str,
    trades: dict

) -> list[Any]:
    positions: dict[str, float] = {}
    vols: dict[str, float] = {}
    costs: dict[str, float] = {}
    position_msgs: list[Any] = []

    for trade in trades.values():
        sign = -1 if trade['type'] == 'sell' else 1
        pair = trade['pair']
        vol = float(trade['vol'])
        vols[pair] = vols.get(pair, 0) + sign * vol
        costs[pair] = costs.get(pair, 0) + sign * float(trade['cost'])
        positions[pair] = costs[pair] / vols[pair] if vols[pair] else 0

    for ticker, pos in positions.items():
        vol = float(vols[ticker])
        if not vol:
            continue
        norm_sym = normalize_symbol(ticker)
        msg = BrokerdPosition(
            broker='kraken',
            account=acc,
            symbol=norm_sym,
            currency=norm_sym[-3:],
            size=vol,
            avg_price=float(pos),
        )
        position_msgs.append(msg.dict())

    return position_msgs


async def handle_order_requests(

    client: Client,
    ems_order_stream: tractor.MsgStream,

) -> None:

    request_msg: dict
    order: BrokerdOrder

    async for request_msg in ems_order_stream:
        log.info(
            'Received order request:\n'
            f'{pformat(request_msg)}'
        )

        action = request_msg['action']

        if action in {'buy', 'sell'}:

            account = request_msg['account']
            if account != 'kraken.spot':
                log.error(
                    'This is a kraken account, \
                    only a `kraken.spot` selection is valid'
                )
                await ems_order_stream.send(BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],

                    # reason=f'Kraken only, No account found: `{account}` ?',
                    reason=(
                        'Kraken only, order mode disabled due to '
                        'https://github.com/pikers/piker/issues/299'
                    ),

                ).dict())
                continue

            # validate
            order = BrokerdOrder(**request_msg)
            # call our client api to submit the order
            resp = await client.submit_limit(
                symbol=order.symbol,
                price=order.price,
                action=order.action,
                size=order.size,
                reqid=order.reqid,
            )

            err = resp['error']
            if err:
                oid = order.oid
                log.error(f'Failed to submit order: {oid}')

                await ems_order_stream.send(
                    BrokerdError(
                        oid=order.oid,
                        reqid=order.reqid,
                        symbol=order.symbol,
                        reason="Failed order submission",
                        broker_details=resp
                    ).dict()
                )
            else:
                # TODO: handle multiple orders (cancels?)
                #       txid is an array of strings
                if order.reqid is None:
                    reqid = resp['result']['txid'][0]
                else:
                    # update the internal pairing of oid to krakens
                    # txid with the new txid that is returned on edit
                    reqid = resp['result']['txid']

                # deliver ack that order has been submitted to broker routing
                await ems_order_stream.send(
                    BrokerdOrderAck(

                        # ems order request id
                        oid=order.oid,

                        # broker specific request id
                        reqid=reqid,

                        # account the made the order
                        account=order.account

                    ).dict()
                )

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)

            # Send order cancellation to kraken
            resp = await client.submit_cancel(
                reqid=msg.reqid
            )

            # Check to make sure there was no error returned by
            # the kraken endpoint. Assert one order was cancelled.
            try:
                result = resp['result']
                count = result['count']

            # check for 'error' key if we received no 'result'
            except KeyError:
                error = resp.get('error')

                await ems_order_stream.send(
                    BrokerdError(
                        oid=msg.oid,
                        reqid=msg.reqid,
                        symbol=msg.symbol,
                        reason="Failed order cancel",
                        broker_details=resp
                    ).dict()
                )

                if not error:
                    raise BrokerError(f'Unknown order cancel response: {resp}')

            else:
                if not count:  # no orders were cancelled?

                    # XXX: what exactly is this from and why would we care?
                    # there doesn't seem to be any docs here?
                    # https://docs.kraken.com/rest/#operation/cancelOrder

                    # Check to make sure the cancellation is NOT pending,
                    # then send the confirmation to the ems order stream
                    pending = result.get('pending')
                    if pending:
                        log.error(f'Order {oid} cancel was not yet successful')

                        await ems_order_stream.send(
                            BrokerdError(
                                oid=msg.oid,
                                reqid=msg.reqid,
                                symbol=msg.symbol,
                                # TODO: maybe figure out if pending
                                # cancels will eventually get cancelled
                                reason="Order cancel is still pending?",
                                broker_details=resp
                            ).dict()
                        )

                else:  # order cancel success case.

                    await ems_order_stream.send(
                        BrokerdStatus(
                            reqid=msg.reqid,
                            account=msg.account,
                            time_ns=time.time_ns(),
                            status='cancelled',
                            reason='Order cancelled',
                            broker_details={'name': 'kraken'}
                        ).dict()
                    )
    else:
        log.error(f'Unknown order command: {request_msg}')


def make_auth_sub(data: dict[str, Any]) -> dict[str, str]:
    '''
    Create a request subscription packet dict.

    ## TODO: point to the auth urls
    https://docs.kraken.com/websockets/#message-subscribe

    '''
    # eg. specific logic for this in kraken's sync client:
    # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
    return {
        'event': 'subscribe',
        'subscription': data,
    }


@tractor.context
async def trades_dialogue(
    ctx: tractor.Context,
    loglevel: str = None,
) -> AsyncIterator[dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    @acm
    async def subscribe(ws: wsproto.WSConnection, token: str):
        # XXX: setup subs
        # https://docs.kraken.com/websockets/#message-subscribe
        # specific logic for this in kraken's shitty sync client:
        # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
        trades_sub = make_auth_sub(
            {'name': 'ownTrades', 'token': token}
        )

        # TODO: we want to eventually allow unsubs which should
        # be completely fine to request from a separate task
        # since internally the ws methods appear to be FIFO
        # locked.
        await ws.send_msg(trades_sub)

        yield

        # unsub from all pairs on teardown
        await ws.send_msg({
            'event': 'unsubscribe',
            'subscription': ['ownTrades'],
        })

        # XXX: do we need to ack the unsub?
        # await ws.recv_msg()

    # Authenticated block
    async with get_client() as client:
        if not client._api_key:
            log.error('Missing Kraken API key: Trades WS connection failed')
            await ctx.started(({}, ['paper']))

            async with (
                ctx.open_stream() as ems_stream,
                trio.open_nursery() as n,
            ):

                client = PaperBoi(
                    'kraken',
                    ems_stream,
                    _buys={},
                    _sells={},

                    _reqids={},

                    # TODO: load paper positions from ``positions.toml``
                    _positions={},
                )

                # TODO: maybe add multiple accounts
                n.start_soon(handle_order_requests, client, ems_stream)

        # pull and deliver trades ledger
        acc_name = 'kraken.' + client._name
        trades = await client.get_trades()
        log.info(
            f'Loaded {len(trades)} trades from account `{acc_name}`'
        )
        position_msgs = pack_positions(acc_name, trades)
        await ctx.started((position_msgs, (acc_name,)))

        # Get websocket token for authenticated data stream
        # Assert that a token was actually received.
        resp = await client.endpoint('GetWebSocketsToken', {})

        # lol wtf is this..
        assert resp['error'] == []
        token = resp['result']['token']

        async with (
            ctx.open_stream() as ems_stream,
            open_autorecon_ws(
                'wss://ws-auth.kraken.com/',
                fixture=subscribe,
                token=token,
            ) as ws,
            trio.open_nursery() as n,
        ):
            # task for processing inbound requests from ems
            n.start_soon(handle_order_requests, client, ems_stream)

            # begin trade event processing
            async for msg in process_trade_msgs(ws):
                for trade in msg:
                    match trade:
                        # prepare and send a filled status update
                        case Trade():
                            filled_msg = BrokerdStatus(
                                reqid=trade.reqid,
                                time_ns=time.time_ns(),

                                account=acc_name,
                                status='filled',
                                filled=float(trade.size),
                                reason='Order filled by kraken',
                                broker_details={
                                    'name': 'kraken',
                                    'broker_time': trade.broker_time
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

                            # send a fill msg for gui update
                            fill_msg = BrokerdFill(
                                reqid=trade.reqid,
                                time_ns=time.time_ns(),

                                action=trade.action,
                                size=float(trade.size),
                                price=float(trade.price),
                                # TODO: maybe capture more msg data i.e fees?
                                broker_details={'name': 'kraken'},
                                broker_time=float(trade.broker_time)
                            )

                            await ems_stream.send(fill_msg.dict())

                        case _:
                            log.warning(f'Unhandled trades msg: {trade}')
                            await tractor.breakpoint()


async def process_trade_msgs(
    ws: NoBsWs,
):
    '''
    Parse and pack trades subscription messages, deliver framed
    sequences of messages?

    '''
    sequence_counter = 0
    async for msg in stream_messages(ws):

        try:
            # check that we are on the ownTrades stream and that msgs
            # are arriving in sequence with kraken For clarification the
            # kraken ws api docs for this stream:
            # https://docs.kraken.com/websockets/#message-ownTrades
            assert msg[1] == 'ownTrades'
            assert msg[2]['sequence'] > sequence_counter
            sequence_counter += 1
            raw_msgs = msg[0]
            trade_msgs = []

            # Check that we are only processing new trades
            if msg[2]['sequence'] != 1:
                # check if its a new order or an update msg
                for trade_msg in raw_msgs:
                    trade = list(trade_msg.values())[0]
                    order_msg = Trade(
                        reqid=trade['ordertxid'],
                        action=trade['type'],
                        price=trade['price'],
                        size=trade['vol'],
                        broker_time=trade['time']
                    )
                    trade_msgs.append(order_msg)

            yield trade_msgs

        except AssertionError:
            print(f'UNHANDLED MSG: {msg}')
            yield msg
