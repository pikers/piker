# piker: trading gear for hackers
# Copyright (C)
#   Guillermo Rodriguez (aka ze jefe)
#   Tyler Goodlet
#   (in stewardship for pikers)

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
Live order control B)

'''
from __future__ import annotations
from pprint import pformat
from typing import (
    Any,
    AsyncIterator,
)
import time
from time import time_ns

from bidict import bidict
import tractor
import trio

from piker.accounting import (
    Asset,
    # MktPair,
)
from piker.brokers._util import (
    get_logger,
)
from piker.data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)
from piker.brokers import (
    open_cached_client,
    BrokerError,
)
from piker.clearing import OrderDialogs
from piker.clearing._messages import (
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdStatus,
    BrokerdPosition,
    BrokerdFill,
    BrokerdCancel,
    BrokerdError,
    Status,
    Order,
)
from .venues import Pair
from .api import Client

log = get_logger('piker.brokers.binance')


async def handle_order_requests(
    ems_order_stream: tractor.MsgStream,
    client: Client,
    dids: bidict[str, str],
    dialogs: OrderDialogs,

) -> None:
    '''
    Receive order requests from `emsd`, translate tramsit API calls and transmit.

    '''
    msg: dict | BrokerdOrder | BrokerdCancel
    async for msg in ems_order_stream:
        log.info(f'Rx order request:\n{pformat(msg)}')
        match msg:
            case {
                'action': 'cancel',
            }:
                cancel = BrokerdCancel(**msg)
                existing: BrokerdOrder | None = dialogs.get(cancel.oid)
                if not existing:
                    log.error(
                        f'NO Existing order-dialog for {cancel.oid}!?'
                    )
                    await ems_order_stream.send(BrokerdError(
                        oid=cancel.oid,

                        # TODO: do we need the symbol?
                        # https://github.com/pikers/piker/issues/514
                        symbol='unknown',

                        reason=(
                            'Invalid `binance` order request dialog oid',
                        )
                    ))
                    continue

                else:
                    symbol: str = existing['symbol']
                    try:
                        await client.submit_cancel(
                            symbol,
                            cancel.oid,
                        )
                    except BrokerError as be:
                        await ems_order_stream.send(
                            BrokerdError(
                                oid=msg['oid'],
                                symbol=symbol,
                                reason=(
                                    '`binance` CANCEL failed:\n'
                                    f'{be}'
                                ))
                        )
                        continue

            case {
                'account': ('binance.usdtm' | 'binance.spot') as account,
                'action': action,
            } if action in {'buy', 'sell'}:

                # validate
                order = BrokerdOrder(**msg)
                oid: str = order.oid  # emsd order id
                modify: bool = False

                # NOTE: check and report edits
                if existing := dialogs.get(order.oid):
                    log.info(
                        f'Existing order for {oid} updated:\n'
                        f'{pformat(existing.maps[-1])} -> {pformat(msg)}'
                    )
                    modify = True

                    # only add new msg AFTER the existing check
                    dialogs.add_msg(oid, msg)

                else:
                    # XXX NOTE: update before the ack!
                    # track latest request state such that map
                    # lookups start at the most recent msg and then
                    # scan reverse-chronologically.
                    dialogs.add_msg(oid, msg)

                    # XXX: ACK the request **immediately** before sending
                    # the api side request to ensure the ems maps the oid ->
                    # reqid correctly!
                    resp = BrokerdOrderAck(
                        oid=oid,  # ems order request id
                        reqid=oid,  # our custom int mapping
                        account='binance',  # piker account
                    )
                    await ems_order_stream.send(resp)

                # call our client api to submit the order
                # NOTE: modifies only require diff key for user oid:
                # https://binance-docs.github.io/apidocs/futures/en/#modify-order-trade
                try:
                    reqid = await client.submit_limit(
                        symbol=order.symbol,
                        side=order.action,
                        quantity=order.size,
                        price=order.price,
                        oid=oid,
                        modify=modify,
                    )

                    # SMH they do gen their own order id: ints.. 
                    # assert reqid == order.oid
                    dids[order.oid] = reqid

                except BrokerError as be:
                    await ems_order_stream.send(
                        BrokerdError(
                            oid=msg['oid'],
                            symbol=msg['symbol'],
                            reason=(
                                '`binance` request failed:\n'
                                f'{be}'
                            ))
                    )
                    continue

            case _:
                account = msg.get('account')
                if account not in {'binance.spot', 'binance.futes'}:
                    log.error(
                        'This is a binance account, \
                        only a `binance.spot/.futes` selection is valid'
                    )
                await ems_order_stream.send(
                    BrokerdError(
                        oid=msg['oid'],
                        symbol=msg['symbol'],
                        reason=(
                            f'Invalid `binance` broker request msg:\n{msg}'
                        ))
                )


@tractor.context
async def open_trade_dialog(
    ctx: tractor.Context,

) -> AsyncIterator[dict[str, Any]]:

    async with open_cached_client('binance') as client:
        if not client.api_key:
            await ctx.started('paper')
            return

    async with (
        open_cached_client('binance') as client,
    ):
        client.mkt_mode: str = 'usdtm_futes'

        # if client.
        venue: str = client.mkt_mode

        wss: NoBsWs
        async with (
            client.manage_listen_key() as listen_key,
            open_autorecon_ws(
                f'wss://stream.binancefuture.com/ws/{listen_key}',
                # f'wss://stream.binance.com:9443/ws/{listen_key}',
            ) as wss,

        ):
            nsid: int = time_ns()
            await wss.send_msg({
                # "method": "SUBSCRIBE",
                "method": "REQUEST",
                "params":
                [
                    f"{listen_key}@account",
                    f"{listen_key}@balance",
                    f"{listen_key}@position",

                    # TODO: does this even work!? seems to cause
                    # a hang on the first msg..? lelelel.
                    # f"{listen_key}@order",
                ],
                "id": nsid
            })

            with trio.fail_after(6):
                msg = await wss.recv_msg()
                assert msg['id'] == nsid

            # TODO: load other market wide data / statistics:
            # - OI: https://binance-docs.github.io/apidocs/futures/en/#open-interest
            # - OI stats: https://binance-docs.github.io/apidocs/futures/en/#open-interest-statistics
            accounts: bidict[str, str] = bidict()
            balances: dict[Asset, float] = {}
            positions: list[BrokerdPosition] = []

            for resp_dict in msg['result']:
                resp = resp_dict['res']
                req: str = resp_dict['req']

                # @account response should be something like:
                # {'accountAlias': 'sRFzFzAuuXsR',
                #  'canDeposit': True,
                #  'canTrade': True,
                #  'canWithdraw': True,
                #  'feeTier': 0}
                if 'account' in req:
                    alias: str = resp['accountAlias']
                    accounts['binance.usdtm'] = alias

                # @balance response:
                # {'accountAlias': 'sRFzFzAuuXsR',
                #      'balances': [{'asset': 'BTC',
                #                    'availableBalance': '0.00000000',
                #                    'balance': '0.00000000',
                #                    'crossUnPnl': '0.00000000',
                #                    'crossWalletBalance': '0.00000000',
                #                    'maxWithdrawAmount': '0.00000000',
                #                    'updateTime': 0}]
                #                     ...
                # }
                elif 'balance' in req:
                    for entry in resp['balances']:
                        name: str = entry['asset']
                        balance: float = float(entry['balance'])
                        last_update_t: int = entry['updateTime']

                        spot_asset: Asset = client._venue2assets['spot'][name]

                        if balance > 0:
                            balances[spot_asset] = (balance, last_update_t)
                            # await tractor.breakpoint()

                # @position response:
                # {'positions': [{'entryPrice': '0.0',
                #                    'isAutoAddMargin': False,
                #                    'isolatedMargin': '0',
                #                    'leverage': 20,
                #                    'liquidationPrice': '0',
                #                    'marginType': 'CROSSED',
                #                    'markPrice': '0.60289650',
                #                    'markPrice': '0.00000000',
                #                    'maxNotionalValue': '25000',
                #                    'notional': '0',
                #                    'positionAmt': '0',
                #                    'positionSide': 'BOTH',
                #                    'symbol': 'ETHUSDT_230630',
                #                    'unRealizedProfit': '0.00000000',
                #                    'updateTime': 1672741444894}
                #                    ...
                # }
                elif 'position' in req:
                    for entry in resp['positions']:
                        bs_mktid: str = entry['symbol']
                        entry_size: float = float(entry['positionAmt'])

                        pair: Pair | None = client._venue2pairs[venue].get(bs_mktid)
                        if (
                            pair
                            and entry_size > 0
                        ):
                            entry_price: float = float(entry['entryPrice'])

                            ppmsg = BrokerdPosition(
                                broker='binance',
                                account='binance.usdtm',

                                # TODO: maybe we should be passing back
                                # a `MktPair` here?
                                symbol=pair.bs_fqme.lower() + '.binance',

                                size=entry_size,
                                avg_price=entry_price,
                            )
                            positions.append(ppmsg)

                        if pair is None:
                            log.warning(
                                f'`{bs_mktid}` Position entry but no market pair?\n'
                                f'{pformat(entry)}\n'
                            )

            await ctx.started((positions, list(accounts)))

            dialogs = OrderDialogs()
            dids: dict[str, int] = bidict()

            # TODO: further init setup things to get full EMS and
            # .accounting support B)
            # - live order loading via user stream subscription and
            #   update to the order dialog table.
            #   - MAKE SURE we add live orders loaded during init
            #   into the dialogs table to ensure they can be
            #   cancelled, meaning we can do a symbol lookup.
            # - position loading using `piker.accounting` subsys
            #   and comparison with binance's own position calcs.
            # - load pps and accounts using accounting apis, write
            #   the ledger and account files
            #   - table: PpTable
            #   - ledger: TransactionLedger

            async with (
                trio.open_nursery() as tn,
                ctx.open_stream() as ems_stream,
            ):
                # deliver all pre-exist open orders to EMS thus syncing
                # state with existing live limits reported by them.
                order: Order
                for order in await client.get_open_orders():
                    status_msg = Status(
                        time_ns=time.time_ns(),
                        resp='open',
                        oid=order.oid,
                        reqid=order.oid,

                        # embedded order info
                        req=order,
                        src='binance',
                    )
                    dialogs.add_msg(order.oid, order.to_dict())
                    await ems_stream.send(status_msg)

                tn.start_soon(
                    handle_order_requests,
                    ems_stream,
                    client,
                    dids,
                    dialogs,
                )
                tn.start_soon(
                    handle_order_updates,
                    venue,
                    client,
                    ems_stream,
                    wss,
                    dialogs,

                )

                await trio.sleep_forever()


async def handle_order_updates(
    venue: str,
    client: Client,
    ems_stream: tractor.MsgStream,
    wss: NoBsWs,
    dialogs: OrderDialogs,

) -> None:
    '''
    Main msg handling loop for all things order management.

    This code is broken out to make the context explicit and state
    variables defined in the signature clear to the reader.

    '''
    async for msg in wss:
        log.info(f'Rx USERSTREAM msg:\n{pformat(msg)}')
        match msg:

            # ORDER update
            # spot: https://binance-docs.github.io/apidocs/spot/en/#payload-balance-update
            # futes: https://binance-docs.github.io/apidocs/futures/en/#event-order-update
            # futes: https://binance-docs.github.io/apidocs/futures/en/#event-balance-and-position-update
            # {'o': {
            #    'L': '0',
            #    'N': 'USDT',
            #    'R': False,
            #    'S': 'BUY',
            #    'T': 1687028772484,
            #    'X': 'NEW',
            #    'a': '0',
            #    'ap': '0',
            #    'b': '7012.06520',
            #    'c': '518d4122-8d3e-49b0-9a1e-1fabe6f62e4c',
            #    'cp': False,
            #    'f': 'GTC',
            #    'i': 3376956924,
            #    'l': '0',
            #    'm': False,
            #    'n': '0',
            #    'o': 'LIMIT',
            #    'ot': 'LIMIT',
            #    'p': '21136.80',
            #    'pP': False,
            #    'ps': 'BOTH',
            #    'q': '0.047',
            #    'rp': '0',
            #    's': 'BTCUSDT',
            #    'si': 0,
            #    'sp': '0',
            #    'ss': 0,
            #    't': 0,
            #    'wt': 'CONTRACT_PRICE',
            #    'x': 'NEW',
            #    'z': '0'}
            # }
            case {
                # 'e': 'executionReport',
                'e': 'ORDER_TRADE_UPDATE',
                'T': int(epoch_ms),
                'o': {
                    's': bs_mktid,

                    # XXX NOTE XXX see special ids for market
                    # events or margin calls:
                    # // special client order id:
                    # // starts with "autoclose-": liquidation order
                    # // "adl_autoclose": ADL auto close order
                    # // "settlement_autoclose-": settlement order
                    #     for delisting or delivery
                    'c': oid,
                    # 'i': reqid,  # binance internal int id

                    # prices
                    'a': submit_price,
                    'ap': avg_price,
                    'L': fill_price,

                    # sizing
                    'q': req_size,
                    'l': clear_size_filled,  # this event
                    'z': accum_size_filled,  # accum

                    # commissions
                    'n': cost,
                    'N': cost_asset,

                    # state
                    'S': side,
                    'X': status,
                },
            } as order_msg:
                log.info(
                    f'{status} for {side} ORDER oid: {oid}\n'
                    f'bs_mktid: {bs_mktid}\n\n'

                    f'order size: {req_size}\n'
                    f'cleared size: {clear_size_filled}\n'
                    f'accum filled size: {accum_size_filled}\n\n'

                    f'submit price: {submit_price}\n'
                    f'fill_price: {fill_price}\n'
                    f'avg clearing price: {avg_price}\n\n'

                    f'cost: {cost}@{cost_asset}\n'
                )

                # status remap from binance to piker's
                # status set:
                # - NEW
                # - PARTIALLY_FILLED
                # - FILLED
                # - CANCELED
                # - EXPIRED
                # https://binance-docs.github.io/apidocs/futures/en/#event-order-update

                req_size: float = float(req_size)
                accum_size_filled: float = float(accum_size_filled)
                fill_price: float = float(fill_price)

                match status:
                    case 'PARTIALLY_FILLED' | 'FILLED':
                        status = 'fill'

                        fill_msg = BrokerdFill(
                            time_ns=time_ns(),
                            # reqid=reqid,
                            reqid=oid,

                            # just use size value for now?
                            # action=action,
                            size=clear_size_filled,
                            price=fill_price,

                            # TODO: maybe capture more msg data
                            # i.e fees?
                            broker_details={'name': 'broker'} | order_msg,
                            broker_time=time.time(),
                        )
                        await ems_stream.send(fill_msg)

                        if accum_size_filled == req_size:
                            status = 'closed'
                            dialogs.pop(oid)

                    case 'NEW':
                        status = 'open'

                    case 'EXPIRED':
                        status = 'canceled'
                        dialogs.pop(oid)

                    case _:
                        status = status.lower()

                resp = BrokerdStatus(
                    time_ns=time_ns(),
                    # reqid=reqid,
                    reqid=oid,
                    # account='binance.usdtm',

                    status=status,

                    filled=accum_size_filled,
                    remaining=req_size - accum_size_filled,
                    broker_details={
                        'name': 'binance',
                        'broker_time': epoch_ms / 1000.
                    }
                )
                await ems_stream.send(resp)

            # ACCOUNT and POSITION update B)
            # {
            #  'E': 1687036749218,
            #  'e': 'ACCOUNT_UPDATE'
            #  'T': 1687036749215,
            #  'a': {'B': [{'a': 'USDT',
            #               'bc': '0',
            #               'cw': '1267.48920735',
            #               'wb': '1410.90245576'}],
            #        'P': [{'cr': '-3292.10973007',
            #               'ep': '26349.90000',
            #               'iw': '143.41324841',
            #               'ma': 'USDT',
            #               'mt': 'isolated',
            #               'pa': '0.038',
            #               'ps': 'BOTH',
            #               's': 'BTCUSDT',
            #               'up': '5.17555453'}],
            #        'm': 'ORDER'},
            # }
            case {
                'T': int(epoch_ms),
                'e': 'ACCOUNT_UPDATE',
                'a': {
                    'P': [{
                        's': bs_mktid,
                        'pa': pos_amount,
                        'ep': entry_price,
                    }],
                },
            }:
                # real-time relay position updates back to EMS
                pair: Pair | None = client._venue2pairs[venue].get(bs_mktid)
                ppmsg = BrokerdPosition(
                    broker='binance',
                    account='binance.usdtm',

                    # TODO: maybe we should be passing back
                    # a `MktPair` here?
                    symbol=pair.bs_fqme.lower() + '.binance',

                    size=float(pos_amount),
                    avg_price=float(entry_price),
                )
                await ems_stream.send(ppmsg)

            case _:
                log.warning(
                    'Unhandled event:\n'
                    f'{pformat(msg)}'
                )
