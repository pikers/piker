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
from collections import ChainMap
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
)
from piker.clearing._messages import (
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdStatus,
    BrokerdPosition,
    BrokerdFill,
    BrokerdCancel,
    BrokerdError,
)
from .venues import Pair
from .api import Client

log = get_logger('piker.brokers.binance')


async def handle_order_requests(
    ems_order_stream: tractor.MsgStream,
    client: Client,

    # TODO: update this from open orders loaded at boot!
    dialogs: ChainMap[str, BrokerdOrder] = ChainMap(),

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
                        symbol=cancel.symbol,
                        reason=(
                            'Invalid `binance` order request dialog oid',
                        )
                    ))
                    continue

                else:
                    await client.submit_cancel(
                        cancel.symbol,
                        cancel.oid,
                    )

            case {
                'account': ('binance.futes' | 'binance.spot') as account,
                'action': action,
            } if action in {'buy', 'sell'}:

                # validate
                order = BrokerdOrder(**msg)

                # NOTE: check and report edits
                if existing := dialogs.get(order.oid):
                    log.info(
                        f'Existing order for {existing.oid} updated:\n'
                        f'{pformat(existing.to_dict())} -> {pformat(msg)}'
                    )
                    # TODO: figure out what special params we have to send?
                    # https://binance-docs.github.io/apidocs/futures/en/#modify-order-trade

                # XXX: ACK the request **immediately** before sending
                # the api side request to ensure the ems maps the oid ->
                # reqid correctly!
                resp = BrokerdOrderAck(
                    oid=order.oid,  # ems order request id
                    reqid=order.oid,  # our custom int mapping
                    account='binance',  # piker account
                )
                await ems_order_stream.send(resp)

                # call our client api to submit the order
                reqid = await client.submit_limit(
                    symbol=order.symbol,
                    side=order.action,
                    quantity=order.size,
                    price=order.price,
                    oid=order.oid
                )
                # thank god at least someone lets us do this XD
                assert reqid == order.oid

                # track latest request state
                dialogs[reqid].maps.append(msg)

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
                            'Invalid `binance` broker request msg:\n{msg}'
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

    # table: PpTable
    # ledger: TransactionLedger

    # TODO: load pps and accounts using accounting apis!
    async with (
        open_cached_client('binance') as client,
    ):
        client.mkt_mode: str = 'usdtm_futes'

        # if client.
        account: str = client.mkt_mode

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
                ],
                "id": nsid
            })

            with trio.fail_after(1):
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
                    accounts['binance.usdtm_futes'] = alias

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

                        pair: Pair | None
                        if (
                            pair := client._venue2pairs[account].get(bs_mktid)
                            and entry_size > 0
                        ):
                            entry_price: float = float(entry['entryPrice'])

                            ppmsg = BrokerdPosition(
                                broker='binance',
                                account='binance.futes',

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

            async with (
                trio.open_nursery() as tn,
                ctx.open_stream() as ems_stream,
            ):

                tn.start_soon(
                    handle_order_requests,
                    ems_stream,
                    client,
                )
                tn.start_soon(
                    handle_order_updates,
                    ems_stream,
                    wss,

                )

                await trio.sleep_forever()


async def handle_order_updates(
    ems_stream: tractor.MsgStream,
    wss: NoBsWs,

    # apiflows: dict[int, ChainMap[dict[str, dict]]],
    # ids: bidict[str, int],
    # reqids2txids: bidict[int, str],

    # table: PpTable,
    # ledger_trans: dict[str, Transaction],

    # acctid: str,
    # acc_name: str,
    # token: str,

) -> None:
    '''
    Main msg handling loop for all things order management.

    This code is broken out to make the context explicit and state
    variables defined in the signature clear to the reader.

    '''
    async for msg in wss:
        match msg:

            # TODO:
            # POSITION update
            # futes: https://binance-docs.github.io/apidocs/futures/en/#event-balance-and-position-update

            # ORDER update
            # spot: https://binance-docs.github.io/apidocs/spot/en/#payload-balance-update
            # futes: https://binance-docs.github.io/apidocs/futures/en/#event-order-update
            case {
                'e': 'executionReport',
                'T': float(epoch_ms),
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

                    # prices
                    'a': float(submit_price),
                    'ap': float(avg_price),
                    'L': float(fill_price),

                    # sizing
                    'q': float(req_size),
                    'l': float(clear_size_filled),  # this event
                    'z': float(accum_size_filled),  # accum

                    # commissions
                    'n': float(cost),
                    'N': str(cost_asset),

                    # state
                    'S': str(side),
                    'X': str(status),
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
                match status:
                    case 'PARTIALLY_FILLED' | 'FILLED':
                        status = 'fill'

                        fill_msg = BrokerdFill(
                            time_ns=time_ns(),
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

                    case 'NEW':
                        status = 'open'

                    case 'EXPIRED':
                        status = 'canceled'

                    case _:
                        status = status.lower()

                resp = BrokerdStatus(
                    time_ns=time_ns(),
                    reqid=oid,

                    status=status,
                    filled=accum_size_filled,
                    remaining=req_size - accum_size_filled,
                    broker_details={
                        'name': 'binance',
                        'broker_time': epoch_ms / 1000.
                    }
                )
                await ems_stream.send(resp)
