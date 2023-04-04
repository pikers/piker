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
Fake trading for forward testing.

"""
from collections import defaultdict
from contextlib import asynccontextmanager as acm
from datetime import datetime
from operator import itemgetter
import itertools
import time
from typing import (
    Callable,
)
import uuid

from bidict import bidict
import pendulum
import trio
import tractor

from .. import data
from ..data.types import Struct
from ..accounting._mktinfo import Symbol
from ..accounting import (
    Position,
    PpTable,
    Transaction,
    TransactionLedger,
    open_trade_ledger,
    open_pps,
)
from ..data._normalize import iterticks
from ..accounting._mktinfo import unpack_fqme
from ._util import (
    log,  # sub-sys logger
)
from ._messages import (
    BrokerdCancel,
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdStatus,
    BrokerdFill,
    BrokerdPosition,
    BrokerdError,
)


class PaperBoi(Struct):
    '''
    Emulates a broker order client providing approximately the same API
    and delivering an order-event response stream but with methods for
    triggering desired events based on forward testing engine
    requirements (eg open, closed, fill msgs).

    '''
    broker: str

    ems_trades_stream: tractor.MsgStream

    ppt: PpTable
    ledger: TransactionLedger

    # map of paper "live" orders which be used
    # to simulate fills based on paper engine settings
    _buys: defaultdict[str, bidict]
    _sells: defaultdict[str, bidict]
    _reqids: bidict
    _syms: dict[str, Symbol] = {}

    # init edge case L1 spread
    last_ask: tuple[float, float] = (float('inf'), 0)  # price, size
    last_bid: tuple[float, float] = (0, 0)

    async def submit_limit(
        self,
        oid: str,  # XXX: see return value
        symbol: str,
        price: float,
        action: str,
        size: float,
        reqid: str | None,

    ) -> int:
        '''
        Place an order and return integer request id provided by client.

        '''
        if action == 'alert':
            # bypass all fill simulation
            return reqid

        entry = self._reqids.get(reqid)
        if entry:
            # order is already existing, this is a modify
            (oid, symbol, action, old_price) = entry
        else:
            # register order internally
            self._reqids[reqid] = (oid, symbol, action, price)

        # TODO: net latency model
        # we checkpoint here quickly particulalry
        # for dark orders since we want the dark_executed
        # to trigger first thus creating a lookup entry
        # in the broker trades event processing loop
        await trio.sleep(0.05)

        if action == 'sell':
            size = -size

        msg = BrokerdStatus(
            status='open',
            # account=f'paper_{self.broker}',
            account='paper',
            reqid=reqid,
            time_ns=time.time_ns(),
            filled=0.0,
            reason='paper_trigger',
            remaining=size,

            broker_details={'name': 'paperboi'},
        )
        await self.ems_trades_stream.send(msg)

        # if we're already a clearing price simulate an immediate fill
        if (
            action == 'buy' and (clear_price := self.last_ask[0]) <= price
            ) or (
            action == 'sell' and (clear_price := self.last_bid[0]) >= price
        ):
            await self.fake_fill(
                symbol,
                clear_price,
                size,
                action,
                reqid,
                oid,
            )

        # register this submissions as a paper live order
        else:
            # set the simulated order in the respective table for lookup
            # and trigger by the simulated clearing task normally
            # running ``simulate_fills()``.
            if action == 'buy':
                orders = self._buys

            elif action == 'sell':
                orders = self._sells

            # {symbol -> bidict[oid, (<price data>)]}
            orders[symbol][oid] = (price, size, reqid, action)

        return reqid

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:

        # TODO: fake market simulation effects
        oid, symbol, action, price = self._reqids[reqid]

        if action == 'buy':
            self._buys[symbol].pop(oid, None)
        elif action == 'sell':
            self._sells[symbol].pop(oid, None)

        # TODO: net latency model
        await trio.sleep(0.05)

        msg = BrokerdStatus(
            status='canceled',
            account='paper',
            reqid=reqid,
            time_ns=time.time_ns(),
            broker_details={'name': 'paperboi'},
        )
        await self.ems_trades_stream.send(msg)

    async def fake_fill(
        self,

        fqme: str,
        price: float,
        size: float,
        action: str,  # one of {'buy', 'sell'}

        reqid: str,
        oid: str,

        # determine whether to send a filled status that has zero
        # remaining lots to fill
        order_complete: bool = True,
        remaining: float = 0,

    ) -> None:
        '''
        Pretend to fill a broker order @ price and size.

        '''
        # TODO: net latency model
        await trio.sleep(0.05)
        fill_time_ns = time.time_ns()
        fill_time_s = time.time()

        fill_msg = BrokerdFill(
            reqid=reqid,
            time_ns=fill_time_ns,
            action=action,
            size=size,
            price=price,
            broker_time=datetime.now().timestamp(),
            broker_details={
                'paper_info': {
                    'oid': oid,
                },
                # mocking ib
                'name': self.broker + '_paper',
            },
        )
        log.info(f'Fake filling order:\n{fill_msg}')
        await self.ems_trades_stream.send(fill_msg)

        if order_complete:
            msg = BrokerdStatus(
                reqid=reqid,
                time_ns=time.time_ns(),
                # account=f'paper_{self.broker}',
                account='paper',
                status='closed',
                filled=size,
                remaining=0 if order_complete else remaining,
            )
            await self.ems_trades_stream.send(msg)

        # NOTE: for paper we set the "bs_mktid" as just the fqme since
        # we don't actually have any unique backend symbol ourselves
        # other then this thing, our fqme address.
        bs_mktid: str = fqme
        t = Transaction(
            fqsn=fqme,
            sym=self._syms[fqme],
            tid=oid,
            size=size,
            price=price,
            cost=0,  # TODO: cost model
            dt=pendulum.from_timestamp(fill_time_s),
            bs_mktid=bs_mktid,
        )

        tx = t.to_dict()
        tx.pop('sym')

        # update in-mem ledger and pos table
        self.ledger.update({oid: tx})
        self.ppt.update_from_trans({oid: t})

        # transmit pp msg to ems
        pp = self.ppt.pps[bs_mktid]
        pp_msg = BrokerdPosition(
            broker=self.broker,
            account='paper',
            symbol=fqme,

            size=pp.size,
            avg_price=pp.ppu,

            # TODO: we need to look up the asset currency from
            # broker info. i guess for crypto this can be
            # inferred from the pair?
            # currency=bs_mktid,
        )
        await self.ems_trades_stream.send(pp_msg)

        # write all updates to filesys
        self.ledger.write_config()
        self.ppt.write_config()


async def simulate_fills(
    quote_stream: tractor.MsgStream,  # noqa
    client: PaperBoi,

) -> None:

    # TODO: more machinery to better simulate real-world market things:

    # - slippage models, check what quantopian has:
    # https://github.com/quantopian/zipline/blob/master/zipline/finance/slippage.py
    #   * this should help with simulating partial fills in a fast moving mkt
    #     afaiu

    # - commisions models, also quantopian has em:
    # https://github.com/quantopian/zipline/blob/master/zipline/finance/commission.py

    # - network latency models ??

    # - position tracking:
    # https://github.com/quantopian/zipline/blob/master/zipline/finance/ledger.py

    # this stream may eventually contain multiple symbols
    async for quotes in quote_stream:
        for sym, quote in quotes.items():
            for tick in iterticks(
                quote,
                # dark order price filter(s)
                types=('ask', 'bid', 'trade', 'last')
            ):
                tick_price = tick['price']

                buys: bidict[str, tuple] = client._buys[sym]
                iter_buys = reversed(sorted(
                    buys.values(),
                    key=itemgetter(0),
                ))

                def buy_on_ask(our_price):
                    return tick_price <= our_price

                sells: bidict[str, tuple] = client._sells[sym]
                iter_sells = sorted(
                    sells.values(),
                    key=itemgetter(0)
                )

                def sell_on_bid(our_price):
                    return tick_price >= our_price

                match tick:

                    # on an ask queue tick, only clear buy entries
                    case {
                        'price': tick_price,
                        'type': 'ask',
                    }:
                        client.last_ask = (
                            tick_price,
                            tick.get('size', client.last_ask[1]),
                        )

                        iter_entries = zip(
                            iter_buys,
                            itertools.repeat(buy_on_ask)
                        )

                    # on a bid queue tick, only clear sell entries
                    case {
                        'price': tick_price,
                        'type': 'bid',
                    }:
                        client.last_bid = (
                            tick_price,
                            tick.get('size', client.last_bid[1]),
                        )

                        iter_entries = zip(
                            iter_sells,
                            itertools.repeat(sell_on_bid)
                        )

                    # TODO: fix this block, though it definitely
                    # costs a lot more CPU-wise
                    # - doesn't seem like clears are happening still on
                    #   "resting" limit orders?
                    case {
                        'price': tick_price,
                        'type': ('trade' | 'last'),
                    }:
                        # in the clearing price / last price case we
                        # want to iterate both sides of our book for
                        # clears since we don't know which direction the
                        # price is going to move (especially with HFT)
                        # and thus we simply interleave both sides (buys
                        # and sells) until one side clears and then
                        # break until the next tick?
                        def interleave():
                            for pair in zip(
                                iter_buys,
                                iter_sells,
                            ):
                                for order_info, pred in zip(
                                    pair,
                                    itertools.cycle([buy_on_ask, sell_on_bid]),
                                ):
                                    yield order_info, pred

                        iter_entries = interleave()

                    # NOTE: all other (non-clearable) tick event types
                    # - we don't want to sping the simulated clear loop
                    # below unecessarily and further don't want to pop
                    # simulated live orders prematurely.
                    case _:
                        continue

                # iterate all potentially clearable book prices
                # in FIFO order per side.
                for order_info, pred in iter_entries:
                    (our_price, size, reqid, action) = order_info

                    # print(order_info)
                    clearable = pred(our_price)
                    if clearable:
                        # pop and retreive order info
                        oid = {
                            'buy': buys,
                            'sell': sells
                        }[action].inverse.pop(order_info)

                        # clearing price would have filled entirely
                        await client.fake_fill(
                            fqme=sym,
                            # todo slippage to determine fill price
                            price=tick_price,
                            size=size,
                            action=action,
                            reqid=reqid,
                            oid=oid,
                        )


async def handle_order_requests(

    client: PaperBoi,
    ems_order_stream: tractor.MsgStream,

) -> None:

    request_msg: dict
    async for request_msg in ems_order_stream:
        match request_msg:
            case {'action': ('buy' | 'sell')}:
                order = BrokerdOrder(**request_msg)
                account = order.account

                # error on bad inputs
                reason = None
                if account != 'paper':
                    reason = f'No account found:`{account}` (paper only)?'

                elif order.size == 0:
                    reason = 'Invalid size: 0'

                if reason:
                    log.error(reason)
                    await ems_order_stream.send(BrokerdError(
                        oid=order.oid,
                        symbol=order.symbol,
                        reason=reason,
                    ))
                    continue

                reqid = order.reqid or str(uuid.uuid4())

                # deliver ack that order has been submitted to broker routing
                await ems_order_stream.send(
                    BrokerdOrderAck(
                        oid=order.oid,
                        reqid=reqid,
                    )
                )

                # call our client api to submit the order
                reqid = await client.submit_limit(
                    oid=order.oid,
                    symbol=f'{order.symbol}.{client.broker}',
                    price=order.price,
                    action=order.action,
                    size=order.size,
                    # XXX: by default 0 tells ``ib_insync`` methods that
                    # there is no existing order so ask the client to create
                    # a new one (which it seems to do by allocating an int
                    # counter - collision prone..)
                    reqid=reqid,
                )
                log.info(f'Submitted paper LIMIT {reqid}:\n{order}')

            case {'action': 'cancel'}:
                msg = BrokerdCancel(**request_msg)
                await client.submit_cancel(
                    reqid=msg.reqid
                )

            case _:
                log.error(f'Unknown order command: {request_msg}')


_reqids: bidict[str, tuple] = {}
_buys: defaultdict[
    str,  # symbol
    bidict[
        str,  # oid
        tuple[float, float, str, str],  # order info
    ]
] = defaultdict(bidict)
_sells: defaultdict[
    str,  # symbol
    bidict[
        str,  # oid
        tuple[float, float, str, str],  # order info
    ]
] = defaultdict(bidict)


@tractor.context
async def trades_dialogue(

    ctx: tractor.Context,
    broker: str,
    fqme: str | None = None,  # if empty, we only boot broker mode
    loglevel: str = 'warning',

) -> None:

    tractor.log.get_console_log(loglevel)

    ppt: PpTable
    ledger: TransactionLedger
    with (
        open_pps(
            broker,
            'paper',
            write_on_exit=True,
        ) as ppt,

        open_trade_ledger(
            broker,
            'paper',
        ) as ledger
    ):
        # update pos table from ledger history
        ppt.update_from_trans(ledger.to_trans())

        pp_msgs: list[BrokerdPosition] = []
        pos: Position
        token: str  # f'{symbol}.{self.broker}'
        for token, pos in ppt.pps.items():
            pp_msgs.append(BrokerdPosition(
                broker=broker,
                account='paper',
                symbol=pos.symbol.fqme,
                size=pos.size,
                avg_price=pos.ppu,
            ))

        await ctx.started((
            pp_msgs,
            ['paper'],
        ))

        # exit early since no fqme was passed,
        # normally this case is just to load
        # positions "offline".
        if fqme is None:
            log.warning(
                'Paper engine only running in position delivery mode!\n'
                'NO SIMULATED CLEARING LOOP IS ACTIVE!'
            )
            await trio.sleep_forever()
            return

        async with (
            data.open_feed(
                [fqme],
                loglevel=loglevel,
            ) as feed,
        ):
            async with (
                ctx.open_stream() as ems_stream,
                trio.open_nursery() as n,
            ):
                client = PaperBoi(
                    broker=broker,
                    ems_trades_stream=ems_stream,
                    ppt=ppt,
                    ledger=ledger,

                    _buys=_buys,
                    _sells=_sells,
                    _reqids=_reqids,

                    # TODO: load postions from ledger file
                    _syms={
                        fqme: flume.symbol
                        for fqme, flume in feed.flumes.items()
                    }
                )

                n.start_soon(
                    handle_order_requests,
                    client,
                    ems_stream,
                )

                # paper engine simulator clearing task
                await simulate_fills(feed.streams[broker], client)


@acm
async def open_paperboi(
    fqme: str | None = None,
    broker: str | None = None,
    loglevel: str | None = None,

) -> Callable:
    '''
    Spawn a paper engine actor and yield through access to
    its context.

    '''
    if not fqme:
        assert broker, 'One of `broker` or `fqme` is required siss..!'
    else:
        broker, symbol, expiry = unpack_fqme(fqme)

    service_name = f'paperboi.{broker}'

    async with (
        tractor.find_actor(service_name) as portal,
        tractor.open_nursery() as tn,
    ):
        # only spawn if no paperboi already is up
        # (we likely don't need more then one proc for basic
        # simulated order clearing)
        if portal is None:
            log.info('Starting new paper-engine actor')
            portal = await tn.start_actor(
                service_name,
                enable_modules=[__name__]
            )

        async with portal.open_context(
            trades_dialogue,
            broker=broker,
            fqme=fqme,
            loglevel=loglevel,

        ) as (ctx, first):
            yield ctx, first
