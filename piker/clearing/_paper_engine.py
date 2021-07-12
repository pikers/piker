# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
from contextlib import asynccontextmanager
from datetime import datetime
from operator import itemgetter
import time
from typing import Tuple, Optional, Callable
import uuid

from bidict import bidict
import trio
import tractor
from dataclasses import dataclass

from .. import data
from ..data._normalize import iterticks
from ..log import get_logger
from ._messages import (
    BrokerdCancel, BrokerdOrder, BrokerdOrderAck, BrokerdStatus,
    BrokerdFill, BrokerdPosition,
)


log = get_logger(__name__)


@dataclass
class PaperBoi:
    """
    Emulates a broker order client providing the same API and
    delivering an order-event response stream but with methods for
    triggering desired events based on forward testing engine
    requirements.

    """
    broker: str

    ems_trades_stream: tractor.MsgStream

    # map of paper "live" orders which be used
    # to simulate fills based on paper engine settings
    _buys: bidict
    _sells: bidict
    _reqids: bidict
    _positions: dict[str, BrokerdPosition]

    # init edge case L1 spread
    last_ask: Tuple[float, float] = (float('inf'), 0)  # price, size
    last_bid: Tuple[float, float] = (0, 0)

    async def submit_limit(
        self,
        oid: str,  # XXX: see return value
        symbol: str,
        price: float,
        action: str,
        size: float,
        reqid: Optional[str],
    ) -> int:
        """Place an order and return integer request id provided by client.

        """
        is_modify: bool = False
        if reqid is None:
            reqid = str(uuid.uuid4())

        else:
            # order is already existing, this is a modify
            (oid, symbol, action, old_price) = self._reqids[reqid]
            assert old_price != price
            is_modify = True

        # register order internally
        self._reqids[reqid] = (oid, symbol, action, price)

        if action == 'alert':
            # bypass all fill simulation
            return reqid

        # TODO: net latency model
        # we checkpoint here quickly particulalry
        # for dark orders since we want the dark_executed
        # to trigger first thus creating a lookup entry
        # in the broker trades event processing loop
        await trio.sleep(0.05)

        if action == 'sell':
            size = -size

        msg = BrokerdStatus(
            status='submitted',
            reqid=reqid,
            broker=self.broker,
            time_ns=time.time_ns(),
            filled=0.0,
            reason='paper_trigger',
            remaining=size,
        )
        await self.ems_trades_stream.send(msg.dict())

        # if we're already a clearing price simulate an immediate fill
        if (
            action == 'buy' and (clear_price := self.last_ask[0]) <= price
            ) or (
            action == 'sell' and (clear_price := self.last_bid[0]) >= price
        ):
            await self.fake_fill(symbol, clear_price, size, action, reqid, oid)

        else:
            # register this submissions as a paper live order

            # submit order to book simulation fill loop
            if action == 'buy':
                orders = self._buys

            elif action == 'sell':
                orders = self._sells

            # set the simulated order in the respective table for lookup
            # and trigger by the simulated clearing task normally
            # running ``simulate_fills()``.

            if is_modify:
                # remove any existing order for the old price
                orders[symbol].pop((oid, old_price))

            # buys/sells: (symbol  -> (price -> order))
            orders.setdefault(symbol, {})[(oid, price)] = (size, reqid, action)

        return reqid

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:

        # TODO: fake market simulation effects
        oid, symbol, action, price = self._reqids[reqid]

        if action == 'buy':
            self._buys[symbol].pop((oid, price))
        elif action == 'sell':
            self._sells[symbol].pop((oid, price))

        # TODO: net latency model
        await trio.sleep(0.05)

        msg = BrokerdStatus(
            status='cancelled',
            oid=oid,
            reqid=reqid,
            broker=self.broker,
            time_ns=time.time_ns(),
        )
        await self.ems_trades_stream.send(msg.dict())

    async def fake_fill(
        self,

        symbol: str,
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
        """Pretend to fill a broker order @ price and size.

        """
        # TODO: net latency model
        await trio.sleep(0.05)

        msg = BrokerdFill(

            reqid=reqid,
            time_ns=time.time_ns(),

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
        await self.ems_trades_stream.send(msg.dict())

        if order_complete:

            msg = BrokerdStatus(

                reqid=reqid,
                time_ns=time.time_ns(),

                status='filled',
                filled=size,
                remaining=0 if order_complete else remaining,

                action=action,
                size=size,
                price=price,

                broker_details={
                    'paper_info': {
                        'oid': oid,
                    },
                    'name': self.broker,
                },
            )
            await self.ems_trades_stream.send(msg.dict())

        # lookup any existing position
        token = f'{symbol}.{self.broker}'
        pp_msg = self._positions.setdefault(
            token,
            BrokerdPosition(
                broker=self.broker,
                account='paper',
                symbol=symbol,
                # TODO: we need to look up the asset currency from
                # broker info. i guess for crypto this can be
                # inferred from the pair?
                currency='',
                size=0.0,
                avg_price=0,
            )
        )

        # "avg position price" calcs
        # TODO: eventually it'd be nice to have a small set of routines
        # to do this stuff from a sequence of cleared orders to enable
        # so called "contextual positions".

        new_size = size + pp_msg.size

        if new_size != 0:
            pp_msg.avg_price = (size*price + pp_msg.avg_price) / new_size
        else:
            pp_msg.avg_price = 0

        pp_msg.size = new_size

        await self.ems_trades_stream.send(pp_msg.dict())


async def simulate_fills(
    quote_stream: 'tractor.ReceiveStream',  # noqa
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
                # print(tick)
                tick_price = tick.get('price')
                ttype = tick['type']

                if ttype in ('ask',):

                    client.last_ask = (
                        tick_price,
                        tick.get('size', client.last_ask[1]),
                    )

                    orders = client._buys.get(sym, {})

                    book_sequence = reversed(
                        sorted(orders.keys(), key=itemgetter(1)))

                    def pred(our_price):
                        return tick_price < our_price

                elif ttype in ('bid',):

                    client.last_bid = (
                        tick_price,
                        tick.get('size', client.last_bid[1]),
                    )

                    orders = client._sells.get(sym, {})
                    book_sequence = sorted(orders.keys(), key=itemgetter(1))

                    def pred(our_price):
                        return tick_price > our_price

                elif ttype in ('trade', 'last'):
                    # TODO: simulate actual book queues and our orders
                    # place in it, might require full L2 data?
                    continue

                # iterate book prices descending
                for oid, our_price in book_sequence:
                    if pred(our_price):

                        # retreive order info
                        (size, reqid, action) = orders.pop((oid, our_price))

                        # clearing price would have filled entirely
                        await client.fake_fill(
                            symbol=sym,
                            # todo slippage to determine fill price
                            price=tick_price,
                            size=size,
                            action=action,
                            reqid=reqid,
                            oid=oid,
                        )
                    else:
                        # prices are iterated in sorted order so we're done
                        break


async def handle_order_requests(

    client: PaperBoi,
    ems_order_stream: tractor.MsgStream,

) -> None:

    # order_request: dict
    async for request_msg in ems_order_stream:

        action = request_msg['action']

        if action in {'buy', 'sell'}:
            # validate
            order = BrokerdOrder(**request_msg)

            # call our client api to submit the order
            reqid = await client.submit_limit(

                oid=order.oid,
                symbol=order.symbol,
                price=order.price,
                action=order.action,
                size=order.size,

                # XXX: by default 0 tells ``ib_insync`` methods that
                # there is no existing order so ask the client to create
                # a new one (which it seems to do by allocating an int
                # counter - collision prone..)
                reqid=order.reqid,
            )

            # deliver ack that order has been submitted to broker routing
            await ems_order_stream.send(
                BrokerdOrderAck(

                    # ems order request id
                    oid=order.oid,

                    # broker specific request id
                    reqid=reqid,

                ).dict()
            )

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)

            await client.submit_cancel(
                reqid=msg.reqid
            )

        else:
            log.error(f'Unknown order command: {request_msg}')


@tractor.context
async def trades_dialogue(

    ctx: tractor.Context,
    broker: str,
    symbol: str,
    loglevel: str = None,

) -> None:

    async with (

        data.open_feed(
            broker,
            [symbol],
            loglevel=loglevel,
        ) as feed,

    ):
        # TODO: load paper positions per broker from .toml config file
        # and pass as symbol to position data mapping: ``dict[str, dict]``
        # await ctx.started(all_positions)
        await ctx.started({})

        async with (
            ctx.open_stream() as ems_stream,
            trio.open_nursery() as n,
        ):

            client = PaperBoi(
                broker,
                ems_stream,
                _buys={},
                _sells={},

                _reqids={},

                # TODO: load paper positions from ``positions.toml``
                _positions={},
            )

            n.start_soon(handle_order_requests, client, ems_stream)

            # paper engine simulator clearing task
            await simulate_fills(feed.stream, client)


@asynccontextmanager
async def open_paperboi(
    broker: str,
    symbol: str,
    loglevel: str,

) -> Callable:
    '''Spawn a paper engine actor and yield through access to
    its context.

    '''
    service_name = f'paperboi.{broker}'

    async with (
        tractor.find_actor(service_name) as portal,
        tractor.open_nursery() as tn,
    ):
        # only spawn if no paperboi already is up
        # (we likely don't need more then one proc for basic
        # simulated order clearing)
        if portal is None:
            portal = await tn.start_actor(
                service_name,
                enable_modules=[__name__]
            )

        async with portal.open_context(
                trades_dialogue,
                broker=broker,
                symbol=symbol,
                loglevel=loglevel,

        ) as (ctx, first):

            yield ctx, first
