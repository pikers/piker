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
from datetime import datetime
from operator import itemgetter
import time
from typing import Tuple, Optional
import uuid

from bidict import bidict
import trio
from dataclasses import dataclass

from ..data._normalize import iterticks


@dataclass
class PaperBoi:
    """
    Emulates a broker order client providing the same API and
    delivering an order-event response stream but with methods for
    triggering desired events based on forward testing engine
    requirements.

    """
    broker: str
    _to_trade_stream: trio.abc.SendChannel
    trade_stream: trio.abc.ReceiveChannel

    # map of paper "live" orders which be used
    # to simulate fills based on paper engine settings
    _buys: bidict
    _sells: bidict
    _reqids: bidict

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
        brid: Optional[str],
    ) -> int:
        """Place an order and return integer request id provided by client.

        """

        if brid is None:
            reqid = str(uuid.uuid4())

            # register order internally
            self._reqids[reqid] = (oid, symbol, action, price)

        else:
            # order is already existing, this is a modify
            (oid, symbol, action, old_price) = self._reqids[brid]
            assert old_price != price
            reqid = brid

        if action == 'alert':
            # bypass all fill simulation
            return reqid

        # TODO: net latency model
        # we checkpoint here quickly particulalry
        # for dark orders since we want the dark_executed
        # to trigger first thus creating a lookup entry
        # in the broker trades event processing loop
        await trio.sleep(0.05)

        await self._to_trade_stream.send({

            'local_trades': ('status', {

                'time_ns': time.time_ns(),
                'reqid': reqid,

                'status': 'submitted',
                'broker': self.broker,
                # 'cmd': cmd,  # original request message

                'paper_info': {
                    'oid': oid,
                },
            }),
        })

        # if we're already a clearing price simulate an immediate fill
        if (
            action == 'buy' and (clear_price := self.last_ask[0]) <= price
            ) or (
            action == 'sell' and (clear_price := self.last_bid[0]) >= price
        ):
            await self.fake_fill(clear_price, size, action, reqid, oid)

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

            # buys/sells: (symbol  -> (price -> order))
            orders.setdefault(symbol, {})[(oid, price)] = (size, reqid, action)

        return reqid

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:

        # TODO: fake market simulation effects
        # await self._to_trade_stream.send(
        oid, symbol, action, price = self._reqids[reqid]

        if action == 'buy':
            self._buys[symbol].pop((oid, price))
        elif action == 'sell':
            self._sells[symbol].pop((oid, price))

        # TODO: net latency model
        await trio.sleep(0.05)

        await self._to_trade_stream.send({

            'local_trades': ('status', {

                'time_ns': time.time_ns(),
                'oid': oid,
                'reqid': reqid,

                'status': 'cancelled',
                'broker': self.broker,
                # 'cmd': cmd,  # original request message

                'paper': True,
            }),
        })

    async def fake_fill(
        self,
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

        # the trades stream expects events in the form
        # {'local_trades': (event_name, msg)}
        await self._to_trade_stream.send({

            'local_trades': ('fill', {

                'status': 'filled',
                'broker': self.broker,
                # converted to float by us in ib backend
                'broker_time': datetime.now().timestamp(),

                'action': action,
                'size': size,
                'price': price,
                'remaining': 0 if order_complete else remaining,

                # normally filled by real `brokerd` daemon
                'time': time.time_ns(),
                'time_ns': time.time_ns(),  # cuz why not

                # fake ids
                'reqid': reqid,

                'paper_info': {
                    'oid': oid,
                },

                # XXX: fields we might not need to emulate?
                # execution id from broker
                # 'execid': execu.execId,
                # 'cmd': cmd,  # original request message?
            }),
        })
        if order_complete:
            await self._to_trade_stream.send({

                'local_trades': ('status', {
                    'reqid': reqid,
                    'status': 'filled',
                    'broker': self.broker,
                    'filled': size,
                    'remaining': 0 if order_complete else remaining,

                    # converted to float by us in ib backend
                    'broker_time': datetime.now().timestamp(),
                    'paper_info': {
                        'oid': oid,
                    },
                }),
            })


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
