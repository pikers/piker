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
In da suit parlances: "Execution management systems"

"""
from pprint import pformat
import time
from datetime import datetime
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    AsyncIterator, Dict, Callable, Tuple,
)
import uuid

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor

from . import data
from .log import get_logger
from .data._source import Symbol
from .data._normalize import iterticks


log = get_logger(__name__)


# TODO: numba all of this
def mk_check(trigger_price, known_last) -> Callable[[float, float], bool]:
    """Create a predicate for given ``exec_price`` based on last known
    price, ``known_last``.

    This is an automatic alert level thunk generator based on where the
    current last known value is and where the specified value of
    interest is; pick an appropriate comparison operator based on
    avoiding the case where the a predicate returns true immediately.

    """
    # str compares:
    # https://stackoverflow.com/questions/46708708/compare-strings-in-numba-compiled-function

    if trigger_price >= known_last:

        def check_gt(price: float) -> bool:
            return price >= trigger_price

        return check_gt

    elif trigger_price <= known_last:

        def check_lt(price: float) -> bool:
            return price <= trigger_price

        return check_lt

    else:
        return None, None


@dataclass
class _DarkBook:
    """Client-side execution book.

    Contains conditions for executions (aka "orders") which are not
    exposed to brokers and thus the market; i.e. these are privacy
    focussed "client side" orders.

    A singleton instance is created per EMS actor (for now).

    """
    broker: str

    # levels which have an executable action (eg. alert, order, signal)
    orders: Dict[
        str,  # symbol
        Dict[
            str,  # uuid
            Tuple[
                Callable[[float], bool],  # predicate
                str,  # name
                dict,  # cmd / msg type
            ]
        ]
    ] = field(default_factory=dict)

    # tracks most recent values per symbol each from data feed
    lasts: Dict[
        Tuple[str, str],
        float
    ] = field(default_factory=dict)

    # mapping of broker order ids to piker ems ids
    _broker2ems_ids: Dict[str, str] = field(default_factory=bidict)


_books: Dict[str, _DarkBook] = {}


def get_dark_book(broker: str) -> _DarkBook:

    global _books
    return _books.setdefault(broker, _DarkBook(broker))


# XXX: this is in place to prevent accidental positions that are too
# big. Now obviously this won't make sense for crypto like BTC, but
# for most traditional brokers it should be fine unless you start
# slinging NQ futes or something.
_DEFAULT_SIZE: float = 1.0


@dataclass
class PaperBoi:
    """Emulates a broker order client providing the same API and
    order-event response event stream format but with methods for
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
    ) -> int:
        """Place an order and return integer request id provided by client.

        """
        # the trades stream expects events in the form
        # {'local_trades': (event_name, msg)}
        reqid = str(uuid.uuid4())

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

        # register order internally
        self._reqids[reqid] = (oid, symbol, action, price)

        # if we're already a clearing price simulate an immediate fill
        if (
            action == 'buy' and (clear_price := self.last_ask[0]) <= price
            ) or (
            action == 'sell' and (clear_price := self.last_bid[0]) >= price
        ):
            await self.fake_fill(clear_price, size, action, reqid, oid)

        else:  # register this submissions as a paper live order

            # submit order to book simulation fill loop
            if action == 'buy':
                orders = self._buys

            elif action == 'sell':
                orders = self._sells

            # buys/sells: (symbol  -> (price -> order))
            orders.setdefault(symbol, {})[price] = (size, oid, reqid, action)

        return reqid

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:

        # TODO: fake market simulation effects
        # await self._to_trade_stream.send(
        oid, symbol, action, price = self._reqids[reqid]

        if action == 'buy':
            self._buys[symbol].pop(price)
        elif action == 'sell':
            self._sells[symbol].pop(price)

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

                    buys = client._buys.get(sym, {})

                    # iterate book prices descending
                    for our_bid in reversed(sorted(buys.keys())):
                        if tick_price < our_bid:

                            # retreive order info
                            (size, oid, reqid, action) = buys.pop(our_bid)

                            # clearing price would have filled entirely
                            await client.fake_fill(
                                # todo slippage to determine fill price
                                tick_price,
                                size,
                                action,
                                reqid,
                                oid,
                            )
                        else:
                            # prices are interated in sorted order so
                            # we're done
                            break

                if ttype in ('bid',):

                    client.last_bid = (
                        tick_price,
                        tick.get('size', client.last_bid[1]),
                    )

                    sells = client._sells.get(sym, {})

                    # iterate book prices ascending
                    for our_ask in sorted(sells.keys()):
                        if tick_price > our_ask:

                            # retreive order info
                            (size, oid, reqid, action) = sells.pop(our_ask)

                            # clearing price would have filled entirely
                            await client.fake_fill(
                                tick_price,
                                size,
                                action,
                                reqid,
                                oid,
                            )
                        else:
                            # prices are interated in sorted order so
                            # we're done
                            break

                if ttype in ('trade', 'last'):
                    # TODO: simulate actual book queues and our orders
                    # place in it, might require full L2 data?
                    pass


async def execute_triggers(
    broker: str,
    symbol: str,
    stream: 'tractor.ReceiveStream',  # noqa
    ctx: tractor.Context,
    client: 'Client',  # noqa
    book: _DarkBook,
) -> None:
    """Core dark order trigger loop.

    Scan the (price) data feed and submit triggered orders
    to broker.

    """
    # this stream may eventually contain multiple symbols
    async for quotes in stream:

        # TODO: numba all this!

        # start = time.time()
        for sym, quote in quotes.items():

            execs = book.orders.get(sym, None)
            if execs is None:
                continue

            for tick in iterticks(
                quote,
                # dark order price filter(s)
                types=('ask', 'bid', 'trade', 'last')
            ):
                price = tick.get('price')
                ttype = tick['type']

                # lel, fuck you ib
                # if price < 0:
                #     log.error(f'!!?!?!VOLUME TICK {tick}!?!?')
                #     continue

                # update to keep new cmds informed
                book.lasts[(broker, symbol)] = price

                for oid, (
                    pred,
                    tf,
                    cmd,
                    percent_away,
                    abs_diff_away
                ) in (
                    tuple(execs.items())
                ):

                    if (ttype not in tf) or (not pred(price)):
                        # majority of iterations will be non-matches
                        continue

                    # submit_price = price + price*percent_away
                    submit_price = price + abs_diff_away

                    log.info(
                        f'Dark order triggered for price {price}\n'
                        f'Submitting order @ price {submit_price}')

                    reqid = await client.submit_limit(
                        oid=oid,
                        symbol=sym,
                        action=cmd['action'],
                        price=submit_price,
                        size=cmd['size'],
                    )

                    # register broker request id to ems id
                    book._broker2ems_ids[reqid] = oid

                    resp = {
                        'resp': 'dark_executed',
                        'time_ns': time.time_ns(),
                        'trigger_price': price,

                        'cmd': cmd,  # original request message

                        'broker_reqid': reqid,
                        'broker': broker,
                        'oid': oid,  # piker order id

                    }

                    # remove exec-condition from set
                    log.info(f'removing pred for {oid}')
                    execs.pop(oid)

                    await ctx.send_yield(resp)

                else:  # condition scan loop complete
                    log.debug(f'execs are {execs}')
                    if execs:
                        book.orders[symbol] = execs

        # print(f'execs scan took: {time.time() - start}')


async def exec_loop(
    ctx: tractor.Context,
    broker: str,
    symbol: str,
    _exec_mode: str,
    task_status: TaskStatus[dict] = trio.TASK_STATUS_IGNORED,
) -> AsyncIterator[dict]:
    """Main scan loop for order execution conditions and submission
    to brokers.

    """
    async with data.open_feed(
        broker,
        [symbol],
        loglevel='info',
    ) as feed:

        # TODO: get initial price quote from target broker
        first_quote = await feed.receive()

        book = get_dark_book(broker)
        book.lasts[(broker, symbol)] = first_quote[symbol]['last']

        # TODO: wrap this in a more re-usable general api
        client_factory = getattr(feed.mod, 'get_client_proxy', None)

        if client_factory is not None and _exec_mode != 'paper':

            # we have an order API for this broker
            client = client_factory(feed._brokerd_portal)

        else:
            # force paper mode
            log.warning(f'Entering paper trading mode for {broker}')

            client = PaperBoi(
                broker,
                *trio.open_memory_channel(100),
                _buys={},
                _sells={},
                _reqids={},
            )

            # for paper mode we need to mock this trades response feed
            # so we pass a duck-typed feed-looking mem chan which is fed
            # fill and submission events from the exec loop
            feed._trade_stream = client.trade_stream

            # init the trades stream
            client._to_trade_stream.send_nowait({'local_trades': 'start'})

            _exec_mode = 'paper'

        # return control to parent task
        task_status.started((first_quote, feed, client))

        ##############################
        # begin price actions sequence
        # XXX: optimize this for speed
        ##############################

        # shield this field so the remote brokerd does not get cancelled
        stream = feed.stream
        with stream.shield():
            async with trio.open_nursery() as n:
                n.start_soon(
                    execute_triggers,
                    broker,
                    symbol,
                    stream,
                    ctx,
                    client,
                    book
                )

                if _exec_mode == 'paper':
                    n.start_soon(simulate_fills, stream.clone(), client)


# TODO: lots of cases still to handle
# XXX: right now this is very very ad-hoc to IB
# - short-sale but securities haven't been located, in this case we
#    should probably keep the order in some kind of weird state or cancel
#    it outright?
# status='PendingSubmit', message=''),
# status='Cancelled', message='Error 404,
#   reqId 1550: Order held while securities are located.'),
# status='PreSubmitted', message='')],

async def process_broker_trades(
    ctx: tractor.Context,
    feed: 'Feed',  # noqa
    book: _DarkBook,
    task_status: TaskStatus[dict] = trio.TASK_STATUS_IGNORED,
) -> AsyncIterator[dict]:
    """Trades update loop - receive updates from broker, convert
    to EMS responses, transmit to ordering client(s).

    This is where trade confirmations from the broker are processed
    and appropriate responses relayed back to the original EMS client
    actor. There is a messaging translation layer throughout.

    Expected message translation(s):

        broker       ems
        'error'  ->  log it locally (for now)
        'status' ->  relabel as 'broker_<status>', if complete send 'executed'
        'fill'   ->  'broker_filled'

    Currently accepted status values from IB:
        {'presubmitted', 'submitted', 'cancelled', 'inactive'}

    """
    broker = feed.mod.name

    with trio.fail_after(5):
        # in the paper engine case this is just a mem receive channel
        trades_stream = await feed.recv_trades_data()
        first = await trades_stream.__anext__()

    # startup msg expected as first from broker backend
    assert first['local_trades'] == 'start'
    task_status.started()

    async for event in trades_stream:

        name, msg = event['local_trades']
        log.info(f'Received broker trade event:\n{pformat(msg)}')

        # Get the broker (order) request id, this **must** be normalized
        # into messaging provided by the broker backend
        reqid = msg['reqid']

        # make response packet to EMS client(s)
        oid = book._broker2ems_ids.get(reqid)

        if oid is None:
            # paper engine race case: ``Client.submit_limit()`` hasn't
            # returned yet and provided an output reqid to register
            # locally, so we need to retreive the oid that was already
            # packed at submission since we already know it ahead of
            # time
            oid = msg['paper_info']['oid']

        resp = {
            'resp': None,  # placeholder
            'oid': oid
        }

        if name in (
            'error',
        ):
            # TODO: figure out how this will interact with EMS clients
            # for ex. on an error do we react with a dark orders
            # management response, like cancelling all dark orders?

            # This looks like a supervision policy for pending orders on
            # some unexpected failure - something we need to think more
            # about.  In most default situations, with composed orders
            # (ex.  brackets), most brokers seem to use a oca policy.

            message = msg['message']

            # XXX should we make one when it's blank?
            log.error(pformat(message))

            # TODO: getting this bs, prolly need to handle status messages
            # 'Market data farm connection is OK:usfarm.nj'

            # another stupid ib error to handle
            # if 10147 in message: cancel

            # don't relay message to order requester client
            continue

        elif name in (
            'status',
        ):
            # TODO: templating the ib statuses in comparison with other
            # brokers is likely the way to go:
            # https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#a17f2a02d6449710b6394d0266a353313
            # short list:
            # - PendingSubmit
            # - PendingCancel
            # - PreSubmitted (simulated orders)
            # - ApiCancelled (cancelled by client before submission to routing)
            # - Cancelled
            # - Filled
            # - Inactive (reject or cancelled but not by trader)

            # everyone doin camel case
            status = msg['status'].lower()

            if status == 'filled':
                # await tractor.breakpoint()

                # conditional execution is fully complete, no more
                # fills for the noted order
                if not msg['remaining']:

                    resp['resp'] = 'broker_executed'

                    log.info(f'Execution for {oid} is complete!')

                # just log it
                else:
                    log.info(f'{broker} filled {msg}')

            else:
                # one of (submitted, cancelled)
                resp['resp'] = 'broker_' + status

        elif name in (
            'fill',
        ):
            # proxy through the "fill" result(s)
            resp['resp'] = 'broker_filled'
            resp.update(msg)

            log.info(f'\nFill for {oid} cleared with:\n{pformat(resp)}')

        # respond to requesting client
        await ctx.send_yield(resp)


@tractor.stream
async def _ems_main(
    ctx: tractor.Context,
    client_actor_name: str,
    broker: str,
    symbol: str,
    _mode: str = 'dark',  # ('paper', 'dark', 'live')
) -> None:
    """EMS (sub)actor entrypoint providing the
    execution management (micro)service which conducts broker
    order control on behalf of clients.

    This is the daemon (child) side routine which starts an EMS runtime
    (one per broker-feed) and and begins streaming back alerts from
    broker executions/fills.

    ``send_order_cmds()`` is called here to execute in a task back in
    the actor which started this service (spawned this actor), presuming
    capabilities allow it, such that requests for EMS executions are
    received in a stream from that client actor and then responses are
    streamed back up to the original calling task in the same client.

    The task tree is:
    - ``_ems_main()``:
      accepts order cmds, registers execs with exec loop

    - ``exec_loop()``:
      run (dark) conditions on inputs and trigger broker submissions

    - ``process_broker_trades()``:
      accept normalized trades responses, process and relay to ems client(s)

    """
    book = get_dark_book(broker)

    # get a portal back to the client
    async with tractor.wait_for_actor(client_actor_name) as portal:

        # spawn one task per broker feed
        async with trio.open_nursery() as n:

            # TODO: eventually support N-brokers

            # start the condition scan loop
            quote, feed, client = await n.start(
                exec_loop,
                ctx,
                broker,
                symbol,
                _mode,
            )

            await n.start(
                process_broker_trades,
                ctx,
                feed,
                book,
            )

            # connect back to the calling actor (the one that is
            # acting as an EMS client and will submit orders) to
            # receive requests pushed over a tractor stream
            # using (for now) an async generator.
            async for cmd in await portal.run(send_order_cmds):

                log.info(f'Received order cmd:\n{pformat(cmd)}')

                action = cmd['action']
                oid = cmd['oid']

                # TODO: can't wait for this stuff to land in 3.10
                # https://www.python.org/dev/peps/pep-0636/#going-to-the-cloud-mappings
                if action in ('cancel',):

                    # check for live-broker order
                    brid = book._broker2ems_ids.inverse.get(oid)
                    if brid:
                        log.info("Submitting cancel for live order")
                        await client.submit_cancel(reqid=brid)

                    # check for EMS active exec
                    else:
                        try:
                            book.orders[symbol].pop(oid, None)

                            await ctx.send_yield({
                                'resp': 'dark_cancelled',
                                'oid': oid
                            })
                        except KeyError:
                            log.exception(f'No dark order for {symbol}?')

                elif action in ('alert', 'buy', 'sell',):

                    sym = cmd['symbol']
                    trigger_price = cmd['price']
                    size = cmd['size']
                    brokers = cmd['brokers']
                    exec_mode = cmd.get('exec_mode', _mode)

                    broker = brokers[0]
                    last = book.lasts[(broker, sym)]

                    if exec_mode == 'live' and action in ('buy', 'sell',):

                        # register broker id for ems id
                        order_id = await client.submit_limit(
                            oid=oid,  # no ib support for this
                            symbol=sym,
                            action=action,
                            price=trigger_price,
                            size=size,
                        )
                        book._broker2ems_ids[order_id] = oid

                        # XXX: the trades data broker response loop
                        # (``process_broker_trades()`` above) will
                        # handle sending the ems side acks back to
                        # the cmd sender from here

                    elif exec_mode in ('dark', 'paper') or action in ('alert'):

                        # TODO: if the predicate resolves immediately send the
                        # execution to the broker asap? Or no?

                        # submit order to local EMS

                        # Auto-gen scanner predicate:
                        # we automatically figure out what the alert check
                        # condition should be based on the current first
                        # price received from the feed, instead of being
                        # like every other shitty tina platform that makes
                        # the user choose the predicate operator.
                        pred = mk_check(trigger_price, last)

                        mt = feed.symbols[sym].tick_size

                        if action == 'buy':
                            tickfilter = ('ask', 'last', 'trade')
                            percent_away = 0.005

                            # TODO: we probably need to scale this based
                            # on some near term historical spread
                            # measure?
                            abs_diff_away = 3 * mt

                        elif action == 'sell':
                            tickfilter = ('bid', 'last', 'trade')
                            percent_away = -0.005
                            abs_diff_away = -3 * mt

                        else:  # alert
                            tickfilter = ('trade', 'utrade', 'last')
                            percent_away = 0
                            abs_diff_away = 0

                        # submit execution/order to EMS scan loop
                        book.orders.setdefault(
                            sym, {}
                        )[oid] = (
                            pred,
                            tickfilter,
                            cmd,
                            percent_away,
                            abs_diff_away
                        )

                        # ack-response that order is live in EMS
                        await ctx.send_yield({
                            'resp': 'dark_submitted',
                            'oid': oid
                        })

            # continue and wait on next order cmd


@dataclass
class OrderBook:
    """Buy-side (client-side ?) order book ctl and tracking.

    A style similar to "model-view" is used here where this api is
    provided as a supervised control for an EMS actor which does all the
    hard/fast work of talking to brokers/exchanges to conduct
    executions.

    Currently, this is mostly for keeping local state to match the EMS
    and use received events to trigger graphics updates.

    """
    # mem channels used to relay order requests to the EMS daemon
    _to_ems: trio.abc.SendChannel
    _from_order_book: trio.abc.ReceiveChannel

    _sent_orders: Dict[str, dict] = field(default_factory=dict)
    _ready_to_receive: trio.Event = trio.Event()

    def send(
        self,
        uuid: str,
        symbol: 'Symbol',
        price: float,
        size: float,
        action: str,
        exec_mode: str,
    ) -> dict:
        cmd = {
            'action': action,
            'price': price,
            'size': size,
            'symbol': symbol.key,
            'brokers': symbol.brokers,
            'oid': uuid,
            'exec_mode': exec_mode,  # dark or live
        }
        self._sent_orders[uuid] = cmd
        self._to_ems.send_nowait(cmd)
        return cmd

    async def modify(self, oid: str, price) -> bool:
        ...

    def cancel(self, uuid: str) -> bool:
        """Cancel an order (or alert) from the EMS.

        """
        cmd = self._sent_orders[uuid]
        msg = {
            'action': 'cancel',
            'oid': uuid,
            'symbol': cmd['symbol'],
        }
        self._to_ems.send_nowait(msg)


_orders: OrderBook = None


def get_orders(
    emsd_uid: Tuple[str, str] = None
) -> OrderBook:
    """"
    OrderBook singleton factory per actor.

    """
    if emsd_uid is not None:
        # TODO: read in target emsd's active book on startup
        pass

    global _orders

    if _orders is None:
        # setup local ui event streaming channels for request/resp
        # streamging with EMS daemon
        _orders = OrderBook(*trio.open_memory_channel(100))

    return _orders


# TODO: make this a ``tractor.msg.pub``
async def send_order_cmds():
    """Order streaming task: deliver orders transmitted from UI
    to downstream consumers.

    This is run in the UI actor (usually the one running Qt but could be
    any other client service code). This process simply delivers order
    messages to the above ``_to_ems`` send channel (from sync code using
    ``.send_nowait()``), these values are pulled from the channel here
    and relayed to any consumer(s) that called this function using
    a ``tractor`` portal.

    This effectively makes order messages look like they're being
    "pushed" from the parent to the EMS where local sync code is likely
    doing the pushing from some UI.

    """
    book = get_orders()
    orders_stream = book._from_order_book

    # signal that ems connection is up and ready
    book._ready_to_receive.set()

    async for cmd in orders_stream:

        # send msg over IPC / wire
        log.info(f'Send order cmd:\n{pformat(cmd)}')
        yield cmd


@asynccontextmanager
async def open_ems(
    broker: str,
    symbol: Symbol,
    # task_status: TaskStatus[str] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Spawn an EMS daemon and begin sending orders and receiving
    alerts.


    This EMS tries to reduce most broker's terrible order entry apis to
    a very simple protocol built on a few easy to grok and/or
    "rantsy" premises:

    - most users will prefer "dark mode" where orders are not submitted
      to a broker until and execution condition is triggered
      (aka client-side "hidden orders")

    - Brokers over-complicate their apis and generally speaking hire
      poor designers to create them. We're better off using creating a super
      minimal, schema-simple, request-event-stream protocol to unify all the
      existing piles of shit (and shocker, it'll probably just end up
      looking like a decent crypto exchange's api)

    - all order types can be implemented with client-side limit orders

    - we aren't reinventing a wheel in this case since none of these
      brokers are exposing FIX protocol; it is they doing the re-invention.


    TODO: make some fancy diagrams using mermaid.io

    the possible set of responses from the stream  is currently:
    - 'dark_submitted', 'broker_submitted'
    - 'dark_cancelled', 'broker_cancelled'
    - 'dark_executed', 'broker_executed'
    - 'broker_filled'
    """
    actor = tractor.current_actor()
    subactor_name = 'emsd'

    # TODO: add ``maybe_spawn_emsd()`` for this
    async with tractor.open_nursery() as n:

        portal = await n.start_actor(
            subactor_name,
            enable_modules=[__name__],
        )
        trades_stream = await portal.run(
            _ems_main,
            client_actor_name=actor.name,
            broker=broker,
            symbol=symbol.key,

        )

        # wait for service to connect back to us signalling
        # ready for order commands
        book = get_orders()

        with trio.fail_after(10):
            await book._ready_to_receive.wait()

        yield book, trades_stream
