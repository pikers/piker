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
from dataclasses import dataclass, field
from typing import (
    AsyncIterator, Dict, Callable, Tuple,
)

from bidict import bidict
import trio
from trio_typing import TaskStatus
import tractor

from .. import data
from ..log import get_logger
from ..data._normalize import iterticks
from ._paper_engine import PaperBoi, simulate_fills


log = get_logger(__name__)


# TODO: numba all of this
def mk_check(
    trigger_price: float,
    known_last: float,
    action: str,
) -> Callable[[float, float], bool]:
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
        return None


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
    # XXX: optimize this for speed!
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

                        # this is a brand new order request for the
                        # underlying broker so we set out "broker request
                        # id" (brid) as nothing so that the broker
                        # client knows that we aren't trying to modify
                        # an existing order.
                        brid=None,

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
    feed: 'Feed',  # noqa
    broker: str,
    symbol: str,
    _exec_mode: str,
    task_status: TaskStatus[dict] = trio.TASK_STATUS_IGNORED,
) -> AsyncIterator[dict]:
    """Main scan loop for order execution conditions and submission
    to brokers.

    """

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

    stream = feed.stream
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
            # TODO: make this an actual broadcast channels as in:
            # https://github.com/python-trio/trio/issues/987
            n.start_soon(simulate_fills, stream, client)


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

        if name == 'position':
            msg['resp'] = 'position'

            # relay through
            await ctx.send_yield(msg)
            continue

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
            paper = msg.get('paper_info')
            if paper:
                oid = paper['oid']

            else:
                msg.get('external')
                if not msg:
                    log.error(f"Unknown trade event {event}")

                continue

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


async def process_order_cmds(
    ctx: tractor.Context,
    cmd_stream: 'tractor.ReceiveStream',  # noqa
    symbol: str,
    feed: 'Feed',  # noqa
    client: 'Client',  # noqa
    dark_book: _DarkBook,
) -> None:

    async for cmd in cmd_stream:

        log.info(f'Received order cmd:\n{pformat(cmd)}')

        action = cmd['action']
        oid = cmd['oid']

        brid = dark_book._broker2ems_ids.inverse.get(oid)

        # TODO: can't wait for this stuff to land in 3.10
        # https://www.python.org/dev/peps/pep-0636/#going-to-the-cloud-mappings
        if action in ('cancel',):

            # check for live-broker order
            if brid:
                log.info("Submitting cancel for live order")
                await client.submit_cancel(reqid=brid)

            # check for EMS active exec
            else:
                try:
                    dark_book.orders[symbol].pop(oid, None)

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
            exec_mode = cmd['exec_mode']

            broker = brokers[0]

            if exec_mode == 'live' and action in ('buy', 'sell',):

                # register broker id for ems id
                order_id = await client.submit_limit(

                    oid=oid,  # no ib support for oids...

                    # if this is None, creates a new order
                    # otherwise will modify any existing one
                    brid=brid,

                    symbol=sym,
                    action=action,
                    price=trigger_price,
                    size=size,
                )

                if brid:
                    assert dark_book._broker2ems_ids[brid] == oid

                    # if we already had a broker order id then
                    # this is likely an order update commmand.
                    log.info(f"Modifying order: {brid}")

                else:
                    dark_book._broker2ems_ids[order_id] = oid

                # XXX: the trades data broker response loop
                # (``process_broker_trades()`` above) will
                # handle sending the ems side acks back to
                # the cmd sender from here

            elif exec_mode in ('dark', 'paper') or (
                action in ('alert')
            ):
                # submit order to local EMS

                # Auto-gen scanner predicate:
                # we automatically figure out what the alert check
                # condition should be based on the current first
                # price received from the feed, instead of being
                # like every other shitty tina platform that makes
                # the user choose the predicate operator.
                last = dark_book.lasts[(broker, sym)]
                pred = mk_check(trigger_price, last, action)

                spread_slap: float = 5
                min_tick = feed.symbols[sym].tick_size

                if action == 'buy':
                    tickfilter = ('ask', 'last', 'trade')
                    percent_away = 0.005

                    # TODO: we probably need to scale this based
                    # on some near term historical spread
                    # measure?
                    abs_diff_away = spread_slap * min_tick

                elif action == 'sell':
                    tickfilter = ('bid', 'last', 'trade')
                    percent_away = -0.005
                    abs_diff_away = -spread_slap * min_tick

                else:  # alert
                    tickfilter = ('trade', 'utrade', 'last')
                    percent_away = 0
                    abs_diff_away = 0

                # submit execution/order to EMS scan loop
                # FYI: this may result in an override of an existing
                # dark book entry if the order id already exists
                dark_book.orders.setdefault(
                    sym, {}
                )[oid] = (
                    pred,
                    tickfilter,
                    cmd,
                    percent_away,
                    abs_diff_away
                )
                # TODO: if the predicate resolves immediately send the
                # execution to the broker asap? Or no?

                # ack-response that order is live in EMS
                await ctx.send_yield({
                    'resp': 'dark_submitted',
                    'oid': oid
                })


@tractor.stream
async def _emsd_main(
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
    - ``_emsd_main()``:
      accepts order cmds, registers execs with exec loop

    - ``exec_loop()``:
      run (dark) conditions on inputs and trigger broker submissions

    - ``process_broker_trades()``:
      accept normalized trades responses, process and relay to ems client(s)

    """
    from ._client import send_order_cmds

    dark_book = get_dark_book(broker)

    # get a portal back to the client
    async with tractor.wait_for_actor(client_actor_name) as portal:

        # spawn one task per broker feed
        async with trio.open_nursery() as n:

            # TODO: eventually support N-brokers
            async with data.open_feed(
                broker,
                [symbol],
                loglevel='info',
            ) as feed:

                # start the condition scan loop
                quote, feed, client = await n.start(
                    exec_loop,
                    ctx,
                    feed,
                    broker,
                    symbol,
                    _mode,
                )

                await n.start(
                    process_broker_trades,
                    ctx,
                    feed,
                    dark_book,
                )

                # connect back to the calling actor (the one that is
                # acting as an EMS client and will submit orders) to
                # receive requests pushed over a tractor stream
                # using (for now) an async generator.
                order_stream = await portal.run(send_order_cmds)

                # start inbound order request processing
                await process_order_cmds(
                    ctx,
                    order_stream,
                    symbol,
                    feed,
                    client,
                    dark_book,
                )
