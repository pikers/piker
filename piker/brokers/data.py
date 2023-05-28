# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
NB: this is the old original implementation that was used way way back
when the project started with ``kivy``.

This code is left for reference but will likely be merged in
appropriately and removed.

'''
import time
from functools import partial
from dataclasses import dataclass, field
import socket
import json
from types import ModuleType
import typing
from typing import (
    Coroutine, Callable, Dict,
    List, Any, Tuple, AsyncGenerator,
    Sequence
)
import contextlib

import trio
import tractor
from tractor.experimental import msgpub
from async_generator import asynccontextmanager

from ._util import (
    log,
    get_console_log,
)
from . import get_brokermod


async def wait_for_network(
    net_func: Callable,
    sleep: int = 1
) -> dict:
    """Wait until the network (DNS) comes back up.
    """
    down = False
    while True:
        try:
            with trio.move_on_after(1) as cancel_scope:
                quotes = await net_func()
                if down:
                    log.warn("Network is back up")
                return quotes
            if cancel_scope.cancelled_caught:
                log.warn("Quote query timed out")
                continue
        except socket.gaierror:
            if not down:  # only report/log network down once
                log.warn("Network is down waiting for re-establishment...")
                down = True
            await trio.sleep(sleep)


# TODO: at this point probably just just make this a class and
# a lot of these functions should be methods. It will definitely
# make stateful UI apps easier to implement
@dataclass
class BrokerFeed:
    """A per broker "client feed" container.

    A structure to keep track of components used by
    real-time data daemons. This is a backend "client" which pulls
    data from broker specific data lakes:

        ``DataFeed`` <- tractor -> ``BrokerFeed`` <- broker IPC -> broker API
    """
    mod: ModuleType
    client: object
    exit_stack: contextlib.AsyncExitStack
    quoter_keys: Tuple[str] = ('stock', 'option')
    locks: Dict[str, trio.StrictFIFOLock] = field(
        default_factory=lambda:
            {'stock': trio.StrictFIFOLock(), 'option': trio.StrictFIFOLock()}
    )
    quoters: Dict[str, typing.Coroutine] = field(default_factory=dict)
    subscriptions: Dict[str, Dict[str, set]] = field(
        default_factory=partial(dict, **{'option': {}, 'stock': {}})
    )


@msgpub(tasks=['stock', 'option'])
async def stream_poll_requests(
    get_topics: Callable,
    get_quotes: Coroutine,
    normalizer: Callable,
    rate: int = 3,  # delay between quote requests
) -> None:
    """Stream requests for quotes for a set of symbols at the given
    ``rate`` (per second).

    This routine is built for brokers who support quote polling for multiple
    symbols per request. The ``get_topics()`` func is called to retreive the
    set of symbols each iteration and ``get_quotes()`` is to retreive
    the quotes.

    A stock-broker client ``get_quotes()`` async function must be
    provided which returns an async quote retrieval function.

    .. note::
        This code is mostly tailored (for now) to the questrade backend.
        It is currently the only broker that doesn't support streaming without
        paying for data. See the note in the diffing section regarding volume
        differentials which needs to be addressed in order to get cross-broker
        support.
    """
    sleeptime = round(1. / rate, 3)
    _cache = {}  # ticker to quote caching

    async def request_quotes():
        """Get quotes for current symbol subscription set.
        """
        symbols = get_topics()
        # subscription can be changed at any time
        return await get_quotes(symbols) if symbols else ()

    while True:  # use an event here to trigger exit?
        prequote_start = time.time()

        with trio.move_on_after(3) as cancel_scope:
            quotes = await request_quotes()

        postquote_start = time.time()

        cancelled = cancel_scope.cancelled_caught
        if cancelled:
            log.warn("Quote query timed out after 3 seconds, retrying...")
            # handle network outages by idling until response is received
            quotes = await wait_for_network(request_quotes)

        new_quotes = {}

        normalized = normalizer(quotes, _cache)
        for symbol, quote in normalized.items():
            # XXX: we append to a list for the options case where the
            # subscription topic (key) is the same for all
            # expiries even though this is uncessary for the
            # stock case (different topic [i.e. symbol] for each
            # quote).
            new_quotes.setdefault(quote['key'], []).append(quote)

        if new_quotes:
            yield new_quotes

        # latency monitoring
        req_time = round(postquote_start - prequote_start, 3)
        proc_time = round(time.time() - postquote_start, 3)
        tot = req_time + proc_time
        log.debug(f"Request + processing took {tot}")
        delay = sleeptime - tot
        if delay <= 0:
            log.warn(
                f"Took {req_time} (request) + {proc_time} (processing) "
                f"= {tot} secs (> {sleeptime}) for processing quotes?")
        else:
            log.debug(f"Sleeping for {delay}")
            await trio.sleep(delay)


async def symbol_data(broker: str, tickers: List[str]):
    """Retrieve baseline symbol info from broker.
    """
    async with get_cached_feed(broker) as feed:
        return await feed.client.symbol_info(tickers)


_feeds_cache = {}


# TODO: use the version of this from .api ?
@asynccontextmanager
async def get_cached_feed(
    brokername: str,
) -> BrokerFeed:
    """Get/create a ``BrokerFeed`` from/in the current actor.
    """
    global _feeds_cache

    # check if a cached feed is in the local actor
    feeds = _feeds_cache.setdefault('feeds', {'_lock': trio.Lock()})
    lock = feeds['_lock']
    feed = None
    try:
        async with lock:
            feed = feeds[brokername]
            log.info(f"Subscribing with existing `{brokername}` daemon")
        yield feed
    except KeyError:
        async with lock:
            log.info(f"Creating new client for broker {brokername}")
            brokermod = get_brokermod(brokername)
            exit_stack = contextlib.AsyncExitStack()
            client = await exit_stack.enter_async_context(
                brokermod.get_client())
            feed = BrokerFeed(
                mod=brokermod,
                client=client,
                exit_stack=exit_stack,
            )
            feeds[brokername] = feed
        yield feed
    finally:
        if feed is not None:
            # destroy the API client
            await feed.exit_stack.aclose()


@tractor.stream
async def start_quote_stream(
    stream: tractor.Context,  # marks this as a streaming func
    broker: str,
    symbols: List[Any],
    feed_type: str = 'stock',
    rate: int = 3,
) -> None:
    '''
    Handle per-broker quote stream subscriptions using a "lazy" pub-sub
    pattern.

    Spawns new quoter tasks for each broker backend on-demand.
    Since most brokers seems to support batch quote requests we
    limit to one task per process (for now).

    '''
    # XXX: why do we need this again?
    get_console_log(tractor.current_actor().loglevel)

    # pull global vars from local actor
    symbols = list(symbols)
    log.info(
        f"{stream.chan.uid} subscribed to {broker} for symbols {symbols}")
    # another actor task may have already created it
    async with get_cached_feed(broker) as feed:

        if feed_type == 'stock':
            get_quotes = feed.quoters.setdefault(
                'stock',
                await feed.mod.stock_quoter(feed.client, symbols)
            )
            # do a smoke quote (note this mutates the input list and filters
            # out bad symbols for now)
            first_quotes = await feed.mod.smoke_quote(get_quotes, symbols)
            formatter = feed.mod.format_stock_quote

        elif feed_type == 'option':
            # FIXME: yeah we need maybe a more general way to specify
            # the arg signature for the option feed beasides a symbol
            # + expiry date.
            get_quotes = feed.quoters.setdefault(
                'option',
                await feed.mod.option_quoter(feed.client, symbols)
            )
            # packetize
            first_quotes = {
                quote['symbol']: quote
                for quote in await get_quotes(symbols)
            }
            formatter = feed.mod.format_option_quote

        sd = await feed.client.symbol_info(symbols)
        feed.mod._symbol_info_cache.update(sd)

        normalize = partial(
            feed.mod.normalize,
            formatter=formatter,
        )

        # pre-process first set of quotes
        payload = {}
        for sym, quote in first_quotes.items():
            fquote, _ = formatter(quote, sd)
            assert fquote['displayable']
            payload[sym] = fquote

        await stream.send_yield(payload)

        await stream_poll_requests(

            # ``trionics.msgpub`` required kwargs
            task_name=feed_type,
            ctx=stream,
            topics=symbols,
            packetizer=feed.mod.packetizer,

            # actual func args
            get_quotes=get_quotes,
            normalizer=normalize,
            rate=rate,
        )
        log.info(
            f"Terminating stream quoter task for {feed.mod.name}")


async def call_client(
    broker: str,
    methname: str,
    **kwargs,
):
    async with get_cached_feed(broker) as feed:
        return await getattr(feed.client, methname)(**kwargs)


class DataFeed:
    '''
    Data feed client for streaming symbol data from and making API
    client calls to a (remote) ``brokerd`` daemon.

    '''
    _allowed = ('stock', 'option')

    def __init__(self, portal, brokermod):
        self.portal = portal
        self.brokermod = brokermod
        self._quote_type = None
        self._symbols = None
        self.quote_gen = None
        self._symbol_data_cache: Dict[str, Any] = {}

    @asynccontextmanager
    async def open_stream(
        self,
        symbols: Sequence[str],
        feed_type: str,
        rate: int = 1,
        test: str = '',
    ) -> (AsyncGenerator, dict):
        if feed_type not in self._allowed:
            raise ValueError(f"Only feed types {self._allowed} are supported")

        self._quote_type = feed_type
        try:
            if self.quote_gen is not None and symbols != self._symbols:
                log.info(
                    f"Stopping existing subscription for {self._symbols}")
                await self.quote_gen.aclose()
                self._symbols = symbols

            if feed_type == 'stock' and not (
                    all(symbol in self._symbol_data_cache
                        for symbol in symbols)
            ):
                # subscribe for tickers (this performs a possible filtering
                # where invalid symbols are discarded)
                sd = await self.portal.run(
                    symbol_data,
                    broker=self.brokermod.name,
                    tickers=symbols
                )
                self._symbol_data_cache.update(sd)

            log.info(f"Starting new stream for {symbols}")

            # start live streaming from broker daemon
            async with self.portal.open_stream_from(
                start_quote_stream,
                broker=self.brokermod.name,
                symbols=symbols,
                feed_type=feed_type,
                rate=rate,
            ) as quote_gen:

                # get first quotes response
                log.debug(f"Waiting on first quote for {symbols}...")
                quotes = {}
                quotes = await quote_gen.__anext__()

                self.quote_gen = quote_gen
                self.first_quotes = quotes
                yield quote_gen, quotes

        except Exception:
            if self.quote_gen:
                await self.quote_gen.aclose()
                self.quote_gen = None
            raise

    def format_quotes(self, quotes, symbol_data={}):
        """Format ``quotes`` using broker defined formatter.
        """
        self._symbol_data_cache.update(symbol_data)
        formatter = getattr(self.brokermod, f'format_{self._quote_type}_quote')
        records, displayables = zip(*[
            formatter(quote, self._symbol_data_cache)
            for quote in quotes.values()
        ])
        return records, displayables

    async def call_client(self, method, **kwargs):
        """Call a broker ``Client`` method using RPC and return result.
        """
        return await self.portal.run(
            call_client,
            broker=self.brokermod.name,
            methname=method,
            **kwargs
        )


async def stream_to_file(
    watchlist_name: str,
    filename: str,
    portal: tractor._portal.Portal,
    tickers: List[str],
    brokermod: ModuleType,
    rate: int,
):
    """Record client side received quotes to file ``filename``.
    """
    # an async generator instance
    async with portal.open_stream_from(
        start_quote_stream,
        broker=brokermod.name,
        symbols=tickers
    ) as agen:

        fname = filename or f'{watchlist_name}.jsonstream'
        with open(fname, 'a') as f:
            async for quotes in agen:
                f.write(json.dumps(quotes))
                f.write('\n--\n')

        return fname


# async def stream_from_file(
#     filename: str,
# ):
#     with open(filename, 'r') as quotes_file:
#         content = quotes_file.read()

#     pkts = content.split('--')[:-1]  # simulate 2 separate quote packets
#     payloads = [json.loads(pkt) for pkt in pkts]
#     for payload in cycle(payloads):
#         yield payload
#         await trio.sleep(0.3)
