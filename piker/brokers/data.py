"""
Real-time data feed machinery
"""
import time
from functools import partial
from dataclasses import dataclass, field
from itertools import cycle
import socket
import json
from types import ModuleType
import typing
from typing import (
    Coroutine, Callable, Dict,
    List, Any, Tuple, AsyncGenerator,
    Sequence,
)
import contextlib
from operator import itemgetter

import trio
import tractor
from async_generator import asynccontextmanager

from ..log import get_logger, get_console_log
from . import get_brokermod


log = get_logger(__name__)


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
                log.warn(f"Network is down waiting for re-establishment...")
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


@tractor.msg.pub(tasks=['stock', 'option'])
async def stream_poll_requests(
    get_topics: typing.Callable,
    get_quotes: Coroutine,
    feed: BrokerFeed,
    rate: int = 3,  # delay between quote requests
    diff_cached: bool = True,  # only deliver "new" quotes to the queue
) -> None:
    """Stream requests for quotes for a set of symbols at the given
    ``rate`` (per second).

    This routine is built for brokers who support quote polling for multiple
    symbols per request. The ``get_topics()`` func is called to retreive the
    set of symbols each iteration and ``get_quotes()`` is to retreive
    the quotes. 


    A stock-broker client ``get_quotes()`` async function must be
    provided which returns an async quote retrieval function.
    """
    broker_limit = getattr(feed.mod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {feed.mod.__name__} query rate to {rate}/sec")

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
        if diff_cached:
            # If cache is enabled then only deliver "new" changes.
            # Useful for polling setups but obviously should be
            # disabled if you're rx-ing per-tick data.
            for quote in quotes:
                symbol = quote['symbol']
                last = _cache.setdefault(symbol, {})

                # find all keys that have match to a new value compared
                # to the last quote received
                new = set(quote.items()) - set(last.items())
                if new:
                    log.info(
                        f"New quote {quote['symbol']}:\n{new}")
                    _cache[symbol] = quote

                    # only ship diff updates and other required fields
                    payload = {k: quote[k] for k, v in new}
                    payload['symbol'] = symbol

                    # if there was volume likely the last size of
                    # shares traded is useful info and it's possible
                    # that the set difference from above will disregard
                    # a "size" value since the same # of shares were traded
                    size = quote.get('size')
                    if size and 'volume' in payload:
                        payload['size'] = size

                    # XXX: we append to a list for the options case where the
                    # subscription topic (key) is the same for all
                    # expiries even though this is uncessary for the
                    # stock case (different topic [i.e. symbol] for each
                    # quote).
                    new_quotes.setdefault(quote['key'], []).append(payload)
        else:
            # log.debug(f"Delivering quotes:\n{quotes}")
            for quote in quotes:
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


async def smoke_quote(get_quotes, tickers, broker):
    """Do an initial "smoke" request for symbols in ``tickers`` filtering
    out any symbols not supported by the broker queried in the call to
    ``get_quotes()``.
    """
    # TODO: trim out with #37
    #################################################
    # get a single quote filtering out any bad tickers
    # NOTE: this code is always run for every new client
    # subscription even when a broker quoter task is already running
    # since the new client needs to know what symbols are accepted
    log.warn(f"Retrieving smoke quote for symbols {tickers}")
    quotes = await get_quotes(tickers)

    # report any tickers that aren't returned in the first quote
    invalid_tickers = set(tickers) - set(map(itemgetter('key'), quotes))
    for symbol in invalid_tickers:
        tickers.remove(symbol)
        log.warn(
            f"Symbol `{symbol}` not found by broker `{broker}`"
        )

    # pop any tickers that return "empty" quotes
    payload = {}
    for quote in quotes:
        symbol = quote['symbol']
        if quote is None:
            log.warn(
                f"Symbol `{symbol}` not found by broker"
                f" `{broker}`")
            # XXX: not this mutates the input list (for now)
            tickers.remove(symbol)
            continue

        # report any unknown/invalid symbols (QT specific)
        if quote.get('low52w', False) is None:
            log.error(
                f"{symbol} seems to be defunct")

        payload[symbol] = quote

    return payload

    # end of section to be trimmed out with #37
    ###########################################


@asynccontextmanager
async def get_cached_feed(
    brokername: str,
) -> BrokerFeed:
    """Get/create a ``BrokerFeed`` from/in the current actor.
    """
    # check if a cached client is in the local actor's statespace
    ss = tractor.current_actor().statespace
    feeds = ss.setdefault('feeds', {'_lock': trio.Lock()})
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
    ctx: tractor.Context,  # marks this as a streaming func
    broker: str,
    symbols: List[Any],
    feed_type: str = 'stock',
    diff_cached: bool = True,
    rate: int = 3,
) -> None:
    """Handle per-broker quote stream subscriptions using a "lazy" pub-sub
    pattern.

    Spawns new quoter tasks for each broker backend on-demand.
    Since most brokers seems to support batch quote requests we
    limit to one task per process (for now).
    """
    # XXX: why do we need this again?
    get_console_log(tractor.current_actor().loglevel)

    # pull global vars from local actor
    symbols = list(symbols)
    log.info(
        f"{ctx.chan.uid} subscribed to {broker} for symbols {symbols}")
    # another actor task may have already created it
    async with get_cached_feed(broker) as feed:
        # function to format packets delivered to subscribers
        packetizer = None

        if feed_type == 'stock':
            get_quotes = feed.quoters.setdefault(
                'stock',
                await feed.mod.stock_quoter(feed.client, symbols)
            )
            # do a smoke quote (note this mutates the input list and filters
            # out bad symbols for now)
            payload = await smoke_quote(get_quotes, symbols, broker)

        elif feed_type == 'option':
            # FIXME: yeah we need maybe a more general way to specify
            # the arg signature for the option feed beasides a symbol
            # + expiry date.
            get_quotes = feed.quoters.setdefault(
                'option',
                await feed.mod.option_quoter(feed.client, symbols)
            )
            # packetize
            payload = {
                quote['symbol']: quote
                for quote in await get_quotes(symbols)
            }

        def packetizer(topic, quotes):
            return {quote['symbol']: quote for quote in quotes}

        # push initial smoke quote response for client initialization
        await ctx.send_yield(payload)

        await stream_poll_requests(

            # ``msg.pub`` required kwargs
            task_name=feed_type,
            ctx=ctx,
            topics=symbols,
            packetizer=packetizer,

            # actual func args
            feed=feed,
            get_quotes=get_quotes,
            diff_cached=diff_cached,
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
    """Data feed client for streaming symbol data from and making API client calls
    to a (remote) ``brokerd`` daemon.
    """
    _allowed = ('stock', 'option')

    def __init__(self, portal, brokermod):
        self.portal = portal
        self.brokermod = brokermod
        self._quote_type = None
        self._symbols = None
        self.quote_gen = None
        self._symbol_data_cache: Dict[str, Any] = {}

    async def open_stream(
        self,
        symbols: Sequence[str],
        feed_type: str,
        rate: int = 1,
        diff_cached: bool = True,
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
                    "piker.brokers.data",
                    'symbol_data',
                    broker=self.brokermod.name,
                    tickers=symbols
                )
                self._symbol_data_cache.update(sd)

            if test:
                # stream from a local test file
                quote_gen = await self.portal.run(
                    "piker.brokers.data",
                    'stream_from_file',
                    filename=test,
                )
            else:
                log.info(f"Starting new stream for {symbols}")
                # start live streaming from broker daemon
                quote_gen = await self.portal.run(
                    "piker.brokers.data",
                    'start_quote_stream',
                    broker=self.brokermod.name,
                    symbols=symbols,
                    feed_type=feed_type,
                    diff_cached=diff_cached,
                    rate=rate,
                )

            # get first quotes response
            log.debug(f"Waiting on first quote for {symbols}...")
            quotes = {}
            quotes = await quote_gen.__anext__()

            self.quote_gen = quote_gen
            self.first_quotes = quotes
            return quote_gen, quotes
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
            'piker.brokers.data',
            'call_client',
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
    agen = await portal.run(
        "piker.brokers.data", 'start_quote_stream',
        broker=brokermod.name, symbols=tickers)

    fname = filename or f'{watchlist_name}.jsonstream'
    with open(fname, 'a') as f:
        async for quotes in agen:
            f.write(json.dumps(quotes))
            f.write('\n--\n')

    return fname


async def stream_from_file(
    filename: str,
):
    with open(filename, 'r') as quotes_file:
        content = quotes_file.read()

    pkts = content.split('--')[:-1]  # simulate 2 separate quote packets
    payloads = [json.loads(pkt) for pkt in pkts]
    for payload in cycle(payloads):
        yield payload
        await trio.sleep(0.3)
