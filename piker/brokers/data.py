"""
Live data feed machinery
"""
import time
from functools import partial
from dataclasses import dataclass, field
from itertools import cycle
import socket
import json
from types import ModuleType
import typing
from typing import Coroutine, Callable, Dict, List, Any, Tuple
import contextlib
from operator import itemgetter

import trio
import tractor

from ..log import get_logger, get_console_log
from . import get_brokermod


log = get_logger('broker.data')


async def wait_for_network(net_func: Callable, sleep: int = 1) -> dict:
    """Wait until the network comes back up.
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


async def stream_quotes(
    get_topics: typing.Callable,
    get_quotes: Coroutine,
    brokermod: ModuleType,
    rate: int = 5,  # delay between quote requests
    diff_cached: bool = True,  # only deliver "new" quotes to the queue
) -> None:
    """Stream quotes for a sequence of tickers at the given ``rate``
    per second.

    A stock-broker client ``get_quotes()`` async context manager must be
    provided which returns an async quote retrieval function.
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
        if diff_cached:
            # If cache is enabled then only deliver "new" changes.
            # Useful for polling setups but obviously should be
            # disabled if you're rx-ing per-tick data.
            for quote in quotes:
                symbol = quote['symbol']
                last = _cache.setdefault(symbol, {})
                new = set(quote.items()) - set(last.items())
                if new:
                    log.info(
                        f"New quote {quote['symbol']}:\n{new}")
                    _cache[symbol] = quote
                    new_quotes[symbol] = quote
        else:
            log.info(f"Delivering quotes:\n{quotes}")
            for quote in quotes:
                newquotes[quote['symbol']] = quote

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


async def fan_out_to_ctxs(
    pub_gen: typing.AsyncGenerator,
    feed: BrokerFeed,
    get_quotes: Coroutine,
    topics2ctxs: Dict[str, tractor.Context],
    topic_key: str = 'key',
    packet_key: str = 'symbol',
    rate: int = 5,  # delay between quote requests
    diff_cached: bool = True,  # only deliver "new" quotes to the queue
) -> None:
    """Request and fan out quotes to each subscribed actor channel.
    """
    broker_limit = getattr(feed.mod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {feed.mod.__name__} query rate to {rate}/sec")

    def get_topics():
        return tuple(topics2ctxs.keys())

    async for published in pub_gen(
        get_topics,
        get_quotes,
        feed.mod,
        rate,
        diff_cached=diff_cached,
    ):
        ctx_payloads = {}
        for packet_key, data in published.items():
            # grab each suscription topic using provided key for lookup
            topic = data[topic_key]
            # build a new dict packet for passing to multiple underlying channels
            packet = {packet_key: data}
            for ctx in topics2ctxs.get(topic, set()):
                ctx_payloads.setdefault(ctx, {}).update(packet),

        # deliver to each subscriber (fan out)
        if ctx_payloads:
            for ctx, payload in ctx_payloads.items():
                try:
                    await ctx.send_yield(payload)
                except (
                    # That's right, anything you can think of...
                    trio.ClosedStreamError, ConnectionResetError,
                    ConnectionRefusedError,
                ):
                    log.warn(f"{ctx.chan} went down?")
                    for ctx_set in topics2ctxs.values():
                        ctx_set.discard(ctx)

        if not any(topics2ctxs.values()):
            log.warn(f"No subs left for broker {feed.mod.name}, exiting task")
            break

    log.info(f"Terminating stream quoter task for {pub_gen.__name__}")


async def symbol_data(broker: str, tickers: List[str]):
    """Retrieve baseline symbol info from broker.
    """
    feed = await get_cached_feed(broker)
    return await feed.client.symbol_data(tickers)


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
            log.warn(
                f"{symbol} seems to be defunct")

        payload[symbol] = quote

    return payload

    # end of section to be trimmed out with #37
    ###########################################


def modify_quote_stream(broker, feed_type, symbols, ctx):
    """Absolute symbol subscription list for each quote stream.

    Effectively a symbol subscription api.
    """
    log.info(f"{ctx.chan} changed symbol subscription to {symbols}")
    ss = tractor.current_actor().statespace
    feed = ss['feeds'].get(broker)
    if feed is None:
        raise RuntimeError(
            "`get_cached_feed()` must be called before modifying its stream"
        )

    symbols2ctxs = feed.subscriptions[feed_type]
    # update map from each symbol to requesting client's chan
    for ticker in symbols:
        symbols2ctxs.setdefault(ticker, set()).add(ctx)

    # remove any existing symbol subscriptions if symbol is not
    # found in ``symbols``
    # TODO: this can likely be factored out into the pub-sub api
    for ticker in filter(
        lambda ticker: ticker not in symbols, symbols2ctxs.copy()
    ):
        ctx_set = symbols2ctxs.get(ticker)
        ctx_set.discard(ctx)

        if not ctx_set:
            # pop empty sets which will trigger bg quoter task termination
            symbols2ctxs.pop(ticker)


async def get_cached_feed(
    brokername: str,
) -> BrokerFeed:
    """Get/create a ``BrokerFeed`` from/in the current actor.
    """
    # check if a cached client is in the local actor's statespace
    ss = tractor.current_actor().statespace
    feeds = ss.setdefault('feeds', {'_lock': trio.Lock()})
    lock = feeds['_lock']
    async with lock:
        try:
            feed = feeds[brokername]
            log.info(f"Subscribing with existing `{brokername}` daemon")
            return feed
        except KeyError:
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
            return feed


async def start_quote_stream(
    broker: str,
    symbols: List[Any],
    feed_type: str = 'stock',
    diff_cached: bool = True,
    ctx: tractor.Context = None,
    rate: int = 3,
) -> None:
    """Handle per-broker quote stream subscriptions using a "lazy" pub-sub
    pattern.

    Spawns new quoter tasks for each broker backend on-demand.
    Since most brokers seems to support batch quote requests we
    limit to one task per process for now.
    """
    # XXX: why do we need this again?
    get_console_log(tractor.current_actor().loglevel)

    # pull global vars from local actor
    symbols = list(symbols)
    log.info(
        f"{ctx.chan.uid} subscribed to {broker} for symbols {symbols}")
    # another actor task may have already created it
    feed = await get_cached_feed(broker)
    symbols2ctxs = feed.subscriptions[feed_type]
    task_is_dead = None

    if feed_type == 'stock':
        get_quotes = feed.quoters.setdefault(
            'stock',
            await feed.mod.stock_quoter(feed.client, symbols)
        )
        # do a smoke quote (note this mutates the input list and filters
        # out bad symbols for now)
        payload = await smoke_quote(get_quotes, symbols, broker)
        # push initial smoke quote response for client initialization
        await ctx.send_yield(payload)
    elif feed_type == 'option':
        # FIXME: yeah we need maybe a more general way to specify
        # the arg signature for the option feed beasides a symbol
        # + expiry date.
        get_quotes = feed.quoters.setdefault(
            'option',
            await feed.mod.option_quoter(feed.client, symbols)
        )
        payload = {
            quote['symbol']: quote
            for quote in await get_quotes(symbols)
        }
        # push initial smoke quote response for client initialization
        await ctx.send_yield(payload)
    try:
        # update map from each symbol to requesting client's chan
        modify_quote_stream(broker, feed_type, symbols, ctx)

        # prevents more then one broker feed task from spawning
        lock = feed.locks.get(feed_type)

        # block and let existing feed task deliver
        # stream data until it is cancelled in which case
        # we'll take over and spawn it again
        async with lock:
            # no data feeder task yet; so start one
            respawn = True
            while respawn:
                respawn = False
                log.info(f"Spawning data feed task for {feed.mod.name}")
                try:
                    # unblocks when no more symbols subscriptions exist and the
                    # quote streamer task terminates
                    await fan_out_to_ctxs(
                        stream_quotes,
                        feed,
                        get_quotes,
                        symbols2ctxs,
                        diff_cached=diff_cached,
                        rate=rate,
                    )
                except trio.BrokenResourceError:
                    log.exception("Respawning failed data feed task")
                    respawn = True
    finally:
        # if we're cancelled externally unsubscribe our quote feed
        modify_quote_stream(broker, feed_type, [], ctx)

        # if there are truly no more subscriptions with this broker
        # drop from broker subs dict
        if not any(symbols2ctxs.values()):
            log.info(f"No more subscriptions for broker {broker}")

            # destroy the API client
            await feed.exit_stack.aclose()


class DataFeed:
    """Data feed client for streaming symbol data from a (remote)
    ``brokerd`` data daemon.
    """
    _allowed = ('stock', 'option')

    def __init__(self, portal, brokermod):
        self.portal = portal
        self.brokermod = brokermod
        self._quote_type = None
        self._symbols = None
        self.quote_gen = None
        self._mutex = trio.StrictFIFOLock()
        self._symbol_data_cache: Dict[str, Any] = {}

    async def open_stream(self, symbols, feed_type, rate=1, test=None):
        if feed_type not in self._allowed:
            raise ValueError(f"Only feed types {self._allowed} are supported")

        self._quote_type = feed_type

        async with self._mutex:
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
                        "piker.brokers.data", 'symbol_data',
                        broker=self.brokermod.name, tickers=symbols)
                    self._symbol_data_cache.update(sd)

                if test:
                    # stream from a local test file
                    quote_gen = await self.portal.run(
                        "piker.brokers.data", 'stream_from_file',
                        filename=test
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
                        rate=rate,
                    )

                # get first quotes response
                log.debug(f"Waiting on first quote for {symbols}...")
                quotes = {}
                # with trio.move_on_after(5):
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
        self._symbol_data_cache.update(symbol_data)
        formatter = getattr(self.brokermod, f'format_{self._quote_type}_quote')
        records, displayables = zip(*[
            formatter(quote, self._symbol_data_cache)
            for quote in quotes.values()
        ])
        return records, displayables


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
        broker=brokermod.name, tickers=tickers)

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
