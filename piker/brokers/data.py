"""
Live data feed machinery
"""
import time
from functools import partial
from itertools import cycle
import socket
import json
from types import ModuleType
import typing
from typing import Coroutine, Callable, Dict, List, Any
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
    brokermod: ModuleType,
    request_quotes: Coroutine,
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

    while True:  # use an event here to trigger exit?

        prequote_start = time.time()

        with trio.move_on_after(3) as cancel_scope:
            quotes = await request_quotes()

        postquote_start = time.time()

        cancelled = cancel_scope.cancelled_caught
        if cancelled:
            log.warn("Quote query timed out after 3 seconds, retrying...")
            # handle network outages by idling until response is received
            # quotes = await wait_for_network(partial(get_quotes, tickers))
            quotes = await wait_for_network(request_quotes)

        new_quotes = []
        if diff_cached:
            # If cache is enabled then only deliver "new" changes.
            # Useful for polling setups but obviously should be
            # disabled if you're rx-ing event data.
            for quote in quotes:
                symbol = quote['symbol']
                last = _cache.setdefault(symbol, {})
                new = set(quote.items()) - set(last.items())
                if new:
                    log.info(
                        f"New quote {quote['symbol']}:\n{new}")
                    _cache[symbol] = quote
                    new_quotes.append(quote)
        else:
            new_quotes = quotes

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


class DataFeed(typing.NamedTuple):
    """A per broker "data feed" container.

    A structure to keep track of components used by
    real-time data daemons.
    """
    mod: ModuleType
    client: object
    quoter_keys: List[str] = ['stock', 'option']
    tasks: Dict[str, trio._core._run.Task] = dict.fromkeys(
        quoter_keys, False)
    quoters: Dict[str, typing.Coroutine] = {}
    subscriptions: Dict[str, Dict[str, set]] = {'option': {}, 'stock': {}}


async def fan_out_to_chans(
    feed: DataFeed,
    get_quotes: Coroutine,
    symbols2chans: Dict[str, tractor.Channel],
    rate: int = 5,  # delay between quote requests
    diff_cached: bool = True,  # only deliver "new" quotes to the queue
    cid: str = None,
) -> None:
    """Request and fan out quotes to each subscribed actor channel.
    """
    broker_limit = getattr(feed.mod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {feed.mod.__name__} query rate to {rate}/sec")

    async def request():
        """Get quotes for current symbol subscription set.
        """
        return await get_quotes(list(symbols2chans.keys()))

    async for quotes in stream_quotes(
        feed.mod, request, rate,
        diff_cached=diff_cached,
    ):
        chan_payloads = {}
        for quote in quotes:
            # is this too QT specific?
            symbol = quote['symbol']
            # set symbol quotes for each subscriber
            for chan, cid in symbols2chans.get(quote['key'], set()):
                chan_payloads.setdefault(
                    chan,
                    {'yield': {}, 'cid': cid}
                )['yield'][symbol] = quote

        # deliver to each subscriber (fan out)
        if chan_payloads:
            for chan, payload in chan_payloads.items():
                try:
                    await chan.send(payload)
                except (
                    # That's right, anything you can think of...
                    trio.ClosedStreamError, ConnectionResetError,
                    ConnectionRefusedError,
                ):
                    log.warn(f"{chan} went down?")
                    for chanset in symbols2chans.values():
                        chanset.discard((chan, cid))

        if not any(symbols2chans.values()):
            log.warn(f"No subs left for broker {feed.mod.name}, exiting task")
            break

    log.info(f"Terminating stream quoter task for {feed.mod.name}")


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


async def modify_quote_stream(broker, feed_type, symbols, chan=None, cid=None):
    """Absolute symbol subscription list for each quote stream.

    Effectively a symbol subscription api.
    """
    log.info(f"{chan} changed symbol subscription to {symbols}")
    feed = await get_cached_feed(broker)
    symbols2chans = feed.subscriptions[feed_type]
    # update map from each symbol to requesting client's chan
    for ticker in symbols:
        symbols2chans.setdefault(ticker, set()).add((chan, cid))

    # remove any existing symbol subscriptions if symbol is not
    # found in ``symbols``
    # TODO: this can likely be factored out into the pub-sub api
    for ticker in filter(
        lambda ticker: ticker not in symbols, symbols2chans.copy()
    ):
        chanset = symbols2chans.get(ticker)
        # XXX: cid will be different on unsub call
        for item in chanset.copy():
            if chan in item:
                chanset.discard(item)

        if not chanset:
            # pop empty sets which will trigger bg quoter task termination
            symbols2chans.pop(ticker)


async def get_cached_feed(
    brokername: str,
) -> DataFeed:
    """Get/create a ``DataFeed`` from/in the current actor.
    """
    # check if a cached client is in the local actor's statespace
    ss = tractor.current_actor().statespace
    feeds = ss['feeds']
    lock = feeds['_lock']
    feed_stack = ss['feed_stacks'][brokername]
    async with lock:
        try:
            feed = feeds[brokername]
            log.info(f"Subscribing with existing `{brokername}` daemon")
            return feed
        except KeyError:
            log.info(f"Creating new client for broker {brokername}")
            brokermod = get_brokermod(brokername)
            client = await feed_stack.enter_async_context(
                brokermod.get_client())
            feed = DataFeed(
                mod=brokermod,
                client=client,
            )
            feeds[brokername] = feed
            return feed


async def start_quote_stream(
    broker: str,
    symbols: List[Any],
    feed_type: str = 'stock',
    diff_cached: bool = True,
    chan: tractor.Channel = None,
    cid: str = None,
) -> None:
    """Handle per-broker quote stream subscriptions using a "lazy" pub-sub
    pattern.

    Spawns new quoter tasks for each broker backend on-demand.
    Since most brokers seems to support batch quote requests we
    limit to one task per process for now.
    """
    actor = tractor.current_actor()
    # set log level after fork
    get_console_log(actor.loglevel)
    # pull global vars from local actor
    ss = actor.statespace
    # broker2symbolsubs = ss.setdefault('broker2symbolsubs', {})
    ss.setdefault('feeds', {'_lock': trio.Lock()})
    feed_stacks = ss.setdefault('feed_stacks', {})
    symbols = list(symbols)
    log.info(
        f"{chan.uid} subscribed to {broker} for symbols {symbols}")
    feed_stack = feed_stacks.setdefault(broker, contextlib.AsyncExitStack())
    # another actor task may have already created it
    feed = await get_cached_feed(broker)
    symbols2chans = feed.subscriptions[feed_type]

    if feed_type == 'stock':
        get_quotes = feed.quoters.setdefault(
            'stock',
            await feed.mod.stock_quoter(feed.client, symbols)
        )
        # do a smoke quote (note this mutates the input list and filters
        # out bad symbols for now)
        payload = await smoke_quote(get_quotes, symbols, broker)
        # push initial smoke quote response for client initialization
        await chan.send({'yield': payload, 'cid': cid})
    elif feed_type == 'option':
        # FIXME: yeah we need maybe a more general way to specify
        # the arg signature for the option feed beasides a symbol
        # + expiry date.
        get_quotes = feed.quoters.setdefault(
            'option',
            await feed.mod.option_quoter(feed.client, symbols)
        )

    # update map from each symbol to requesting client's chan
    await modify_quote_stream(broker, feed_type, symbols, chan, cid)

    try:
        if not feed.tasks.get(feed_type):
            # no data feeder task yet; so start one
            respawn = True
            log.info(f"Spawning data feed task for {feed.mod.name}")
            while respawn:
                respawn = False
                try:
                    async with trio.open_nursery() as nursery:
                        nursery.start_soon(
                            partial(
                                fan_out_to_chans, feed, get_quotes,
                                symbols2chans,
                                diff_cached=diff_cached,
                                cid=cid
                            )
                        )
                        feed.tasks[feed_type] = True
                except trio.BrokenResourceError:
                    log.exception("Respawning failed data feed task")
                    respawn = True
            # unblocks when no more symbols subscriptions exist and the
            # quote streamer task terminates (usually because another call
            # was made to `modify_quoter` to unsubscribe from streaming
            # symbols)
    finally:
        log.info(f"Terminated {feed_type} quoter task for {feed.mod.name}")
        feed.tasks.pop(feed_type)
        # if there are truly no more subscriptions with this broker
        # drop from broker subs dict
        if not any(symbols2chans.values()):
            log.info(f"No more subscriptions for {broker}")
            # broker2symbolsubs.pop(broker, None)

            # destroy the API client
            await feed_stack.aclose()


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
