"""
Live data feed machinery
"""
import time
from functools import partial
from itertools import cycle
import socket
import json
from types import ModuleType
from typing import Coroutine, Callable, Dict, List

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

        # tickers = list(tickers2chans.keys())
        with trio.move_on_after(3) as cancel_scope:
            quotes = await request_quotes()

        cancelled = cancel_scope.cancelled_caught
        if cancelled:
            log.warn("Quote query timed out after 3 seconds, retrying...")
            # handle network outages by idling until response is received
            # quotes = await wait_for_network(partial(get_quotes, tickers))
            quotes = await wait_for_network(request_quotes)

        postquote_start = time.time()
        new_quotes = {}
        if diff_cached:
            # if cache is enabled then only deliver "new" changes
            for symbol, quote in quotes.items():
                    last = _cache.setdefault(symbol, {})
                    new = set(quote.items()) - set(last.items())
                    if new:
                        log.info(
                            f"New quote {quote['symbol']}:\n{new}")
                        _cache[symbol] = quote
                        new_quotes[symbol] = quote
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


async def fan_out_to_chans(
    brokermod: ModuleType,
    get_quotes: Coroutine,
    tickers2chans: Dict[str, tractor.Channel],
    rate: int = 5,  # delay between quote requests
    diff_cached: bool = True,  # only deliver "new" quotes to the queue
    cid: str = None,
) -> None:
    """Request and fan out quotes to each subscribed actor channel.
    """
    broker_limit = getattr(brokermod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {brokermod.__name__} query rate to {rate}/sec")

    async def request():
        """Get quotes for current symbol subscription set.
        """
        return await get_quotes(list(tickers2chans.keys()))

    async for quotes in stream_quotes(brokermod, request, rate):
        chan_payloads = {}
        for symbol, quote in quotes.items():
            # set symbol quotes for each subscriber
            for chan, cid in tickers2chans.get(symbol, set()):
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
                    for chanset in tickers2chans.values():
                        chanset.discard((chan, cid))

        if not any(tickers2chans.values()):
            log.warn(f"No subs left for broker {brokermod.name}, exiting task")
            break

    log.info(f"Terminating stream quoter task for {brokermod.name}")


async def get_cached_client(broker, tickers):
    """Get or create the current actor's cached broker client.
    """
    # check if a cached client is in the local actor's statespace
    clients = tractor.current_actor().statespace.setdefault('clients', {})
    try:
        return clients[broker]
    except KeyError:
        log.info(f"Creating new client for broker {broker}")
        brokermod = get_brokermod(broker)
        # TODO: move to AsyncExitStack in 3.7
        client_cntxmng = brokermod.get_client()
        client = await client_cntxmng.__aenter__()
        get_quotes = await brokermod.quoter(client, tickers)
        clients[broker] = (
            brokermod, client, client_cntxmng, get_quotes)

        return brokermod, client, client_cntxmng, get_quotes


async def symbol_data(broker, tickers):
    """Retrieve baseline symbol info from broker.
    """
    _, client, _, get_quotes = await get_cached_client(broker, tickers)
    return await client.symbol_data(tickers)


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
    invalid_tickers = set(tickers) - set(quotes)
    for symbol in invalid_tickers:
        tickers.remove(symbol)
        log.warn(
            f"Symbol `{symbol}` not found by broker `{broker}`"
        )

    # pop any tickers that return "empty" quotes
    payload = {}
    for symbol, quote in quotes.items():
        if quote is None:
            log.warn(
                f"Symbol `{symbol}` not found by broker"
                f" `{broker}`")
            # XXX: not this mutates the input list (for now)
            tickers.remove(symbol)
            continue
        payload[symbol] = quote

    return payload

    # end of section to be trimmed out with #37
    ###########################################


def modify_quote_stream(broker, tickers, chan=None, cid=None):
    """Absolute symbol subscription list for each quote stream.

    Effectively a consumer subscription api.
    """
    log.info(f"{chan} changed symbol subscription to {tickers}")
    ss = tractor.current_actor().statespace
    broker2tickersubs = ss['broker2tickersubs']
    tickers2chans = broker2tickersubs.get(broker)
    # update map from each symbol to requesting client's chan
    for ticker in tickers:
        tickers2chans.setdefault(ticker, set()).add((chan, cid))

    for ticker in filter(
        lambda ticker: ticker not in tickers, tickers2chans.copy()
    ):
        chanset = tickers2chans.get(ticker)
        # XXX: cid will be different on unsub call
        for item in chanset.copy():
            if chan in item:
                chanset.discard(item)

        if not chanset:
            # pop empty sets which will trigger bg quoter task termination
            tickers2chans.pop(ticker)


async def start_quote_stream(
    broker: str,
    tickers: List[str],
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
    broker2tickersubs = ss['broker2tickersubs']
    clients = ss['clients']
    dtasks = ss['dtasks']
    tickers = list(tickers)
    log.info(
        f"{chan.uid} subscribed to {broker} for tickers {tickers}")

    brokermod, client, _, get_quotes = await get_cached_client(broker, tickers)
    if broker not in broker2tickersubs:
        tickers2chans = broker2tickersubs.setdefault(broker, {})
    else:
        log.info(f"Subscribing with existing `{broker}` daemon")
        tickers2chans = broker2tickersubs[broker]

    # do a smoke quote (note this mutates the input list and filters
    # out bad symbols for now)
    payload = await smoke_quote(get_quotes, tickers, broker)
    # push initial smoke quote response for client initialization
    await chan.send({'yield': payload, 'cid': cid})

    # update map from each symbol to requesting client's chan
    modify_quote_stream(broker, tickers, chan=chan, cid=cid)

    try:
        if broker not in dtasks:
            # no quoter task yet so start a daemon task
            log.info(f"Spawning quoter task for {brokermod.name}")
            async with trio.open_nursery() as nursery:
                nursery.start_soon(partial(
                    fan_out_to_chans, brokermod, get_quotes, tickers2chans,
                    cid=cid)
                )
                dtasks.add(broker)

            # unblocks when no more symbols subscriptions exist and the
            # quote streamer task terminates (usually because another call
            # was made to `modify_quoter` to unsubscribe from streaming
            # symbols)
            log.info(f"Terminated quoter task for {brokermod.name}")

            # TODO: move to AsyncExitStack in 3.7
            for _, _, cntxmng, _ in clients.values():
                # FIXME: yes I know there's no error handling..
                await cntxmng.__aexit__(None, None, None)
    finally:
        # if there are truly no more subscriptions with this broker
        # drop from broker subs dict
        if not any(tickers2chans.values()):
            log.info(f"No more subscriptions for {broker}")
            broker2tickersubs.pop(broker, None)
            dtasks.discard(broker)


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
