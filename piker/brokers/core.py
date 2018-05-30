"""
Core broker-daemon tasks and API.
"""
import time
import inspect
from functools import partial
import socket
from types import ModuleType
from typing import Coroutine, Callable

import trio

from ..log import get_logger
from ..ipc import StreamQueue
from . import get_brokermod
log = get_logger('broker.core')


async def api(brokermod: ModuleType, methname: str, **kwargs) -> dict:
    """Make (proxy through) an api call by name and return its result.
    """
    async with brokermod.get_client() as client:
        meth = getattr(client.api, methname, None)
        if meth is None:
            log.error(f"No api method `{methname}` could be found?")
            return
        elif not kwargs:
            # verify kwargs requirements are met
            sig = inspect.signature(meth)
            if sig.parameters:
                log.error(
                    f"Argument(s) are required by the `{methname}` method: "
                    f"{tuple(sig.parameters.keys())}")
                return

        return await meth(**kwargs)


async def quote(brokermod: ModuleType, tickers: [str]) -> dict:
    """Return quotes dict for ``tickers``.
    """
    async with brokermod.get_client() as client:
        results = await client.quote(tickers)
        for key, val in results.items():
            if val is None:
                brokermod.log.warn(f"Could not find symbol {key}?")

        return results


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
    get_quotes: Coroutine,
    tickers2qs: {str: StreamQueue},
    rate: int = 5,  # delay between quote requests
    diff_cached: bool = True,  # only deliver "new" quotes to the queue
) -> None:
    """Stream quotes for a sequence of tickers at the given ``rate``
    per second.

    A broker-client ``quoter`` async context manager must be provided which
    returns an async quote function.
    """
    broker_limit = getattr(brokermod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {brokermod.__name__} query rate to {rate}/sec")

    sleeptime = round(1. / rate, 3)
    _cache = {}  # ticker to quote caching

    while True:  # use an event here to trigger exit?
        prequote_start = time.time()

        if not any(tickers2qs.values()):
            log.warn(f"No subs left for broker {brokermod.name}, exiting task")
            break

        tickers = list(tickers2qs.keys())
        with trio.move_on_after(3) as cancel_scope:
            quotes = await get_quotes(tickers)

        cancelled = cancel_scope.cancelled_caught
        if cancelled:
            log.warn("Quote query timed out after 3 seconds, retrying...")
            # handle network outages by idling until response is received
            quotes = await wait_for_network(partial(get_quotes, tickers))

        postquote_start = time.time()
        q_payloads = {}
        for symbol, quote in quotes.items():
            if diff_cached:
                # if cache is enabled then only deliver "new" changes
                last = _cache.setdefault(symbol, {})
                new = set(quote.items()) - set(last.items())
                if new:
                    log.info(
                        f"New quote {quote['symbol']}:\n{new}")
                    _cache[symbol] = quote
                    for queue in tickers2qs[symbol]:
                        q_payloads.setdefault(queue, {})[symbol] = quote
            else:
                for queue in tickers2qs[symbol]:
                    q_payloads.setdefault(queue, {})[symbol] = quote

        # deliver to each subscriber
        if q_payloads:
            for queue, payload in q_payloads.items():
                try:
                    await queue.put(payload)
                except (
                    # That's right, anything you can think of...
                    trio.ClosedStreamError, ConnectionResetError,
                    ConnectionRefusedError,
                ):
                    log.warn(f"{queue.peer} went down?")
                    for qset in tickers2qs.values():
                        qset.discard(queue)

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


async def start_quoter(
    broker2tickersubs: dict,
    clients: dict,
    dtasks: set,  # daemon task registry
    nursery: "Nusery",
    stream: trio.SocketStream,
) -> None:
    """Handle per-broker quote stream subscriptions.

    Spawns new quoter tasks for each broker backend on-demand.
    Since most brokers seems to support batch quote requests we
    limit to one task per process for now.
    """
    queue = StreamQueue(stream)  # wrap in a shabby queue-like api
    log.info(f"Accepted new connection from {queue.peer}")
    async with queue.stream:
        async for broker, tickers in queue:
            log.info(
                f"{queue.peer} subscribed to {broker} for tickers {tickers}")

            if broker not in broker2tickersubs:
                brokermod = get_brokermod(broker)

                # TODO: move to AsyncExitStack in 3.7
                client_cntxmng = brokermod.get_client()
                client = await client_cntxmng.__aenter__()
                get_quotes = await brokermod.quoter(client, tickers)
                clients[broker] = (
                    brokermod, client, client_cntxmng, get_quotes)
                tickers2qs = broker2tickersubs.setdefault(broker, {})
            else:
                log.info(f"Subscribing with existing `{broker}` daemon")
                brokermod, client, _, get_quotes = clients[broker]
                tickers2qs = broker2tickersubs[broker]

            # beginning of section to be trimmed out with #37
            #################################################
            # get a single quote filtering out any bad tickers
            # NOTE: this code is always run for every new client
            # subscription even when a broker quoter task is already running
            # since the new client needs to know what symbols are accepted
            log.warn(f"Retrieving smoke quote for {queue.peer}")
            quotes = await get_quotes(tickers)
            # report any tickers that aren't returned in the first quote
            invalid_tickers = set(tickers) - set(quotes)
            for symbol in invalid_tickers:
                tickers.remove(symbol)
                log.warn(
                    f"Symbol `{symbol}` not found by broker `{brokermod.name}`"
                )

            # pop any tickers that return "empty" quotes
            payload = {}
            for symbol, quote in quotes.items():
                if quote is None:
                    log.warn(
                        f"Symbol `{symbol}` not found by broker"
                        f" `{brokermod.name}`")
                    tickers.remove(symbol)
                    continue
                payload[symbol] = quote

            # end of section to be trimmed out with #37
            ###########################################

            # first respond with symbol data for all tickers (allows
            # clients to receive broker specific setup info)
            sd = await client.symbol_data(tickers)
            assert sd, "No symbol data could be found?"
            await queue.put(sd)

            # update map from each symbol to requesting client's queue
            for ticker in tickers:
                tickers2qs.setdefault(ticker, set()).add(queue)

            # push initial quotes response for client initialization
            await queue.put(payload)

            if broker not in dtasks:  # no quoter task yet
                # task should begin on the next checkpoint/iteration
                log.info(f"Spawning quoter task for {brokermod.name}")
                nursery.start_soon(
                    stream_quotes, brokermod, get_quotes, tickers2qs)
                dtasks.add(broker)

            log.debug("Waiting on subscription request")
        else:
            log.info(f"client @ {queue.peer} disconnected")
            # drop any lingering subscriptions
            for ticker, qset in tickers2qs.items():
                qset.discard(queue)

            # if there are no more subscriptions with this broker
            # drop from broker subs dict
            if not any(tickers2qs.values()):
                log.info(f"No more subscriptions for {broker}")
                broker2tickersubs.pop(broker, None)
                dtasks.discard(broker)

        # TODO: move to AsyncExitStack in 3.7
        for _, _, cntxmng, _ in clients.values():
            # FIXME: yes I know it's totally wrong...
            await cntxmng.__aexit__(None, None, None)


async def _brokerd_main(host) -> None:
    """Entry point for the broker daemon which waits for connections
    before spawning micro-services.
    """
    # global space for broker-daemon subscriptions
    broker2tickersubs = {}
    clients = {}
    dtasks = set()

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(
            partial(
                trio.serve_tcp,
                partial(
                    start_quoter, broker2tickersubs, clients,
                    dtasks, nursery
                ),
                1616, host=host,
            )
        )
        log.debug(f"Spawned {listeners}")
