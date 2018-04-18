"""
Core broker-daemon tasks and API.
"""
import time
import inspect
import json
from functools import partial
import socket
from types import ModuleType
from typing import Coroutine

import trio

from ..log import get_logger
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


async def wait_for_network(get_quotes, sleep=1):
    """Wait until the network comes back up.
    """
    down = False
    while True:
        try:
            with trio.move_on_after(1) as cancel_scope:
                quotes = await get_quotes()
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


class Disconnect(trio.Cancelled):
    "Stream was closed"


class StreamQueue:
    """Stream wrapped as a queue that delivers json serialized "packets"
    delimited by ``delim``.
    """
    def __init__(self, stream, delim=b'\n'):
        self.stream = stream
        self._delim = delim
        self.peer = stream.socket.getpeername()
        self._agen = self._iter_packets()

    async def _iter_packets(self):
        """Yield packets from the underlying stream.
        """
        delim = self._delim
        buff = b''
        while True:
            packets = []
            try:
                data = await self.stream.receive_some(2**10)
            except trio.BrokenStreamError as err:
                log.debug("Stream connection was broken")
                return

            log.trace(f"Data is {data}")
            if data == b'':
                log.debug("Stream connection was closed")
                return

            if buff:  # last received packet was segmented
                data = buff + data

            # if last packet has not fully arrived it will
            # be a truncated byte-stream
            packets = data.split(delim)
            buff = packets.pop()

            for packet in packets:
                try:
                    yield json.loads(packet)
                except json.decoder.JSONDecodeError:
                    log.exception(f"Failed to process JSON packet: {buff}")
                    continue

    async def put(self, data):
        return await self.stream.send_all(json.dumps(data).encode() + b'\n')

    async def get(self):
        return await self._agen.asend(None)

    async def __aiter__(self):
        return self._agen


async def poll_tickers(
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
    sleeptime = round(1. / rate, 3)
    _cache = {}  # ticker to quote caching

    broker_limit = getattr(brokermod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {brokermod.__name__} query rate to {rate}/sec")

    while True:  # use an event here to trigger exit?
        prequote_start = time.time()

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
            # FIXME: None is returned if a symbol can't be found.
            # Consider filtering out such symbols before starting poll loop
            if quote is None:
                continue

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
                await queue.put(payload)

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


async def start_quoter(stream):
    """Handle per-broker quote stream subscriptions.

    Spawns new quoter tasks for each broker backend on-demand.
    """
    broker2tickersubs = {}
    tickers2qs = {}
    clients = {}

    queue = StreamQueue(stream)  # wrap in a shabby queue-like api
    log.debug(f"Accepted new connection from {queue.peer}")
    async with trio.open_nursery() as nursery:
        async with queue.stream:
            async for (broker, tickers) in queue:
                log.info(
                    f"{queue.peer} subscribed to {broker} for tickers {tickers}")

                if broker not in broker2tickersubs:
                    tickers2qs = broker2tickersubs.setdefault(
                        broker, {}.fromkeys(tickers, {queue,}))
                    brokermod = get_brokermod(broker)
                    log.info(f"Spawning quote streamer for broker {broker}")

                    # TODO: move to AsyncExitStack in 3.7
                    client = await brokermod.get_client().__aenter__()
                    get_quotes = await brokermod.quoter(client, tickers)
                else:
                    brokermod, client, get_quotes = clients[broker]
                    tickers2qs = broker2tickersubs[broker]
                    # update map from each symbol to requesting client's queue
                    for ticker in tickers:
                        tickers2qs.setdefault(ticker, set()).add(queue)
                    # remove stale ticker subscriptions
                    for ticker in set(tickers2qs) - set(tickers):
                        tickers2qs[ticker].remove(queue)

                # run a single quote filtering out any bad tickers
                quotes = await get_quotes(tickers)
                # pop any tickers that aren't returned in the first quote
                for ticker in set(tickers) - set(quotes):
                    log.warn(
                        f"Symbol `{ticker}` not found by broker `{brokermod.name}`")
                    tickers2qs.pop(ticker)

                # pop any tickers that return "empty" quotes
                payload = {}
                for symbol, quote in quotes.items():
                    if quote is None:
                        log.warn(
                            f"Symbol `{symbol}` not found by broker"
                            f" `{brokermod.name}`")
                        tickers2qs.pop(symbol, None)
                        continue
                    payload[symbol] = quote

                if broker not in clients:  # no quoter task yet
                    clients[broker] = (brokermod, client, get_quotes)
                    # push initial quotes response for client initialization
                    await queue.put(payload)

                    # task should begin on the next checkpoint/iteration
                    log.info(f"Spawning quoter task for {brokermod.name}")
                    nursery.start_soon(
                        poll_tickers, brokermod, get_quotes, tickers2qs)
            else:
                log.info(f"{queue.peer} was disconnected")
                nursery.cancel_scope.cancel()

            # TODO: move to AsyncExitStack in 3.7
            for _, client, _ in clients.values():
                await client.__aexit__()


async def _daemon_main(brokermod):
    """Entry point for the broker daemon.
    """
    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(
            partial(trio.serve_tcp, start_quoter, 1616, host='127.0.0.1')
        )
        log.debug(f"Spawned {listeners}")
