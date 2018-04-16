"""
Core broker-daemon tasks and API.
"""
import time
import inspect
import json
from functools import partial
import socket
from types import ModuleType
from typing import AsyncContextManager

import trio

from ..log import get_logger
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
        """Get a packet from the underlying stream.
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
    client: 'Client',
    quoter: AsyncContextManager,
    tickers: [str],
    queue: StreamQueue,
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

    async with quoter(client, tickers) as get_quotes:
        # run a first quote smoke test filtering out any bad tickers
        first_quotes_dict = await get_quotes(tickers)
        # FIXME: oh god it's so hideous
        tickers[:] = list(first_quotes_dict.keys())[:]

        while True:  # use an event here to trigger exit?
            prequote_start = time.time()

            with trio.move_on_after(3) as cancel_scope:
                quotes = await get_quotes(tickers)

            cancelled = cancel_scope.cancelled_caught
            if cancelled:
                log.warn("Quote query timed out after 3 seconds, retrying...")
                # handle network outages by idling until response is received
                quotes = await wait_for_network(partial(get_quotes, tickers))

            postquote_start = time.time()
            payload = {}
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
                        payload[symbol] = quote
                else:
                    payload[symbol] = quote

            if payload:
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


async def _handle_subs(
    queue,
    stream2tickers,
    nursery,
    task_status=trio.TASK_STATUS_IGNORED
):
    """Handle quote stream subscriptions.
    """
    async with queue.stream:
        async for tickers in queue:
            task_status.started(tickers)
            log.info(f"{queue.peer} subscribed for tickers {tickers}")
            stream2tickers[queue.peer] = tickers
        else:
            log.info(f"{queue.peer} was disconnected")
            nursery.cancel_scope.cancel()


async def _daemon_main(brokermod):
    """Main entry point for the piker daemon.
    """
    rate = 5
    broker_limit = getattr(brokermod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {brokermod.__name__} query rate to {rate}/sec")

    stream2tickers = {}
    async with brokermod.get_client() as client:

        async def start_quoter(stream):
            queue = StreamQueue(stream)  # wrap in a shabby queue-like api
            log.debug(f"Accepted new connection from {queue.peer}")

            # spawn request handler
            async with trio.open_nursery() as nursery:
                await nursery.start(
                    _handle_subs, queue, stream2tickers, nursery)
                nursery.start_soon(
                    partial(
                        poll_tickers, client, brokermod.quoter,
                        stream2tickers[queue.peer], queue, rate=rate)
                )

        async with trio.open_nursery() as nursery:
            listeners = await nursery.start(
                partial(trio.serve_tcp, start_quoter, 1616, host='127.0.0.1')
            )
            log.debug(f"Spawned {listeners}")
