"""
Core broker-daemon tasks and API.
"""
import time
import inspect
from types import ModuleType
from typing import AsyncContextManager

import trio

from .questrade import QuestradeError
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


async def poll_tickers(
    client: 'Client',
    quoter: AsyncContextManager,
    tickers: [str],
    q: trio.Queue,
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
        while True:  # use an event here to trigger exit?
            prequote_start = time.time()
            quotes = await get_quotes(tickers)
            postquote_start = time.time()
            payload = []
            for symbol, quote in quotes.items():
                # FIXME: None is returned if a symbol can't be found.
                # Consider filtering out such symbols before starting poll loop
                if quote is None:
                    continue

                if quote.get('delay', 0) > 0:
                    log.warning(f"Delayed quote:\n{quote}")

                if diff_cached:
                    # if cache is enabled then only deliver "new" changes
                    symbol = quote['symbol']
                    last = _cache.setdefault(symbol, {})
                    new = set(quote.items()) - set(last.items())
                    if new:
                        log.info(
                            f"New quote {quote['symbol']}:\n{new}")
                        _cache[symbol] = quote
                        payload.append(quote)
                else:
                    payload.append(quote)

            if payload:
                q.put_nowait(payload)

            req_time = round(postquote_start - prequote_start, 3)
            proc_time = round(time.time() - postquote_start, 3)
            tot = req_time + proc_time
            log.debug(f"Request + processing took {tot}")
            delay = sleeptime - tot
            if delay <= 0:
                log.warn(
                    f"Took {req_time} (request) + {proc_time} (processing) = {tot}"
                    f" secs (> {sleeptime}) for processing quotes?")
            else:
                log.debug(f"Sleeping for {delay}")
                await trio.sleep(delay)
