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
Cacheing apis and toolz.

"""
from typing import Optional
from contextlib import (
    asynccontextmanager,
    AsyncExitStack,
    contextmanager,
)

import trio

from .brokers import get_brokermod
from .log import get_logger
from . import data
from .data.feed import Feed


log = get_logger(__name__)


_cache: dict[str, 'Client'] = {}  # noqa


@asynccontextmanager
async def open_cached_client(
    brokername: str,
    *args,
    **kwargs,
) -> 'Client':  # noqa
    """Get a cached broker client from the current actor's local vars.

    If one has not been setup do it and cache it.
    """
    global _cache

    clients = _cache.setdefault('clients', {'_lock': trio.Lock()})

    # global cache task lock
    lock = clients['_lock']

    client = None

    try:
        log.info(f"Loading existing `{brokername}` client")

        async with lock:
            client = clients[brokername]
            client._consumers += 1

        yield client

    except KeyError:
        log.info(f"Creating new client for broker {brokername}")

        async with lock:
            brokermod = get_brokermod(brokername)
            exit_stack = AsyncExitStack()

            client = await exit_stack.enter_async_context(
                brokermod.get_client()
            )
            client._consumers = 0
            client._exit_stack = exit_stack
            clients[brokername] = client

        yield client

    finally:
        if client is not None:
            # if no more consumers, teardown the client
            client._consumers -= 1
            if client._consumers <= 0:
                await client._exit_stack.aclose()


class cache:
    '''Globally (processs wide) cached, task access to a
    kept-alive-while-in-use data feed.

    '''
    lock = trio.Lock()
    users: int = 0
    feeds: dict[tuple[str, str], Feed] = {}
    no_more_users: Optional[trio.Event] = None


@asynccontextmanager
async def maybe_open_feed(

    broker: str,
    symbol: str,
    loglevel: str,

) -> Feed:

    key = (broker, symbol)

    @contextmanager
    def get_and_use() -> Feed:
        # key error must bubble here
        feed = cache.feeds[key]
        log.info(f'Reusing cached feed for {key}')
        try:
            cache.users += 1
            yield feed
        finally:
            cache.users -= 1
            if cache.users == 0:
                # signal to original allocator task feed use is complete
                cache.no_more_users.set()

    try:
        with get_and_use() as feed:
            yield feed
    except KeyError:
        # lock feed acquisition around task racing  / ``trio``'s
        # scheduler protocol
        await cache.lock.acquire()
        try:
            with get_and_use() as feed:
                cache.lock.release()
                yield feed
                return

        except KeyError:
            # **critical section** that should prevent other tasks from
            # checking the cache until complete otherwise the scheduler
            # may switch and by accident we create more then one feed.

            cache.no_more_users = trio.Event()

            log.info(f'Allocating new feed for {key}')
            # TODO: eventually support N-brokers
            async with (
                data.open_feed(
                    broker,
                    [symbol],
                    loglevel=loglevel,
                ) as feed,
            ):
                cache.feeds[key] = feed
                cache.lock.release()
                try:
                    yield feed
                finally:
                    # don't tear down the feed until there are zero
                    # users of it left.
                    if cache.users > 0:
                        await cache.no_more_users.wait()

                    log.warning('De-allocating feed for {key}')
                    cache.feeds.pop(key)
