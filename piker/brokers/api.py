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
Actor-aware broker agnostic interface.

"""
from typing import Dict
from contextlib import asynccontextmanager, AsyncExitStack

import trio

from . import get_brokermod
from ..log import get_logger


log = get_logger(__name__)


_cache: Dict[str, 'Client'] = {}  # noqa


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
