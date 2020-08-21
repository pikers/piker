"""
Actor-aware broker agnostic interface.
"""
from contextlib import asynccontextmanager, AsyncExitStack

import trio
import tractor

from . import get_brokermod
from ..log import get_logger


log = get_logger(__name__)


@asynccontextmanager
async def get_cached_client(
    brokername: str,
    *args,
    **kwargs,
) -> 'Client':  # noqa
    """Get a cached broker client from the current actor's local vars.

    If one has not been setup do it and cache it.
    """
    # check if a cached client is in the local actor's statespace
    ss = tractor.current_actor().statespace
    clients = ss.setdefault('clients', {'_lock': trio.Lock()})
    lock = clients['_lock']
    client = None
    try:
        log.info(f"Loading existing `{brokername}` daemon")
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
        client._consumers -= 1
        if client._consumers <= 0:
            # teardown the client
            await client._exit_stack.aclose()
