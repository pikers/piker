"""
Data feed apis and infra.

We provide tsdb integrations for retrieving
and storing data from your brokers as well as
sharing your feeds with other fellow pikers.
"""
from contextlib import asynccontextmanager
from importlib import import_module
from types import ModuleType
from typing import (
    Dict, List, Any,
    Sequence, AsyncIterator, Optional
)

import trio
import tractor

from ..brokers import get_brokermod
from ..log import get_logger, get_console_log
from ._normalize import iterticks
from ._sharedmem import (
    maybe_open_shared_array, attach_shared_array, open_shared_array,
)
from ._buffer import incr_buffer


__all__ = [
 'maybe_open_shared_array',
 'attach_shared_array',
 'open_shared_array',
 'iterticks',
 'incr_buffer',
]


log = get_logger(__name__)

__ingestors__ = [
    'marketstore',
]


def get_ingestormod(name: str) -> ModuleType:
    """Return the imported ingestor module by name.
    """
    module = import_module('.' + name, 'piker.data')
    # we only allow monkeying because it's for internal keying
    module.name = module.__name__.split('.')[-1]
    return module


_data_mods = [
    'piker.brokers.core',
    'piker.brokers.data',
    'piker.data',
]


@asynccontextmanager
async def maybe_spawn_brokerd(
    brokername: str,
    sleep: float = 0.5,
    loglevel: Optional[str] = None,
    expose_mods: List = [],
    **tractor_kwargs,
) -> tractor._portal.Portal:
    """If no ``brokerd.{brokername}`` daemon-actor can be found,
    spawn one in a local subactor and return a portal to it.
    """
    if loglevel:
        get_console_log(loglevel)

    tractor_kwargs['loglevel'] = loglevel

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'
    async with tractor.find_actor(dname) as portal:
        # WTF: why doesn't this work?
        if portal is not None:
            yield portal
        else:
            log.info(f"Spawning {brokername} broker daemon")
            tractor_kwargs = getattr(brokermod, '_spawn_kwargs', {})
            async with tractor.open_nursery() as nursery:
                try:
                    # spawn new daemon
                    portal = await nursery.start_actor(
                        dname,
                        rpc_module_paths=_data_mods + [brokermod.__name__],
                        loglevel=loglevel,
                        **tractor_kwargs
                    )
                    async with tractor.wait_for_actor(dname) as portal:
                        yield portal
                finally:
                    # client code may block indefinitely so cancel when
                    # teardown is invoked
                    await nursery.cancel()


@asynccontextmanager
async def open_feed(
    name: str,
    symbols: Sequence[str],
    loglevel: Optional[str] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Open a "data feed" which provides streamed real-time quotes.
    """
    try:
        mod = get_brokermod(name)
    except ImportError:
        mod = get_ingestormod(name)

    if loglevel is None:
        loglevel = tractor.current_actor().loglevel

    with maybe_open_shared_array(
        name=f'{name}.{symbols[0]}.buf',
        readonly=True,  # we expect the sub-actor to write
    ) as shmarr:
        async with maybe_spawn_brokerd(
            mod.name,
            loglevel=loglevel,
        ) as portal:
            stream = await portal.run(
                mod.__name__,
                'stream_quotes',
                symbols=symbols,
                shared_array_token=shmarr.token,
                topics=symbols,
            )
            # Feed is required to deliver an initial quote asap.
            # TODO: should we timeout and raise a more explicit error?
            # with trio.fail_after(5):
            with trio.fail_after(float('inf')):
                # Retreive initial quote for each symbol
                # such that consumer code can know the data layout
                first_quote, child_shmarr_token = await stream.__anext__()
                log.info(f"Received first quote {first_quote}")

            if child_shmarr_token is not None:
                # we are the buffer writer task
                increment_stream = await portal.run(
                    'piker.data',
                    'incr_buffer',
                    shm_token=child_shmarr_token,
                )

                assert child_shmarr_token == shmarr.token
            else:
                increment_stream = None

            yield (first_quote, stream, increment_stream, shmarr)
