"""
Data feed apis and infra.

We ship some tsdb integrations for retrieving
and storing data from your brokers as well as
sharing your feeds with other fellow pikers.
"""
from contextlib import asynccontextmanager
from typing import (
    Dict, List, Any,
    Sequence, AsyncIterator, Optional
)

import tractor

from ..brokers import get_brokermod
from ..log import get_logger


log = get_logger(__name__)


_data_mods = [
    'piker.brokers.core',
    'piker.brokers.data',
]


@asynccontextmanager
async def maybe_spawn_brokerd(
    brokername: str,
    sleep: float = 0.5,
    tries: int = 10,
    loglevel: Optional[str] = None,
    expose_mods: List = [],
    **tractor_kwargs,
) -> tractor._portal.Portal:
    """If no ``brokerd.{brokername}`` daemon-actor can be found,
    spawn one in a local subactor and return a portal to it.
    """
    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'
    async with tractor.find_actor(dname) as portal:
        # WTF: why doesn't this work?
        log.info(f"YOYOYO {__name__}")
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
) -> AsyncIterator[Dict[str, Any]]:
    try:
        mod = get_brokermod(name)
    except ImportError:
        # TODO: try to pull up ingest feeds
        # - market store
        # - influx
        raise

    async with maybe_spawn_brokerd(
        mod.name,
    ) as portal:
        stream = await portal.run(
            mod.__name__,
            'stream_quotes',
            symbols=symbols,
        )
        yield stream
