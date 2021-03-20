"""
pikerd daemon lifecylcle & rpc
"""
from typing import Optional
from contextlib import asynccontextmanager

import tractor

from .log import get_logger, get_console_log
from .brokers import get_brokermod


log = get_logger(__name__)

_root_nursery: Optional[tractor._trionics.ActorNursery] = None
_root_dname = 'pikerd'

_root_modules = [
    __name__,
    'piker.exchange._ems',
    'piker.exchange._client',
]


@asynccontextmanager
async def maybe_open_pikerd(
    loglevel: Optional[str] = None
) -> Optional[tractor._portal.Portal]:
    """If no ``pikerd`` daemon-root-actor can be found,
    assume that role and return a portal to myself

    """
    global _root_nursery

    if loglevel:
        get_console_log(loglevel)

    async with tractor.find_actor(_root_dname) as portal:

        if portal is not None:  # pikerd exists
            yield portal

        else:  # assume role
            async with tractor.open_root_actor(
                name=_root_dname,
                loglevel=loglevel,
                enable_modules=_root_modules
            ):
                # init root nursery
                try:
                    async with tractor.open_nursery() as nursery:
                        _root_nursery = nursery
                        yield None
                finally:
                    # client code may block indefinitely so cancel when
                    # teardown is invoked
                    await nursery.cancel()


# brokerd enabled modules
_data_mods = [
    'piker.brokers.core',
    'piker.brokers.data',
    'piker.data',
    'piker.data._buffer'
]


async def spawn_brokerd(
    brokername,
    loglevel: Optional[str] = None,
    **tractor_kwargs
) -> tractor._portal.Portal:

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'
    log.info(f'Spawning {brokername} broker daemon')
    tractor_kwargs = getattr(brokermod, '_spawnkwargs', {})

    # TODO: raise exception when _root_nursery == None?
    global _root_nursery
    portal = await _root_nursery.start_actor(
        dname,
        enable_modules=_data_mods + [brokermod.__name__],
        loglevel=loglevel,
        **tractor_kwargs
    )
    return portal
