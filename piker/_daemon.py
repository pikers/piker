"""
pikerd daemon lifecylcle & rpc
"""
from typing import Optional
from contextlib import asynccontextmanager

import tractor

from .log import get_logger, get_console_log
from .brokers import get_brokermod


log = get_logger(__name__)

root_nursery: Optional[tractor._trionics.ActorNursery] = None
root_dname = 'pikerd'
root_modules = [
    __name__,
    'piker._ems'
]

@asynccontextmanager
async def maybe_spawn_pikerd(
    loglevel: Optional[str] = None
) -> Optional[tractor._portal.Portal]:
    """If no ``pikerd`` daemon-root-actor can be found,
    assume that role and return a portal to myself
    """
    global root_nursery

    if loglevel:
        get_console_log(loglevel)

    async with tractor.find_actor(root_dname) as portal:

        if portal is not None:  # pikerd exists
            yield portal

        else:  # assume role
            async with tractor.open_root_actor(
                name=root_dname,
                loglevel=loglevel,
                enable_modules=root_modules
            ) as actor:

                # init root nursery
                async with tractor.open_nursery() as nursery:
                    root_nursery = nursery
                    yield None


# brokerd enable modules
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
):

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'
    log.info(f'Spawning {brokername} broker daemon')
    tractor_kwargs = getattr(brokermod, '_spawnkwargs', {})

    # TODO: raise exception when root_nursery == None?
    global root_nursery
    await root_nursery.start_actor(
        dname,
        enable_modules=_data_mods + [brokermod.__name__],
        loglevel=loglevel,
        **tractor_kwargs
    )

