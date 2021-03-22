"""
pikerd daemon lifecylcle & rpc
"""
from typing import Optional, Dict, Callable
from contextlib import asynccontextmanager

import tractor

from .log import get_logger, get_console_log
from .brokers import get_brokermod


log = get_logger(__name__)

_root_nursery: Optional[tractor._trionics.ActorNursery] = None
_root_dname = 'pikerd'
_root_modules = [
    __name__,
    'piker.clearing._ems',
    'piker.clearing._client',
]


@asynccontextmanager
async def open_pikerd(
    loglevel: Optional[str] = None
) -> Optional[tractor._portal.Portal]:

    global _root_nursery

    async with tractor.open_root_actor(
        name=_root_dname,
        loglevel=loglevel,

        # TODO: eventually we should be able to avoid
        # having the root have more then permissions to
        # spawn other specialized daemons I think?
        # enable_modules=[__name__],
        enable_modules=_root_modules
    ):
        # init root nursery
        try:
            async with tractor.open_nursery() as nursery:
                _root_nursery = nursery
                yield nursery
        finally:
            # client code may block indefinitely so cancel when
            # teardown is invoked
            await nursery.cancel()


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
            async with open_pikerd(loglevel) as nursery:
                # in the case where we're starting up the
                # tractor-piker runtime stack in **this** process
                # we want to hand off a nursery for starting (as a sub)
                # whatever actor is requesting pikerd.
                yield None


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

    log.info(f'Spawning {brokername} broker daemon')

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'

    extra_tractor_kwargs = getattr(brokermod, '_spawn_kwargs', {})
    tractor_kwargs.update(extra_tractor_kwargs)

    # TODO: raise exception when _root_nursery == None?
    global _root_nursery
    portal = await _root_nursery.start_actor(
        dname,
        enable_modules=_data_mods + [brokermod.__name__],
        loglevel=loglevel,
        **tractor_kwargs
    )
    return dname


async def spawn_emsd(
    brokername,
    loglevel: Optional[str] = None,
    **extra_tractor_kwargs
) -> tractor._portal.Portal:

    from .clearing import _client

    log.info('Spawning emsd')

    # TODO: raise exception when _root_nursery == None?
    global _root_nursery
    assert _root_nursery

    portal = await _root_nursery.start_actor(
        'emsd',
        enable_modules=[
            'piker.clearing._ems',
            'piker.clearing._client',
        ],
        loglevel=loglevel,
        **extra_tractor_kwargs
    )
    return 'emsd'
