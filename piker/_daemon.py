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
    'piker.clearing._ems',
    'piker.clearing._client',
]


@asynccontextmanager
async def open_pikerd(
    loglevel: Optional[str] = None,
    **kwargs,
) -> Optional[tractor._portal.Portal]:

    global _root_nursery

    # XXX: this may open a root actor as well
    async with tractor.open_nursery(
        name=_root_dname,

        # TODO: eventually we should be able to avoid
        # having the root have more then permissions to
        # spawn other specialized daemons I think?
        # enable_modules=[__name__],
        enable_modules=_root_modules,

        loglevel=loglevel,
    ) as nursery:
        _root_nursery = nursery
        yield nursery


@asynccontextmanager
async def maybe_open_pikerd(
    loglevel: Optional[str] = None,
    **kwargs,
) -> Optional[tractor._portal.Portal]:
    """If no ``pikerd`` daemon-root-actor can be found,
    assume that role and return a portal to myself

    """
    global _root_nursery

    if loglevel:
        get_console_log(loglevel)

    try:
        async with tractor.find_actor(_root_dname) as portal:
            if portal is not None:  # pikerd exists
                yield portal
                return

    except RuntimeError:  # tractor runtime not started yet
        pass

    # assume pikerd role
    async with open_pikerd(
        loglevel,
        **kwargs,
    ):
        assert _root_nursery

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

    await _root_nursery.start_actor(
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

    log.info('Spawning emsd')

    # TODO: raise exception when _root_nursery == None?
    global _root_nursery

    await _root_nursery.start_actor(
        'emsd',
        enable_modules=[
            'piker.clearing._ems',
            'piker.clearing._client',
        ],
        loglevel=loglevel,
        **extra_tractor_kwargs
    )
    return 'emsd'
