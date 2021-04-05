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
Structured, daemon tree service management.

"""
from functools import partial
from typing import Optional, Union
from contextlib import asynccontextmanager

from pydantic import BaseModel
import trio
import tractor

from .log import get_logger, get_console_log
from .brokers import get_brokermod


log = get_logger(__name__)

_root_dname = 'pikerd'
_root_modules = [
    __name__,
    'piker.clearing._ems',
    'piker.clearing._client',
]


class Services(BaseModel):
    actor_n: tractor._trionics.ActorNursery
    service_n: trio.Nursery

    class Config:
        arbitrary_types_allowed = True


_services: Optional[Services] = None


@asynccontextmanager
async def open_pikerd(
    loglevel: Optional[str] = None,
    **kwargs,
) -> Optional[tractor._portal.Portal]:
    """Start a root piker daemon who's lifetime extends indefinitely
    until cancelled.

    A root actor nursery is created which can be used to create and keep
    alive underling services (see below).

    """
    global _services
    assert _services is None

    # XXX: this may open a root actor as well
    async with tractor.open_root_actor(
            # passed through to ``open_root_actor``
            name=_root_dname,
            loglevel=loglevel,
            # TODO: eventually we should be able to avoid
            # having the root have more then permissions to
            # spawn other specialized daemons I think?
            # enable_modules=[__name__],
            enable_modules=_root_modules,
    ) as _, tractor.open_nursery() as actor_nursery:
        async with trio.open_nursery() as service_nursery:

            # assign globally for future daemon/task creation
            _services = Services(
                actor_n=actor_nursery,
                service_n=service_nursery
            )

            yield _services


@asynccontextmanager
async def maybe_open_runtime(
    loglevel: Optional[str] = None,
    **kwargs,
) -> None:
    if not tractor.current_actor(err_on_no_runtime=False):
        async with tractor.open_root_actor(loglevel=loglevel, **kwargs):
            yield
    else:
        yield


@asynccontextmanager
async def maybe_open_pikerd(
    loglevel: Optional[str] = None,
    **kwargs,
) -> Union[tractor._portal.Portal, Services]:
    """If no ``pikerd`` daemon-root-actor can be found start it and
    yield up (we should probably figure out returning a portal to self
    though).

    """
    if loglevel:
        get_console_log(loglevel)

    # subtle, we must have the runtime up here or portal lookup will fail
    async with maybe_open_runtime(loglevel, **kwargs):
        async with tractor.find_actor(_root_dname) as portal:
            # assert portal is not None
            if portal is not None:
                yield portal
                return

    # presume pikerd role
    async with open_pikerd(
        loglevel,
        **kwargs,
    ) as _:
        # in the case where we're starting up the
        # tractor-piker runtime stack in **this** process
        # we return no portal to self.
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

    from .data import _setup_persistent_brokerd

    log.info(f'Spawning {brokername} broker daemon')

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'

    extra_tractor_kwargs = getattr(brokermod, '_spawn_kwargs', {})
    tractor_kwargs.update(extra_tractor_kwargs)

    global _services
    assert _services

    portal = await _services.actor_n.start_actor(
        dname,
        enable_modules=_data_mods + [brokermod.__name__],
        loglevel=loglevel,
        **tractor_kwargs
    )

    # TODO: so i think this is the perfect use case for supporting
    # a cross-actor async context manager api instead of this
    # shoort-and-forget task spawned in the root nursery, we'd have an
    # async exit stack that we'd register the `portal.open_context()`
    # call with and then have the ability to unwind the call whenevs.

    # non-blocking setup of brokerd service nursery
    _services.service_n.start_soon(
        partial(
            portal.run,
            _setup_persistent_brokerd,
            brokername=brokername,
        )
    )

    return dname


@asynccontextmanager
async def maybe_spawn_brokerd(
    brokername: str,
    loglevel: Optional[str] = None,

    # XXX: you should pretty much never want debug mode
    # for data daemons when running in production.
    debug_mode: bool = True,
) -> tractor._portal.Portal:
    """If no ``brokerd.{brokername}`` daemon-actor can be found,
    spawn one in a local subactor and return a portal to it.

    """
    if loglevel:
        get_console_log(loglevel)

    dname = f'brokerd.{brokername}'

    # attach to existing brokerd if possible
    async with tractor.find_actor(dname) as portal:
        if portal is not None:
            yield portal
            return

    # ask root ``pikerd`` daemon to spawn the daemon we need if
    # pikerd is not live we now become the root of the
    # process tree
    async with maybe_open_pikerd(
        loglevel=loglevel
    ) as pikerd_portal:

        if pikerd_portal is None:
            # we are root so spawn brokerd directly in our tree
            # the root nursery is accessed through process global state
            await spawn_brokerd(brokername, loglevel=loglevel)

        else:
            await pikerd_portal.run(
                spawn_brokerd,
                brokername=brokername,
                loglevel=loglevel,
                debug_mode=debug_mode,
            )

        async with tractor.wait_for_actor(dname) as portal:
            yield portal


async def spawn_emsd(
    brokername,
    loglevel: Optional[str] = None,
    **extra_tractor_kwargs
) -> tractor._portal.Portal:

    log.info('Spawning emsd')

    # TODO: raise exception when _services == None?
    global _services

    await _services.actor_n.start_actor(
        'emsd',
        enable_modules=[
            'piker.clearing._ems',
            'piker.clearing._client',
        ],
        loglevel=loglevel,
        **extra_tractor_kwargs
    )
    return 'emsd'
