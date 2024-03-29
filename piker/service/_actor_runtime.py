# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
``tractor`` wrapping + default config to bootstrap the `pikerd`.

"""
from __future__ import annotations
import os
from typing import (
    Optional,
    Any,
    ClassVar,
)
from contextlib import (
    asynccontextmanager as acm,
)

import tractor
import trio

from ._util import (
    get_console_log,
)
from ._mngr import (
    Services,
)
from ._registry import (  # noqa
    _tractor_kwargs,
    _default_reg_addr,
    open_registry,
)


def get_tractor_runtime_kwargs() -> dict[str, Any]:
    '''
    Deliver ``tractor`` related runtime variables in a `dict`.

    '''
    return _tractor_kwargs


@acm
async def open_piker_runtime(
    name: str,
    enable_modules: list[str] = [],
    loglevel: Optional[str] = None,

    # XXX NOTE XXX: you should pretty much never want debug mode
    # for data daemons when running in production.
    debug_mode: bool = False,

    registry_addr: None | tuple[str, int] = None,

    # TODO: once we have `rsyscall` support we will read a config
    # and spawn the service tree distributed per that.
    start_method: str = 'trio',

    tractor_runtime_overrides: dict | None = None,
    **tractor_kwargs,

) -> tuple[
    tractor.Actor,
    tuple[str, int],
]:
    '''
    Start a piker actor who's runtime will automatically sync with
    existing piker actors on the local link based on configuration.

    Can be called from a subactor or any program that needs to start
    a root actor.

    '''
    try:
        # check for existing runtime
        actor = tractor.current_actor().uid

    except tractor._exceptions.NoRuntime:
        tractor._state._runtime_vars[
            'piker_vars'] = tractor_runtime_overrides

        registry_addr = registry_addr or _default_reg_addr

        async with (
            tractor.open_root_actor(

                # passed through to ``open_root_actor``
                arbiter_addr=registry_addr,
                name=name,
                loglevel=loglevel,
                debug_mode=debug_mode,
                start_method=start_method,

                # TODO: eventually we should be able to avoid
                # having the root have more then permissions to
                # spawn other specialized daemons I think?
                enable_modules=enable_modules,

                **tractor_kwargs,
            ) as _,

            open_registry(registry_addr, ensure_exists=False) as addr,
        ):
            yield (
                tractor.current_actor(),
                addr,
            )
    else:
        async with open_registry(registry_addr) as addr:
            yield (
                actor,
                addr,
            )


_root_dname = 'pikerd'
_root_modules = [
    __name__,
    'piker.service._daemon',
    'piker.brokers._daemon',

    'piker.clearing._ems',
    'piker.clearing._client',

    'piker.data._sampling',
]


@acm
async def open_pikerd(

    loglevel: str | None = None,

    # XXX: you should pretty much never want debug mode
    # for data daemons when running in production.
    debug_mode: bool = False,
    registry_addr: None | tuple[str, int] = None,

    **kwargs,

) -> Services:
    '''
    Start a root piker daemon with an indefinite lifetime.

    A root actor nursery is created which can be used to create and keep
    alive underling services (see below).

    '''
    async with (
        open_piker_runtime(

            name=_root_dname,
            # TODO: eventually we should be able to avoid
            # having the root have more then permissions to
            # spawn other specialized daemons I think?
            enable_modules=_root_modules,
            loglevel=loglevel,
            debug_mode=debug_mode,
            registry_addr=registry_addr,

            **kwargs,

        ) as (root_actor, reg_addr),
        tractor.open_nursery() as actor_nursery,
        trio.open_nursery() as service_nursery,
    ):
        if root_actor.accept_addr != reg_addr:
            raise RuntimeError(
                f'`pikerd` failed to bind on {reg_addr}!\n'
                'Maybe you have another daemon already running?'
            )

        # assign globally for future daemon/task creation
        Services.actor_n = actor_nursery
        Services.service_n = service_nursery
        Services.debug_mode = debug_mode

        try:
            yield Services

        finally:
            # TODO: is this more clever/efficient?
            # if 'samplerd' in Services.service_tasks:
            #     await Services.cancel_service('samplerd')
            service_nursery.cancel_scope.cancel()


# TODO: do we even need this?
# @acm
# async def maybe_open_runtime(
#     loglevel: Optional[str] = None,
#     **kwargs,

# ) -> None:
#     '''
#     Start the ``tractor`` runtime (a root actor) if none exists.

#     '''
#     name = kwargs.pop('name')

#     if not tractor.current_actor(err_on_no_runtime=False):
#         async with open_piker_runtime(
#             name,
#             loglevel=loglevel,
#             **kwargs,
#         ) as (_, addr):
#             yield addr,
#     else:
#         async with open_registry() as addr:
#             yield addr


@acm
async def maybe_open_pikerd(
    loglevel: Optional[str] = None,
    registry_addr: None | tuple = None,

    **kwargs,

) -> tractor._portal.Portal | ClassVar[Services]:
    '''
    If no ``pikerd`` daemon-root-actor can be found start it and
    yield up (we should probably figure out returning a portal to self
    though).

    '''
    if loglevel:
        get_console_log(loglevel)

    # subtle, we must have the runtime up here or portal lookup will fail
    query_name = kwargs.pop(
        'name',
        f'piker_query_{os.getpid()}',
    )

    # TODO: if we need to make the query part faster we could not init
    # an actor runtime and instead just hit the socket?
    # from tractor._ipc import _connect_chan, Channel
    # async with _connect_chan(host, port) as chan:
    #     async with open_portal(chan) as arb_portal:
    #         yield arb_portal

    async with (
        open_piker_runtime(
            name=query_name,
            registry_addr=registry_addr,
            loglevel=loglevel,
            **kwargs,
        ) as _,

        tractor.find_actor(
            _root_dname,
            arbiter_sockaddr=registry_addr,
        ) as portal
    ):
        # connect to any existing daemon presuming
        # its registry socket was selected.
        if (
            portal is not None
        ):
            yield portal
            return

    # presume pikerd role since no daemon could be found at
    # configured address
    async with open_pikerd(
        loglevel=loglevel,
        registry_addr=registry_addr,

        # passthrough to ``tractor`` init
        **kwargs,

    ) as service_manager:
        # in the case where we're starting up the
        # tractor-piker runtime stack in **this** process
        # we return no portal to self.
        assert service_manager
        yield service_manager
