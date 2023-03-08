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
Actor-runtime service orchestration machinery.

"""
from __future__ import annotations
from pprint import pformat
from functools import partial
import os
from typing import (
    Optional,
    Callable,
    Any,
    ClassVar,
)
from contextlib import (
    asynccontextmanager as acm,
)

import tractor
import trio

from ..log import (
    get_logger,
    get_console_log,
)
from ..brokers import get_brokermod
from ._mngr import (
    Services,
)
from ._registry import (  # noqa
    _tractor_kwargs,
    _default_reg_addr,
    _default_registry_host,
    _default_registry_port,
    open_registry,
    find_service,
    check_for_service,
)

log = get_logger(__name__)


__all__ = [
    'check_for_service',
]

_root_dname = 'pikerd'


def get_tractor_runtime_kwargs() -> dict[str, Any]:
    '''
    Deliver ``tractor`` related runtime variables in a `dict`.

    '''
    return _tractor_kwargs


_root_modules = [
    __name__,
    'piker.clearing._ems',
    'piker.clearing._client',
    'piker.data._sampling',
]


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


@acm
async def open_pikerd(

    loglevel: str | None = None,

    # XXX: you should pretty much never want debug mode
    # for data daemons when running in production.
    debug_mode: bool = False,
    registry_addr: None | tuple[str, int] = None,

    # db init flags
    tsdb: bool = False,
    es: bool = False,

) -> Services:
    '''
    Start a root piker daemon who's lifetime extends indefinitely until
    cancelled.

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

        ) as (root_actor, reg_addr),
        tractor.open_nursery() as actor_nursery,
        trio.open_nursery() as service_nursery,
    ):
        if root_actor.accept_addr != reg_addr:
            raise RuntimeError(f'Daemon failed to bind on {reg_addr}!?')

        # assign globally for future daemon/task creation
        Services.actor_n = actor_nursery
        Services.service_n = service_nursery
        Services.debug_mode = debug_mode

        if tsdb:
            from ._ahab import start_ahab
            from .marketstore import start_marketstore

            log.info('Spawning `marketstore` supervisor')
            ctn_ready, config, (cid, pid) = await service_nursery.start(
                partial(
                    start_ahab,
                    'marketstored',
                    start_marketstore,
                    loglevel=loglevel,
                )

            )
            log.info(
                f'`marketstored` up!\n'
                f'pid: {pid}\n'
                f'container id: {cid[:12]}\n'
                f'config: {pformat(config)}'
            )

        if es:
            from piker.data._ahab import start_ahab
            from piker.data.elastic import start_elasticsearch

            log.info('Spawning `elasticsearch` supervisor')
            ctn_ready, config, (cid, pid) = await service_nursery.start(
                partial(
                    start_ahab,
                    'elasticsearch',
                    start_elasticsearch,
                    loglevel=loglevel,
                )
            )

            log.info(
                f'`elasticsearch` up!\n'
                f'pid: {pid}\n'
                f'container id: {cid[:12]}\n'
                f'config: {pformat(config)}'
            )

        try:
            yield Services

        finally:
            # TODO: is this more clever/efficient?
            # if 'samplerd' in Services.service_tasks:
            #     await Services.cancel_service('samplerd')
            service_nursery.cancel_scope.cancel()


@acm
async def maybe_open_runtime(
    loglevel: Optional[str] = None,
    **kwargs,

) -> None:
    '''
    Start the ``tractor`` runtime (a root actor) if none exists.

    '''
    name = kwargs.pop('name')

    if not tractor.current_actor(err_on_no_runtime=False):
        async with open_piker_runtime(
            name,
            loglevel=loglevel,
            **kwargs,
        ) as (_, addr):
            yield addr,
    else:
        async with open_registry() as addr:
            yield addr


@acm
async def maybe_open_pikerd(
    loglevel: Optional[str] = None,
    registry_addr: None | tuple = None,
    tsdb: bool = False,
    es: bool = False,

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
    query_name = kwargs.pop('name', f'piker_query_{os.getpid()}')

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
        debug_mode=kwargs.get('debug_mode', False),
        registry_addr=registry_addr,
        tsdb=tsdb,
        es=es,

    ) as service_manager:
        # in the case where we're starting up the
        # tractor-piker runtime stack in **this** process
        # we return no portal to self.
        assert service_manager
        yield service_manager


# `brokerd` enabled modules
# NOTE: keeping this list as small as possible is part of our caps-sec
# model and should be treated with utmost care!
_data_mods = [
    'piker.brokers.core',
    'piker.brokers.data',
    'piker.data',
    'piker.data.feed',
    'piker.data._sampling'
]


@acm
async def maybe_spawn_daemon(

    service_name: str,
    service_task_target: Callable,
    spawn_args: dict[str, Any],
    loglevel: Optional[str] = None,

    singleton: bool = False,
    **kwargs,

) -> tractor.Portal:
    '''
    If no ``service_name`` daemon-actor can be found,
    spawn one in a local subactor and return a portal to it.

    If this function is called from a non-pikerd actor, the
    spawned service will persist as long as pikerd does or
    it is requested to be cancelled.

    This can be seen as a service starting api for remote-actor
    clients.

    '''
    if loglevel:
        get_console_log(loglevel)

    # serialize access to this section to avoid
    # 2 or more tasks racing to create a daemon
    lock = Services.locks[service_name]
    await lock.acquire()

    async with find_service(service_name) as portal:
        if portal is not None:
            lock.release()
            yield portal
            return

    log.warning(f"Couldn't find any existing {service_name}")

    # TODO: really shouldn't the actor spawning be part of the service
    # starting method `Services.start_service()` ?

    # ask root ``pikerd`` daemon to spawn the daemon we need if
    # pikerd is not live we now become the root of the
    # process tree
    async with maybe_open_pikerd(

        loglevel=loglevel,
        **kwargs,

    ) as pikerd_portal:

        # we are the root and thus are `pikerd`
        # so spawn the target service directly by calling
        # the provided target routine.
        # XXX: this assumes that the target is well formed and will
        # do the right things to setup both a sub-actor **and** call
        # the ``_Services`` api from above to start the top level
        # service task for that actor.
        started: bool
        if pikerd_portal is None:
            started = await service_task_target(**spawn_args)

        else:
            # tell the remote `pikerd` to start the target,
            # the target can't return a non-serializable value
            # since it is expected that service startingn is
            # non-blocking and the target task will persist running
            # on `pikerd` after the client requesting it's start
            # disconnects.
            started = await pikerd_portal.run(
                service_task_target,
                **spawn_args,
            )

        if started:
            log.info(f'Service {service_name} started!')

        async with tractor.wait_for_actor(service_name) as portal:
            lock.release()
            yield portal
            await portal.cancel_actor()


async def spawn_brokerd(

    brokername: str,
    loglevel: Optional[str] = None,
    **tractor_kwargs,

) -> bool:

    log.info(f'Spawning {brokername} broker daemon')

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'

    extra_tractor_kwargs = getattr(brokermod, '_spawn_kwargs', {})
    tractor_kwargs.update(extra_tractor_kwargs)

    # ask `pikerd` to spawn a new sub-actor and manage it under its
    # actor nursery
    modpath = brokermod.__name__
    broker_enable = [modpath]
    for submodname in getattr(
        brokermod,
        '__enable_modules__',
        [],
    ):
        subpath = f'{modpath}.{submodname}'
        broker_enable.append(subpath)

    portal = await Services.actor_n.start_actor(
        dname,
        enable_modules=_data_mods + broker_enable,
        loglevel=loglevel,
        debug_mode=Services.debug_mode,
        **tractor_kwargs
    )

    # non-blocking setup of brokerd service nursery
    from ..data import _setup_persistent_brokerd

    await Services.start_service_task(
        dname,
        portal,
        _setup_persistent_brokerd,
        brokername=brokername,
    )
    return True


@acm
async def maybe_spawn_brokerd(

    brokername: str,
    loglevel: Optional[str] = None,
    **kwargs,

) -> tractor.Portal:
    '''
    Helper to spawn a brokerd service *from* a client
    who wishes to use the sub-actor-daemon.

    '''
    async with maybe_spawn_daemon(

        f'brokerd.{brokername}',
        service_task_target=spawn_brokerd,
        spawn_args={
            'brokername': brokername,
            'loglevel': loglevel,
        },
        loglevel=loglevel,
        **kwargs,

    ) as portal:
        yield portal


async def spawn_emsd(

    loglevel: Optional[str] = None,
    **extra_tractor_kwargs

) -> bool:
    """
    Start the clearing engine under ``pikerd``.

    """
    log.info('Spawning emsd')

    portal = await Services.actor_n.start_actor(
        'emsd',
        enable_modules=[
            'piker.clearing._ems',
            'piker.clearing._client',
        ],
        loglevel=loglevel,
        debug_mode=Services.debug_mode,  # set by pikerd flag
        **extra_tractor_kwargs
    )

    # non-blocking setup of clearing service
    from ..clearing._ems import _setup_persistent_emsd

    await Services.start_service_task(
        'emsd',
        portal,
        _setup_persistent_emsd,
    )
    return True


@acm
async def maybe_open_emsd(

    brokername: str,
    loglevel: Optional[str] = None,
    **kwargs,

) -> tractor._portal.Portal:  # noqa

    async with maybe_spawn_daemon(

        'emsd',
        service_task_target=spawn_emsd,
        spawn_args={'loglevel': loglevel},
        loglevel=loglevel,
        **kwargs,

    ) as portal:
        yield portal
