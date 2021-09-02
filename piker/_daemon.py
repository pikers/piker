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
from typing import Optional, Union, Callable, Any
from contextlib import asynccontextmanager
from collections import defaultdict

from pydantic import BaseModel
import trio
from trio_typing import TaskStatus
import tractor

from .log import get_logger, get_console_log
from .brokers import get_brokermod


log = get_logger(__name__)

_root_dname = 'pikerd'
_tractor_kwargs: dict[str, Any] = {
    # use a different registry addr then tractor's default
    'arbiter_addr':  ('127.0.0.1', 6116),
}
_root_modules = [
    __name__,
    'piker.clearing._ems',
    'piker.clearing._client',
]


class Services(BaseModel):

    actor_n: tractor._trionics.ActorNursery
    service_n: trio.Nursery
    debug_mode: bool  # tractor sub-actor debug mode flag
    service_tasks: dict[str, tuple[trio.CancelScope, tractor.Portal]] = {}

    class Config:
        arbitrary_types_allowed = True

    async def start_service_task(
        self,
        name: str,
        portal: tractor.Portal,
        target: Callable,
        **kwargs,

    ) -> (trio.CancelScope, tractor.Context):
        '''
        Open a context in a service sub-actor, add to a stack
        that gets unwound at ``pikerd`` teardown.

        This allows for allocating long-running sub-services in our main
        daemon and explicitly controlling their lifetimes.

        '''
        async def open_context_in_task(
            task_status: TaskStatus[
                trio.CancelScope] = trio.TASK_STATUS_IGNORED,

        ) -> Any:

            with trio.CancelScope() as cs:

                async with portal.open_context(
                    target,
                    **kwargs,

                ) as (ctx, first):

                    # unblock once the remote context has started
                    task_status.started((cs, first))

                    # wait on any context's return value
                    ctx_res = await ctx.result()
                    log.info(
                        f'`pikerd` service {name} started with value {ctx_res}'
                    )

                # wait on any error from the sub-actor
                # NOTE: this will block indefinitely until cancelled
                # either by error from the target context function or
                # by being cancelled here by the surroundingn cancel
                # scope
                return await (portal.result(), ctx_res)

        cs, first = await self.service_n.start(open_context_in_task)

        # store the cancel scope and portal for later cancellation or
        # retstart if needed.
        self.service_tasks[name] = (cs, portal)

        return cs, first

    async def cancel_service(
        self,
        name: str,

    ) -> Any:

        log.info(f'Cancelling `pikerd` service {name}')
        cs, portal = self.service_tasks[name]
        cs.cancel()
        return await portal.cancel_actor()


_services: Optional[Services] = None


@asynccontextmanager
async def open_pikerd(
    start_method: str = 'trio',
    loglevel: Optional[str] = None,

    # XXX: you should pretty much never want debug mode
    # for data daemons when running in production.
    debug_mode: bool = False,

) -> Optional[tractor._portal.Portal]:
    '''
    Start a root piker daemon who's lifetime extends indefinitely
    until cancelled.

    A root actor nursery is created which can be used to create and keep
    alive underling services (see below).

    '''
    global _services
    assert _services is None

    # XXX: this may open a root actor as well
    async with (
        tractor.open_root_actor(

            # passed through to ``open_root_actor``
            arbiter_addr=_tractor_kwargs['arbiter_addr'],
            name=_root_dname,
            loglevel=loglevel,
            debug_mode=debug_mode,
            start_method=start_method,

            # TODO: eventually we should be able to avoid
            # having the root have more then permissions to
            # spawn other specialized daemons I think?
            enable_modules=_root_modules,
        ) as _,

        tractor.open_nursery() as actor_nursery,
    ):
        async with trio.open_nursery() as service_nursery:

            # # setup service mngr singleton instance
            # async with AsyncExitStack() as stack:

            # assign globally for future daemon/task creation
            _services = Services(
                actor_n=actor_nursery,
                service_n=service_nursery,
                debug_mode=debug_mode,
            )

            yield _services


@asynccontextmanager
async def maybe_open_runtime(
    loglevel: Optional[str] = None,
    **kwargs,

) -> None:
    """
    Start the ``tractor`` runtime (a root actor) if none exists.

    """
    settings = _tractor_kwargs
    settings.update(kwargs)

    if not tractor.current_actor(err_on_no_runtime=False):
        async with tractor.open_root_actor(
            loglevel=loglevel,
            **settings,
        ):
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

    # presume pikerd role since no daemon could be found at
    # configured address
    async with open_pikerd(

        loglevel=loglevel,
        debug_mode=kwargs.get('debug_mode', False),

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
    'piker.data.feed',
    'piker.data._sampling'
]


class Brokerd:
    locks = defaultdict(trio.Lock)


@asynccontextmanager
async def maybe_spawn_daemon(

    service_name: str,
    service_task_target: Callable,
    spawn_args: dict[str, Any],
    loglevel: Optional[str] = None,
    **kwargs,

) -> tractor.Portal:
    """
    If no ``service_name`` daemon-actor can be found,
    spawn one in a local subactor and return a portal to it.

    If this function is called from a non-pikerd actor, the
    spawned service will persist as long as pikerd does or
    it is requested to be cancelled.

    This can be seen as a service starting api for remote-actor
    clients.

    """
    if loglevel:
        get_console_log(loglevel)

    # serialize access to this section to avoid
    # 2 or more tasks racing to create a daemon
    lock = Brokerd.locks[service_name]
    await lock.acquire()

    # attach to existing daemon by name if possible
    async with tractor.find_actor(service_name) as portal:
        if portal is not None:
            lock.release()
            yield portal
            return

    # ask root ``pikerd`` daemon to spawn the daemon we need if
    # pikerd is not live we now become the root of the
    # process tree
    async with maybe_open_pikerd(

        loglevel=loglevel,
        **kwargs,

    ) as pikerd_portal:

        if pikerd_portal is None:
            # we are the root and thus are `pikerd`
            # so spawn the target service directly by calling
            # the provided target routine.
            # XXX: this assumes that the target is well formed and will
            # do the right things to setup both a sub-actor **and** call
            # the ``_Services`` api from above to start the top level
            # service task for that actor.
            await service_task_target(**spawn_args)

        else:
            # tell the remote `pikerd` to start the target,
            # the target can't return a non-serializable value
            # since it is expected that service startingn is
            # non-blocking and the target task will persist running
            # on `pikerd` after the client requesting it's start
            # disconnects.
            await pikerd_portal.run(
                service_task_target,
                **spawn_args,
            )

        async with tractor.wait_for_actor(service_name) as portal:
            lock.release()
            yield portal


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

    global _services
    assert _services

    # ask `pikerd` to spawn a new sub-actor and manage it under its
    # actor nursery
    portal = await _services.actor_n.start_actor(
        dname,
        enable_modules=_data_mods + [brokermod.__name__],
        loglevel=loglevel,
        debug_mode=_services.debug_mode,
        **tractor_kwargs
    )

    # non-blocking setup of brokerd service nursery
    from .data import _setup_persistent_brokerd

    await _services.start_service_task(
        dname,
        portal,
        _setup_persistent_brokerd,
        brokername=brokername,
    )
    return True


@asynccontextmanager
async def maybe_spawn_brokerd(

    brokername: str,
    loglevel: Optional[str] = None,
    **kwargs,

) -> tractor.Portal:
    '''Helper to spawn a brokerd service.

    '''
    async with maybe_spawn_daemon(

        f'brokerd.{brokername}',
        service_task_target=spawn_brokerd,
        spawn_args={'brokername': brokername, 'loglevel': loglevel},
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

    global _services
    assert _services

    portal = await _services.actor_n.start_actor(
        'emsd',
        enable_modules=[
            'piker.clearing._ems',
            'piker.clearing._client',
        ],
        loglevel=loglevel,
        debug_mode=_services.debug_mode,  # set by pikerd flag
        **extra_tractor_kwargs
    )

    # non-blocking setup of clearing service
    from .clearing._ems import _setup_persistent_emsd

    await _services.start_service_task(
        'emsd',
        portal,
        _setup_persistent_emsd,
    )
    return True


@asynccontextmanager
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
