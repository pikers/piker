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
Daemon-actor spawning "endpoint-hooks".

"""
from __future__ import annotations
from typing import (
    Callable,
    Any,
)
from contextlib import (
    asynccontextmanager as acm,
)

import tractor

from ._util import (
    log,  # sub-sys logger
)
from ._mngr import (
    Services,
)
from ._actor_runtime import maybe_open_pikerd
from ._registry import find_service


@acm
async def maybe_spawn_daemon(

    service_name: str,
    service_task_target: Callable,

    spawn_args: dict[str, Any],

    loglevel: str | None = None,
    singleton: bool = False,

    **pikerd_kwargs,

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
    # serialize access to this section to avoid
    # 2 or more tasks racing to create a daemon
    lock = Services.locks[service_name]
    await lock.acquire()

    async with find_service(
        service_name,
        registry_addrs=[('127.0.0.1', 6116)],
    ) as portal:
        if portal is not None:
            lock.release()
            yield portal
            return

    log.warning(
        f"Couldn't find any existing {service_name}\n"
        'Attempting to spawn new daemon-service..'
    )

    # ask root ``pikerd`` daemon to spawn the daemon we need if
    # pikerd is not live we now become the root of the
    # process tree
    async with maybe_open_pikerd(
        loglevel=loglevel,
        **pikerd_kwargs,

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
            started = await service_task_target(
                loglevel=loglevel,
                **spawn_args,
            )

        else:
            # request a remote `pikerd` (service manager) to start the
            # target daemon-task, the target can't return
            # a non-serializable value since it is expected that service
            # starting is non-blocking and the target task will persist
            # running "under" or "within" the `pikerd` actor tree after
            # the questing client disconnects. in other words this
            # spawns a persistent daemon actor that continues to live
            # for the lifespan of whatever the service manager inside
            # `pikerd` says it should.
            started = await pikerd_portal.run(
                service_task_target,
                loglevel=loglevel,
                **spawn_args,
            )

        if started:
            log.info(f'Service {service_name} started!')

        # block until we can discover (by IPC connection) to the newly
        # spawned daemon-actor and then deliver the portal to the
        # caller.
        async with tractor.wait_for_actor(service_name) as portal:
            lock.release()
            yield portal
            await portal.cancel_actor()


async def spawn_emsd(

    loglevel: str | None = None,
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

        # signature of target root-task endpoint
        _setup_persistent_emsd,
        loglevel=loglevel,
    )
    return True


@acm
async def maybe_open_emsd(

    brokername: str,
    loglevel: str | None = None,

    **pikerd_kwargs,

) -> tractor.Portal:  # noqa

    async with maybe_spawn_daemon(
        'emsd',
        service_task_target=spawn_emsd,
        spawn_args={},
        loglevel=loglevel,
        **pikerd_kwargs,

    ) as portal:
        yield portal
