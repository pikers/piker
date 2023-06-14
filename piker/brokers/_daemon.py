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

'''
Broker-daemon-actor "endpoint-hooks": the service task entry points for
``brokerd``.

'''
from contextlib import (
    asynccontextmanager as acm,
)

import tractor
import trio

from . import _util
from . import get_brokermod

# `brokerd` enabled modules
# TODO: move this def to the `.data` subpkg..
# NOTE: keeping this list as small as possible is part of our caps-sec
# model and should be treated with utmost care!
_data_mods = [
    'piker.brokers.core',
    'piker.brokers.data',
    'piker.brokers._daemon',
    'piker.data',
    'piker.data.feed',
    'piker.data._sampling'
]


# TODO: we should rename the daemon to datad prolly once we split up
# broker vs. data tasks into separate actors?
@tractor.context
async def _setup_persistent_brokerd(
    ctx: tractor.Context,
    brokername: str,
    loglevel: str | None = None,

) -> None:
    '''
    Allocate a actor-wide service nursery in ``brokerd``
    such that feeds can be run in the background persistently by
    the broker backend as needed.

    '''
    # NOTE: we only need to setup logging once (and only) here
    # since all hosted daemon tasks will reference this same
    # log instance's (actor local) state and thus don't require
    # any further (level) configuration on their own B)
    log = _util.get_console_log(
        loglevel or tractor.current_actor().loglevel,
        name=f'{_util.subsys}.{brokername}',
    )
    # set global for this actor to this new process-wide instance B)
    _util.log = log

    from piker.data.feed import (
        _bus,
        get_feed_bus,
    )
    global _bus
    assert not _bus

    async with trio.open_nursery() as service_nursery:
        # assign a nursery to the feeds bus for spawning
        # background tasks from clients
        get_feed_bus(brokername, service_nursery)

        # unblock caller
        await ctx.started()

        # we pin this task to keep the feeds manager active until the
        # parent actor decides to tear it down
        await trio.sleep_forever()


async def spawn_brokerd(

    brokername: str,
    loglevel: str | None = None,

    **tractor_kwargs,

) -> bool:

    from piker.service import Services
    from piker.service._util import log  # use service mngr log

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
    await Services.start_service_task(
        dname,
        portal,

        # signature of target root-task endpoint
        _setup_persistent_brokerd,
        brokername=brokername,
        loglevel=loglevel,
    )
    return True


@acm
async def maybe_spawn_brokerd(

    brokername: str,
    loglevel: str | None = None,

    **pikerd_kwargs,

) -> tractor.Portal:
    '''
    Helper to spawn a brokerd service *from* a client
    who wishes to use the sub-actor-daemon.

    '''
    from piker.service import maybe_spawn_daemon

    async with maybe_spawn_daemon(

        f'brokerd.{brokername}',
        service_task_target=spawn_brokerd,
        spawn_args={
            'brokername': brokername,
        },
        loglevel=loglevel,

        **pikerd_kwargs,

    ) as portal:
        yield portal
