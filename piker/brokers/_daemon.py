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
from __future__ import annotations
from contextlib import (
    asynccontextmanager as acm,
)
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    AsyncContextManager,
)
import exceptiongroup as eg

import tractor
import trio

from . import _util
from . import get_brokermod

if TYPE_CHECKING:
    from ..data import _FeedsBus

# `brokerd` enabled modules
# TODO: move this def to the `.data` subpkg..
# NOTE: keeping this list as small as possible is part of our caps-sec
# model and should be treated with utmost care!
_data_mods: str = [
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

    # further, set the log level on any broker broker specific
    # logger instance.

    from piker.data import feed
    assert not feed._bus

    # allocate a nursery to the bus for spawning background
    # tasks to service client IPC requests, normally
    # `tractor.Context` connections to explicitly required
    # `brokerd` endpoints such as:
    # - `stream_quotes()`,
    # - `manage_history()`,
    # - `allocate_persistent_feed()`,
    # - `open_symbol_search()`
    # NOTE: see ep invocation details inside `.data.feed`.
    try:
        async with trio.open_nursery() as service_nursery:
            bus: _FeedsBus = feed.get_feed_bus(
                brokername,
                service_nursery,
            )
            assert bus is feed._bus

            # unblock caller
            await ctx.started()

            # we pin this task to keep the feeds manager active until the
            # parent actor decides to tear it down
            await trio.sleep_forever()

    except eg.ExceptionGroup:
        # TODO: likely some underlying `brokerd` IPC connection
        # broke so here we handle a respawn and re-connect attempt!
        # This likely should pair with development of the OCO task
        # nusery in dev over @ `tractor` B)
        # https://github.com/goodboy/tractor/pull/363
        raise


def broker_init(
    brokername: str,
    loglevel: str | None = None,

    **start_actor_kwargs,

) -> tuple[
    ModuleType,
    dict,
    AsyncContextManager,
]:
    '''
    Given an input broker name, load all named arguments
    which can be passed for daemon endpoint + context spawn
    as required in every `brokerd` (actor) service.

    This includes:
    - load the appropriate <brokername>.py pkg module,
    - reads any declared `__enable_modules__: listr[str]` which will be
      passed to `tractor.ActorNursery.start_actor(enabled_modules=<this>)`
      at actor start time,
    - deliver a references to the daemon lifetime fixture, which
      for now is always the `_setup_persistent_brokerd()` context defined
      above.

    '''
    from ..brokers import get_brokermod
    brokermod = get_brokermod(brokername)
    modpath: str = brokermod.__name__

    start_actor_kwargs['name'] = f'brokerd.{brokername}'
    start_actor_kwargs.update(
        getattr(
            brokermod,
            '_spawn_kwargs',
            {},
        )
    )

    # XXX TODO: make this not so hacky/monkeypatched..
    # -> we need a sane way to configure the logging level for all
    # code running in brokerd.
    # if utilmod := getattr(brokermod, '_util', False):
    #     utilmod.log.setLevel(loglevel.upper())

    # lookup actor-enabled modules declared by the backend offering the
    # `brokerd` endpoint(s).
    enabled: list[str]
    enabled = start_actor_kwargs['enable_modules'] = [
        __name__,  # so that eps from THIS mod can be invoked
        modpath,
    ]
    for submodname in getattr(
        brokermod,
        '__enable_modules__',
        [],
    ):
        subpath: str = f'{modpath}.{submodname}'
        enabled.append(subpath)

        # TODO XXX: DO WE NEED THIS?
        # enabled.append('piker.data.feed')

    return (
        brokermod,
        start_actor_kwargs,  # to `ActorNursery.start_actor()`

        # XXX see impl above; contains all (actor global)
        # setup/teardown expected in all `brokerd` actor instances.
        _setup_persistent_brokerd,
    )


async def spawn_brokerd(

    brokername: str,
    loglevel: str | None = None,

    **tractor_kwargs,

) -> bool:

    from piker.service._util import log  # use service mngr log
    log.info(f'Spawning {brokername} broker daemon')

    (
        brokermode,
        tractor_kwargs,
        daemon_fixture_ep,
    ) = broker_init(
        brokername,
        loglevel,
        **tractor_kwargs,
    )

    brokermod = get_brokermod(brokername)
    extra_tractor_kwargs = getattr(brokermod, '_spawn_kwargs', {})
    tractor_kwargs.update(extra_tractor_kwargs)

    # ask `pikerd` to spawn a new sub-actor and manage it under its
    # actor nursery
    from piker.service import Services

    dname: str = tractor_kwargs.pop('name')  # f'brokerd.{brokername}'
    portal = await Services.actor_n.start_actor(
        dname,
        enable_modules=_data_mods + tractor_kwargs.pop('enable_modules'),
        debug_mode=Services.debug_mode,
        **tractor_kwargs
    )

    # NOTE: the service mngr expects an already spawned actor + its
    # portal ref in order to do non-blocking setup of brokerd
    # service nursery.
    await Services.start_service_task(
        dname,
        portal,

        # signature of target root-task endpoint
        daemon_fixture_ep,
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
    Helper to spawn a brokerd service *from* a client who wishes to
    use the sub-actor-daemon but is fine with re-using any existing
    and contactable `brokerd`.

    Mas o menos, acts as a cached-actor-getter factory.

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
