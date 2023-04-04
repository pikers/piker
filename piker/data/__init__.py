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
Data infra.

We provide tsdb integrations for retrieving
and storing data from your brokers as well as
sharing live streams over a network.

"""
import tractor
import trio

from ..log import (
    get_console_log,
)
from ._normalize import iterticks
from ._sharedmem import (
    maybe_open_shm_array,
    attach_shm_array,
    open_shm_array,
    get_shm_token,
    ShmArray,
)
from .feed import (
    open_feed,
)


__all__ = [
    'open_feed',
    'ShmArray',
    'iterticks',
    'maybe_open_shm_array',
    'attach_shm_array',
    'open_shm_array',
    'get_shm_token',
]


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
    get_console_log(
        loglevel or tractor.current_actor().loglevel,
    )

    from .feed import (
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
