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
daemon-service management API.

"""
from collections import defaultdict
from typing import (
    Callable,
    Any,
)

import trio
from trio_typing import TaskStatus
import tractor

from ..log import (
    get_logger,
)

log = get_logger(__name__)


# TODO: factor this into a ``tractor.highlevel`` extension
# pack for the library.
class Services:

    actor_n: tractor._supervise.ActorNursery
    service_n: trio.Nursery
    debug_mode: bool  # tractor sub-actor debug mode flag
    service_tasks: dict[
        str,
        tuple[
            trio.CancelScope,
            tractor.Portal,
            trio.Event,
        ]
    ] = {}
    locks = defaultdict(trio.Lock)

    @classmethod
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
                tuple[
                    trio.CancelScope,
                    trio.Event,
                    Any,
                ]
            ] = trio.TASK_STATUS_IGNORED,

        ) -> Any:

            with trio.CancelScope() as cs:
                async with portal.open_context(
                    target,
                    **kwargs,

                ) as (ctx, first):

                    # unblock once the remote context has started
                    complete = trio.Event()
                    task_status.started((cs, complete, first))
                    log.info(
                        f'`pikerd` service {name} started with value {first}'
                    )
                    try:
                        # wait on any context's return value
                        # and any final portal result from the
                        # sub-actor.
                        ctx_res = await ctx.result()

                        # NOTE: blocks indefinitely until cancelled
                        # either by error from the target context
                        # function or by being cancelled here by the
                        # surrounding cancel scope.
                        return (await portal.result(), ctx_res)

                    finally:
                        await portal.cancel_actor()
                        complete.set()
                        self.service_tasks.pop(name)

        cs, complete, first = await self.service_n.start(open_context_in_task)

        # store the cancel scope and portal for later cancellation or
        # retstart if needed.
        self.service_tasks[name] = (cs, portal, complete)

        return cs, first

    @classmethod
    async def cancel_service(
        self,
        name: str,

    ) -> Any:
        '''
        Cancel the service task and actor for the given ``name``.

        '''
        log.info(f'Cancelling `pikerd` service {name}')
        cs, portal, complete = self.service_tasks[name]
        cs.cancel()
        await complete.wait()
        assert name not in self.service_tasks, \
            f'Serice task for {name} not terminated?'
