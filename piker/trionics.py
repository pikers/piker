# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship of piker0)

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
sugarz for trio/tractor conc peeps.

'''
from typing import AsyncContextManager
from typing import TypeVar
from contextlib import asynccontextmanager as acm

import trio


# A regular invariant generic type
T = TypeVar("T")


async def _enter_and_sleep(

    mngr: AsyncContextManager[T],
    to_yield: dict[int, T],
    all_entered: trio.Event,
    # task_status: TaskStatus[T] = trio.TASK_STATUS_IGNORED,

) -> T:
    '''Open the async context manager deliver it's value
    to this task's spawner and sleep until cancelled.

    '''
    async with mngr as value:
        to_yield[id(mngr)] = value

        if all(to_yield.values()):
            all_entered.set()

        # sleep until cancelled
        await trio.sleep_forever()


@acm
async def async_enter_all(

    *mngrs: list[AsyncContextManager[T]],

) -> tuple[T]:

    to_yield = {}.fromkeys(id(mngr) for mngr in mngrs)

    all_entered = trio.Event()

    async with trio.open_nursery() as n:
        for mngr in mngrs:
            n.start_soon(
                _enter_and_sleep,
                mngr,
                to_yield,
                all_entered,
            )

        # deliver control once all managers have started up
        await all_entered.wait()
        yield tuple(to_yield.values())

        # tear down all sleeper tasks thus triggering individual
        # mngr ``__aexit__()``s.
        n.cancel_scope.cancel()
