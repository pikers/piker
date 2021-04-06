# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
Data buffers for fast shared humpy.
"""
from typing import Dict, List

import tractor
import trio
from trio_typing import TaskStatus

from ._sharedmem import ShmArray
from ..log import get_logger


log = get_logger(__name__)


# TODO: we could stick these in a composed type to avoid
# angering the "i hate module scoped variables crowd" (yawn).
_shms: Dict[int, List[ShmArray]] = {}
_start_increment: Dict[str, trio.Event] = {}
_incrementers: Dict[int, trio.CancelScope] = {}
_subscribers: Dict[str, tractor.Context] = {}


def shm_incrementing(shm_token_name: str) -> trio.Event:
    global _start_increment
    return _start_increment.setdefault(shm_token_name, trio.Event())


async def increment_ohlc_buffer(
    delay_s: int,
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
):
    """Task which inserts new bars into the provide shared memory array
    every ``delay_s`` seconds.

    This task fulfills 2 purposes:
    - it takes the subscribed set of shm arrays and increments them
      on a common time period
    - broadcast of this increment "signal" message to other actor
      subscribers

    Note that if **no** actor has initiated this task then **none** of
    the underlying buffers will actually be incremented.
    """

    # # wait for brokerd to signal we should start sampling
    # await shm_incrementing(shm_token['shm_name']).wait()

    # TODO: right now we'll spin printing bars if the last time stamp is
    # before a large period of no market activity.  Likely the best way
    # to solve this is to make this task aware of the instrument's
    # tradable hours?

    global _incrementers

    # adjust delay to compensate for trio processing time
    ad = min(_shms.keys()) - 0.001

    total_s = 0  # total seconds counted
    lowest = min(_shms.keys())
    ad = lowest - 0.001

    with trio.CancelScope() as cs:

        # register this time period step as active
        _incrementers[delay_s] = cs
        task_status.started(cs)

        while True:
            # TODO: do we want to support dynamically
            # adding a "lower" lowest increment period?
            await trio.sleep(ad)
            total_s += lowest

            # increment all subscribed shm arrays
            # TODO: this in ``numba``
            for delay_s, shms in _shms.items():
                if total_s % delay_s != 0:
                    continue

                # TODO: numa this!
                for shm in shms:
                    # TODO: in theory we could make this faster by copying the
                    # "last" readable value into the underlying larger buffer's
                    # next value and then incrementing the counter instead of
                    # using ``.push()``?

                    # append new entry to buffer thus "incrementing" the bar
                    array = shm.array
                    last = array[-1:][shm._write_fields].copy()
                    # (index, t, close) = last[0][['index', 'time', 'close']]
                    (t, close) = last[0][['time', 'close']]

                    # this copies non-std fields (eg. vwap) from the last datum
                    last[
                        ['time', 'volume', 'open', 'high', 'low', 'close']
                    ][0] = (t + delay_s, 0, close, close, close, close)

                    # write to the buffer
                    shm.push(last)

                # broadcast the buffer index step
                # yield {'index': shm._last.value}
                for ctx in _subscribers.get(delay_s, ()):
                    try:
                        await ctx.send_yield({'index': shm._last.value})
                    except (
                        trio.BrokenResourceError,
                        trio.ClosedResourceError
                    ):
                        log.error(f'{ctx.chan.uid} dropped connection')


@tractor.stream
async def iter_ohlc_periods(
    ctx: tractor.Context,
    delay_s: int,
) -> None:
    """
    Subscribe to OHLC sampling "step" events: when the time
    aggregation period increments, this event stream emits an index
    event.

    """
    # add our subscription
    global _subscribers
    subs = _subscribers.setdefault(delay_s, [])
    subs.append(ctx)

    try:
        # stream and block until cancelled
        await trio.sleep_forever()
    finally:
        subs.remove(ctx)
