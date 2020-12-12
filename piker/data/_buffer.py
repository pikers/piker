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
from typing import Tuple, Callable, Dict
# import time

import tractor
import trio

from ._sharedmem import ShmArray


_shms: Dict[int, ShmArray] = {}


@tractor.msg.pub
async def increment_ohlc_buffer(
    shm_token: dict,
    get_topics: Callable[..., Tuple[str]],
    # delay_s: Optional[float] = None,
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
    # TODO: right now we'll spin printing bars if the last time stamp is
    # before a large period of no market activity.  Likely the best way
    # to solve this is to make this task aware of the instrument's
    # tradable hours?

    # adjust delay to compensate for trio processing time
    ad = min(_shms.keys()) - 0.001

    # async def sleep():
    #     """Sleep until next time frames worth has passed from last bar.
    #     """
    #     # last_ts = shm.array[-1]['time']
    #     # delay = max((last_ts + ad) - time.time(), 0)
    #     # await trio.sleep(delay)
    #     await trio.sleep(ad)

    total_s = 0  # total seconds counted
    lowest = min(_shms.keys())
    ad = lowest - 0.001

    while True:
        # TODO: do we want to support dynamically
        # adding a "lower" lowest increment period?
        await trio.sleep(ad)
        total_s += lowest

        # # sleep for duration of current bar
        # await sleep()

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
        yield {'index': shm._last.value}


def subscribe_ohlc_for_increment(
    shm: ShmArray,
    delay: int,
) -> None:
    """Add an OHLC ``ShmArray`` to the increment set.
    """
    _shms.setdefault(delay, []).append(shm)
