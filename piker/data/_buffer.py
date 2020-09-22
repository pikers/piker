"""
Data buffers for fast shared humpy.
"""
from typing import Tuple, Callable
import time

import tractor
import trio

from ._sharedmem import attach_shm_array


@tractor.msg.pub
async def incr_buffer(
    shm_token: dict,
    get_topics: Callable[..., Tuple[str]],
    # delay_s: Optional[float] = None,
):
    """Task which inserts new bars into the provide shared memory array
    every ``delay_s`` seconds.
    """
    # TODO: right now we'll spin printing bars if the last time
    # stamp is before a large period of no market activity.
    # Likely the best way to solve this is to make this task
    # aware of the instrument's tradable hours?

    shm = attach_shm_array(
        token=shm_token,
        readonly=False,
    )

    # determine ohlc delay between bars
    # to determine time step between datums
    times = shm.array['time']
    delay_s = times[-1] - times[times != times[-1]][-1]

    # adjust delay to compensate for trio processing time
    ad = delay_s - 0.002

    async def sleep():
        """Sleep until next time frames worth has passed from last bar.
        """
        # last_ts = shm.array[-1]['time']
        # delay = max((last_ts + ad) - time.time(), 0)
        # await trio.sleep(delay)
        await trio.sleep(ad)

    while True:
        # sleep for duration of current bar
        await sleep()

        # TODO: in theory we could make this faster by copying the
        # "last" readable value into the underlying larger buffer's
        # next value and then incrementing the counter instead of
        # using ``.push()``?

        # append new entry to buffer thus "incrementing" the bar
        array = shm.array
        last = array[-1:].copy()
        (index, t, close) = last[0][['index', 'time', 'close']]

        # this copies non-std fields (eg. vwap) from the last datum
        last[
            ['index', 'time', 'volume', 'open', 'high', 'low', 'close']
        ][0] = (index + 1, t + delay_s, 0, close, close, close, close)

        # write to the buffer
        shm.push(last)
        # print('incrementing array')

        # print(get_topics())

        # broadcast the buffer index step
        yield {'index': shm._i.value}
