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
import time
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

                # TODO: ``numba`` this!
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
                subs = _subscribers.get(delay_s, ())

                for ctx in subs:
                    try:
                        await ctx.send_yield({'index': shm._last.value})
                    except (
                        trio.BrokenResourceError,
                        trio.ClosedResourceError
                    ):
                        log.error(f'{ctx.chan.uid} dropped connection')
                        subs.remove(ctx)


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
        try:
            subs.remove(ctx)
        except ValueError:
            log.error(
                f'iOHLC step stream was already dropped for {ctx.chan.uid}?'
            )


async def sample_and_broadcast(

    bus: '_FeedBus',  # noqa
    shm: ShmArray,
    quote_stream: trio.abc.ReceiveChannel,
    sum_tick_vlm: bool = True,

) -> None:

    log.info("Started shared mem bar writer")

    # iterate stream delivered by broker
    async for quotes in quote_stream:

        # TODO: ``numba`` this!
        for sym, quote in quotes.items():

            # TODO: in theory you can send the IPC msg *before*
            # writing to the sharedmem array to decrease latency,
            # however, that will require `tractor.msg.pub` support
            # here or at least some way to prevent task switching
            # at the yield such that the array write isn't delayed
            # while another consumer is serviced..

            # start writing the shm buffer with appropriate
            # trade data
            for tick in quote['ticks']:

                ticktype = tick['type']

                # write trade events to shm last OHLC sample
                if ticktype in ('trade', 'utrade'):

                    last = tick['price']

                    # update last entry
                    # benchmarked in the 4-5 us range
                    o, high, low, v = shm.array[-1][
                        ['open', 'high', 'low', 'volume']
                    ]

                    new_v = tick.get('size', 0)

                    if v == 0 and new_v:
                        # no trades for this bar yet so the open
                        # is also the close/last trade price
                        o = last

                    if sum_tick_vlm:
                        volume = v + new_v
                    else:
                        # presume backend takes care of summing
                        # it's own vlm
                        volume = quote['volume']

                    shm.array[[
                        'open',
                        'high',
                        'low',
                        'close',
                        'bar_wap',  # can be optionally provided
                        'volume',
                    ]][-1] = (
                        o,
                        max(high, last),
                        min(low, last),
                        last,
                        quote.get('bar_wap', 0),
                        volume,
                    )

            # XXX: we need to be very cautious here that no
            # context-channel is left lingering which doesn't have
            # a far end receiver actor-task. In such a case you can
            # end up triggering backpressure which which will
            # eventually block this producer end of the feed and
            # thus other consumers still attached.
            subs = bus._subscribers[sym.lower()]

            lags = 0
            for (stream, tick_throttle) in subs:

                try:
                    with trio.move_on_after(0.2) as cs:
                        if tick_throttle:
                            # this is a send mem chan that likely
                            # pushes to the ``uniform_rate_send()`` below.
                            await stream.send((sym, quote))

                        else:
                            await stream.send({sym: quote})

                    if cs.cancelled_caught:
                        lags += 1
                        if lags > 10:
                            await tractor.breakpoint()

                except (
                    trio.BrokenResourceError,
                    trio.ClosedResourceError
                ):
                    # XXX: do we need to deregister here
                    # if it's done in the fee bus code?
                    # so far seems like no since this should all
                    # be single-threaded.
                    log.warning(
                        f'{stream._ctx.chan.uid} dropped  '
                        '`brokerd`-quotes-feed connection'
                    )
                    subs.remove((stream, tick_throttle))


# TODO: a less naive throttler, here's some snippets:
# token bucket by njs:
# https://gist.github.com/njsmith/7ea44ec07e901cb78ebe1dd8dd846cb9

async def uniform_rate_send(

    rate: float,
    quote_stream: trio.abc.ReceiveChannel,
    stream: tractor.MsgStream,

) -> None:

    sleep_period = 1/rate - 0.000616
    last_send = time.time()
    aname = stream._ctx.chan.uid[0]
    fsp = False
    if 'fsp' in aname:
        fsp = True

    while True:

        sym, first_quote = await quote_stream.receive()
        start = time.time()

        # append quotes since last iteration into the last quote's
        # tick array/buffer.

        # TODO: once we decide to get fancy really we should have
        # a shared mem tick buffer that is just continually filled and
        # the UI just ready from it at it's display rate.
        # we'll likely head toward this once we get this issue going:
        #
        while True:
            try:
                sym, next_quote = quote_stream.receive_nowait()
                ticks = next_quote.get('ticks')

                if ticks:
                    first_quote['ticks'].extend(ticks)

            except trio.WouldBlock:
                now = time.time()
                rate = 1 / (now - last_send)
                last_send = now

                # log.info(f'{rate} Hz sending quotes')  # \n{first_quote}')

                # TODO: now if only we could sync this to the display
                # rate timing exactly lul
                try:
                    await stream.send({sym: first_quote})
                    break
                except trio.ClosedResourceError:
                    # if the feed consumer goes down then drop
                    # out of this rate limiter
                    log.warning(f'{stream} closed')
                    return

        end = time.time()
        diff = end - start

        # throttle to provided transmit rate
        period = max(sleep_period - diff, 0)
        if period > 0:
            await trio.sleep(period)
