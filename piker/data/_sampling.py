# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of pikers)

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
Sampling and broadcast machinery for (soft) real-time delivery of
financial data flows.

"""
from __future__ import annotations
from collections import Counter
import time
from typing import TYPE_CHECKING, Optional, Union

import tractor
import trio
from trio_typing import TaskStatus

from ..log import get_logger

if TYPE_CHECKING:
    from ._sharedmem import ShmArray
    from .feed import _FeedsBus

log = get_logger(__name__)


_default_delay_s: float = 1.0


class sampler:
    '''
    Global sampling engine registry.

    Manages state for sampling events, shm incrementing and
    sample period logic.

    '''
    # TODO: we could stick these in a composed type to avoid
    # angering the "i hate module scoped variables crowd" (yawn).
    ohlcv_shms: dict[int, list[ShmArray]] = {}

    # holds one-task-per-sample-period tasks which are spawned as-needed by
    # data feed requests with a given detected time step usually from
    # history loading.
    incrementers: dict[int, trio.CancelScope] = {}

    # holds all the ``tractor.Context`` remote subscriptions for
    # a particular sample period increment event: all subscribers are
    # notified on a step.
    subscribers: dict[int, tractor.Context] = {}


async def increment_ohlc_buffer(
    delay_s: int,
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
):
    '''
    Task which inserts new bars into the provide shared memory array
    every ``delay_s`` seconds.

    This task fulfills 2 purposes:
    - it takes the subscribed set of shm arrays and increments them
      on a common time period
    - broadcast of this increment "signal" message to other actor
      subscribers

    Note that if **no** actor has initiated this task then **none** of
    the underlying buffers will actually be incremented.

    '''
    # # wait for brokerd to signal we should start sampling
    # await shm_incrementing(shm_token['shm_name']).wait()

    # TODO: right now we'll spin printing bars if the last time stamp is
    # before a large period of no market activity.  Likely the best way
    # to solve this is to make this task aware of the instrument's
    # tradable hours?

    # adjust delay to compensate for trio processing time
    ad = min(sampler.ohlcv_shms.keys()) - 0.001

    total_s = 0  # total seconds counted
    lowest = min(sampler.ohlcv_shms.keys())
    lowest_shm = sampler.ohlcv_shms[lowest][0]
    ad = lowest - 0.001

    with trio.CancelScope() as cs:

        # register this time period step as active
        sampler.incrementers[delay_s] = cs
        task_status.started(cs)

        while True:
            # TODO: do we want to support dynamically
            # adding a "lower" lowest increment period?
            await trio.sleep(ad)
            total_s += delay_s

            # increment all subscribed shm arrays
            # TODO:
            # - this in ``numba``
            # - just lookup shms for this step instead of iterating?
            for this_delay_s, shms in sampler.ohlcv_shms.items():

                # short-circuit on any not-ready because slower sample
                # rate consuming shm buffers.
                if total_s % this_delay_s != 0:
                    # print(f'skipping `{this_delay_s}s` sample update')
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

            await broadcast(delay_s, shm=lowest_shm)


async def broadcast(
    delay_s: int,
    shm: Optional[ShmArray] = None,

) -> None:
    '''
    Broadcast the given ``shm: ShmArray``'s buffer index step to any
    subscribers for a given sample period.

    The sent msg will include the first and last index which slice into
    the buffer's non-empty data.

    '''
    subs = sampler.subscribers.get(delay_s, ())
    first = last = -1

    if shm is None:
        periods = sampler.ohlcv_shms.keys()
        # if this is an update triggered by a history update there
        # might not actually be any sampling bus setup since there's
        # no "live feed" active yet.
        if periods:
            lowest = min(periods)
            shm = sampler.ohlcv_shms[lowest][0]
            first = shm._first.value
            last = shm._last.value

    for stream in subs:
        try:
            await stream.send({
                'first': first,
                'last': last,
                'index': last,
            })
        except (
            trio.BrokenResourceError,
            trio.ClosedResourceError
        ):
            log.error(
                f'{stream._ctx.chan.uid} dropped connection'
            )
            try:
                subs.remove(stream)
            except ValueError:
                log.warning(
                    f'{stream._ctx.chan.uid} sub already removed!?'
                )


@tractor.context
async def iter_ohlc_periods(
    ctx: tractor.Context,
    delay_s: int,

) -> None:
    '''
    Subscribe to OHLC sampling "step" events: when the time
    aggregation period increments, this event stream emits an index
    event.

    '''
    # add our subscription
    subs = sampler.subscribers.setdefault(delay_s, [])
    await ctx.started()
    async with ctx.open_stream() as stream:
        subs.append(stream)

        try:
            # stream and block until cancelled
            await trio.sleep_forever()
        finally:
            try:
                subs.remove(stream)
            except ValueError:
                log.error(
                    f'iOHLC step stream was already dropped {ctx.chan.uid}?'
                )


async def sample_and_broadcast(

    bus: _FeedsBus,  # noqa
    shm: ShmArray,
    quote_stream: trio.abc.ReceiveChannel,
    brokername: str,
    sum_tick_vlm: bool = True,

) -> None:

    log.info("Started shared mem bar writer")

    overruns = Counter()

    # iterate stream delivered by broker
    async for quotes in quote_stream:
        # TODO: ``numba`` this!
        for broker_symbol, quote in quotes.items():
            # TODO: in theory you can send the IPC msg *before* writing
            # to the sharedmem array to decrease latency, however, that
            # will require at least some way to prevent task switching
            # at the yield such that the array write isn't delayed while
            # another consumer is serviced..

            # start writing the shm buffer with appropriate
            # trade data

            # TODO: we should probably not write every single
            # value to an OHLC sample stream XD
            # for a tick stream sure.. but this is excessive..
            ticks = quote['ticks']
            for tick in ticks:
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
            subs: list[
                tuple[
                    Union[tractor.MsgStream, trio.MemorySendChannel],
                    tractor.Context,
                    Optional[float],  # tick throttle in Hz
                ]
            ] = bus._subscribers[broker_symbol.lower()]

            # NOTE: by default the broker backend doesn't append
            # it's own "name" into the fqsn schema (but maybe it
            # should?) so we have to manually generate the correct
            # key here.
            bsym = f'{broker_symbol}.{brokername}'
            lags: int = 0

            for (stream, ctx, tick_throttle) in subs:

                try:
                    with trio.move_on_after(0.2) as cs:
                        if tick_throttle:
                            # this is a send mem chan that likely
                            # pushes to the ``uniform_rate_send()`` below.
                            try:
                                stream.send_nowait(
                                    (bsym, quote)
                                )
                            except trio.WouldBlock:
                                chan = ctx.chan
                                if ctx:
                                    log.warning(
                                        f'Feed overrun {bus.brokername} ->'
                                        f'{chan.uid} !!!'
                                    )
                                else:
                                    key = id(stream)
                                    overruns[key] += 1
                                    log.warning(
                                        f'Feed overrun {broker_symbol}'
                                        '@{bus.brokername} -> '
                                        f'feed @ {tick_throttle} Hz'
                                    )
                                    if overruns[key] > 6:
                                        # TODO: should we check for the
                                        # context being cancelled? this
                                        # could happen but the
                                        # channel-ipc-pipe is still up.
                                        if not chan.connected():
                                            log.warning(
                                                'Dropping broken consumer:\n'
                                                f'{broker_symbol}:'
                                                f'{ctx.cid}@{chan.uid}'
                                            )
                                            await stream.aclose()
                                            raise trio.BrokenResourceError
                                        else:
                                            log.warning(
                                                'Feed getting overrun bro!\n'
                                                f'{broker_symbol}:'
                                                f'{ctx.cid}@{chan.uid}'
                                            )
                                            continue

                        else:
                            await stream.send(
                                {bsym: quote}
                            )

                    if cs.cancelled_caught:
                        lags += 1
                        if lags > 10:
                            await tractor.breakpoint()

                except (
                    trio.BrokenResourceError,
                    trio.ClosedResourceError,
                    trio.EndOfChannel,
                ):
                    chan = ctx.chan
                    if ctx:
                        log.warning(
                            'Dropped `brokerd`-quotes-feed connection:\n'
                            f'{broker_symbol}:'
                            f'{ctx.cid}@{chan.uid}'
                        )
                    if tick_throttle:
                        assert stream._closed

                    # XXX: do we need to deregister here
                    # if it's done in the fee bus code?
                    # so far seems like no since this should all
                    # be single-threaded. Doing it anyway though
                    # since there seems to be some kinda race..
                    try:
                        subs.remove((stream, tick_throttle))
                    except ValueError:
                        log.error(
                            f'Stream was already removed from subs!?\n'
                            f'{broker_symbol}:'
                            f'{ctx.cid}@{chan.uid}'
                        )


# TODO: a less naive throttler, here's some snippets:
# token bucket by njs:
# https://gist.github.com/njsmith/7ea44ec07e901cb78ebe1dd8dd846cb9

async def uniform_rate_send(

    rate: float,
    quote_stream: trio.abc.ReceiveChannel,
    stream: tractor.MsgStream,

    task_status: TaskStatus = trio.TASK_STATUS_IGNORED,

) -> None:

    # TODO: compute the approx overhead latency per cycle
    left_to_sleep = throttle_period = 1/rate - 0.000616

    # send cycle state
    first_quote = last_quote = None
    last_send = time.time()
    diff = 0

    task_status.started()

    while True:

        # compute the remaining time to sleep for this throttled cycle
        left_to_sleep = throttle_period - diff

        if left_to_sleep > 0:
            with trio.move_on_after(left_to_sleep) as cs:
                try:
                    sym, last_quote = await quote_stream.receive()
                except trio.EndOfChannel:
                    log.exception(f"feed for {stream} ended?")
                    break

                diff = time.time() - last_send

                if not first_quote:
                    first_quote = last_quote

                if (throttle_period - diff) > 0:
                    # received a quote but the send cycle period hasn't yet
                    # expired we aren't supposed to send yet so append
                    # to the tick frame.

                    # append quotes since last iteration into the last quote's
                    # tick array/buffer.
                    ticks = last_quote.get('ticks')

                    # XXX: idea for frame type data structure we could
                    # use on the wire instead of a simple list?
                    # frames = {
                    #     'index': ['type_a', 'type_c', 'type_n', 'type_n'],

                    #     'type_a': [tick0, tick1, tick2, .., tickn],
                    #     'type_b': [tick0, tick1, tick2, .., tickn],
                    #     'type_c': [tick0, tick1, tick2, .., tickn],
                    #     ...
                    #     'type_n': [tick0, tick1, tick2, .., tickn],
                    # }

                    # TODO: once we decide to get fancy really we should
                    # have a shared mem tick buffer that is just
                    # continually filled and the UI just ready from it
                    # at it's display rate.
                    if ticks:
                        first_quote['ticks'].extend(ticks)

                    # send cycle isn't due yet so continue waiting
                    continue

        if cs.cancelled_caught:
            # 2 cases:
            # no quote has arrived yet this cycle so wait for
            # the next one.
            if not first_quote:
                # if no last quote was received since the last send
                # cycle **AND** if we timed out waiting for a most
                # recent quote **but** the throttle cycle is now due to
                # be sent -> we want to immediately send the next
                # received quote ASAP.
                sym, first_quote = await quote_stream.receive()

            # we have a quote already so send it now.

        # measured_rate = 1 / (time.time() - last_send)
        # log.info(
        #     f'`{sym}` throttled send hz: {round(measured_rate, ndigits=1)}'
        # )

        # TODO: now if only we could sync this to the display
        # rate timing exactly lul
        try:
            await stream.send({sym: first_quote})
        except (
            # NOTE: any of these can be raised by ``tractor``'s IPC
            # transport-layer and we want to be highly resilient
            # to consumers which crash or lose network connection.
            # I.e. we **DO NOT** want to crash and propagate up to
            # ``pikerd`` these kinds of errors!
            trio.ClosedResourceError,
            trio.BrokenResourceError,
            ConnectionResetError,
        ):
            # if the feed consumer goes down then drop
            # out of this rate limiter
            log.warning(f'{stream} closed')
            await stream.aclose()
            return

        # reset send cycle state
        first_quote = last_quote = None
        diff = 0
        last_send = time.time()
