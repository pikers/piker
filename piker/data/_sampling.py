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
from collections import (
    Counter,
    defaultdict,
)
from contextlib import asynccontextmanager as acm
import time
from typing import (
    AsyncIterator,
    TYPE_CHECKING,
)

import tractor
from tractor.trionics import (
    maybe_open_nursery,
)
import trio
from trio_typing import TaskStatus

from ._util import (
    log,
    get_console_log,
)
from ..service import maybe_spawn_daemon

if TYPE_CHECKING:
    from ._sharedmem import (
        ShmArray,
    )
    from .feed import _FeedsBus


# highest frequency sample step is 1 second by default, though in
# the future we may want to support shorter periods or a dynamic style
# tick-event stream.
_default_delay_s: float = 1.0


class Sampler:
    '''
    Global sampling engine registry.

    Manages state for sampling events, shm incrementing and
    sample period logic.

    This non-instantiated type is meant to be a singleton within
    a `samplerd` actor-service spawned once by the user wishing to
    time-step-sample (real-time) quote feeds, see
    ``.service.maybe_open_samplerd()`` and the below
    ``register_with_sampler()``.

    '''
    service_nursery: None | trio.Nursery = None

    # TODO: we could stick these in a composed type to avoid
    # angering the "i hate module scoped variables crowd" (yawn).
    ohlcv_shms: dict[float, list[ShmArray]] = {}

    # holds one-task-per-sample-period tasks which are spawned as-needed by
    # data feed requests with a given detected time step usually from
    # history loading.
    incr_task_cs: trio.CancelScope | None = None

    # holds all the ``tractor.Context`` remote subscriptions for
    # a particular sample period increment event: all subscribers are
    # notified on a step.
    subscribers: defaultdict[
        float,
        list[
            float,
            set[tractor.MsgStream]
        ],
    ] = defaultdict(
        lambda: [
            round(time.time()),
            set(),
        ]
    )

    @classmethod
    async def increment_ohlc_buffer(
        self,
        period_s: float,
        task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
    ):
        '''
        Task which inserts new bars into the provide shared memory array
        every ``period_s`` seconds.

        This task fulfills 2 purposes:
        - it takes the subscribed set of shm arrays and increments them
          on a common time period
        - broadcast of this increment "signal" message to other actor
          subscribers

        Note that if **no** actor has initiated this task then **none** of
        the underlying buffers will actually be incremented.

        '''
        # TODO: right now we'll spin printing bars if the last time stamp is
        # before a large period of no market activity.  Likely the best way
        # to solve this is to make this task aware of the instrument's
        # tradable hours?

        total_s: float = 0  # total seconds counted
        ad = period_s - 0.001  # compensate for trio processing time

        with trio.CancelScope() as cs:
            # register this time period step as active
            task_status.started(cs)

            # sample step loop:
            # includes broadcasting to all connected consumers on every
            # new sample step as well incrementing any registered
            # buffers by registered sample period.
            while True:
                await trio.sleep(ad)
                total_s += period_s

                # increment all subscribed shm arrays
                # TODO:
                # - this in ``numba``
                # - just lookup shms for this step instead of iterating?

                i_epoch = round(time.time())
                broadcasted: set[float] = set()

                # print(f'epoch: {i_epoch} -> REGISTRY {self.ohlcv_shms}')
                for shm_period_s, shms in self.ohlcv_shms.items():

                    # short-circuit on any not-ready because slower sample
                    # rate consuming shm buffers.
                    if total_s % shm_period_s != 0:
                        # print(f'skipping `{shm_period_s}s` sample update')
                        continue

                    # update last epoch stamp for this period group
                    if shm_period_s not in broadcasted:
                        sub_pair = self.subscribers[shm_period_s]
                        sub_pair[0] = i_epoch
                        broadcasted.add(shm_period_s)

                    # TODO: ``numba`` this!
                    for shm in shms:
                        # print(f'UPDATE {shm_period_s}s STEP for {shm.token}')

                        # append new entry to buffer thus "incrementing"
                        # the bar
                        array = shm.array
                        last = array[-1:][shm._write_fields].copy()

                        # guard against startup backfilling races where
                        # the buffer has not yet been filled.
                        if not last.size:
                            continue

                        (t, close) = last[0][[
                            'time',
                            'close',
                        ]]

                        next_t = t + shm_period_s

                        if shm_period_s <= 1:
                            next_t = i_epoch

                        # this copies non-std fields (eg. vwap) from the
                        # last datum
                        last[[
                            'time',

                            'open',
                            'high',
                            'low',
                            'close',

                            'volume',
                        ]][0] = (
                            # epoch timestamp
                            next_t,

                            # OHLC
                            close,
                            close,
                            close,
                            close,

                            0,  # vlm
                        )

                        # TODO: in theory we could make this faster by
                        # copying the "last" readable value into the
                        # underlying larger buffer's next value and then
                        # incrementing the counter instead of using
                        # ``.push()``?

                        # write to the buffer
                        shm.push(last)

                # broadcast increment msg to all updated subs per period
                for shm_period_s in broadcasted:
                    await self.broadcast(
                        period_s=shm_period_s,
                        time_stamp=i_epoch,
                    )

    @classmethod
    async def broadcast(
        self,
        period_s: float,
        time_stamp: float | None = None,

    ) -> None:
        '''
        Broadcast the period size and last index step value to all
        subscribers for a given sample period.

        '''
        pair: list[float, set]
        pair = self.subscribers[period_s]

        last_ts: float
        subs: set
        last_ts, subs = pair

        task = trio.lowlevel.current_task()
        log.debug(
            f'SUBS {self.subscribers}\n'
            f'PAIR {pair}\n'
            f'TASK: {task}: {id(task)}\n'
            f'broadcasting {period_s} -> {last_ts}\n'
            # f'consumers: {subs}'
        )
        borked: set[tractor.MsgStream] = set()
        sent: set[tractor.MsgStream] = set()
        while True:
            try:
                for stream in (subs - sent):
                    try:
                        await stream.send({
                            'index': time_stamp or last_ts,
                            'period': period_s,
                        })
                        sent.add(stream)

                    except (
                        trio.BrokenResourceError,
                        trio.ClosedResourceError
                    ):
                        log.error(
                            f'{stream._ctx.chan.uid} dropped connection'
                        )
                        borked.add(stream)
                else:
                    break
            except RuntimeError:
                log.warning(f'Client subs {subs} changed while broadcasting')
                continue

        for stream in borked:
            try:
                subs.remove(stream)
            except KeyError:
                log.warning(
                    f'{stream._ctx.chan.uid} sub already removed!?'
                )

    @classmethod
    async def broadcast_all(self) -> None:
        for period_s in self.subscribers:
            await self.broadcast(period_s)


@tractor.context
async def register_with_sampler(
    ctx: tractor.Context,
    period_s: float,
    shms_by_period: dict[float, dict] | None = None,

    open_index_stream: bool = True,  # open a 2way stream for sample step msgs?
    sub_for_broadcasts: bool = True,  # sampler side to send step updates?

) -> None:

    get_console_log(tractor.current_actor().loglevel)
    incr_was_started: bool = False

    try:
        async with maybe_open_nursery(
            Sampler.service_nursery
        ) as service_nursery:

            # init startup, create (actor-)local service nursery and start
            # increment task
            Sampler.service_nursery = service_nursery

            # always ensure a period subs entry exists
            last_ts, subs = Sampler.subscribers[float(period_s)]

            async with trio.Lock():
                if Sampler.incr_task_cs is None:
                    Sampler.incr_task_cs = await service_nursery.start(
                        Sampler.increment_ohlc_buffer,
                        1.,
                    )
                    incr_was_started = True

            # insert the base 1s period (for OHLC style sampling) into
            # the increment buffer set to update and shift every second.
            if shms_by_period is not None:
                from ._sharedmem import (
                    attach_shm_array,
                    _Token,
                )
                for period in shms_by_period:

                    # load and register shm handles
                    shm_token_msg = shms_by_period[period]
                    shm = attach_shm_array(
                        _Token.from_msg(shm_token_msg),
                        readonly=False,
                    )
                    shms_by_period[period] = shm
                    Sampler.ohlcv_shms.setdefault(period, []).append(shm)

                assert Sampler.ohlcv_shms

            # unblock caller
            await ctx.started(set(Sampler.ohlcv_shms.keys()))

            if open_index_stream:
                try:
                    async with ctx.open_stream(
                        allow_overruns=True,
                    ) as stream:
                        if sub_for_broadcasts:
                            subs.add(stream)

                        # except broadcast requests from the subscriber
                        async for msg in stream:
                            if msg == 'broadcast_all':
                                await Sampler.broadcast_all()
                finally:
                    if (
                        sub_for_broadcasts
                        and subs
                    ):
                        try:
                            subs.remove(stream)
                        except KeyError:
                            log.warning(
                                f'{stream._ctx.chan.uid} sub already removed!?'
                            )
            else:
                # if no shms are passed in we just wait until cancelled
                # by caller.
                await trio.sleep_forever()

    finally:
        # TODO: why tf isn't this working?
        if shms_by_period is not None:
            for period, shm in shms_by_period.items():
                Sampler.ohlcv_shms[period].remove(shm)

        if incr_was_started:
            Sampler.incr_task_cs.cancel()
            Sampler.incr_task_cs = None


async def spawn_samplerd(

    loglevel: str | None = None,
    **extra_tractor_kwargs

) -> bool:
    '''
    Daemon-side service task: start a sampling daemon for common step
    update and increment count write and stream broadcasting.

    '''
    from piker.service import Services

    dname = 'samplerd'
    log.info(f'Spawning `{dname}`')

    # singleton lock creation of ``samplerd`` since we only ever want
    # one daemon per ``pikerd`` proc tree.
    # TODO: make this built-into the service api?
    async with Services.locks[dname + '_singleton']:

        if dname not in Services.service_tasks:

            portal = await Services.actor_n.start_actor(
                dname,
                enable_modules=[
                    'piker.data._sampling',
                ],
                loglevel=loglevel,
                debug_mode=Services.debug_mode,  # set by pikerd flag
                **extra_tractor_kwargs
            )

            await Services.start_service_task(
                dname,
                portal,
                register_with_sampler,
                period_s=1,
                sub_for_broadcasts=False,
            )
            return True

        return False


@acm
async def maybe_open_samplerd(

    loglevel: str | None = None,
    **pikerd_kwargs,

) -> tractor.Portal:  # noqa
    '''
    Client-side helper to maybe startup the ``samplerd`` service
    under the ``pikerd`` tree.

    '''
    dname = 'samplerd'

    async with maybe_spawn_daemon(
        dname,
        service_task_target=spawn_samplerd,
        spawn_args={},
        loglevel=loglevel,
        **pikerd_kwargs,

    ) as portal:
        yield portal


@acm
async def open_sample_stream(
    period_s: float,
    shms_by_period: dict[float, dict] | None = None,
    open_index_stream: bool = True,
    sub_for_broadcasts: bool = True,

    cache_key: str | None = None,
    allow_new_sampler: bool = True,

) -> AsyncIterator[dict[str, float]]:
    '''
    Subscribe to OHLC sampling "step" events: when the time aggregation
    period increments, this event stream emits an index event.

    This is a client-side endpoint that does all the work of ensuring
    the `samplerd` actor is up and that mult-consumer-tasks are given
    a broadcast stream when possible.

    '''
    # TODO: wrap this manager with the following to make it cached
    # per client-multitasks entry.
    # maybe_open_context(
    #     acm_func=partial(
    #         portal.open_context,
    #         register_with_sampler,
    #     ),
    #     key=cache_key or period_s,
    # )
    # if cache_hit:
    #     # add a new broadcast subscription for the quote stream
    #     # if this feed is likely already in use
    #     async with istream.subscribe() as bistream:
    #         yield bistream
    # else:

    async with (
        # XXX: this should be singleton on a host,
        # a lone broker-daemon per provider should be
        # created for all practical purposes
        maybe_open_samplerd() as portal,

        portal.open_context(
            register_with_sampler,
            **{
                'period_s': period_s,
                'shms_by_period': shms_by_period,
                'open_index_stream': open_index_stream,
                'sub_for_broadcasts': sub_for_broadcasts,
            },
        ) as (ctx, first)
    ):
        assert len(first) > 1
        async with (
            ctx.open_stream() as istream,

            # TODO: we don't need this task-bcasting right?
            # istream.subscribe() as istream,
        ):
            yield istream


async def sample_and_broadcast(

    bus: _FeedsBus,  # noqa
    rt_shm: ShmArray,
    hist_shm: ShmArray,
    quote_stream: trio.abc.ReceiveChannel,
    brokername: str,
    sum_tick_vlm: bool = True,

) -> None:
    '''
    `brokerd`-side task which writes latest datum sampled data.

    This task is meant to run in the same actor (mem space) as the
    `brokerd` real-time quote feed which is being sampled to
    a ``ShmArray`` buffer.

    '''
    log.info("Started shared mem bar writer")

    overruns = Counter()

    # iterate stream delivered by broker
    async for quotes in quote_stream:
        # print(quotes)

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

                    # more compact inline-way to do this assignment
                    # to both buffers?
                    for shm in [rt_shm, hist_shm]:
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
                            # 'bar_wap',  # can be optionally provided
                            'volume',
                        ]][-1] = (
                            o,
                            max(high, last),
                            min(low, last),
                            last,
                            # quote.get('bar_wap', 0),
                            volume,
                        )

            # TODO: PUT THIS IN A ``_FeedsBus.broadcast()`` method!
            # XXX: we need to be very cautious here that no
            # context-channel is left lingering which doesn't have
            # a far end receiver actor-task. In such a case you can
            # end up triggering backpressure which which will
            # eventually block this producer end of the feed and
            # thus other consumers still attached.
            sub_key: str = broker_symbol.lower()
            subs: list[
                tuple[
                    tractor.MsgStream | trio.MemorySendChannel,
                    float | None,  # tick throttle in Hz
                ]
            ] = bus.get_subs(sub_key)

            # NOTE: by default the broker backend doesn't append
            # it's own "name" into the fqme schema (but maybe it
            # should?) so we have to manually generate the correct
            # key here.
            fqme = f'{broker_symbol}.{brokername}'
            lags: int = 0

            # TODO: speed up this loop in an AOT compiled lang (like
            # rust or nim or zig) and/or instead of doing a fan out to
            # TCP sockets here, we add a shm-style tick queue which
            # readers can pull from instead of placing the burden of
            # broadcast on solely on this `brokerd` actor. see issues:
            # - https://github.com/pikers/piker/issues/98
            # - https://github.com/pikers/piker/issues/107

            for (stream, tick_throttle) in subs.copy():
                try:
                    with trio.move_on_after(0.2) as cs:
                        if tick_throttle:
                            # this is a send mem chan that likely
                            # pushes to the ``uniform_rate_send()`` below.
                            try:
                                stream.send_nowait(
                                    (fqme, quote)
                                )
                            except trio.WouldBlock:
                                overruns[sub_key] += 1
                                ctx = stream._ctx
                                chan = ctx.chan

                                log.warning(
                                    f'Feed OVERRUN {sub_key}'
                                    '@{bus.brokername} -> \n'
                                    f'feed @ {chan.uid}\n'
                                    f'throttle = {tick_throttle} Hz'
                                )

                                if overruns[sub_key] > 6:
                                    # TODO: should we check for the
                                    # context being cancelled? this
                                    # could happen but the
                                    # channel-ipc-pipe is still up.
                                    if (
                                        not chan.connected()
                                        or ctx._cancel_called
                                    ):
                                        log.warning(
                                            'Dropping broken consumer:\n'
                                            f'{sub_key}:'
                                            f'{ctx.cid}@{chan.uid}'
                                        )
                                        await stream.aclose()
                                        raise trio.BrokenResourceError
                        else:
                            await stream.send(
                                {fqme: quote}
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
                    ctx = stream._ctx
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
                    bus.remove_subs(
                        sub_key,
                        {(stream, tick_throttle)},
                    )


# a working tick-type-classes template
_tick_groups = {
    'clears': {'trade', 'dark_trade', 'last'},
    'bids': {'bid', 'bsize'},
    'asks': {'ask', 'asize'},
}


def frame_ticks(
    first_quote: dict,
    last_quote: dict,
    ticks_by_type: dict,
) -> None:
    # append quotes since last iteration into the last quote's
    # tick array/buffer.
    ticks = last_quote.get('ticks')

    # TODO: once we decide to get fancy really we should
    # have a shared mem tick buffer that is just
    # continually filled and the UI just ready from it
    # at it's display rate.
    if ticks:
        # TODO: do we need this any more or can we just
        # expect the receiver to unwind the below
        # `ticks_by_type: dict`?
        # => undwinding would potentially require a
        # `dict[str, set | list]` instead with an
        # included `'types' field which is an (ordered)
        # set of tick type fields in the order which
        # types arrived?
        first_quote['ticks'].extend(ticks)

        # XXX: build a tick-by-type table of lists
        # of tick messages. This allows for less
        # iteration on the receiver side by allowing for
        # a single "latest tick event" look up by
        # indexing the last entry in each sub-list.
        # tbt = {
        #     'types': ['bid', 'asize', 'last', .. '<type_n>'],

        #     'bid': [tick0, tick1, tick2, .., tickn],
        #     'asize': [tick0, tick1, tick2, .., tickn],
        #     'last': [tick0, tick1, tick2, .., tickn],
        #     ...
        #     '<type_n>': [tick0, tick1, tick2, .., tickn],
        # }

        # append in reverse FIFO order for in-order iteration on
        # receiver side.
        for tick in ticks:
            ttype = tick['type']
            ticks_by_type[ttype].append(tick)


async def uniform_rate_send(

    rate: float,
    quote_stream: trio.abc.ReceiveChannel,
    stream: tractor.MsgStream,

    task_status: TaskStatus = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Throttle a real-time (presumably tick event) stream to a uniform
    transmissiom rate, normally for the purposes of throttling a data
    flow being consumed by a graphics rendering actor which itself is limited
    by a fixed maximum display rate.

    Though this function isn't documented (nor was intentially written
    to be) a token-bucket style algo, it effectively operates as one (we
    think?).

    TODO: a less naive throttler, here's some snippets:
    token bucket by njs:
    https://gist.github.com/njsmith/7ea44ec07e901cb78ebe1dd8dd846cb9

    '''
    # TODO: compute the approx overhead latency per cycle
    left_to_sleep = throttle_period = 1/rate - 0.000616

    # send cycle state
    first_quote = last_quote = None
    last_send = time.time()
    diff = 0

    task_status.started()
    ticks_by_type: defaultdict[
        str,
        list[dict],
    ] = defaultdict(list)

    clear_types = _tick_groups['clears']

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
                    # first_quote['tbt'] = ticks_by_type

                if (throttle_period - diff) > 0:
                    # received a quote but the send cycle period hasn't yet
                    # expired we aren't supposed to send yet so append
                    # to the tick frame.
                    frame_ticks(
                        first_quote,
                        last_quote,
                        ticks_by_type,
                    )

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

                frame_ticks(
                    first_quote,
                    first_quote,
                    ticks_by_type,
                )

            # we have a quote already so send it now.

        with trio.move_on_after(throttle_period) as cs:
            while (
                not set(ticks_by_type).intersection(clear_types)
            ):
                try:
                    sym, last_quote = await quote_stream.receive()
                except trio.EndOfChannel:
                    log.exception(f"feed for {stream} ended?")
                    break

                frame_ticks(
                    first_quote,
                    last_quote,
                    ticks_by_type,
                )

        # measured_rate = 1 / (time.time() - last_send)
        # log.info(
        #     f'`{sym}` throttled send hz: {round(measured_rate, ndigits=1)}'
        # )
        first_quote['tbt'] = ticks_by_type

        # TODO: now if only we could sync this to the display
        # rate timing exactly lul
        try:
            await stream.send({sym: first_quote})
        except tractor.RemoteActorError as rme:
            if rme.type is not tractor._exceptions.StreamOverrun:
                raise
            ctx = stream._ctx
            chan = ctx.chan
            log.warning(
                'Throttled quote-stream overrun!\n'
                f'{sym}:{ctx.cid}@{chan.uid}'
            )

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
        ticks_by_type.clear()
