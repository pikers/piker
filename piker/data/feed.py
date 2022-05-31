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
Data feed apis and infra.

This module is enabled for ``brokerd`` daemons.

"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from contextlib import asynccontextmanager
from functools import partial
from pprint import pformat
from types import ModuleType
from typing import (
    Any,
    AsyncIterator, Optional,
    Generator,
    Awaitable,
    TYPE_CHECKING,
)

import trio
from trio.abc import ReceiveChannel
from trio_typing import TaskStatus
import trimeter
import tractor
from tractor.trionics import maybe_open_context
from pydantic import BaseModel
import pendulum
import numpy as np

from ..brokers import get_brokermod
from .._cacheables import maybe_open_context
from ..calc import humanize
from ..log import get_logger, get_console_log
from .._daemon import (
    maybe_spawn_brokerd,
    check_for_service,
)
from ._sharedmem import (
    maybe_open_shm_array,
    attach_shm_array,
    ShmArray,
)
from .ingest import get_ingestormod
from ._source import (
    base_iohlc_dtype,
    Symbol,
    unpack_fqsn,
)
from ..ui import _search
from ._sampling import (
    sampler,
    broadcast,
    increment_ohlc_buffer,
    iter_ohlc_periods,
    sample_and_broadcast,
    uniform_rate_send,
)
from ..brokers._util import (
    NoData,
    DataUnavailable,
)

if TYPE_CHECKING:
    from .marketstore import Storage

log = get_logger(__name__)


class _FeedsBus(BaseModel):
    '''
    Data feeds broadcaster and persistence management.

    This is a brokerd side api used to manager persistent real-time
    streams that can be allocated and left alive indefinitely. A bus is
    associated one-to-one with a particular broker backend where the
    "bus" refers so a multi-symbol bus where quotes are interleaved in
    time.

    Each "entry" in the bus includes:
        - a stream used to push real time quotes (up to tick rates)
          which is executed as a lone task that is cancellable via
          a dedicated cancel scope.

    '''
    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = False

    brokername: str
    nursery: trio.Nursery
    feeds: dict[str, tuple[dict, dict]] = {}

    task_lock: trio.StrictFIFOLock = trio.StrictFIFOLock()

    # XXX: so weird but, apparently without this being `._` private
    # pydantic will complain about private `tractor.Context` instance
    # vars (namely `._portal` and `._cancel_scope`) at import time.
    # Reported this bug:
    # https://github.com/samuelcolvin/pydantic/issues/2816
    _subscribers: dict[
        str,
        list[tuple[tractor.MsgStream, Optional[float]]]
    ] = {}

    async def start_task(
        self,
        target: Awaitable,
        *args,

    ) -> None:

        async def start_with_cs(
            task_status: TaskStatus[
                trio.CancelScope] = trio.TASK_STATUS_IGNORED,
        ) -> None:
            with trio.CancelScope() as cs:
                await self.nursery.start(
                    target,
                    *args,
                )
                task_status.started(cs)

        return await self.nursery.start(start_with_cs)

    # def cancel_task(
    #     self,
    #     task: trio.lowlevel.Task,
    # ) -> bool:
    #     ...


_bus: _FeedsBus = None


def get_feed_bus(
    brokername: str,
    nursery: Optional[trio.Nursery] = None,

) -> _FeedsBus:
    '''
    Retreive broker-daemon-local data feeds bus from process global
    scope. Serialize task access to lock.

    '''
    global _bus

    if nursery is not None:
        assert _bus is None, "Feeds manager is already setup?"

        # this is initial setup by parent actor
        _bus = _FeedsBus(
            brokername=brokername,
            nursery=nursery,
        )
        assert not _bus.feeds

    assert _bus.brokername == brokername, "Uhhh wtf"
    return _bus


@tractor.context
async def _setup_persistent_brokerd(
    ctx: tractor.Context,
    brokername: str,

) -> None:
    '''
    Allocate a actor-wide service nursery in ``brokerd``
    such that feeds can be run in the background persistently by
    the broker backend as needed.

    '''
    get_console_log(tractor.current_actor().loglevel)

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


def diff_history(
    array,
    start_dt,
    end_dt,
    last_tsdb_dt: Optional[datetime] = None

) -> np.ndarray:

    to_push = array

    if last_tsdb_dt:
        s_diff = (start_dt - last_tsdb_dt).seconds

        # if we detect a partial frame's worth of data
        # that is new, slice out only that history and
        # write to shm.
        if (
            s_diff < 0
        ):
            if abs(s_diff) < len(array):
                # the + 1 is because ``last_tsdb_dt`` is pulled from
                # the last row entry for the ``'time'`` field retreived
                # from the tsdb.
                to_push = array[abs(s_diff) + 1:]

            else:
                # pass back only the portion of the array that is
                # greater then the last time stamp in the tsdb.
                time = array['time']
                to_push = array[time >= last_tsdb_dt.timestamp()]

            log.info(
                f'Pushing partial frame {to_push.size} to shm'
            )

    return to_push


async def start_backfill(
    mod: ModuleType,
    bfqsn: str,
    shm: ShmArray,

    last_tsdb_dt: Optional[datetime] = None,
    storage: Optional[Storage] = None,
    write_tsdb: bool = True,
    tsdb_is_up: bool = False,

    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,

) -> int:

    async with mod.open_history_client(bfqsn) as (hist, config):

        # get latest query's worth of history all the way
        # back to what is recorded in the tsdb
        array, start_dt, end_dt = await hist(end_dt=None)

        times = array['time']

        # sample period step size in seconds
        step_size_s = (
            pendulum.from_timestamp(times[-1])
            - pendulum.from_timestamp(times[-2])
        ).seconds

        # "frame"'s worth of sample period steps in seconds
        frame_size_s = len(array) * step_size_s

        to_push = diff_history(
            array,
            start_dt,
            end_dt,
            last_tsdb_dt=last_tsdb_dt,
        )

        log.info(f'Pushing {to_push.size} to shm!')
        shm.push(to_push)

        for delay_s in sampler.subscribers:
            await broadcast(delay_s)

        # signal that backfilling to tsdb's end datum is complete
        bf_done = trio.Event()

        # let caller unblock and deliver latest history frame
        task_status.started((shm, start_dt, end_dt, bf_done))

        # based on the sample step size, maybe load a certain amount history
        if last_tsdb_dt is None:
            if step_size_s not in (1, 60):
                raise ValueError(
                    '`piker` only needs to support 1m and 1s sampling '
                    'but ur api is trying to deliver a longer '
                    f'timeframe of {step_size_s} ' 'seconds.. so ye, dun '
                    'do dat brudder.'
                )

            # when no tsdb "last datum" is provided, we just load
            # some near-term history.
            periods = {
                1: {'days': 1},
                60: {'days': 14},
            }

            if tsdb_is_up:
                # do a decently sized backfill and load it into storage.
                periods = {
                    1: {'days': 6},
                    60: {'years': 2},
                }

            kwargs = periods[step_size_s]
            last_tsdb_dt = start_dt.subtract(**kwargs)

        # configure async query throttling
        erlangs = config.get('erlangs', 1)
        rate = config.get('rate', 1)
        frames = {}

        def iter_dts(start: datetime):

            while True:

                hist_period = pendulum.period(
                    start,
                    last_tsdb_dt,
                )
                dtrange = list(hist_period.range('seconds', frame_size_s))
                log.debug(f'New datetime index:\n{pformat(dtrange)}')

                for end_dt in dtrange:
                    log.info(f'Yielding next frame start {end_dt}')
                    start = yield end_dt

                    # if caller sends a new start date, reset to that
                    if start is not None:
                        log.warning(f'Resetting date range: {start}')
                        break
                else:
                    # from while
                    return

        # pull new history frames until we hit latest
        # already in the tsdb or a max count.
        count = 0

        # NOTE: when gaps are detected in the retreived history (by
        # comparisor of the end - start versus the expected "frame size"
        # in seconds) we need a way to alert the async request code not
        # to continue to query for data "within the gap". This var is
        # set in such cases such that further requests in that period
        # are discarded and further we reset the "datetimem query frame
        # index" in such cases to avoid needless noop requests.
        earliest_end_dt: Optional[datetime] = start_dt

        async def get_ohlc_frame(
            input_end_dt: datetime,
            iter_dts_gen: Generator[datetime],

        ) -> np.ndarray:

            nonlocal count, frames, earliest_end_dt, frame_size_s
            count += 1

            if input_end_dt > earliest_end_dt:
                # if a request comes in for an inter-gap frame we
                # discard it since likely this request is still
                # lingering from before the reset of ``iter_dts()`` via
                # ``.send()`` below.
                log.info(f'Discarding request history ending @ {input_end_dt}')

                # signals to ``trimeter`` loop to discard and
                # ``continue`` in it's schedule loop.
                return None

            try:
                log.info(
                    f'Requesting {step_size_s}s frame ending in {input_end_dt}'
                )
                array, start_dt, end_dt = await hist(end_dt=input_end_dt)
                assert array['time'][0] == start_dt.timestamp()

            except NoData:
                log.warning(
                    f'NO DATA for {frame_size_s}s frame @ {input_end_dt} ?!?'
                )
                return None  # discard signal

            except DataUnavailable as duerr:
                # broker is being a bish and we can't pull
                # any more..
                log.warning('backend halted on data deliver !?!?')

                # ugh, what's a better way?
                # TODO: fwiw, we probably want a way to signal a throttle
                # condition (eg. with ib) so that we can halt the
                # request loop until the condition is resolved?
                return duerr

            diff = end_dt - start_dt
            frame_time_diff_s = diff.seconds
            expected_frame_size_s = frame_size_s + step_size_s

            if frame_time_diff_s > expected_frame_size_s:

                # XXX: query result includes a start point prior to our
                # expected "frame size" and thus is likely some kind of
                # history gap (eg. market closed period, outage, etc.)
                # so indicate to the request loop that this gap is
                # expected by both,
                # - resetting the ``iter_dts()`` generator to start at
                #   the new start point delivered in this result
                # - setting the non-locally scoped ``earliest_end_dt``
                #   to this new value so that the request loop doesn't
                #   get tripped up thinking there's an out of order
                #   request-result condition.

                log.warning(
                    f'History frame ending @ {end_dt} appears to have a gap:\n'
                    f'{diff} ~= {frame_time_diff_s} seconds'
                )

                # reset dtrange gen to new start point
                try:
                    next_end = iter_dts_gen.send(start_dt)
                    log.info(
                        f'Reset frame index to start at {start_dt}\n'
                        f'Was at {next_end}'
                    )

                    # NOTE: manually set "earliest end datetime" index-value
                    # to avoid the request loop getting confused about
                    # new frames that are earlier in history - i.e. this
                    # **is not** the case of out-of-order frames from
                    # an async batch request.
                    earliest_end_dt = start_dt

                except StopIteration:
                    # gen already terminated meaning we probably already
                    # exhausted it via frame requests.
                    log.info(
                        "Datetime index already exhausted, can't reset.."
                    )

            to_push = diff_history(
                array,
                start_dt,
                end_dt,
                last_tsdb_dt=last_tsdb_dt,
            )
            ln = len(to_push)
            if ln:
                log.info(f'{ln} bars for {start_dt} -> {end_dt}')
                frames[input_end_dt.timestamp()] = (to_push, start_dt, end_dt)
                return to_push, start_dt, end_dt

            else:
                log.warning(
                    f'{ln} BARS TO PUSH after diff?!: {start_dt} -> {end_dt}'
                )
                return None

        # initial dt index starts at the start of the first query result
        idts = iter_dts(start_dt)

        async with trimeter.amap(
            partial(
                get_ohlc_frame,
                # we close in the ``iter_dt()`` gen in so we can send
                # reset signals as needed for gap dection in the
                # history.
                iter_dts_gen=idts,
            ),
            idts,

            capture_outcome=True,
            include_value=True,

            # better technical names bruv...
            max_at_once=erlangs,
            max_per_second=rate,

        ) as outcomes:

            # Then iterate over the return values, as they become available
            # (i.e., not necessarily in the original order)
            async for input_end_dt, outcome in outcomes:

                try:
                    out = outcome.unwrap()

                    if out is None:
                        # skip signal
                        continue

                    elif isinstance(out, DataUnavailable):
                        # no data available case signal.. so just kill
                        # further requests and basically just stop
                        # trying...
                        break

                except Exception:
                    log.exception('uhh trimeter bail')
                    raise
                else:
                    to_push, start_dt, end_dt = out

                if not len(to_push):
                    # diff returned no new data (i.e. we probablyl hit
                    # the ``last_tsdb_dt`` point).
                    # TODO: raise instead?
                    log.warning(f'No history for range {start_dt} -> {end_dt}')
                    continue

                # pipeline-style pull frames until we need to wait for
                # the next in order to arrive.
                # i = end_dts.index(input_end_dt)
                # print(f'latest end_dt {end_dt} found at index {i}')

                epochs = list(reversed(sorted(frames)))
                for epoch in epochs:

                    start = shm.array['time'][0]
                    last_shm_prepend_dt = pendulum.from_timestamp(start)
                    earliest_frame_queue_dt = pendulum.from_timestamp(epoch)

                    diff = start - epoch

                    if diff < 0:
                        log.warning(
                            'Discarding out of order frame:\n'
                            f'{earliest_frame_queue_dt}'
                        )
                        frames.pop(epoch)
                        continue
                        # await tractor.breakpoint()

                    if diff > step_size_s:

                        if earliest_end_dt < earliest_frame_queue_dt:
                            # XXX: an expected gap was encountered (see
                            # logic in ``get_ohlc_frame()``, so allow
                            # this frame through to the storage layer.
                            log.warning(
                                f'Expected history gap of {diff}s:\n'
                                f'{earliest_frame_queue_dt} <- '
                                f'{earliest_end_dt}'
                            )

                        elif (
                            erlangs > 1
                        ):
                            # we don't yet have the next frame to push
                            # so break back to the async request loop
                            # while we wait for more async frame-results
                            # to arrive.
                            if len(frames) >= erlangs:
                                log.warning(
                                    'Frame count in async-queue is greater '
                                    'then erlangs?\n'
                                    'There seems to be a gap between:\n'
                                    f'{earliest_frame_queue_dt} <- '
                                    f'{last_shm_prepend_dt}\n'
                                    'Conducting manual call for frame ending: '
                                    f'{last_shm_prepend_dt}'
                                )
                                (
                                    to_push,
                                    start_dt,
                                    end_dt,
                                ) = await get_ohlc_frame(
                                    input_end_dt=last_shm_prepend_dt,
                                    iter_dts_gen=idts,
                                )
                                last_epoch = to_push['time'][-1]
                                diff = start - last_epoch

                                if diff > step_size_s:
                                    await tractor.breakpoint()
                                    raise DataUnavailable(
                                        'An awkward frame was found:\n'
                                        f'{start_dt} -> {end_dt}:\n{to_push}'
                                    )

                                else:
                                    frames[last_epoch] = (
                                        to_push, start_dt, end_dt)
                                    break

                            expect_end = pendulum.from_timestamp(start)
                            expect_start = expect_end.subtract(
                                seconds=frame_size_s)
                            log.warning(
                                'waiting on out-of-order history frame:\n'
                                f'{expect_end - expect_start}'
                            )
                            break

                    to_push, start_dt, end_dt = frames.pop(epoch)
                    ln = len(to_push)

                    # bail gracefully on shm allocation overrun/full condition
                    try:
                        shm.push(to_push, prepend=True)
                    except ValueError:
                        log.info(
                            f'Shm buffer overrun on: {start_dt} -> {end_dt}?'
                        )
                        break

                    log.info(
                        f'Shm pushed {ln} frame:\n'
                        f'{start_dt} -> {end_dt}'
                    )
                    # keep track of most recent "prepended" ``start_dt``
                    # both for detecting gaps and ensuring async
                    # frame-result order.
                    earliest_end_dt = start_dt

                    if (
                        storage is not None
                        and write_tsdb
                    ):
                        log.info(
                            f'Writing {ln} frame to storage:\n'
                            f'{start_dt} -> {end_dt}'
                        )
                        await storage.write_ohlcv(
                            f'{bfqsn}.{mod.name}',  # lul..
                            to_push,
                        )

                # TODO: can we only trigger this if the respective
                # history in "in view"?!?
                # XXX: extremely important, there can be no checkpoints
                # in the block above to avoid entering new ``frames``
                # values while we're pipelining the current ones to
                # memory...
                for delay_s in sampler.subscribers:
                    await broadcast(delay_s)

        bf_done.set()


async def manage_history(
    mod: ModuleType,
    bus: _FeedsBus,
    fqsn: str,
    some_data_ready: trio.Event,
    feed_is_live: trio.Event,

    task_status: TaskStatus = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Load and manage historical data including the loading of any
    available series from `marketstore` as well as conducting real-time
    update of both that existing db and the allocated shared memory
    buffer.

    '''
    # (maybe) allocate shm array for this broker/symbol which will
    # be used for fast near-term history capture and processing.
    shm, opened = maybe_open_shm_array(
        key=fqsn,

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', base_iohlc_dtype),

        # we expect the sub-actor to write
        readonly=False,
    )
    # TODO: history validation
    if not opened:
        raise RuntimeError(
            "Persistent shm for sym was already open?!"
        )

    log.info('Scanning for existing `marketstored`')

    is_up = await check_for_service('marketstored')

    # for now only do backfilling if no tsdb can be found
    do_legacy_backfill = not is_up and opened

    bfqsn = fqsn.replace('.' + mod.name, '')
    open_history_client = getattr(mod, 'open_history_client', None)

    if is_up and opened and open_history_client:

        log.info('Found existing `marketstored`')
        from . import marketstore
        async with marketstore.open_storage_client(
            fqsn,
        ) as storage:

            # TODO: this should be used verbatim for the pure
            # shm backfiller approach below.

            # start history anal and load missing new data via backend.
            series, _, last_tsdb_dt = await storage.load(fqsn)

            broker, symbol, expiry = unpack_fqsn(fqsn)
            (
                shm,
                latest_start_dt,
                latest_end_dt,
                bf_done,
            ) = await bus.nursery.start(
                partial(
                    start_backfill,
                    mod,
                    bfqsn,
                    shm,
                    last_tsdb_dt=last_tsdb_dt,
                    tsdb_is_up=True,
                    storage=storage,
                )
            )

            # if len(shm.array) < 2:
            # TODO: there's an edge case here to solve where if the last
            # frame before market close (at least on ib) was pushed and
            # there was only "1 new" row pushed from the first backfill
            # query-iteration, then the sample step sizing calcs will
            # break upstream from here since you can't diff on at least
            # 2 steps... probably should also add logic to compute from
            # the tsdb series and stash that somewhere as meta data on
            # the shm buffer?.. no se.

            task_status.started(shm)
            some_data_ready.set()

            await bf_done.wait()
            # do diff against last start frame of history and only fill
            # in from the tsdb an allotment that allows for most recent
            # to be loaded into mem *before* tsdb data.
            if last_tsdb_dt:
                dt_diff_s = (
                    latest_start_dt - last_tsdb_dt
                ).seconds
            else:
                dt_diff_s = 0

            # await trio.sleep_forever()
            # TODO: see if there's faster multi-field reads:
            # https://numpy.org/doc/stable/user/basics.rec.html#accessing-multiple-fields
            # re-index  with a `time` and index field
            prepend_start = shm._first.value

            # sanity check on most-recent-data loading
            assert prepend_start > dt_diff_s

            history = list(series.values())
            if history:
                fastest = history[0]
                to_push = fastest[:prepend_start]

                shm.push(
                    to_push,

                    # insert the history pre a "days worth" of samples
                    # to leave some real-time buffer space at the end.
                    prepend=True,
                    # update_first=False,
                    # start=prepend_start,
                    field_map=marketstore.ohlc_key_map,
                )

                # load as much from storage into shm as space will
                # allow according to user's shm size settings.
                count = 0
                end = fastest['Epoch'][0]

                while shm._first.value > 0:
                    count += 1
                    series = await storage.read_ohlcv(
                        fqsn,
                        end=end,
                    )
                    history = list(series.values())
                    fastest = history[0]
                    end = fastest['Epoch'][0]
                    prepend_start -= len(to_push)
                    to_push = fastest[:prepend_start]

                    shm.push(
                        to_push,

                        # insert the history pre a "days worth" of samples
                        # to leave some real-time buffer space at the end.
                        prepend=True,
                        # update_first=False,
                        # start=prepend_start,
                        field_map=marketstore.ohlc_key_map,
                    )

                    # manually trigger step update to update charts/fsps
                    # which need an incremental update.
                    # NOTE: the way this works is super duper
                    # un-intuitive right now:
                    # - the broadcaster fires a msg to the fsp subsystem.
                    # - fsp subsys then checks for a sample step diff and
                    #   possibly recomputes prepended history.
                    # - the fsp then sends back to the parent actor
                    #   (usually a chart showing graphics for said fsp)
                    #   which tells the chart to conduct a manual full
                    #   graphics loop cycle.
                    for delay_s in sampler.subscribers:
                        await broadcast(delay_s)

                    if count > 6:
                        break

                log.info(f'Loaded {to_push.shape} datums from storage')

                # TODO: write new data to tsdb to be ready to for next read.

    if do_legacy_backfill:
        # do a legacy incremental backfill from the provider.
        log.info('No existing `marketstored` found..')

        # start history backfill task ``backfill_bars()`` is
        # a required backend func this must block until shm is
        # filled with first set of ohlc bars
        await bus.nursery.start(
            partial(
                start_backfill,
                mod,
                bfqsn,
                shm,
            )
        )

        # yield back after client connect with filled shm
        task_status.started(shm)

        # indicate to caller that feed can be delivered to
        # remote requesting client since we've loaded history
        # data that can be used.
        some_data_ready.set()

    # history retreival loop depending on user interaction and thus
    # a small RPC-prot for remotely controllinlg what data is loaded
    # for viewing.
    await trio.sleep_forever()


async def allocate_persistent_feed(
    bus: _FeedsBus,

    brokername: str,
    symbol: str,

    loglevel: str,
    start_stream: bool = True,

    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Create and maintain a "feed bus" which allocates tasks for real-time
    streaming and optional historical data storage per broker/data provider
    backend; this normally task runs *in* a `brokerd` actor.

    If none exists, this allocates a ``_FeedsBus`` which manages the
    lifetimes of streaming tasks created for each requested symbol.


    2 tasks are created:
    - a real-time streaming task which connec

    '''
    # load backend module
    try:
        mod = get_brokermod(brokername)
    except ImportError:
        mod = get_ingestormod(brokername)

    # mem chan handed to broker backend so it can push real-time
    # quotes to this task for sampling and history storage (see below).
    send, quote_stream = trio.open_memory_channel(10)

    # data sync signals for both history loading and market quotes
    some_data_ready = trio.Event()
    feed_is_live = trio.Event()

    # establish broker backend quote stream by calling
    # ``stream_quotes()``, which is a required broker backend endpoint.
    init_msg, first_quote = await bus.nursery.start(
        partial(
            mod.stream_quotes,
            send_chan=send,
            feed_is_live=feed_is_live,
            symbols=[symbol],
            loglevel=loglevel,
        )
    )
    # the broker-specific fully qualified symbol name,
    # but ensure it is lower-cased for external use.
    bfqsn = init_msg[symbol]['fqsn'].lower()
    init_msg[symbol]['fqsn'] = bfqsn

    # HISTORY, run 2 tasks:
    # - a history loader / maintainer
    # - a real-time streamer which consumers and sends new data to any
    #   consumers as well as writes to storage backends (as configured).

    # XXX: neither of these will raise but will cause an inf hang due to:
    # https://github.com/python-trio/trio/issues/2258
    # bus.nursery.start_soon(
    # await bus.start_task(
    shm = await bus.nursery.start(
        manage_history,
        mod,
        bus,
        '.'.join((bfqsn, brokername)),
        some_data_ready,
        feed_is_live,
    )

    # we hand an IPC-msg compatible shm token to the caller so it
    # can read directly from the memory which will be written by
    # this task.
    msg = init_msg[symbol]
    msg['shm_token'] = shm.token

    # true fqsn
    fqsn = '.'.join((bfqsn, brokername))
    # add a fqsn entry that includes the ``.<broker>`` suffix
    # and an entry that includes the broker-specific fqsn (including
    # any new suffixes or elements as injected by the backend).
    init_msg[fqsn] = msg
    init_msg[bfqsn] = msg

    # TODO: pretty sure we don't need this? why not just leave 1s as
    # the fastest "sample period" since we'll probably always want that
    # for most purposes.
    # pass OHLC sample rate in seconds (be sure to use python int type)
    # init_msg[symbol]['sample_rate'] = 1 #int(delay_s)

    # yield back control to starting nursery once we receive either
    # some history or a real-time quote.
    log.info(f'waiting on history to load: {fqsn}')
    await some_data_ready.wait()

    # append ``.<broker>`` suffix to each quote symbol
    acceptable_not_fqsn_with_broker_suffix = symbol + f'.{brokername}'

    generic_first_quotes = {
        acceptable_not_fqsn_with_broker_suffix: first_quote,
        fqsn: first_quote,
    }

    bus.feeds[symbol] = bus.feeds[bfqsn] = (
        init_msg,
        generic_first_quotes,
    )
    # for ambiguous names we simply apply the retreived
    # feed to that name (for now).

    # task_status.started((init_msg,  generic_first_quotes))
    task_status.started()

    if not start_stream:
        await trio.sleep_forever()

    # begin real-time updates of shm and tsb once the feed goes live and
    # the backend will indicate when real-time quotes have begun.
    await feed_is_live.wait()

    # start shm incrementer task for OHLC style sampling
    # at the current detected step period.
    times = shm.array['time']
    delay_s = times[-1] - times[times != times[-1]][-1]

    sampler.ohlcv_shms.setdefault(delay_s, []).append(shm)
    if sampler.incrementers.get(delay_s) is None:
        await bus.start_task(
            increment_ohlc_buffer,
            delay_s,
        )

    sum_tick_vlm: bool = init_msg.get(
        'shm_write_opts', {}
    ).get('sum_tick_vlm', True)

    # start sample loop
    try:
        await sample_and_broadcast(
            bus,
            shm,
            quote_stream,
            brokername,
            sum_tick_vlm
        )
    finally:
        log.warning(f'{fqsn} feed task terminated')


@tractor.context
async def open_feed_bus(

    ctx: tractor.Context,
    brokername: str,
    symbol: str,  # normally expected to the broker-specific fqsn
    loglevel: str,
    tick_throttle: Optional[float] = None,
    start_stream: bool = True,

) -> None:
    '''
    Open a data feed "bus": an actor-persistent per-broker task-oriented
    data feed registry which allows managing real-time quote streams per
    symbol.

    '''
    if loglevel is None:
        loglevel = tractor.current_actor().loglevel

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    # local state sanity checks
    # TODO: check for any stale shm entries for this symbol
    # (after we also group them in a nice `/dev/shm/piker/` subdir).
    # ensure we are who we think we are
    servicename = tractor.current_actor().name
    assert 'brokerd' in servicename
    assert brokername in servicename

    bus = get_feed_bus(brokername)

    # if no cached feed for this symbol has been created for this
    # brokerd yet, start persistent stream and shm writer task in
    # service nursery
    entry = bus.feeds.get(symbol)
    if entry is None:
        # allocate a new actor-local stream bus which
        # will persist for this `brokerd`'s service lifetime.
        async with bus.task_lock:
            await bus.nursery.start(
                partial(
                    allocate_persistent_feed,

                    bus=bus,
                    brokername=brokername,
                    # here we pass through the selected symbol in native
                    # "format" (i.e. upper vs. lowercase depending on
                    # provider).
                    symbol=symbol,
                    loglevel=loglevel,
                    start_stream=start_stream,
                )
            )
            # TODO: we can remove this?
            assert isinstance(bus.feeds[symbol], tuple)

    # XXX: ``first_quotes`` may be outdated here if this is secondary
    # subscriber
    init_msg, first_quotes = bus.feeds[symbol]

    msg = init_msg[symbol]
    bfqsn = msg['fqsn'].lower()

    # true fqsn
    fqsn = '.'.join([bfqsn, brokername])
    assert fqsn in first_quotes
    assert bus.feeds[bfqsn]

    # broker-ambiguous symbol (provided on cli - eg. mnq.globex.ib)
    bsym = symbol + f'.{brokername}'
    assert bsym in first_quotes

    # we use the broker-specific fqsn (bfqsn) for
    # the sampler subscription since the backend isn't (yet)
    # expected to append it's own name to the fqsn, so we filter
    # on keys which *do not* include that name (e.g .ib) .
    bus._subscribers.setdefault(bfqsn, [])

    # send this even to subscribers to existing feed?
    # deliver initial info message a first quote asap
    await ctx.started((
        init_msg,
        first_quotes,
    ))

    if not start_stream:
        log.warning(f'Not opening real-time stream for {fqsn}')
        await trio.sleep_forever()

    # real-time stream loop
    async with (
        ctx.open_stream() as stream,
    ):
        # re-send to trigger display loop cycle (necessary especially
        # when the mkt is closed and no real-time messages are
        # expected).
        await stream.send({fqsn: first_quotes})

        # open a bg task which receives quotes over a mem chan
        # and only pushes them to the target actor-consumer at
        # a max ``tick_throttle`` instantaneous rate.
        if tick_throttle:
            send, recv = trio.open_memory_channel(2**10)
            cs = await bus.start_task(
                uniform_rate_send,
                tick_throttle,
                recv,
                stream,
            )
            sub = (send, tick_throttle)

        else:
            sub = (stream, tick_throttle)

        subs = bus._subscribers[bfqsn]
        subs.append(sub)

        try:
            uid = ctx.chan.uid

            # ctrl protocol for start/stop of quote streams based on UI
            # state (eg. don't need a stream when a symbol isn't being
            # displayed).
            async for msg in stream:

                if msg == 'pause':
                    if sub in subs:
                        log.info(
                            f'Pausing {fqsn} feed for {uid}')
                        subs.remove(sub)

                elif msg == 'resume':
                    if sub not in subs:
                        log.info(
                            f'Resuming {fqsn} feed for {uid}')
                        subs.append(sub)
                else:
                    raise ValueError(msg)
        finally:
            log.info(
                f'Stopping {symbol}.{brokername} feed for {ctx.chan.uid}')

            if tick_throttle:
                # TODO: a one-cancels-one nursery
                # n.cancel_scope.cancel()
                cs.cancel()
            try:
                bus._subscribers[bfqsn].remove(sub)
            except ValueError:
                log.warning(f'{sub} for {symbol} was already removed?')


@asynccontextmanager
async def open_sample_step_stream(
    portal: tractor.Portal,
    delay_s: int,

) -> tractor.ReceiveMsgStream:

    # XXX: this should be singleton on a host,
    # a lone broker-daemon per provider should be
    # created for all practical purposes
    async with maybe_open_context(
        acm_func=partial(
            portal.open_context,
            iter_ohlc_periods,
        ),
        kwargs={'delay_s': delay_s},

    ) as (cache_hit, (ctx, first)):
        async with ctx.open_stream() as istream:
            if cache_hit:
                # add a new broadcast subscription for the quote stream
                # if this feed is likely already in use
                async with istream.subscribe() as bistream:
                    yield bistream
            else:
                yield istream


@dataclass
class Feed:
    '''
    A data feed for client-side interaction with far-process real-time
    data sources.

    This is an thin abstraction on top of ``tractor``'s portals for
    interacting with IPC streams and storage APIs (shm and time-series
    db).

    '''
    name: str
    shm: ShmArray
    mod: ModuleType
    first_quotes: dict  # symbol names to first quote dicts
    _portal: tractor.Portal
    stream: trio.abc.ReceiveChannel[dict[str, Any]]
    status: dict[str, Any]

    throttle_rate: Optional[int] = None

    _trade_stream: Optional[AsyncIterator[dict[str, Any]]] = None
    _max_sample_rate: int = 1

    # cache of symbol info messages received as first message when
    # a stream startsc.
    symbols: dict[str, Symbol] = field(default_factory=dict)

    @property
    def portal(self) -> tractor.Portal:
        return self._portal

    async def receive(self) -> dict:
        return await self.stream.receive()

    @asynccontextmanager
    async def index_stream(
        self,
        delay_s: Optional[int] = None

    ) -> AsyncIterator[int]:

        delay_s = delay_s or self._max_sample_rate

        async with open_sample_step_stream(
            self.portal,
            delay_s,
        ) as istream:
            yield istream

    async def pause(self) -> None:
        await self.stream.send('pause')

    async def resume(self) -> None:
        await self.stream.send('resume')


@asynccontextmanager
async def install_brokerd_search(

    portal: tractor.Portal,
    brokermod: ModuleType,

) -> None:

    async with portal.open_context(
        brokermod.open_symbol_search
    ) as (ctx, cache):

        # shield here since we expect the search rpc to be
        # cancellable by the user as they see fit.
        async with ctx.open_stream() as stream:

            async def search(text: str) -> dict[str, Any]:
                await stream.send(text)
                return await stream.receive()

            async with _search.register_symbol_search(

                provider_name=brokermod.name,
                search_routine=search,

                # TODO: should be make this a required attr of
                # a backend module?
                pause_period=getattr(
                    brokermod, '_search_conf', {}
                ).get('pause_period', 0.0616),
            ):
                yield


@asynccontextmanager
async def open_feed(

    fqsns: list[str],

    loglevel: Optional[str] = None,
    backpressure: bool = True,
    start_stream: bool = True,
    tick_throttle: Optional[float] = None,  # Hz

) -> Feed:
    '''
    Open a "data feed" which provides streamed real-time quotes.

    '''
    fqsn = fqsns[0].lower()

    brokername, key, suffix = unpack_fqsn(fqsn)
    bfqsn = fqsn.replace('.' + brokername, '')

    try:
        mod = get_brokermod(brokername)
    except ImportError:
        mod = get_ingestormod(brokername)

    # no feed for broker exists so maybe spawn a data brokerd
    async with (

        # if no `brokerd` for this backend exists yet we spawn
        # and actor for one.
        maybe_spawn_brokerd(
            brokername,
            loglevel=loglevel
        ) as portal,

        # (allocate and) connect to any feed bus for this broker
        portal.open_context(
            open_feed_bus,
            brokername=brokername,
            symbol=bfqsn,
            loglevel=loglevel,
            start_stream=start_stream,
            tick_throttle=tick_throttle,

        ) as (ctx, (init_msg, first_quotes)),

        ctx.open_stream(
            # XXX: be explicit about stream backpressure since we should
            # **never** overrun on feeds being too fast, which will
            # pretty much always happen with HFT XD
            backpressure=backpressure,
        ) as stream,

    ):
        # we can only read from shm
        shm = attach_shm_array(
            token=init_msg[bfqsn]['shm_token'],
            readonly=True,
        )
        assert fqsn in first_quotes

        feed = Feed(
            name=brokername,
            shm=shm,
            mod=mod,
            first_quotes=first_quotes,
            stream=stream,
            _portal=portal,
            status={},
            throttle_rate=tick_throttle,
        )

        # fill out "status info" that the UI can show
        host, port = feed.portal.channel.raddr
        if host == '127.0.0.1':
            host = 'localhost'

        feed.status.update({
            'actor_name': feed.portal.channel.uid[0],
            'host': host,
            'port': port,
            'shm': f'{humanize(feed.shm._shm.size)}',
            'throttle_rate': feed.throttle_rate,
        })
        feed.status.update(init_msg.pop('status', {}))

        for sym, data in init_msg.items():
            si = data['symbol_info']
            fqsn = data['fqsn'] + f'.{brokername}'
            symbol = Symbol.from_fqsn(
                fqsn,
                info=si,
            )

            # symbol.broker_info[brokername] = si
            feed.symbols[fqsn] = symbol
            feed.symbols[sym] = symbol

            # cast shm dtype to list... can't member why we need this
            shm_token = data['shm_token']

            # XXX: msgspec won't relay through the tuples XD
            shm_token['dtype_descr'] = tuple(
                map(tuple, shm_token['dtype_descr']))

            assert shm_token == shm.token  # sanity

        feed._max_sample_rate = 1

        try:
            yield feed
        finally:
            # drop the infinite stream connection
            await ctx.cancel()


@asynccontextmanager
async def maybe_open_feed(

    fqsns: list[str],
    loglevel: Optional[str] = None,

    **kwargs,

) -> (
    Feed,
    ReceiveChannel[dict[str, Any]],
):
    '''
    Maybe open a data to a ``brokerd`` daemon only if there is no
    local one for the broker-symbol pair, if one is cached use it wrapped
    in a tractor broadcast receiver.

    '''
    fqsn = fqsns[0]

    async with maybe_open_context(
        acm_func=open_feed,
        kwargs={
            'fqsns': fqsns,
            'loglevel': loglevel,
            'tick_throttle': kwargs.get('tick_throttle'),

            # XXX: super critical to have bool defaults here XD
            'backpressure': kwargs.get('backpressure', True),
            'start_stream': kwargs.get('start_stream', True),
        },
        key=fqsn,

    ) as (cache_hit, feed):

        if cache_hit:
            log.info(f'Using cached feed for {fqsn}')
            # add a new broadcast subscription for the quote stream
            # if this feed is likely already in use
            async with feed.stream.subscribe() as bstream:
                yield feed, bstream
        else:
            yield feed, feed.stream
