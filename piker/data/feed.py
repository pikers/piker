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
from collections import (
    defaultdict,
    Counter,
)
from contextlib import asynccontextmanager as acm
from decimal import Decimal
from datetime import datetime
from functools import partial
import time
from types import ModuleType
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Optional,
    Awaitable,
    Sequence,
    TYPE_CHECKING,
    Union,
)

import trio
from trio.abc import ReceiveChannel
from trio_typing import TaskStatus
import tractor
from tractor.trionics import (
    maybe_open_context,
    gather_contexts,
)
import pendulum
import numpy as np

from ..brokers import get_brokermod
from ..calc import humanize
from ..log import (
    get_logger,
    get_console_log,
)
from ..service import (
    maybe_spawn_brokerd,
    check_for_service,
)
from .flows import Flume
from ._sharedmem import (
    maybe_open_shm_array,
    ShmArray,
    _secs_in_day,
)
from .ingest import get_ingestormod
from .types import Struct
from ..accounting._mktinfo import (
    Asset,
    MktPair,
    unpack_fqme,
    Symbol,
)
from ._source import base_iohlc_dtype
from ..ui import _search
from ._sampling import (
    open_sample_stream,
    sample_and_broadcast,
    uniform_rate_send,
)
from ..brokers._util import (
    DataUnavailable,
)

if TYPE_CHECKING:
    from ..service.marketstore import Storage

log = get_logger(__name__)


class _FeedsBus(Struct):
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
    brokername: str
    nursery: trio.Nursery
    feeds: dict[str, tuple[dict, dict]] = {}

    task_lock: trio.StrictFIFOLock = trio.StrictFIFOLock()

    _subscribers: defaultdict[
        str,
        set[
            tuple[
                tractor.MsgStream | trio.MemorySendChannel,
                # tractor.Context,
                float | None,  # tick throttle in Hz
            ]
        ]
    ] = defaultdict(set)

    async def start_task(
        self,
        target: Awaitable,
        *args,

    ) -> trio.CancelScope:

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

    def get_subs(
        self,
        key: str,
    ) -> set[
        tuple[
            Union[tractor.MsgStream, trio.MemorySendChannel],
            # tractor.Context,
            float | None,  # tick throttle in Hz
        ]
    ]:
        '''
        Get the ``set`` of consumer subscription entries for the given key.

        '''
        return self._subscribers[key]

    def add_subs(
        self,
        key: str,
        subs: set[tuple[
            tractor.MsgStream | trio.MemorySendChannel,
            # tractor.Context,
            float | None,  # tick throttle in Hz
        ]],
    ) -> set[tuple]:
        '''
        Add a ``set`` of consumer subscription entries for the given key.

        '''
        _subs = self._subscribers[key]
        _subs.update(subs)
        return _subs

    def remove_subs(
        self,
        key: str,
        subs: set[tuple],

    ) -> set[tuple]:
        '''
        Remove a ``set`` of consumer subscription entries for key.

        '''
        _subs = self.get_subs(key)
        _subs.difference_update(subs)
        return _subs


_bus: _FeedsBus = None


def get_feed_bus(
    brokername: str,
    nursery: Optional[trio.Nursery] = None,

) -> _FeedsBus:
    '''
    Retrieve broker-daemon-local data feeds bus from process global
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


def diff_history(
    array: np.ndarray,
    timeframe: int,
    start_dt: datetime,
    end_dt: datetime,
    last_tsdb_dt: datetime | None = None

) -> np.ndarray:

    # no diffing with tsdb dt index possible..
    if last_tsdb_dt is None:
        return array

    time = array['time']
    return array[time > last_tsdb_dt.timestamp()]


async def start_backfill(
    mod: ModuleType,
    bfqsn: str,
    shm: ShmArray,
    timeframe: float,
    sampler_stream: tractor.MsgStream,
    feed_is_live: trio.Event,

    last_tsdb_dt: Optional[datetime] = None,
    storage: Optional[Storage] = None,
    write_tsdb: bool = True,
    tsdb_is_up: bool = False,

    task_status: TaskStatus[tuple] = trio.TASK_STATUS_IGNORED,

) -> int:

    hist: Callable[
        [int, datetime, datetime],
        tuple[np.ndarray, str]
    ]
    config: dict[str, int]
    async with mod.open_history_client(bfqsn) as (hist, config):

        # get latest query's worth of history all the way
        # back to what is recorded in the tsdb
        array, start_dt, end_dt = await hist(
            timeframe,
            end_dt=None,
        )
        times = array['time']

        # sample period step size in seconds
        step_size_s = (
            pendulum.from_timestamp(times[-1])
            - pendulum.from_timestamp(times[-2])
        ).seconds

        # if the market is open (aka we have a live feed) but the
        # history sample step index seems off we report the surrounding
        # data and drop into a bp. this case shouldn't really ever
        # happen if we're doing history retrieval correctly.
        if (
            step_size_s == 60
            and feed_is_live.is_set()
        ):
            inow = round(time.time())
            diff = inow - times[-1]
            if abs(diff) > 60:
                surr = array[-6:]
                diff_in_mins = round(diff/60., ndigits=2)
                log.warning(
                    f'STEP ERROR `{bfqsn}` for period {step_size_s}s:\n'
                    f'Off by `{diff}` seconds (or `{diff_in_mins}` mins)\n'
                    'Surrounding 6 time stamps:\n'
                    f'{list(surr["time"])}\n'
                    'Here is surrounding 6 samples:\n'
                    f'{surr}\nn'
                )

                # uncomment this for a hacker who wants to investigate
                # this case manually..
                # await tractor.breakpoint()

        # frame's worth of sample-period-steps, in seconds
        frame_size_s = len(array) * step_size_s

        to_push = diff_history(
            array,
            timeframe,
            start_dt,
            end_dt,
            last_tsdb_dt=last_tsdb_dt,
        )

        log.info(f'Pushing {to_push.size} to shm!')
        shm.push(to_push, prepend=True)

        # TODO: *** THIS IS A BUG ***
        # we need to only broadcast to subscribers for this fqsn..
        # otherwise all fsps get reset on every chart..
        await sampler_stream.send('broadcast_all')

        # signal that backfilling to tsdb's end datum is complete
        bf_done = trio.Event()

        # let caller unblock and deliver latest history frame
        task_status.started((
            start_dt,
            end_dt,
            bf_done,
        ))

        # based on the sample step size, maybe load a certain amount history
        if last_tsdb_dt is None:
            if step_size_s not in (1, 60):
                raise ValueError(
                    '`piker` only needs to support 1m and 1s sampling '
                    'but ur api is trying to deliver a longer '
                    f'timeframe of {step_size_s} seconds..\n'
                    'So yuh.. dun do dat brudder.'
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
                    60: {'years': 6},
                }

            period_duration = periods[step_size_s]

            # NOTE: manually set the "latest" datetime which we intend to
            # backfill history "until" so as to adhere to the history
            # settings above when the tsdb is detected as being empty.
            last_tsdb_dt = start_dt.subtract(**period_duration)

        # configure async query throttling
        # rate = config.get('rate', 1)
        # XXX: legacy from ``trimeter`` code but unsupported now.
        # erlangs = config.get('erlangs', 1)

        # avoid duplicate history frames with a set of datetime frame
        # starts and associated counts of how many duplicates we see
        # per time stamp.
        starts: Counter[datetime] = Counter()

        # inline sequential loop where we simply pass the
        # last retrieved start dt to the next request as
        # it's end dt.
        while end_dt > last_tsdb_dt:
            log.debug(
                f'Requesting {step_size_s}s frame ending in {start_dt}'
            )

            try:
                array, next_start_dt, end_dt = await hist(
                    timeframe,
                    end_dt=start_dt,
                )

            # broker says there never was or is no more history to pull
            except DataUnavailable:
                log.warning(
                    f'NO-MORE-DATA: backend {mod.name} halted history!?'
                )

                # ugh, what's a better way?
                # TODO: fwiw, we probably want a way to signal a throttle
                # condition (eg. with ib) so that we can halt the
                # request loop until the condition is resolved?
                return

            if (
                next_start_dt in starts
                and starts[next_start_dt] <= 6
            ):
                start_dt = min(starts)
                log.warning(
                    f"{bfqsn}: skipping duplicate frame @ {next_start_dt}"
                )
                starts[start_dt] += 1
                continue

            elif starts[next_start_dt] > 6:
                log.warning(
                    f'NO-MORE-DATA: backend {mod.name} before {next_start_dt}?'
                )
                return

            # only update new start point if not-yet-seen
            start_dt = next_start_dt
            starts[start_dt] += 1

            assert array['time'][0] == start_dt.timestamp()

            diff = end_dt - start_dt
            frame_time_diff_s = diff.seconds
            expected_frame_size_s = frame_size_s + step_size_s

            if frame_time_diff_s > expected_frame_size_s:

                # XXX: query result includes a start point prior to our
                # expected "frame size" and thus is likely some kind of
                # history gap (eg. market closed period, outage, etc.)
                # so just report it to console for now.
                log.warning(
                    f'History frame ending @ {end_dt} appears to have a gap:\n'
                    f'{diff} ~= {frame_time_diff_s} seconds'
                )

            to_push = diff_history(
                array,
                timeframe,
                start_dt,
                end_dt,
                last_tsdb_dt=last_tsdb_dt,
            )
            ln = len(to_push)
            if ln:
                log.info(f'{ln} bars for {start_dt} -> {end_dt}')

            else:
                log.warning(
                    f'{ln} BARS TO PUSH after diff?!: {start_dt} -> {end_dt}'
                )

            # bail gracefully on shm allocation overrun/full condition
            try:
                shm.push(to_push, prepend=True)
            except ValueError:
                log.info(
                    f'Shm buffer overrun on: {start_dt} -> {end_dt}?'
                )
                # can't push the entire frame? so
                # push only the amount that can fit..
                break

            log.info(
                f'Shm pushed {ln} frame:\n'
                f'{start_dt} -> {end_dt}'
            )

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
                    timeframe,
                )

        # TODO: can we only trigger this if the respective
        # history in "in view"?!?

        # XXX: extremely important, there can be no checkpoints
        # in the block above to avoid entering new ``frames``
        # values while we're pipelining the current ones to
        # memory...
        await sampler_stream.send('broadcast_all')

        # short-circuit (for now)
        bf_done.set()


async def basic_backfill(
    bus: _FeedsBus,
    mod: ModuleType,
    bfqsn: str,
    shms: dict[int, ShmArray],
    sampler_stream: tractor.MsgStream,
    feed_is_live: trio.Event,

) -> None:

    # do a legacy incremental backfill from the provider.
    log.info('No TSDB (marketstored) found, doing basic backfill..')

    # start history backfill task ``backfill_bars()`` is
    # a required backend func this must block until shm is
    # filled with first set of ohlc bars
    for timeframe, shm in shms.items():
        try:
            await bus.nursery.start(
                partial(
                    start_backfill,
                    mod,
                    bfqsn,
                    shm,
                    timeframe,
                    sampler_stream,
                    feed_is_live,
                )
            )
        except DataUnavailable:
            # XXX: timeframe not supported for backend
            continue


async def tsdb_backfill(
    mod: ModuleType,
    marketstore: ModuleType,
    bus: _FeedsBus,
    storage: Storage,
    fqsn: str,
    bfqsn: str,
    shms: dict[int, ShmArray],
    sampler_stream: tractor.MsgStream,
    feed_is_live: trio.Event,

    task_status: TaskStatus[
        tuple[ShmArray, ShmArray]
    ] = trio.TASK_STATUS_IGNORED,

) -> None:

    # TODO: this should be used verbatim for the pure
    # shm backfiller approach below.
    dts_per_tf: dict[int, datetime] = {}

    # start history anal and load missing new data via backend.
    for timeframe, shm in shms.items():
        # loads a (large) frame of data from the tsdb depending
        # on the db's query size limit.
        tsdb_history, first_tsdb_dt, last_tsdb_dt = await storage.load(
            fqsn,
            timeframe=timeframe,
        )

        broker, symbol, expiry = unpack_fqme(fqsn)
        try:
            (
                latest_start_dt,
                latest_end_dt,
                bf_done,
            ) = await bus.nursery.start(
                partial(
                    start_backfill,
                    mod,
                    bfqsn,
                    shm,
                    timeframe,
                    sampler_stream,
                    feed_is_live,

                    last_tsdb_dt=last_tsdb_dt,
                    tsdb_is_up=True,
                    storage=storage,
                )
            )
        except DataUnavailable:
            # XXX: timeframe not supported for backend
            dts_per_tf[timeframe] = (
                tsdb_history,
                last_tsdb_dt,
                None,
                None,
                None,
            )
            continue

        # tsdb_history = series.get(timeframe)
        dts_per_tf[timeframe] = (
            tsdb_history,
            last_tsdb_dt,
            latest_start_dt,
            latest_end_dt,
            bf_done,
        )

        # if len(hist_shm.array) < 2:
        # TODO: there's an edge case here to solve where if the last
        # frame before market close (at least on ib) was pushed and
        # there was only "1 new" row pushed from the first backfill
        # query-iteration, then the sample step sizing calcs will
        # break upstream from here since you can't diff on at least
        # 2 steps... probably should also add logic to compute from
        # the tsdb series and stash that somewhere as meta data on
        # the shm buffer?.. no se.

    # unblock the feed bus management task
    # assert len(shms[1].array)
    task_status.started()

    async def back_load_from_tsdb(
        timeframe: int,
        shm: ShmArray,
    ):
        (
            tsdb_history,
            last_tsdb_dt,
            latest_start_dt,
            latest_end_dt,
            bf_done,
        ) = dts_per_tf[timeframe]

        # sync to backend history task's query/load completion
        if bf_done:
            await bf_done.wait()

        # TODO: eventually it'd be nice to not require a shm array/buffer
        # to accomplish this.. maybe we can do some kind of tsdb direct to
        # graphics format eventually in a child-actor?

        # TODO: see if there's faster multi-field reads:
        # https://numpy.org/doc/stable/user/basics.rec.html#accessing-multiple-fields
        # re-index  with a `time` and index field
        prepend_start = shm._first.value
        array = shm.array
        if len(array):
            shm_last_dt = pendulum.from_timestamp(shm.array[0]['time'])
        else:
            shm_last_dt = None

        if last_tsdb_dt:
            assert shm_last_dt >= last_tsdb_dt

        # do diff against start index of last frame of history and only
        # fill in an amount of datums from tsdb allows for most recent
        # to be loaded into mem *before* tsdb data.
        if (
            last_tsdb_dt
            and latest_start_dt
        ):
            backfilled_size_s = (
                latest_start_dt - last_tsdb_dt
            ).seconds
            # if the shm buffer len is not large enough to contain
            # all missing data between the most recent backend-queried frame
            # and the most recent dt-index in the db we warn that we only
            # want to load a portion of the next tsdb query to fill that
            # space.
            log.info(
                f'{backfilled_size_s} seconds worth of {timeframe}s loaded'
            )

        # Load TSDB history into shm buffer (for display) if there is
        # remaining buffer space.

        if (
            len(tsdb_history)
        ):
            # load the first (smaller) bit of history originally loaded
            # above from ``Storage.load()``.
            to_push = tsdb_history[-prepend_start:]
            shm.push(
                to_push,

                # insert the history pre a "days worth" of samples
                # to leave some real-time buffer space at the end.
                prepend=True,
                # update_first=False,
                # start=prepend_start,
                field_map=marketstore.ohlc_key_map,
            )

            tsdb_last_frame_start = tsdb_history['Epoch'][0]

            if timeframe == 1:
                times = shm.array['time']
                assert (times[1] - times[0]) == 1

            # load as much from storage into shm possible (depends on
            # user's shm size settings).
            while shm._first.value > 0:

                tsdb_history = await storage.read_ohlcv(
                    fqsn,
                    timeframe=timeframe,
                    end=tsdb_last_frame_start,
                )

                # empty query
                if not len(tsdb_history):
                    break

                next_start = tsdb_history['Epoch'][0]
                if next_start >= tsdb_last_frame_start:
                    # no earlier data detected
                    break
                else:
                    tsdb_last_frame_start = next_start

                prepend_start = shm._first.value
                to_push = tsdb_history[-prepend_start:]

                # insert the history pre a "days worth" of samples
                # to leave some real-time buffer space at the end.
                shm.push(
                    to_push,
                    prepend=True,
                    field_map=marketstore.ohlc_key_map,
                )
                log.info(f'Loaded {to_push.shape} datums from storage')

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
                await sampler_stream.send('broadcast_all')

                # TODO: write new data to tsdb to be ready to for next read.

    # backload from db (concurrently per timeframe) once backfilling of
    # recent dat a loaded from the backend provider (see
    # ``bf_done.wait()`` call).
    async with trio.open_nursery() as nurse:
        for timeframe, shm in shms.items():
            nurse.start_soon(
                back_load_from_tsdb,
                timeframe,
                shm,
            )


async def manage_history(
    mod: ModuleType,
    bus: _FeedsBus,
    fqsn: str,
    some_data_ready: trio.Event,
    feed_is_live: trio.Event,
    timeframe: float = 60,  # in seconds

    task_status: TaskStatus[
        tuple[ShmArray, ShmArray]
    ] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Load and manage historical data including the loading of any
    available series from `marketstore` as well as conducting real-time
    update of both that existing db and the allocated shared memory
    buffer.

    '''

    # TODO: is there a way to make each shm file key
    # actor-tree-discovery-addr unique so we avoid collisions
    # when doing tests which also allocate shms for certain instruments
    # that may be in use on the system by some other running daemons?
    # from tractor._state import _runtime_vars
    # port = _runtime_vars['_root_mailbox'][1]

    uid = tractor.current_actor().uid
    suffix = '.'.join(uid)

    # (maybe) allocate shm array for this broker/symbol which will
    # be used for fast near-term history capture and processing.
    hist_shm, opened = maybe_open_shm_array(
        # key=f'{fqsn}_hist_p{port}',
        key=f'{fqsn}_hist.{suffix}',

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', base_iohlc_dtype),

        # we expect the sub-actor to write
        readonly=False,
    )
    hist_zero_index = hist_shm.index - 1

    # TODO: history validation
    if not opened:
        raise RuntimeError(
            "Persistent shm for sym was already open?!"
        )

    rt_shm, opened = maybe_open_shm_array(
        # key=f'{fqsn}_rt_p{port}',
        key=f'{fqsn}_rt.{suffix}',

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', base_iohlc_dtype),

        # we expect the sub-actor to write
        readonly=False,
        size=3*_secs_in_day,
    )

    # (for now) set the rt (hft) shm array with space to prepend
    # only a few days worth of 1s history.
    days = 2
    start_index = days*_secs_in_day
    rt_shm._first.value = start_index
    rt_shm._last.value = start_index
    rt_zero_index = rt_shm.index - 1

    if not opened:
        raise RuntimeError(
            "Persistent shm for sym was already open?!"
        )

    # register 1s and 1m buffers with the global incrementer task
    async with open_sample_stream(
        period_s=1.,
        shms_by_period={
            1.: rt_shm.token,
            60.: hist_shm.token,
        },

        # NOTE: we want to only open a stream for doing broadcasts on
        # backfill operations, not receive the sample index-stream
        # (since there's no code in this data feed layer that needs to
        # consume it).
        open_index_stream=True,
        sub_for_broadcasts=False,

    ) as sample_stream:

        log.info('Scanning for existing `marketstored`')
        tsdb_is_up = await check_for_service('marketstored')

        bfqsn = fqsn.replace('.' + mod.name, '')
        open_history_client = getattr(mod, 'open_history_client', None)
        assert open_history_client

        if (
            tsdb_is_up
            and opened
            and open_history_client
        ):
            log.info('Found existing `marketstored`')

            from ..service import marketstore
            async with (
                marketstore.open_storage_client(fqsn)as storage,
            ):
                # TODO: drop returning the output that we pass in?
                await bus.nursery.start(
                    tsdb_backfill,
                    mod,
                    marketstore,
                    bus,
                    storage,
                    fqsn,
                    bfqsn,
                    {
                        1: rt_shm,
                        60: hist_shm,
                    },
                    sample_stream,
                    feed_is_live,
                )

                # yield back after client connect with filled shm
                task_status.started((
                    hist_zero_index,
                    hist_shm,
                    rt_zero_index,
                    rt_shm,
                ))

                # indicate to caller that feed can be delivered to
                # remote requesting client since we've loaded history
                # data that can be used.
                some_data_ready.set()

                # history retreival loop depending on user interaction
                # and thus a small RPC-prot for remotely controllinlg
                # what data is loaded for viewing.
                await trio.sleep_forever()

        # load less history if no tsdb can be found
        elif (
            not tsdb_is_up
            and opened
        ):
            await basic_backfill(
                bus,
                mod,
                bfqsn,
                {
                    1: rt_shm,
                    60: hist_shm,
                },
                sample_stream,
                feed_is_live,
            )
            task_status.started((
                hist_zero_index,
                hist_shm,
                rt_zero_index,
                rt_shm,
            ))
            some_data_ready.set()
            await trio.sleep_forever()


async def allocate_persistent_feed(
    bus: _FeedsBus,
    sub_registered: trio.Event,

    brokername: str,
    symstr: str,

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
    send, quote_stream = trio.open_memory_channel(616)

    # data sync signals for both history loading and market quotes
    some_data_ready = trio.Event()
    feed_is_live = trio.Event()

    symstr = symstr.lower()

    # establish broker backend quote stream by calling
    # ``stream_quotes()``, which is a required broker backend endpoint.
    init_msg, first_quote = await bus.nursery.start(
        partial(
            mod.stream_quotes,
            send_chan=send,
            feed_is_live=feed_is_live,
            symbols=[symstr],
            loglevel=loglevel,
        )
    )
    # TODO: this is indexed by symbol for now since we've planned (for
    # some time) to expect backends to handle single
    # ``.stream_quotes()`` calls with multiple symbols inputs to just
    # work such that a backend can do its own multiplexing if desired.
    #
    # Likely this will require some design changes:
    # - the .started() should return some config output determining
    #   whether the backend does indeed multiplex multi-symbol quotes
    #   internally or whether separate task spawns should be done per
    #   symbol (as it is right now).
    # - information about discovery of non-local host daemons which can
    #   be contacted in the case where we want to support load disti
    #   over multi-use clusters; eg. some new feed request is
    #   re-directed to another daemon cluster because the current one is
    #   at max capacity.
    # - the same ideas ^ but when a local core is maxxed out (like how
    #   binance does often with hft XD
    # - if a brokerd is non-local then we can't just allocate a mem
    #   channel here and have the brokerd write it, we instead need
    #   a small streaming machine around the remote feed which can then
    #   do the normal work of sampling and writing shm buffers
    #   (depending on if we want sampling done on the far end or not?)
    msg = init_msg[symstr]

    # the broker-specific fully qualified symbol name,
    # but ensure it is lower-cased for external use.
    bs_mktid = msg['fqsn'].lower()

    # true fqme including broker/provider suffix
    fqme = '.'.join((bs_mktid, brokername))

    mktinfo = msg.get('mkt_info')
    if not mktinfo:

        mktinfo = msg['symbol_info']

        # TODO: read out renamed/new tick size fields in block below!
        price_tick = mktinfo.get(
            'price_tick_size',
            Decimal('0.01'),
        )
        size_tick = mktinfo.get(
            'lot_tick_size',
            Decimal('0.0'),
        )

        log.warning(f'FQME: {fqme} -> backend needs port to `MktPair`')
        mkt = MktPair.from_fqme(
            fqme,
            price_tick=price_tick,
            size_tick=size_tick,
            bs_mktid=bs_mktid,

            _atype=mktinfo['asset_type']
        )

    else:
        # the new msg-protocol is to expect an already packed
        # ``Asset`` and ``MktPair`` object from the backend
        mkt = mktinfo
        assert isinstance(mkt, MktPair)
        assert isinstance(mkt.dst, Asset)

    assert mkt.type_key

    # HISTORY storage, run 2 tasks:
    # - a history loader / maintainer
    # - a real-time streamer which consumers and sends new data to any
    #   consumers as well as writes to storage backends (as configured).

    # XXX: neither of these will raise but will cause an inf hang due to:
    # https://github.com/python-trio/trio/issues/2258
    # bus.nursery.start_soon(
    # await bus.start_task(

    (
        izero_hist,
        hist_shm,
        izero_rt,
        rt_shm,
    ) = await bus.nursery.start(
        manage_history,
        mod,
        bus,
        fqme,
        some_data_ready,
        feed_is_live,
    )

    # yield back control to starting nursery once we receive either
    # some history or a real-time quote.
    log.info(f'waiting on history to load: {fqme}')
    await some_data_ready.wait()

    symbol = Symbol.from_fqsn(
        fqsn=fqme,
        info=msg['symbol_info'],
    )
    flume = Flume(
        # TODO: we have to use this for now since currently the
        # MktPair above doesn't render the correct output key it seems
        # when we provide the `MktInfo` here?..?
        symbol=symbol,
        first_quote=first_quote,
        _rt_shm_token=rt_shm.token,
        _hist_shm_token=hist_shm.token,
        izero_hist=izero_hist,
        izero_rt=izero_rt,
    )

    # for ambiguous names we simply apply the retreived
    # feed to that name (for now).
    bus.feeds[symstr] = bus.feeds[bs_mktid] = flume

    task_status.started()

    if not start_stream:
        await trio.sleep_forever()

    # begin real-time updates of shm and tsb once the feed goes live and
    # the backend will indicate when real-time quotes have begun.
    await feed_is_live.wait()

    sum_tick_vlm: bool = init_msg.get(
        'shm_write_opts', {}
    ).get('sum_tick_vlm', True)

    # NOTE: if no high-freq sampled data has (yet) been loaded,
    # seed the buffer with a history datum - this is most handy
    # for many backends which don't sample @ 1s OHLC but do have
    # slower data such as 1m OHLC.
    if (
        not len(rt_shm.array)
        and hist_shm.array.size
    ):
        rt_shm.push(hist_shm.array[-3:-1])
        ohlckeys = ['open', 'high', 'low', 'close']
        rt_shm.array[ohlckeys][-2:] = hist_shm.array['close'][-1]
        rt_shm.array['volume'][-2:] = 0

        # set fast buffer time step to 1s
        ts = round(time.time())
        rt_shm.array['time'][0] = ts
        rt_shm.array['time'][1] = ts + 1

    elif hist_shm.array.size == 0:
        await tractor.breakpoint()

    # wait the spawning parent task to register its subscriber
    # send-stream entry before we start the sample loop.
    await sub_registered.wait()

    # start sample loop and shm incrementer task for OHLC style sampling
    # at the above registered step periods.
    try:
        log.info(f'Starting sampler task for {fqme}')
        await sample_and_broadcast(
            bus,
            rt_shm,
            hist_shm,
            quote_stream,
            brokername,
            sum_tick_vlm
        )
    finally:
        log.warning(f'{fqme} feed task terminated')


@tractor.context
async def open_feed_bus(

    ctx: tractor.Context,
    brokername: str,
    symbols: list[str],  # normally expected to the broker-specific fqsn

    loglevel: str = 'error',
    tick_throttle: Optional[float] = None,
    start_stream: bool = True,

) -> dict[
    str,  # fqsn
    tuple[dict, dict]  # pair of dicts of the initmsg and first quotes
]:
    '''
    Open a data feed "bus": an actor-persistent per-broker task-oriented
    data feed registry which allows managing real-time quote streams per
    symbol.

    '''
    # ensure that a quote feed stream which is pushing too fast doesn't
    # cause and overrun in the client.
    ctx._backpressure = True

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
    sub_registered = trio.Event()

    flumes: dict[str, Flume] = {}

    for symbol in symbols:

        # we always use lower case keys internally
        symbol = symbol.lower()

        # if no cached feed for this symbol has been created for this
        # brokerd yet, start persistent stream and shm writer task in
        # service nursery
        flume = bus.feeds.get(symbol)
        if flume is None:
            # allocate a new actor-local stream bus which
            # will persist for this `brokerd`'s service lifetime.
            async with bus.task_lock:
                await bus.nursery.start(
                    partial(
                        allocate_persistent_feed,

                        bus=bus,
                        sub_registered=sub_registered,
                        brokername=brokername,
                        # here we pass through the selected symbol in native
                        # "format" (i.e. upper vs. lowercase depending on
                        # provider).
                        symstr=symbol,
                        loglevel=loglevel,
                        start_stream=start_stream,
                    )
                )
                # TODO: we can remove this?
                # assert isinstance(bus.feeds[symbol], tuple)

        # XXX: ``.first_quote`` may be outdated here if this is secondary
        # subscriber
        flume = bus.feeds[symbol]
        sym = flume.symbol
        bs_mktid = sym.key
        fqsn = sym.fqsn  # true fqsn
        assert bs_mktid in fqsn and brokername in fqsn

        if sym.suffix:
            bs_mktid = fqsn.removesuffix(f'.{brokername}')
            log.warning(f'{brokername} expanded symbol {symbol} -> {bs_mktid}')

        # pack for ``.started()`` sync msg
        flumes[fqsn] = flume

        # we use the broker-specific market id (bs_mktid) for the
        # sampler subscription since the backend isn't (yet) expected to
        # append it's own name to the fqsn, so we filter on keys which
        # *do not* include that name (e.g .ib) .
        bus._subscribers.setdefault(bs_mktid, set())

    # sync feed subscribers with flume handles
    await ctx.started(
        {fqsn: flume.to_msg() for fqsn, flume in flumes.items()}
    )

    if not start_stream:
        log.warning(f'Not opening real-time stream for {fqsn}')
        await trio.sleep_forever()

    # real-time stream loop
    async with (
        ctx.open_stream() as stream,
    ):

        local_subs: dict[str, set[tuple]] = {}
        for fqsn, flume in flumes.items():
            # re-send to trigger display loop cycle (necessary especially
            # when the mkt is closed and no real-time messages are
            # expected).
            await stream.send({fqsn: flume.first_quote})

            # set a common msg stream for all requested symbols
            assert stream
            flume.stream = stream

            # Add a real-time quote subscription to feed bus:
            # This ``sub`` subscriber entry is added to the feed bus set so
            # that the ``sample_and_broadcast()`` task (spawned inside
            # ``allocate_persistent_feed()``) will push real-time quote
            # (ticks) to this new consumer.

            if tick_throttle:
                flume.throttle_rate = tick_throttle

                # open a bg task which receives quotes over a mem chan
                # and only pushes them to the target actor-consumer at
                # a max ``tick_throttle`` instantaneous rate.
                send, recv = trio.open_memory_channel(2**10)

                ctx._backpressure = False
                cs = await bus.start_task(
                    uniform_rate_send,
                    tick_throttle,
                    recv,
                    stream,
                )
                # NOTE: so the ``send`` channel here is actually a swapped
                # in trio mem chan which gets pushed by the normal sampler
                # task but instead of being sent directly over the IPC msg
                # stream it's the throttle task does the work of
                # incrementally forwarding to the IPC stream at the throttle
                # rate.
                send._ctx = ctx  # mock internal ``tractor.MsgStream`` ref
                sub = (send, tick_throttle)

            else:
                sub = (stream, tick_throttle)

            # TODO: add an api for this on the bus?
            # maybe use the current task-id to key the sub list that's
            # added / removed? Or maybe we can add a general
            # pause-resume by sub-key api?
            bs_mktid = fqsn.removesuffix(f'.{brokername}')
            local_subs.setdefault(bs_mktid, set()).add(sub)
            bus.add_subs(bs_mktid, {sub})

        # sync caller with all subs registered state
        sub_registered.set()

        uid = ctx.chan.uid
        try:
            # ctrl protocol for start/stop of quote streams based on UI
            # state (eg. don't need a stream when a symbol isn't being
            # displayed).
            async for msg in stream:

                if msg == 'pause':
                    for bs_mktid, subs in local_subs.items():
                        log.info(
                            f'Pausing {bs_mktid} feed for {uid}')
                        bus.remove_subs(bs_mktid, subs)

                elif msg == 'resume':
                    for bs_mktid, subs in local_subs.items():
                        log.info(
                            f'Resuming {bs_mktid} feed for {uid}')
                        bus.add_subs(bs_mktid, subs)

                else:
                    raise ValueError(msg)
        finally:
            log.info(
                f'Stopping {symbol}.{brokername} feed for {ctx.chan.uid}')

            if tick_throttle:
                # TODO: a one-cancels-one nursery
                # n.cancel_scope.cancel()
                cs.cancel()

            # drop all subs for this task from the bus
            for bs_mktid, subs in local_subs.items():
                bus.remove_subs(bs_mktid, subs)


class Feed(Struct):
    '''
    A per-provider API for client-side consumption from real-time data
    (streaming) sources, normally brokers and data services.

    This is a somewhat thin abstraction on top of
    a ``tractor.MsgStream`` plus associate share memory buffers which
    can be read in a readers-writer-lock style IPC configuration.

    Furhter, there is direct access to slower sampled historical data through
    similarly allocated shm arrays.

    '''
    mods: dict[str, ModuleType] = {}
    portals: dict[ModuleType, tractor.Portal] = {}
    flumes: dict[str, Flume] = {}
    streams: dict[
        str,
        trio.abc.ReceiveChannel[dict[str, Any]],
    ] = {}

    # used for UI to show remote state
    status: dict[str, Any] = {}

    @acm
    async def open_multi_stream(
        self,
        brokers: Sequence[str] | None = None,

    ) -> trio.abc.ReceiveChannel:

        if brokers is None:
            mods = self.mods
            brokers = list(self.mods)
        else:
            mods = {name: self.mods[name] for name in brokers}

        if len(mods) == 1:
            # just pass the brokerd stream directly if only one provider
            # was detected.
            stream = self.streams[list(brokers)[0]]
            async with stream.subscribe() as bstream:
                yield bstream
                return

        # start multiplexing task tree
        tx, rx = trio.open_memory_channel(616)

        async def relay_to_common_memchan(stream: tractor.MsgStream):
            async with tx:
                async for msg in stream:
                    await tx.send(msg)

        async with trio.open_nursery() as nurse:
            # spawn a relay task for each stream so that they all
            # multiplex to a common channel.
            for brokername in mods:
                stream = self.streams[brokername]
                nurse.start_soon(relay_to_common_memchan, stream)

            try:
                yield rx
            finally:
                nurse.cancel_scope.cancel()

    _max_sample_rate: int = 1

    # @property
    # def portal(self) -> tractor.Portal:
    #     return self._portal

    # @property
    # def name(self) -> str:
    #     return self.mod.name

    async def pause(self) -> None:
        for stream in set(self.streams.values()):
            await stream.send('pause')

    async def resume(self) -> None:
        for stream in set(self.streams.values()):
            await stream.send('resume')


@acm
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
                try:
                    return await stream.receive()
                except trio.EndOfChannel:
                    return {}

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


@acm
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

            async with gather_contexts(
                mngrs=[stream.subscribe() for stream in feed.streams.values()]
            ) as bstreams:
                for bstream, flume in zip(bstreams, feed.flumes.values()):
                    # XXX: TODO: horrible hackery that needs fixing..
                    # i guess we have to create context proxies?
                    bstream._ctx = flume.stream._ctx
                    flume.stream = bstream

                yield feed
        else:
            yield feed


@acm
async def open_feed(

    fqsns: list[str],

    loglevel: str | None = None,
    backpressure: bool = True,
    start_stream: bool = True,
    tick_throttle: float | None = None,  # Hz

) -> Feed:
    '''
    Open a "data feed" which provides streamed real-time quotes.

    '''
    providers: dict[ModuleType, list[str]] = {}
    feed = Feed()

    for fqsn in fqsns:
        brokername, key, suffix = unpack_fqme(fqsn)
        bfqsn = fqsn.replace('.' + brokername, '')

        try:
            mod = get_brokermod(brokername)
        except ImportError:
            mod = get_ingestormod(brokername)

        # built a per-provider map to instrument names
        providers.setdefault(mod, []).append(bfqsn)
        feed.mods[mod.name] = mod

    # one actor per brokerd for now
    brokerd_ctxs = []

    for brokermod, bfqsns in providers.items():

        # if no `brokerd` for this backend exists yet we spawn
        # a daemon actor for it.
        brokerd_ctxs.append(
            maybe_spawn_brokerd(
                brokermod.name,
                loglevel=loglevel
            )
        )

    portals: tuple[tractor.Portal]
    async with gather_contexts(
        brokerd_ctxs,
    ) as portals:

        bus_ctxs: list[AsyncContextManager] = []
        for (
            portal,
            (brokermod, bfqsns),
        ) in zip(portals, providers.items()):

            feed.portals[brokermod] = portal

            # fill out "status info" that the UI can show
            host, port = portal.channel.raddr
            if host == '127.0.0.1':
                host = 'localhost'

            feed.status.update({
                'actor_name': portal.channel.uid[0],
                'host': host,
                'port': port,
                'hist_shm': 'NA',
                'rt_shm': 'NA',
                'throttle_rate': tick_throttle,
            })
            # feed.status.update(init_msg.pop('status', {}))

            # (allocate and) connect to any feed bus for this broker
            bus_ctxs.append(
                portal.open_context(
                    open_feed_bus,
                    brokername=brokermod.name,
                    symbols=bfqsns,
                    loglevel=loglevel,
                    start_stream=start_stream,
                    tick_throttle=tick_throttle,
                )
            )

        assert len(feed.mods) == len(feed.portals)

        async with (
            gather_contexts(bus_ctxs) as ctxs,
        ):
            stream_ctxs = []
            for (
                (ctx, flumes_msg_dict),
                (brokermod, bfqsns),
            ) in zip(ctxs, providers.items()):

                # NOTE: do it asap to avoid overruns during multi-feed setup?
                ctx._backpressure = backpressure

                for fqsn, flume_msg in flumes_msg_dict.items():
                    flume = Flume.from_msg(flume_msg)
                    assert flume.symbol.fqsn == fqsn
                    feed.flumes[fqsn] = flume

                    # TODO: do we need this?
                    flume.feed = feed

                    # attach and cache shm handles
                    rt_shm = flume.rt_shm
                    assert rt_shm
                    hist_shm = flume.hist_shm
                    assert hist_shm

                    feed.status['hist_shm'] = (
                        f'{humanize(hist_shm._shm.size)}'
                    )
                    feed.status['rt_shm'] = f'{humanize(rt_shm._shm.size)}'

                stream_ctxs.append(
                    ctx.open_stream(
                        # XXX: be explicit about stream backpressure
                        # since we should **never** overrun on feeds
                        # being too fast, which will pretty much
                        # always happen with HFT XD
                        backpressure=backpressure,
                    )
                )

            async with (
                gather_contexts(stream_ctxs) as streams,
            ):
                for (
                    stream,
                    (brokermod, bfqsns),
                ) in zip(streams, providers.items()):

                    assert stream
                    feed.streams[brokermod.name] = stream

                    # apply `brokerd`-common steam to each flume
                    # tracking a symbol from that provider.
                    for fqsn, flume in feed.flumes.items():
                        if brokermod.name == flume.symbol.broker:
                            flume.stream = stream

                assert len(feed.mods) == len(feed.portals) == len(feed.streams)

                yield feed
