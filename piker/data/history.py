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

'''
Historical data business logic for load, backfill and tsdb storage.

'''
from __future__ import annotations
from collections import (
    Counter,
)
from datetime import datetime
from functools import partial
import time
from types import ModuleType
from typing import (
    Callable,
    TYPE_CHECKING,
)

import trio
from trio_typing import TaskStatus
import tractor
import pendulum
import numpy as np

from ..accounting import (
    MktPair,
)
from ._util import (
    log,
)
from ._sharedmem import (
    maybe_open_shm_array,
    ShmArray,
    _secs_in_day,
)
from ._source import def_iohlcv_fields
from ._sampling import (
    open_sample_stream,
)
from ..brokers._util import (
    DataUnavailable,
)

if TYPE_CHECKING:
    from ..service.marketstore import StorageClient
    from .feed import _FeedsBus


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
    mkt: MktPair,
    shm: ShmArray,
    timeframe: float,
    sampler_stream: tractor.MsgStream,
    feed_is_live: trio.Event,

    last_tsdb_dt: datetime | None = None,
    storage: StorageClient | None = None,
    write_tsdb: bool = True,
    tsdb_is_up: bool = False,

    task_status: TaskStatus[tuple] = trio.TASK_STATUS_IGNORED,

) -> int:

    hist: Callable[
        [int, datetime, datetime],
        tuple[np.ndarray, str]
    ]
    config: dict[str, int]

    async with mod.open_history_client(
        mkt,
    ) as (hist, config):
        log.info(f'{mod} history client returned backfill config: {config}')

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
                    f'STEP ERROR `{mkt.fqme}` for period {step_size_s}s:\n'
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
        # we need to only broadcast to subscribers for this fqme..
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
                    f"{mkt.fqme}: skipping duplicate frame @ {next_start_dt}"
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

                if mkt.dst.atype not in {'crypto', 'crypto_currency'}:
                    # for now, our table key schema is not including
                    # the dst[/src] source asset token.
                    col_sym_key: str = mkt.get_fqme(
                        delim_char='',
                        without_src=True,
                    )
                else:
                    col_sym_key: str = mkt.get_fqme(delim_char='')

                await storage.write_ohlcv(
                    col_sym_key,
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
    mkt: MktPair,
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
                    mkt,
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
    storemod: ModuleType,
    bus: _FeedsBus,
    storage: StorageClient,
    mkt: MktPair,
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
    fqme: str = mkt.fqme

    # start history anal and load missing new data via backend.
    timeframe: int
    for timeframe, shm in shms.items():
        # loads a (large) frame of data from the tsdb depending
        # on the db's query size limit.
        tsdb_history, first_tsdb_dt, last_tsdb_dt = await storage.load(
            fqme,
            timeframe=timeframe,
        )

        try:
            (
                latest_start_dt,
                latest_end_dt,
                bf_done,
            ) = await bus.nursery.start(
                partial(
                    start_backfill,
                    mod,
                    mkt,
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
            # above from ``StorageClient.load()``.
            to_push = tsdb_history[-prepend_start:]
            shm.push(
                to_push,

                # insert the history pre a "days worth" of samples
                # to leave some real-time buffer space at the end.
                prepend=True,
                # update_first=False,
                # start=prepend_start,
                field_map=storemod.ohlc_key_map,
            )

            tsdb_last_frame_start = tsdb_history['Epoch'][0]

            if timeframe == 1:
                times = shm.array['time']
                assert (times[1] - times[0]) == 1

            # load as much from storage into shm possible (depends on
            # user's shm size settings).
            while shm._first.value > 0:

                tsdb_history = await storage.read_ohlcv(
                    fqme,
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
                    field_map=storemod.ohlc_key_map,
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
    mkt: MktPair,
    some_data_ready: trio.Event,
    feed_is_live: trio.Event,
    timeframe: float = 60,  # in seconds

    task_status: TaskStatus[
        tuple[ShmArray, ShmArray]
    ] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Load and manage historical data including the loading of any
    available series from any connected tsdb as well as conduct
    real-time update of both that existing db and the allocated shared
    memory buffer.

    '''
    # TODO: is there a way to make each shm file key
    # actor-tree-discovery-addr unique so we avoid collisions
    # when doing tests which also allocate shms for certain instruments
    # that may be in use on the system by some other running daemons?
    # from tractor._state import _runtime_vars
    # port = _runtime_vars['_root_mailbox'][1]

    uid = tractor.current_actor().uid
    name, uuid = uid
    service = name.rstrip(f'.{mod.name}')

    fqme: str = mkt.get_fqme(delim_char='')

    # (maybe) allocate shm array for this broker/symbol which will
    # be used for fast near-term history capture and processing.
    hist_shm, opened = maybe_open_shm_array(
        key=f'piker.{service}[{uuid[:16]}].{fqme}.hist',

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', def_iohlcv_fields),

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
        key=f'piker.{service}[{uuid[:16]}].{fqme}.rt',

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', def_iohlcv_fields),

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

        open_history_client = getattr(
            mod,
            'open_history_client',
            None,
        )
        assert open_history_client
        from .. import storage
        try:
            async with storage.open_storage_client() as (storemod, client):
                log.info(f'Found existing `{storemod.name}`')
                # TODO: drop returning the output that we pass in?
                await bus.nursery.start(
                    tsdb_backfill,
                    mod,
                    storemod,
                    bus,
                    client,
                    mkt,
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

        except storage.StorageConnectionError:
            log.exception(
                "Can't connect to tsdb backend!?\n"
                'Starting basic backfille to shm..'
            )
            await basic_backfill(
                bus,
                mod,
                mkt,
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
