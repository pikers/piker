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
# from collections import (
#     Counter,
# )
from datetime import datetime
from functools import partial
# import time
from types import ModuleType
from typing import (
    Callable,
    TYPE_CHECKING,
)

import trio
from trio_typing import TaskStatus
import tractor
from pendulum import (
    Duration,
    from_timestamp,
)
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
)
from ._source import def_iohlcv_fields
from ._sampling import (
    open_sample_stream,
)
from ..brokers._util import (
    DataUnavailable,
)

if TYPE_CHECKING:
    from bidict import bidict
    from ..service.marketstore import StorageClient
    from .feed import _FeedsBus


# `ShmArray` buffer sizing configuration:
_mins_in_day = int(60 * 24)
# how much is probably dependent on lifestyle
# but we reco a buncha times (but only on a
# run-every-other-day kinda week).
_secs_in_day = int(60 * _mins_in_day)
_days_in_week: int = 7

_days_worth: int = 3
_default_hist_size: int = 6 * 365 * _mins_in_day
_hist_buffer_start = int(
    _default_hist_size - round(7 * _mins_in_day)
)

_default_rt_size: int = _days_worth * _secs_in_day
# NOTE: start the append index in rt buffer such that 1 day's worth
# can be appenened before overrun.
_rt_buffer_start = int((_days_worth - 1) * _secs_in_day)


def diff_history(
    array: np.ndarray,
    append_until_dt: datetime | None = None,
    prepend_until_dt: datetime | None = None,

) -> np.ndarray:

    # no diffing with tsdb dt index possible..
    if (
        prepend_until_dt is None
        and append_until_dt is None
    ):
        return array

    times = array['time']

    if append_until_dt:
        return array[times < append_until_dt.timestamp()]
    else:
        return array[times >= prepend_until_dt.timestamp()]


async def shm_push_in_between(
    shm: ShmArray,
    to_push: np.ndarray,
    prepend_index: int,

    update_start_on_prepend: bool = False,

) -> int:
    shm.push(
        to_push,
        prepend=True,

        # XXX: only update the ._first index if no tsdb
        # segment was previously prepended by the
        # parent task.
        update_first=update_start_on_prepend,

        # XXX: only prepend from a manually calculated shm
        # index if there was already a tsdb history
        # segment prepended (since then the
        # ._first.value is going to be wayyy in the
        # past!)
        start=(
            prepend_index
            if not update_start_on_prepend
            else None
        ),
    )
    # XXX: extremely important, there can be no checkpoints
    # in the block above to avoid entering new ``frames``
    # values while we're pipelining the current ones to
    # memory...
    array = shm.array
    zeros = array[array['low'] == 0]

    # always backfill gaps with the earliest (price) datum's
    # value to avoid the y-ranger including zeros and completely
    # stretching the y-axis..
    if 0 < zeros.size:
        zeros[[
            'open',
            'high',
            'low',
            'close',
        ]] = shm._array[zeros['index'][0] - 1]['close']
        # await tractor.pause()


async def start_backfill(
    get_hist,
    mod: ModuleType,
    mkt: MktPair,
    shm: ShmArray,
    timeframe: float,

    backfill_from_shm_index: int,
    backfill_from_dt: datetime,

    sampler_stream: tractor.MsgStream,

    backfill_until_dt: datetime | None = None,
    storage: StorageClient | None = None,

    write_tsdb: bool = True,

    task_status: TaskStatus[tuple] = trio.TASK_STATUS_IGNORED,

) -> int:

        # let caller unblock and deliver latest history frame
        # and use to signal that backfilling the shm gap until
        # the tsdb end is complete!
        bf_done = trio.Event()
        task_status.started(bf_done)

        # based on the sample step size, maybe load a certain amount history
        update_start_on_prepend: bool = False
        if backfill_until_dt is None:

            # TODO: drop this right and just expose the backfill
            # limits inside a [storage] section in conf.toml?
            # when no tsdb "last datum" is provided, we just load
            # some near-term history.
            # periods = {
            #     1: {'days': 1},
            #     60: {'days': 14},
            # }

            # do a decently sized backfill and load it into storage.
            periods = {
                1: {'days': 6},
                60: {'years': 6},
            }
            period_duration: int = periods[timeframe]

            update_start_on_prepend = True

            # NOTE: manually set the "latest" datetime which we intend to
            # backfill history "until" so as to adhere to the history
            # settings above when the tsdb is detected as being empty.
            backfill_until_dt = backfill_from_dt.subtract(**period_duration)


        # TODO: can we drop this? without conc i don't think this
        # is necessary any more?
        # configure async query throttling
        # rate = config.get('rate', 1)
        # XXX: legacy from ``trimeter`` code but unsupported now.
        # erlangs = config.get('erlangs', 1)
        # avoid duplicate history frames with a set of datetime frame
        # starts and associated counts of how many duplicates we see
        # per time stamp.
        # starts: Counter[datetime] = Counter()

        # conduct "backward history gap filling" where we push to
        # the shm buffer until we have history back until the
        # latest entry loaded from the tsdb's table B)
        last_start_dt: datetime = backfill_from_dt
        next_prepend_index: int = backfill_from_shm_index

        while last_start_dt > backfill_until_dt:

            log.debug(
                f'Requesting {timeframe}s frame ending in {last_start_dt}'
            )

            try:
                (
                    array,
                    next_start_dt,
                    next_end_dt,
                ) = await get_hist(
                    timeframe,
                    end_dt=last_start_dt,
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

            # TODO: drop this? see todo above..
            # if (
            #     next_start_dt in starts
            #     and starts[next_start_dt] <= 6
            # ):
            #     start_dt = min(starts)
            #     log.warning(
            #         f"{mkt.fqme}: skipping duplicate frame @ {next_start_dt}"
            #     )
            #     starts[start_dt] += 1
            #     await tractor.pause()
            #     continue

            # elif starts[next_start_dt] > 6:
            #     log.warning(
            #         f'NO-MORE-DATA: backend {mod.name} before {next_start_dt}?'
            #     )
            #     return

            # # only update new start point if not-yet-seen
            # starts[next_start_dt] += 1

            assert array['time'][0] == next_start_dt.timestamp()

            diff = last_start_dt - next_start_dt
            frame_time_diff_s = diff.seconds

            # frame's worth of sample-period-steps, in seconds
            frame_size_s = len(array) * timeframe
            expected_frame_size_s = frame_size_s + timeframe
            if frame_time_diff_s > expected_frame_size_s:

                # XXX: query result includes a start point prior to our
                # expected "frame size" and thus is likely some kind of
                # history gap (eg. market closed period, outage, etc.)
                # so just report it to console for now.
                log.warning(
                    f'History frame ending @ {last_start_dt} appears to have a gap:\n'
                    f'{diff} ~= {frame_time_diff_s} seconds'
                )

            to_push = diff_history(
                array,
                prepend_until_dt=backfill_until_dt,
            )
            ln = len(to_push)
            if ln:
                log.info(f'{ln} bars for {next_start_dt} -> {last_start_dt}')

            else:
                log.warning(
                    '0 BARS TO PUSH after diff!?\n'
                    f'{next_start_dt} -> {last_start_dt}'
                )

            # bail gracefully on shm allocation overrun/full
            # condition
            try:
                await shm_push_in_between(
                    shm,
                    to_push,
                    prepend_index=next_prepend_index,
                    update_start_on_prepend=update_start_on_prepend,
                )
                await sampler_stream.send({
                    'broadcast_all': {
                        'backfilling': (mkt.fqme, timeframe),
                    },
                })

                # decrement next prepend point
                next_prepend_index = next_prepend_index - ln
                last_start_dt = next_start_dt

            except ValueError as ve:
                _ve = ve
                log.error(
                    f'Shm prepend OVERRUN on: {next_start_dt} -> {last_start_dt}?'
                )

                if next_prepend_index < ln:
                    log.warning(
                        f'Shm buffer can only hold {next_prepend_index} more rows..\n'
                        f'Appending those from recent {ln}-sized frame, no more!'
                    )

                to_push = to_push[-next_prepend_index + 1:]
                await shm_push_in_between(
                    shm,
                    to_push,
                    prepend_index=next_prepend_index,
                    update_start_on_prepend=update_start_on_prepend,
                )
                await sampler_stream.send({
                    'broadcast_all': {
                        'backfilling': (mkt.fqme, timeframe),
                    },
                })

                # can't push the entire frame? so
                # push only the amount that can fit..
                break

            log.info(
                f'Shm pushed {ln} frame:\n'
                f'{next_start_dt} -> {last_start_dt}'
            )

            # FINALLY, maybe write immediately to the tsdb backend for
            # long-term storage.
            if (
                storage is not None
                and write_tsdb
            ):
                log.info(
                    f'Writing {ln} frame to storage:\n'
                    f'{next_start_dt} -> {last_start_dt}'
                )

                # always drop the src asset token for
                # non-currency-pair like market types (for now)
                if mkt.dst.atype not in {
                    'crypto',
                    'crypto_currency',
                    'fiat',  # a "forex pair"
                }:
                    # for now, our table key schema is not including
                    # the dst[/src] source asset token.
                    col_sym_key: str = mkt.get_fqme(
                        delim_char='',
                        without_src=True,
                    )
                else:
                    col_sym_key: str = mkt.get_fqme(delim_char='')

                # TODO: implement parquet append!?
                await storage.write_ohlcv(
                    col_sym_key,
                    shm.array,
                    timeframe,
                )
        else:
            # finally filled gap
            log.info(
                f'Finished filling gap to tsdb start @ {backfill_until_dt}!'
            )
            # conduct tsdb timestamp gap detection and backfill any
            # seemingly missing sequence segments..
            # TODO: ideally these never exist but somehow it seems
            # sometimes we're writing zero-ed segments on certain
            # (teardown) cases?
            from ._timeseries import detect_null_time_gap

            gap_indices: tuple | None = detect_null_time_gap(shm)
            while gap_indices:
                (
                    istart,
                    start,
                    end,
                    iend,
                ) = gap_indices

                start_dt = from_timestamp(start)
                end_dt = from_timestamp(end)
                (
                    array,
                    next_start_dt,
                    next_end_dt,
                ) = await get_hist(
                    timeframe,
                    start_dt=start_dt,
                    end_dt=end_dt,
                )

                # XXX TODO: pretty sure if i plot tsla, btcusdt.binance
                # and mnq.cme.ib this causes a Qt crash XXDDD

                # make sure we don't overrun the buffer start
                len_to_push: int = min(iend, array.size)
                to_push: np.ndarray = array[-len_to_push:]
                await shm_push_in_between(
                    shm,
                    to_push,
                    prepend_index=iend,
                    update_start_on_prepend=False,
                )

                # TODO: UI side needs IPC event to update..
                # - make sure the UI actually always handles
                #  this update!
                # - remember that in the display side, only refersh this
                #   if the respective history is actually "in view".
                #   loop
                await sampler_stream.send({
                    'broadcast_all': {
                        'backfilling': (mkt.fqme, timeframe),
                    },
                })
                gap_indices: tuple | None = detect_null_time_gap(shm)

        # XXX: extremely important, there can be no checkpoints
        # in the block above to avoid entering new ``frames``
        # values while we're pipelining the current ones to
        # memory...
        # await sampler_stream.send('broadcast_all')

        # short-circuit (for now)
        bf_done.set()


async def back_load_from_tsdb(
    storemod: ModuleType,
    storage: StorageClient,

    fqme: str,

    tsdb_history: np.ndarray,

    last_tsdb_dt: datetime,
    latest_start_dt: datetime,
    latest_end_dt: datetime,

    bf_done: trio.Event,

    timeframe: int,
    shm: ShmArray,
):
    assert len(tsdb_history)

    # sync to backend history task's query/load completion
    # if bf_done:
    #     await bf_done.wait()

    # TODO: eventually it'd be nice to not require a shm array/buffer
    # to accomplish this.. maybe we can do some kind of tsdb direct to
    # graphics format eventually in a child-actor?
    if storemod.name == 'nativedb':
        return

        await tractor.pause()
        assert shm._first.value == 0

    array = shm.array

    # if timeframe == 1:
    #     times = shm.array['time']
    #     assert (times[1] - times[0]) == 1

    if len(array):
        shm_last_dt = from_timestamp(
            shm.array[0]['time']
        )
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

    time_key: str = 'time'
    if getattr(storemod, 'ohlc_key_map', False):
        keymap: bidict = storemod.ohlc_key_map
        time_key: str = keymap.inverse['time']

    # if (
    #     not len(tsdb_history)
    # ):
    #     return

    tsdb_last_frame_start: datetime = last_tsdb_dt
    # load as much from storage into shm possible (depends on
    # user's shm size settings).
    while shm._first.value > 0:

        tsdb_history = await storage.read_ohlcv(
            fqme,
            timeframe=timeframe,
            end=tsdb_last_frame_start,
        )

        # # empty query
        # if not len(tsdb_history):
        #     break

        next_start = tsdb_history[time_key][0]
        if next_start >= tsdb_last_frame_start:
            # no earlier data detected
            break

        else:
            tsdb_last_frame_start = next_start

        # TODO: see if there's faster multi-field reads:
        # https://numpy.org/doc/stable/user/basics.rec.html#accessing-multiple-fields
        # re-index  with a `time` and index field
        prepend_start = shm._first.value

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

        log.info(f'Loaded {to_push.shape} datums from storage')
        tsdb_last_frame_start = tsdb_history[time_key][0]

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
        # await sampler_stream.send('broadcast_all')


async def tsdb_backfill(
    mod: ModuleType,
    storemod: ModuleType,
    tn: trio.Nursery,

    storage: StorageClient,
    mkt: MktPair,
    shm: ShmArray,
    timeframe: float,

    sampler_stream: tractor.MsgStream,

    task_status: TaskStatus[
        tuple[ShmArray, ShmArray]
    ] = trio.TASK_STATUS_IGNORED,

) -> None:

    get_hist: Callable[
        [int, datetime, datetime],
        tuple[np.ndarray, str]
    ]
    config: dict[str, int]
    async with mod.open_history_client(
        mkt,
    ) as (get_hist, config):
        log.info(f'{mod} history client returned backfill config: {config}')

        # get latest query's worth of history all the way
        # back to what is recorded in the tsdb
        try:
            array, mr_start_dt, mr_end_dt = await get_hist(
                timeframe,
                end_dt=None,
            )

        # XXX: timeframe not supported for backend (since
        # above exception type), terminate immediately since
        # there's no backfilling possible.
        except DataUnavailable:
            task_status.started()
            return

        # NOTE: removed for now since it'll always break
        # on the first 60s of the venue open..
        # times: np.ndarray = array['time']
        # # sample period step size in seconds
        # step_size_s = (
        #     from_timestamp(times[-1])
        #     - from_timestamp(times[-2])
        # ).seconds

        # if step_size_s not in (1, 60):
        #     log.error(f'Last 2 sample period is off!? -> {step_size_s}')
        #     step_size_s = (
        #         from_timestamp(times[-2])
        #         - from_timestamp(times[-3])
        #     ).seconds

        # NOTE: on the first history, most recent history
        # frame we PREPEND from the current shm ._last index
        # and thus a gap between the earliest datum loaded here
        # and the latest loaded from the tsdb may exist!
        log.info(f'Pushing {array.size} to shm!')
        shm.push(
            array,
            prepend=True,  # append on first frame
        )
        backfill_gap_from_shm_index: int = shm._first.value + 1

        # tell parent task to continue
        task_status.started()

        # loads a (large) frame of data from the tsdb depending
        # on the db's query size limit; our "nativedb" (using
        # parquet) generally can load the entire history into mem
        # but if not then below the remaining history can be lazy
        # loaded?
        fqme: str = mkt.fqme
        tsdb_entry: tuple | None =  await storage.load(
            fqme,
            timeframe=timeframe,
        )

        last_tsdb_dt: datetime | None = None
        if tsdb_entry:
            (
                tsdb_history,
                first_tsdb_dt,
                last_tsdb_dt,
            ) = tsdb_entry

            # calc the index from which the tsdb data should be
            # prepended, presuming there is a gap between the
            # latest frame (loaded/read above) and the latest
            # sample loaded from the tsdb.
            backfill_diff: Duration =  mr_start_dt - last_tsdb_dt
            offset_s: float = backfill_diff.in_seconds()
            offset_samples: int = round(offset_s / timeframe)

            # TODO: see if there's faster multi-field reads:
            # https://numpy.org/doc/stable/user/basics.rec.html#accessing-multiple-fields
            # re-index  with a `time` and index field
            prepend_start = shm._first.value - offset_samples + 1

            # tsdb history is so far in the past we can't fit it in
            # shm buffer space so simply don't load it!
            if prepend_start > 0:
                to_push = tsdb_history[-prepend_start:]
                shm.push(
                    to_push,

                    # insert the history pre a "days worth" of samples
                    # to leave some real-time buffer space at the end.
                    prepend=True,
                    # update_first=False,
                    start=prepend_start,
                    field_map=storemod.ohlc_key_map,
                )

                log.info(f'Loaded {to_push.shape} datums from storage')

        # TODO: maybe start history anal and load missing "history
        # gaps" via backend..

        if timeframe not in (1, 60):
            raise ValueError(
                '`piker` only needs to support 1m and 1s sampling '
                'but ur api is trying to deliver a longer '
                f'timeframe of {timeframe} seconds..\n'
                'So yuh.. dun do dat brudder.'
            )
        # if there is a gap to backfill from the first
        # history frame until the last datum loaded from the tsdb
        # continue that now in the background
        bf_done = await tn.start(
            partial(
                start_backfill,
                get_hist,
                mod,
                mkt,
                shm,
                timeframe,

                backfill_from_shm_index=backfill_gap_from_shm_index,
                backfill_from_dt=mr_start_dt,

                sampler_stream=sampler_stream,

                backfill_until_dt=last_tsdb_dt,
                storage=storage,
            )
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

        # backload any further data from tsdb (concurrently per
        # timeframe) if not all data was able to be loaded (in memory)
        # from the ``StorageClient.load()`` call above.
        try:
            await trio.sleep_forever()
        finally:
            return

        # IF we need to continue backloading incrementall from the
        # tsdb client..
        tn.start_soon(
            back_load_from_tsdb,

            storemod,
            storage,
            fqme,

            tsdb_history,
            last_tsdb_dt,
            mr_start_dt,
            mr_end_dt,
            bf_done,

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
    real-time update of both that existing db and the allocated
    shared memory buffer.

    Init sequence:
    - allocate shm (numpy array) buffers for 60s & 1s sample rates
    - configure "zero index" for each buffer: the index where
      history will prepended *to* and new live data will be
      appened *from*.
    - open a ``.storage.StorageClient`` and load any existing tsdb
      history as well as (async) start a backfill task which loads
      missing (newer) history from the data provider backend:
      - tsdb history is loaded first and pushed to shm ASAP.
      - the backfill task loads the most recent history before
        unblocking its parent task, so that the `ShmArray._last` is
        up to date to allow the OHLC sampler to begin writing new
        samples as the correct buffer index once the provider feed
        engages.

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
        size=_default_hist_size,
        append_start_index=_hist_buffer_start,

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
        size=_default_rt_size,
        append_start_index=_rt_buffer_start,
        key=f'piker.{service}[{uuid[:16]}].{fqme}.rt',

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', def_iohlcv_fields),

        # we expect the sub-actor to write
        readonly=False,
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

    open_history_client = getattr(
        mod,
        'open_history_client',
        None,
    )
    assert open_history_client

    # TODO: maybe it should be a subpkg of `.data`?
    from piker import storage

    async with (
        storage.open_storage_client() as (storemod, client),
        trio.open_nursery() as tn,
    ):
        log.info(
            f'Connecting to storage backend `{storemod.name}`:\n'
            f'location: {client.address}\n'
            f'db cardinality: {client.cardinality}\n'
            # TODO: show backend config, eg:
            # - network settings
            # - storage size with compression
            # - number of loaded time series?
        )

        # NOTE: this call ONLY UNBLOCKS once the latest-most frame
        # (i.e. history just before the live feed latest datum) of
        # history has been loaded and written to the shm buffer:
        # - the backfiller task can write in reverse chronological
        #   to the shm and tsdb
        # - the tsdb data can be loaded immediately and the
        #   backfiller can do a single append from it's end datum and
        #   then prepends backward to that from the current time
        #   step.
        tf2mem: dict = {
            1: rt_shm,
            60: hist_shm,
        }
        async with open_sample_stream(
            period_s=1.,
            shms_by_period={
                1.: rt_shm.token,
                60.: hist_shm.token,
            },

            # NOTE: we want to only open a stream for doing
            # broadcasts on backfill operations, not receive the
            # sample index-stream (since there's no code in this
            # data feed layer that needs to consume it).
            open_index_stream=True,
            sub_for_broadcasts=False,

        ) as sample_stream:
            # register 1s and 1m buffers with the global incrementer task
            log.info(f'Connected to sampler stream: {sample_stream}')

            for timeframe in [60, 1]:
                await tn.start(
                    tsdb_backfill,
                    mod,
                    storemod,
                    tn,
                    # bus,
                    client,
                    mkt,
                    tf2mem[timeframe],
                    timeframe,

                    sample_stream,
                )

            # indicate to caller that feed can be delivered to
            # remote requesting client since we've loaded history
            # data that can be used.
            some_data_ready.set()

            # wait for a live feed before starting the sampler.
            await feed_is_live.wait()

            # yield back after client connect with filled shm
            task_status.started((
                hist_zero_index,
                hist_shm,
                rt_zero_index,
                rt_shm,
            ))

            # history retreival loop depending on user interaction
            # and thus a small RPC-prot for remotely controllinlg
            # what data is loaded for viewing.
            await trio.sleep_forever()
