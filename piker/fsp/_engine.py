# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship of pikers)

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
core task logic for processing chains

'''
from dataclasses import dataclass
from functools import partial
from typing import (
    AsyncIterator, Callable, Optional,
    Union,
)

import numpy as np
import pyqtgraph as pg
import trio
from trio_typing import TaskStatus
import tractor
from tractor.msg import NamespacePath

from ..log import get_logger, get_console_log
from .. import data
from ..data import attach_shm_array
from ..data.feed import Feed
from ..data._sharedmem import ShmArray
from ._api import Fsp
from ._api import _load_builtins

log = get_logger(__name__)


@dataclass
class TaskTracker:
    complete: trio.Event
    cs: trio.CancelScope


async def filter_quotes_by_sym(

    sym: str,
    quote_stream: tractor.MsgStream,

) -> AsyncIterator[dict]:
    '''
    Filter quote stream by target symbol.

    '''
    # TODO: make this the actual first quote from feed
    # XXX: this allows for a single iteration to run for history
    # processing without waiting on the real-time feed for a new quote
    yield {}

    async for quotes in quote_stream:
        quote = quotes.get(sym)
        if quote:
            yield quote


async def fsp_compute(

    ctx: tractor.Context,
    symbol: str,
    feed: Feed,
    quote_stream: trio.abc.ReceiveChannel,

    src: ShmArray,
    dst: ShmArray,

    func: Callable,

    attach_stream: bool = False,
    task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED,

) -> None:

    profiler = pg.debug.Profiler(
        delayed=False,
        disabled=True
    )

    out_stream = func(

        # TODO: do we even need this if we do the feed api right?
        # shouldn't a local stream do this before we get a handle
        # to the async iterable? it's that or we do some kinda
        # async itertools style?
        filter_quotes_by_sym(symbol, quote_stream),
        feed.shm,
    )

    # Conduct a single iteration of fsp with historical bars input
    # and get historical output
    history_output: Union[
        dict[str, np.ndarray],  # multi-output case
        np.ndarray,  # single output case
    ]
    history_output = await out_stream.__anext__()

    func_name = func.__name__
    profiler(f'{func_name} generated history')

    # build struct array with an 'index' field to push as history

    # TODO: push using a[['f0', 'f1', .., 'fn']] = .. syntax no?
    # if the output array is multi-field then push
    # each respective field.
    # await tractor.breakpoint()
    fields = getattr(dst.array.dtype, 'fields', None).copy()
    fields.pop('index')
    # TODO: nptyping here!
    history: Optional[np.ndarray] = None
    if fields and len(fields) > 1 and fields:
        if not isinstance(history_output, dict):
            raise ValueError(
                f'`{func_name}` is a multi-output FSP and should yield a '
                '`dict[str, np.ndarray]` for history'
            )

        for key in fields.keys():
            if key in history_output:
                output = history_output[key]

                if history is None:

                    if output is None:
                        length = len(src.array)
                    else:
                        length = len(output)

                    # using the first output, determine
                    # the length of the struct-array that
                    # will be pushed to shm.
                    history = np.zeros(
                        length,
                        dtype=dst.array.dtype
                    )

                if output is None:
                    continue

                history[key] = output

    # single-key output stream
    else:
        if not isinstance(history_output, np.ndarray):
            raise ValueError(
                f'`{func_name}` is a single output FSP and should yield an '
                '`np.ndarray` for history'
            )
        history = np.zeros(
            len(history_output),
            dtype=dst.array.dtype
        )
        history[func_name] = history_output

    # TODO: XXX:
    # THERE'S A BIG BUG HERE WITH THE `index` field since we're
    # prepending a copy of the first value a few times to make
    # sub-curves align with the parent bar chart.
    # This likely needs to be fixed either by,
    # - manually assigning the index and historical data
    #   seperately to the shm array (i.e. not using .push())
    # - developing some system on top of the shared mem array that
    #   is `index` aware such that historical data can be indexed
    #   relative to the true first datum? Not sure if this is sane
    #   for incremental compuations.
    first = dst._first.value = src._first.value

    # TODO: can we use this `start` flag instead of the manual
    # setting above?
    index = dst.push(history, start=first)

    profiler(f'{func_name} pushed history')
    profiler.finish()

    # TODO: UGH, what is the right way to do something like this?
    if not ctx._started_called:
        await ctx.started(index)

    # setup a respawn handle
    with trio.CancelScope() as cs:
        tracker = TaskTracker(trio.Event(), cs)
        task_status.started((tracker, index))
        profiler(f'{func_name} yield last index')

        # import time
        # last = time.time()

        try:
            # rt stream
            async with ctx.open_stream() as stream:
                async for processed in out_stream:

                    log.debug(f"{func_name}: {processed}")
                    key, output = processed
                    index = src.index
                    dst.array[-1][key] = output

                    # NOTE: for now we aren't streaming this to the consumer
                    # stream latest array index entry which basically just acts
                    # as trigger msg to tell the consumer to read from shm
                    if attach_stream:
                        await stream.send(index)

                    # period = time.time() - last
                    # hz = 1/period if period else float('nan')
                    # if hz > 60:
                    #     log.info(f'FSP quote too fast: {hz}')
                    # last = time.time()
        finally:
            tracker.complete.set()


@tractor.context
async def cascade(

    ctx: tractor.Context,

    # data feed key
    brokername: str,
    symbol: str,

    src_shm_token: dict,
    dst_shm_token: tuple[str, np.dtype],

    ns_path: NamespacePath,

    zero_on_step: bool = False,
    loglevel: Optional[str] = None,

) -> None:
    '''
    Chain streaming signal processors and deliver output to
    destination shm array buffer.

    '''
    profiler = pg.debug.Profiler(delayed=False, disabled=False)

    if loglevel:
        get_console_log(loglevel)

    src = attach_shm_array(token=src_shm_token)
    dst = attach_shm_array(readonly=False, token=dst_shm_token)

    reg = _load_builtins()
    lines = '\n'.join([f'{key.rpartition(":")[2]} => {key}' for key in reg])
    log.info(
        f'Registered FSP set:\n{lines}'
    )
    func: Fsp = reg.get(
        NamespacePath(ns_path)
    )

    if not func:
        # TODO: assume it's a func target path
        raise ValueError(f'Unknown fsp target: {ns_path}')

    # open a data feed stream with requested broker
    async with data.feed.maybe_open_feed(
        brokername,
        [symbol],

        # TODO throttle tick outputs from *this* daemon since
        # it'll emit tons of ticks due to the throttle only
        # limits quote arrival periods, so the consumer of *this*
        # needs to get throttled the ticks we generate.
        # tick_throttle=60,

    ) as (feed, quote_stream):

        profiler(f'{func}: feed up')

        assert src.token == feed.shm.token
        # last_len = new_len = len(src.array)

        func_name = func.__name__
        async with (
            trio.open_nursery() as n,
        ):

            fsp_target = partial(

                fsp_compute,
                ctx=ctx,
                symbol=symbol,
                feed=feed,
                quote_stream=quote_stream,

                # shm
                src=src,
                dst=dst,

                # func_name=func_name,
                func=func
            )

            tracker, index = await n.start(fsp_target)

            if zero_on_step:
                last = dst.array[-1:]
                zeroed = np.zeros(last.shape, dtype=last.dtype)

            profiler(f'{func_name}: fsp up')

            async def resync(tracker: TaskTracker) -> tuple[TaskTracker, int]:
                # TODO: adopt an incremental update engine/approach
                # where possible here eventually!
                log.warning(f're-syncing fsp {func_name} to source')
                tracker.cs.cancel()
                await tracker.complete.wait()
                return await n.start(fsp_target)

            def is_synced(
                src: ShmArray,
                dst: ShmArray
            ) -> tuple[bool, int, int]:
                '''Predicate to dertmine if a destination FSP
                output array is aligned to its source array.

                '''
                step_diff = src.index - dst.index
                len_diff = abs(len(src.array) - len(dst.array))
                return not (
                    # the source is likely backfilling and we must
                    # sync history calculations
                    len_diff > 2 or

                    # we aren't step synced to the source and may be
                    # leading/lagging by a step
                    step_diff > 1 or
                    step_diff < 0
                ), step_diff, len_diff

            async def poll_and_sync_to_step(

                tracker: TaskTracker,
                src: ShmArray,
                dst: ShmArray,

            ) -> tuple[TaskTracker, int]:

                synced, step_diff, _ = is_synced(src, dst)
                while not synced:
                    tracker, index = await resync(tracker)
                    synced, step_diff, _ = is_synced(src, dst)

                return tracker, step_diff

            s, step, ld = is_synced(src, dst)

            # Increment the underlying shared memory buffer on every
            # "increment" msg received from the underlying data feed.
            async with feed.index_stream() as stream:

                profiler(f'{func_name}: sample stream up')
                profiler.finish()

                async for msg in stream:

                    # respawn the compute task if the source
                    # array has been updated such that we compute
                    # new history from the (prepended) source.
                    synced, step_diff, _ = is_synced(src, dst)
                    if not synced:
                        tracker, step_diff = await poll_and_sync_to_step(
                            tracker,
                            src,
                            dst,
                        )

                        # skip adding a last bar since we should already
                        # be step alinged
                        if step_diff == 0:
                            continue

                    # read out last shm row, copy and write new row
                    array = dst.array

                    # some metrics like vlm should be reset
                    # to zero every step.
                    if zero_on_step:
                        last = zeroed
                    else:
                        last = array[-1:].copy()

                    dst.push(last)
