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
Financial signal processing for the peeps.
"""
from functools import partial
from typing import AsyncIterator, Callable, Tuple

import trio
from trio_typing import TaskStatus
import tractor
import numpy as np

from ..log import get_logger
from .. import data
from ._momo import _rsi, _wma
from ._volume import _tina_vwap
from ..data import attach_shm_array
from ..data.feed import Feed
from ..data._sharedmem import ShmArray

log = get_logger(__name__)


_fsps = {
    'rsi': _rsi,
    'wma': _wma,
    'vwap': _tina_vwap,
}


async def latency(
    source: 'TickStream[Dict[str, float]]',  # noqa
    ohlcv: np.ndarray
) -> AsyncIterator[np.ndarray]:
    """Latency measurements, broker to piker.
    """
    # TODO: do we want to offer yielding this async
    # before the rt data connection comes up?

    # deliver zeros for all prior history
    yield np.zeros(len(ohlcv))

    async for quote in source:
        ts = quote.get('broker_ts')
        if ts:
            # This is codified in the per-broker normalization layer
            # TODO: Add more measure points and diffs for full system
            # stack tracing.
            value = quote['brokerd_ts'] - quote['broker_ts']
            yield value


async def fsp_compute(
    ctx: tractor.Context,
    symbol: str,
    feed: Feed,
    stream: trio.abc.ReceiveChannel,

    src: ShmArray,
    dst: ShmArray,

    fsp_func_name: str,
    func: Callable,

    task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED,

) -> None:

    # TODO: load appropriate fsp with input args

    async def filter_by_sym(
        sym: str,
        stream,
    ):

        # TODO: make this the actualy first quote from feed
        # XXX: this allows for a single iteration to run for history
        # processing without waiting on the real-time feed for a new quote
        yield {}

        # task cancellation won't kill the channel
        # since we shielded at the `open_feed()` call
        async for quotes in stream:
            for symbol, quotes in quotes.items():
                if symbol == sym:
                    yield quotes

    out_stream = func(
        filter_by_sym(symbol, stream),
        feed.shm,
    )

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
    dst._first.value = src._first.value
    dst._last.value = src._first.value

    # Conduct a single iteration of fsp with historical bars input
    # and get historical output
    history_output = await out_stream.__anext__()

    # build a struct array which includes an 'index' field to push
    # as history
    history = np.array(
        np.arange(len(history_output)),
        dtype=dst.array.dtype
    )
    history[fsp_func_name] = history_output

    # check for data length mis-allignment and fill missing values
    diff = len(src.array) - len(history)
    if diff >= 0:
        print(f"WTF DIFF SIGNAL to HISTORY {diff}")
        for _ in range(diff):
            dst.push(history[:1])

    # compare with source signal and time align
    index = dst.push(history)

    await ctx.send_yield(index)

    # setup a respawn handle
    with trio.CancelScope() as cs:
        task_status.started(cs)

        # rt stream
        async for processed in out_stream:
            log.debug(f"{fsp_func_name}: {processed}")
            index = src.index
            dst.array[-1][fsp_func_name] = processed

            # stream latest shm array index entry
            await ctx.send_yield(index)


@tractor.stream
async def cascade(
    ctx: tractor.Context,
    brokername: str,
    src_shm_token: dict,
    dst_shm_token: Tuple[str, np.dtype],
    symbol: str,
    fsp_func_name: str,

) -> None:
    """Chain streaming signal processors and deliver output to
    destination mem buf.

    """
    src = attach_shm_array(token=src_shm_token)
    dst = attach_shm_array(readonly=False, token=dst_shm_token)

    func: Callable = _fsps[fsp_func_name]

    # open a data feed stream with requested broker
    async with data.feed.maybe_open_feed(
        brokername,
        [symbol],
        shielded_stream=True,
    ) as (feed, stream):

        assert src.token == feed.shm.token

        last_len = new_len = len(src.array)

        fsp_target = partial(
            fsp_compute,
            ctx=ctx,
            symbol=symbol,
            feed=feed,
            stream=stream,

            src=src,
            dst=dst,

            fsp_func_name=fsp_func_name,
            func=func
        )

        async with trio.open_nursery() as n:

            cs = await n.start(fsp_target)

            # Increment the underlying shared memory buffer on every
            # "increment" msg received from the underlying data feed.

            async with feed.index_stream() as stream:
                async for msg in stream:

                    new_len = len(src.array)

                    if new_len > last_len + 1:
                        # respawn the signal compute task if the source
                        # signal has been updated
                        cs.cancel()
                        cs = await n.start(fsp_target)

                        # TODO: adopt an incremental update engine/approach
                        # where possible here eventually!

                    # read out last shm row
                    array = dst.array
                    last = array[-1:].copy()

                    # write new row to the shm buffer
                    dst.push(last)

                    last_len = new_len
