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
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from functools import partial
from typing import (
    AsyncIterator,
    Callable,
)

import numpy as np
import trio
from trio_typing import TaskStatus
import tractor
from tractor.msg import NamespacePath

from piker.types import Struct
from ..log import get_logger, get_console_log
from .. import data
from ..data.feed import (
    Flume,
    Feed,
)
from ..data._sharedmem import ShmArray
from ..data._sampling import (
    _default_delay_s,
    open_sample_stream,
)
from ..accounting import MktPair
from ._api import (
    Fsp,
    _load_builtins,
    _Token,
)
from ..toolz import Profiler

log = get_logger(__name__)


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

# TODO: unifying the abstractions in this FSP subsys/layer:
# -[ ] move the `.data.flows.Flume` type into this
#   module/subsys/pkg?
# -[ ] ideas for further abstractions as per
#   - https://github.com/pikers/piker/issues/216,
#   - https://github.com/pikers/piker/issues/270:
#   - a (financial signal) ``Flow`` would be the a "collection" of such
#     minmial cascades. Some engineering based jargon concepts:
#     - https://en.wikipedia.org/wiki/Signal_chain
#     - https://en.wikipedia.org/wiki/Daisy_chain_(electrical_engineering)
#     - https://en.wikipedia.org/wiki/Audio_signal_flow
#     - https://en.wikipedia.org/wiki/Digital_signal_processing#Implementation
#     - https://en.wikipedia.org/wiki/Dataflow_programming
#     - https://en.wikipedia.org/wiki/Signal_programming
#     - https://en.wikipedia.org/wiki/Incremental_computing
#     - https://en.wikipedia.org/wiki/Signal-flow_graph
#     - https://en.wikipedia.org/wiki/Signal-flow_graph#Basic_components

# -[ ] we probably want to eval THE BELOW design and unify with the
#   proto `TaskManager` in the `tractor` dev branch as well as with
#   our below idea for `Cascade`:
#   - https://github.com/goodboy/tractor/pull/363
class Cascade(Struct):
    '''
    As per sig-proc engineering parlance, this is a chaining of
    `Flume`s, which are themselves collections of "Streams"
    implemented currently via `ShmArray`s.

    A `Cascade` is be the minimal "connection" of 2 `Flumes`
    as per circuit parlance:
    https://en.wikipedia.org/wiki/Two-port_network#Cascade_connection

    TODO:
      -[ ] could cover the combination of our `FspAdmin` and the
        backend `.fsp._engine` related machinery to "connect" one flume
        to another?

    '''
    # TODO: make these `Flume`s
    src: Flume
    dst: Flume
    tn: trio.Nursery
    fsp: Fsp  # UI-side middleware ctl API

    # filled during cascade/.bind_func() (fsp_compute) init phases
    bind_func: Callable | None = None
    complete: trio.Event | None = None
    cs: trio.CancelScope | None = None
    client_stream: tractor.MsgStream | None = None

    async def resync(self) -> int:
        # TODO: adopt an incremental update engine/approach
        # where possible here eventually!
        log.info(f're-syncing fsp {self.fsp.name} to source')
        self.cs.cancel()
        await self.complete.wait()
        index: int = await self.tn.start(self.bind_func)

        # always trigger UI refresh after history update,
        # see ``piker.ui._fsp.FspAdmin.open_chain()`` and
        # ``piker.ui._display.trigger_update()``.
        dst_shm: ShmArray = self.dst.rt_shm
        await self.client_stream.send({
            'fsp_update': {
                'key': dst_shm.token,
                'first': dst_shm._first.value,
                'last': dst_shm._last.value,
            }
        })
        return index

    def is_synced(self) -> tuple[bool, int, int]:
        '''
        Predicate to dertmine if a destination FSP
        output array is aligned to its source array.

        '''
        src_shm: ShmArray = self.src.rt_shm
        dst_shm: ShmArray = self.dst.rt_shm
        step_diff = src_shm.index - dst_shm.index
        len_diff = abs(len(src_shm.array) - len(dst_shm.array))
        synced: bool = not (
            # the source is likely backfilling and we must
            # sync history calculations
            len_diff > 2

            # we aren't step synced to the source and may be
            # leading/lagging by a step
            or step_diff > 1
            or step_diff < 0
        )
        if not synced:
            fsp: Fsp = self.fsp
            log.warning(
                '***DESYNCED FSP***\n'
                f'{fsp.ns_path}@{src_shm.token}\n'
                f'step_diff: {step_diff}\n'
                f'len_diff: {len_diff}\n'
            )
        return (
            synced,
            step_diff,
            len_diff,
        )

    async def poll_and_sync_to_step(self) -> int:
        synced, step_diff, _ = self.is_synced()
        while not synced:
            await self.resync()
            synced, step_diff, _ = self.is_synced()

        return step_diff

    @acm
    async def open_edge(
        self,
        bind_func: Callable,
    ) -> int:
        self.bind_func = bind_func
        index = await self.tn.start(bind_func)
        yield index
        # TODO: what do we want on teardown/error?
        # -[ ] dynamic reconnection after update?


async def connect_streams(
    casc: Cascade,
    mkt: MktPair,
    quote_stream: trio.abc.ReceiveChannel,
    src: Flume,
    dst: Flume,

    edge_func: Callable,

    # attach_stream: bool = False,
    task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Stream and per-sample compute and write the cascade of
    2 `Flumes`/streams given some operating `func`.

    https://en.wikipedia.org/wiki/Signal-flow_graph#Basic_components

    Not literally, but something like:

        edge_func(Flume_in) -> Flume_out

    '''
    profiler = Profiler(
        delayed=False,
        disabled=True
    )

    # TODO: just pull it from src.mkt.fqme no?
    # fqme: str = mkt.fqme
    fqme: str = src.mkt.fqme

    # TODO: dynamic introspection of what the underlying (vertex)
    # function actually requires from input node (flumes) then
    # deliver those inputs as part of a graph "compilation" step?
    out_stream = edge_func(

        # TODO: do we even need this if we do the feed api right?
        # shouldn't a local stream do this before we get a handle
        # to the async iterable? it's that or we do some kinda
        # async itertools style?
        filter_quotes_by_sym(fqme, quote_stream),

        # XXX: currently the ``ohlcv`` arg, but we should allow
        # (dynamic) requests for src flume (node) streams?
        src.rt_shm,
    )

    # HISTORY COMPUTE PHASE
    # conduct a single iteration of fsp with historical bars input
    # and get historical output.
    history_output: (
        dict[str, np.ndarray]  # multi-output case
        | np.ndarray,  # single output case
    )
    history_output = await anext(out_stream)

    func_name = edge_func.__name__
    profiler(f'{func_name} generated history')

    # build struct array with an 'index' field to push as history

    # TODO: push using a[['f0', 'f1', .., 'fn']] = .. syntax no?
    # if the output array is multi-field then push
    # each respective field.
    dst_shm: ShmArray = dst.rt_shm
    fields = getattr(dst_shm.array.dtype, 'fields', None).copy()
    fields.pop('index')
    history_by_field: np.ndarray | None = None
    src_shm: ShmArray = src.rt_shm
    src_time = src_shm.array['time']

    if (
        fields and
        len(fields) > 1
    ):
        if not isinstance(history_output, dict):
            raise ValueError(
                f'`{func_name}` is a multi-output FSP and should yield a '
                '`dict[str, np.ndarray]` for history'
            )

        for key in fields.keys():
            if key in history_output:
                output = history_output[key]

                if history_by_field is None:

                    if output is None:
                        length = len(src_shm.array)
                    else:
                        length = len(output)

                    # using the first output, determine
                    # the length of the struct-array that
                    # will be pushed to shm.
                    history_by_field = np.zeros(
                        length,
                        dtype=dst_shm.array.dtype
                    )

                if output is None:
                    continue

                history_by_field[key] = output

    # single-key output stream
    else:
        if not isinstance(history_output, np.ndarray):
            raise ValueError(
                f'`{func_name}` is a single output FSP and should yield an '
                '`np.ndarray` for history'
            )
        history_by_field = np.zeros(
            len(history_output),
            dtype=dst_shm.array.dtype
        )
        history_by_field[func_name] = history_output

    history_by_field['time'] = src_time[-len(history_by_field):]

    history_output['time'] = src_shm.array['time']

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
    first = dst_shm._first.value = src_shm._first.value

    # TODO: can we use this `start` flag instead of the manual
    # setting above?
    index = dst_shm.push(
        history_by_field,
        start=first,
    )

    profiler(f'{func_name} pushed history')
    profiler.finish()

    # setup a respawn handle
    with trio.CancelScope() as cs:

        casc.cs = cs
        casc.complete = trio.Event()
        task_status.started(index)

        profiler(f'{func_name} yield last index')

        # import time
        # last = time.time()

        try:

            async for processed in out_stream:

                log.debug(f"{func_name}: {processed}")
                key, output = processed
                # dst.array[-1][key] = output
                dst_shm.array[[key, 'time']][-1] = (
                    output,
                    # TODO: what about pushing ``time.time_ns()``
                    # in which case we'll need to round at the graphics
                    # processing / sampling layer?
                    src_shm.array[-1]['time']
                )

                # NOTE: for now we aren't streaming this to the consumer
                # stream latest array index entry which basically just acts
                # as trigger msg to tell the consumer to read from shm
                # TODO: further this should likely be implemented much
                # like our `Feed` api where there is one background
                # "service" task which computes output and then sends to
                # N-consumers who subscribe for the real-time output,
                # which we'll likely want to implement using local-mem
                # chans for the fan out?
                # index = src_shm.index
                # if attach_stream:
                #     await client_stream.send(index)

                # period = time.time() - last
                # hz = 1/period if period else float('nan')
                # if hz > 60:
                #     log.info(f'FSP quote too fast: {hz}')
                # last = time.time()
        finally:
            casc.complete.set()


@tractor.context
async def cascade(

    ctx: tractor.Context,

    # data feed key
    fqme: str,

    # flume pair cascaded using an "edge function"
    src_flume_addr: dict,
    dst_flume_addr: dict,
    ns_path: NamespacePath,

    shm_registry: dict[str, _Token],

    zero_on_step: bool = False,
    loglevel: str | None = None,

) -> None:
    '''
    Chain streaming signal processors and deliver output to
    destination shm array buffer.

    '''
    profiler = Profiler(
        delayed=False,
        disabled=False
    )

    if loglevel:
        get_console_log(loglevel)

    src: Flume = Flume.from_msg(src_flume_addr)
    dst: Flume = Flume.from_msg(
        dst_flume_addr,
        readonly=False,
    )

    # src: ShmArray = attach_shm_array(token=src_shm_token)
    # dst: ShmArray = attach_shm_array(readonly=False, token=dst_shm_token)

    reg = _load_builtins()
    lines = '\n'.join([f'{key.rpartition(":")[2]} => {key}' for key in reg])
    log.info(
        f'Registered FSP set:\n{lines}'
    )

    # NOTE XXX: update actorlocal flows table which registers
    # readonly "instances" of this fsp for symbol/source so that
    # consumer fsps can look it up by source + fsp.
    # TODO: ugh i hate this wind/unwind to list over the wire but
    # not sure how else to do it.
    for (token, fsp_name, dst_token) in shm_registry:
        Fsp._flow_registry[(
            _Token.from_msg(token),
            fsp_name,
        )] = _Token.from_msg(dst_token), None

    fsp: Fsp = reg.get(
        NamespacePath(ns_path)
    )
    func: Callable = fsp.func

    if not func:
        # TODO: assume it's a func target path
        raise ValueError(f'Unknown fsp target: {ns_path}')

    _fqme: str = src.mkt.fqme
    assert _fqme == fqme

    # open a data feed stream with requested broker
    feed: Feed
    async with data.feed.maybe_open_feed(
        [fqme],

        # TODO throttle tick outputs from *this* daemon since
        # it'll emit tons of ticks due to the throttle only
        # limits quote arrival periods, so the consumer of *this*
        # needs to get throttled the ticks we generate.
        # tick_throttle=60,

    ) as feed:

        flume: Flume = feed.flumes[fqme]
        # XXX: can't do this since flume.feed will be set XD
        # assert flume == src
        assert flume.mkt == src.mkt
        mkt: MktPair = flume.mkt

        # NOTE: FOR NOW, sanity checks around the feed as being
        # always the src flume (until we get to fancier/lengthier
        # chains/graphs.
        assert src.rt_shm.token == flume.rt_shm.token

        # XXX: won't work bc the _hist_shm_token value will be
        # list[list] after IPC..
        # assert flume.to_msg() == src_flume_addr

        profiler(f'{func}: feed up')

        func_name: str = func.__name__
        async with (
            trio.open_nursery() as tn,
        ):
            # TODO: might be better to just make a "restart" method where
            # the target task is spawned implicitly and then the event is
            # set via some higher level api? At that poing we might as well
            # be writing a one-cancels-one nursery though right?
            casc = Cascade(
                src,
                dst,
                tn,
                fsp,
            )

            # TODO: this seems like it should be wrapped somewhere?
            fsp_target = partial(
                connect_streams,
                casc=casc,
                mkt=mkt,
                quote_stream=flume.stream,

                # flumes and shm passthrough
                src=src,
                dst=dst,

                # chain function which takes src flume input(s)
                # and renders dst flume output(s)
                edge_func=func
            )
            async with casc.open_edge(
                bind_func=fsp_target,
            ) as index:
                # casc.bind_func = fsp_target
                # index = await tn.start(fsp_target)
                dst_shm: ShmArray = dst.rt_shm
                src_shm: ShmArray = src.rt_shm

                if zero_on_step:
                    last = dst.rt_shm.array[-1:]
                    zeroed = np.zeros(last.shape, dtype=last.dtype)

                profiler(f'{func_name}: fsp up')

                # sync to client-side actor
                await ctx.started(index)

                # XXX:  rt stream with client which we MUST
                # open here (and keep it open) in order to make
                # incremental "updates" as history prepends take
                # place.
                async with ctx.open_stream() as client_stream:
                    casc.client_stream: tractor.MsgStream = client_stream

                    s, step, ld = casc.is_synced()

                    # detect sample period step for subscription to increment
                    # signal
                    times = src.rt_shm.array['time']
                    if len(times) > 1:
                        last_ts = times[-1]
                        delay_s: float = float(last_ts - times[times != last_ts][-1])
                    else:
                        # our default "HFT" sample rate.
                        delay_s: float = _default_delay_s

                    # sub and increment the underlying shared memory buffer
                    # on every step msg received from the global `samplerd`
                    # service.
                    async with open_sample_stream(
                        float(delay_s)
                    ) as istream:

                        profiler(f'{func_name}: sample stream up')
                        profiler.finish()

                        async for i in istream:
                            # print(f'FSP incrementing {i}')

                            # respawn the compute task if the source
                            # array has been updated such that we compute
                            # new history from the (prepended) source.
                            synced, step_diff, _ = casc.is_synced()
                            if not synced:
                                step_diff: int = await casc.poll_and_sync_to_step()

                                # skip adding a last bar since we should already
                                # be step alinged
                                if step_diff == 0:
                                    continue

                            # read out last shm row, copy and write new row
                            array = dst_shm.array

                            # some metrics like vlm should be reset
                            # to zero every step.
                            if zero_on_step:
                                last = zeroed
                            else:
                                last = array[-1:].copy()

                            dst.rt_shm.push(last)

                            # sync with source buffer's time step
                            src_l2 = src_shm.array[-2:]
                            src_li, src_lt = src_l2[-1][['index', 'time']]
                            src_2li, src_2lt = src_l2[-2][['index', 'time']]
                            dst_shm._array['time'][src_li] = src_lt
                            dst_shm._array['time'][src_2li] = src_2lt

                            # last2 = dst.array[-2:]
                            # if (
                            #     last2[-1]['index'] != src_li
                            #     or last2[-2]['index'] != src_2li
                            # ):
                            #     dstl2 = list(last2)
                            #     srcl2 = list(src_l2)
                            #     print(
                            #         # f'{dst.token}\n'
                            #         f'src: {srcl2}\n'
                            #         f'dst: {dstl2}\n'
                            #     )
