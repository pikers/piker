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
Data feed apis and infra.

This module is enabled for ``brokerd`` daemons and includes mostly
endpoints and middleware to support our real-time, provider agnostic,
live market quotes layer. Historical data loading and processing is also
initiated in parts of the feed bus startup but business logic and
functionality is generally located in the sibling `.data.history`
module.

'''
from __future__ import annotations
from collections import (
    defaultdict,
)
from contextlib import asynccontextmanager as acm
from functools import partial
import time
from types import ModuleType
from typing import (
    Any,
    AsyncContextManager,
    Optional,
    Awaitable,
    Sequence,
)

import trio
from trio.abc import ReceiveChannel
from trio_typing import TaskStatus
import tractor
from tractor.trionics import (
    maybe_open_context,
    gather_contexts,
)

from ..brokers import get_brokermod
from ..calc import humanize
from ._util import (
    log,
    get_console_log,
)
from ..service import (
    maybe_spawn_brokerd,
)
from .flows import Flume
from .validate import (
    FeedInit,
    validate_backend,
)
from .history import (
    manage_history,
)
from .ingest import get_ingestormod
from .types import Struct
from ..accounting import (
    MktPair,
    unpack_fqme,
)
from ..ui import _search
from ._sampling import (
    sample_and_broadcast,
    uniform_rate_send,
)


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
            tractor.MsgStream | trio.MemorySendChannel,
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
            float | None,  # tick throttle in Hz
        ]],
    ) -> set[tuple]:
        '''
        Add a ``set`` of consumer subscription entries for the given key.

        '''
        _subs: set[tuple] = self._subscribers[key]
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
        _subs: set[tuple] = self.get_subs(key)
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


async def allocate_persistent_feed(
    bus: _FeedsBus,
    sub_registered: trio.Event,

    brokername: str,
    symstr: str,

    loglevel: str,
    start_stream: bool = True,
    init_timeout: float = 616,

    task_status: TaskStatus[FeedInit] = trio.TASK_STATUS_IGNORED,

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

    # establish broker backend quote stream by calling
    # ``stream_quotes()``, a required broker backend endpoint.
    init_msgs: (
        list[FeedInit]  # new
        | dict[str, dict[str, str]]  # legacy / deprecated
    )

    # TODO: probably make a struct msg type for this as well
    # since eventually we do want to have more efficient IPC..
    first_quote: dict[str, Any]
    with trio.fail_after(init_timeout):
        (
            init_msgs,
            first_quote,
        ) = await bus.nursery.start(
            partial(
                mod.stream_quotes,
                send_chan=send,
                feed_is_live=feed_is_live,

                # NOTE / TODO: eventualy we may support providing more then
                # one input here such that a datad daemon can multiplex
                # multiple live feeds from one task, instead of getting
                # a new request (and thus new task) for each subscription.
                symbols=[symstr],
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
    init: FeedInit = validate_backend(
        mod,
        [symstr],
        init_msgs,  # NOTE: only 1 should be delivered for now..
    )
    mkt: MktPair = init.mkt_info
    fqme: str = mkt.fqme

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
        mkt,
        some_data_ready,
        feed_is_live,
    )

    # yield back control to starting nursery once we receive either
    # some history or a real-time quote.
    log.info(f'loading OHLCV history: {fqme}')
    await some_data_ready.wait()

    flume = Flume(

        # TODO: we have to use this for now since currently the
        # MktPair above doesn't render the correct output key it seems
        # when we provide the `MktInfo` here?..?
        mkt=mkt,

        first_quote=first_quote,
        _rt_shm_token=rt_shm.token,
        _hist_shm_token=hist_shm.token,
        izero_hist=izero_hist,
        izero_rt=izero_rt,

        # NOTE: some instruments don't have this provided,
        # eg. commodities and forex from ib.
        _has_vlm=init.shm_write_opts['has_vlm'],
    )

    # for ambiguous names we simply register the
    # flume for all possible name (sub) sets.
    # feed to that name (for now).
    bus.feeds.update({
        symstr: flume,
        fqme: flume,
        mkt.bs_fqme: flume,
    })

    # signal the ``open_feed_bus()`` caller task to continue since
    # we now have (some) history pushed to the shm buffer.
    task_status.started(init)

    if not start_stream:
        await trio.sleep_forever()

    # begin real-time updates of shm and tsb once the feed goes live and
    # the backend will indicate when real-time quotes have begun.
    await feed_is_live.wait()

    # NOTE: if not configured otherwise, we always sum tick volume
    # values in the OHLCV sampler.
    sum_tick_vlm: bool = True
    if init.shm_write_opts:
        sum_tick_vlm: bool = init.shm_write_opts.get(
            'sum_tick_vlm',
            True,
        )

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
        raise RuntimeError(f'History (1m) Shm for {fqme} is empty!?')

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
    symbols: list[str],  # normally expected to the broker-specific fqme

    loglevel: str = 'error',
    tick_throttle: Optional[float] = None,
    start_stream: bool = True,

) -> dict[
    str,  # fqme
    tuple[dict, dict]  # pair of dicts of the initmsg and first quotes
]:
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
    sub_registered = trio.Event()

    flumes: dict[str, Flume] = {}

    for symbol in symbols:

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
        flume: Flume = bus.feeds[symbol]
        mkt: MktPair = flume.mkt
        bs_fqme: str = mkt.bs_fqme
        fqme: str = mkt.fqme
        assert brokername in fqme

        if mkt.suffix:
            log.warning(f'{brokername} expanded symbol {symbol} -> {bs_fqme}')

        # pack for ``.started()`` sync msg
        flumes[fqme] = flume

        # we use the broker-specific fqme (bs_fqme) for the
        # sampler subscription since the backend isn't (yet) expected to
        # append it's own name to the fqme, so we filter on keys which
        # *do not* include that name (e.g .ib) .
        bus._subscribers.setdefault(bs_fqme, set())

    # sync feed subscribers with flume handles
    await ctx.started(
        {fqme: flume.to_msg()
         for fqme, flume in flumes.items()}
    )

    if not start_stream:
        log.warning(f'Not opening real-time stream for {fqme}')
        await trio.sleep_forever()

    # real-time stream loop
    async with (
        ctx.open_stream(
            # NOTE we allow this since it's common to have the live
            # quote feed actor's sampling task push faster then the
            # the local UI-graphics code during startup.
            # allow_overruns=True,
        ) as stream,
    ):

        local_subs: dict[str, set[tuple]] = {}
        for fqme, flume in flumes.items():
            # re-send to trigger display loop cycle (necessary especially
            # when the mkt is closed and no real-time messages are
            # expected).
            await stream.send({fqme: flume.first_quote})

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
            bs_fqme = fqme.removesuffix(f'.{brokername}')
            local_subs.setdefault(bs_fqme, set()).add(sub)
            bus.add_subs(bs_fqme, {sub})

        # sync caller with all subs registered state
        sub_registered.set()

        uid = ctx.chan.uid
        try:
            # ctrl protocol for start/stop of quote streams based on UI
            # state (eg. don't need a stream when a symbol isn't being
            # displayed).
            async for msg in stream:

                if msg == 'pause':
                    for bs_fqme, subs in local_subs.items():
                        log.info(
                            f'Pausing {bs_fqme} feed for {uid}')
                        bus.remove_subs(bs_fqme, subs)

                elif msg == 'resume':
                    for bs_fqme, subs in local_subs.items():
                        log.info(
                            f'Resuming {bs_fqme} feed for {uid}')
                        bus.add_subs(bs_fqme, subs)

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
            for bs_fqme, subs in local_subs.items():
                bus.remove_subs(bs_fqme, subs)


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
        '''
        Open steams to multiple data providers (``brokers``) and
        multiplex their msgs onto a common mem chan for
        only-requires-a-single-thread style consumption.

        '''
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
    ) as (ctx, _):

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

    fqmes: list[str],
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
    fqme = fqmes[0]

    async with maybe_open_context(
        acm_func=open_feed,
        kwargs={
            'fqmes': fqmes,
            'loglevel': loglevel,
            'tick_throttle': kwargs.get('tick_throttle'),

            # XXX: super critical to have bool defaults here XD
            'allow_overruns': kwargs.get('allow_overruns', True),
            'start_stream': kwargs.get('start_stream', True),
        },
        key=fqme,

    ) as (cache_hit, feed):

        if cache_hit:
            log.info(f'Using cached feed for {fqme}')
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

    fqmes: list[str],

    loglevel: str | None = None,
    allow_overruns: bool = True,
    start_stream: bool = True,
    tick_throttle: float | None = None,  # Hz

) -> Feed:
    '''
    Open a "data feed" which provides streamed real-time quotes.

    '''
    providers: dict[ModuleType, list[str]] = {}
    feed = Feed()

    for fqme in fqmes:
        brokername, *_ = unpack_fqme(fqme)
        bs_fqme = fqme.replace('.' + brokername, '')

        try:
            mod = get_brokermod(brokername)
        except ImportError:
            mod = get_ingestormod(brokername)

        # built a per-provider map to instrument names
        providers.setdefault(mod, []).append(bs_fqme)
        feed.mods[mod.name] = mod

    # one actor per brokerd for now
    brokerd_ctxs = []

    for brokermod, bfqmes in providers.items():

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
            (brokermod, bfqmes),
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
                    symbols=bfqmes,
                    loglevel=loglevel,
                    start_stream=start_stream,
                    tick_throttle=tick_throttle,

                    # XXX: super important to avoid
                    # the brokerd from some other
                    # backend overruning the task here
                    # bc some other brokerd took longer
                    # to startup before we hit the `.open_stream()`
                    # loop below XD .. really we should try to do each
                    # of these stream open sequences sequentially per
                    # backend? .. need some thot!
                    allow_overruns=True,
                )
            )

        assert len(feed.mods) == len(feed.portals)

        async with (
            gather_contexts(bus_ctxs) as ctxs,
        ):
            stream_ctxs: list[tractor.MsgStream] = []
            for (
                (ctx, flumes_msg_dict),
                (brokermod, bfqmes),
            ) in zip(ctxs, providers.items()):

                for fqme, flume_msg in flumes_msg_dict.items():
                    flume = Flume.from_msg(flume_msg)

                    # assert flume.mkt.fqme == fqme
                    feed.flumes[fqme] = flume

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
                        # XXX: be explicit about stream overruns
                        # since we should **never** overrun on feeds
                        # being too fast, which will pretty much
                        # always happen with HFT XD
                        allow_overruns=allow_overruns,
                    )
                )

            stream: tractor.MsgStream
            brokermod: ModuleType
            fqmes: list[str]
            async with (
                gather_contexts(stream_ctxs) as streams,
            ):
                for (
                    stream,
                    (brokermod, bfqmes),
                ) in zip(streams, providers.items()):

                    assert stream
                    feed.streams[brokermod.name] = stream

                    # apply `brokerd`-common stream to each flume
                    # tracking a live market feed from that provider.
                    for fqme, flume in feed.flumes.items():
                        if brokermod.name == flume.mkt.broker:
                            flume.stream = stream

                assert len(feed.mods) == len(feed.portals) == len(feed.streams)

                yield feed
