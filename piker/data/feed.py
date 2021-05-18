# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from functools import partial
from types import ModuleType
from typing import (
    Dict, Any, Sequence,
    AsyncIterator, Optional,
    List, Awaitable, Callable,
)

import trio
from trio_typing import TaskStatus
import tractor
from pydantic import BaseModel

from ..brokers import get_brokermod
from ..log import get_logger, get_console_log
from .._daemon import (
    maybe_spawn_brokerd,
)
from ._sharedmem import (
    maybe_open_shm_array,
    attach_shm_array,
    ShmArray,
)
from .ingest import get_ingestormod
from ._source import base_iohlc_dtype, Symbol
from ..ui import _search
from ._sampling import (
    _shms,
    _incrementers,
    increment_ohlc_buffer,
    iter_ohlc_periods,
    sample_and_broadcast,
)


log = get_logger(__name__)


class _FeedsBus(BaseModel):
    """Data feeds broadcaster and persistence management.

    This is a brokerd side api used to manager persistent real-time
    streams that can be allocated and left alive indefinitely.

    """
    brokername: str
    nursery: trio.Nursery
    feeds: Dict[str, trio.CancelScope] = {}

    task_lock: trio.StrictFIFOLock = trio.StrictFIFOLock()

    # XXX: so weird but, apparently without this being `._` private
    # pydantic will complain about private `tractor.Context` instance
    # vars (namely `._portal` and `._cancel_scope`) at import time.
    # Reported this bug:
    # https://github.com/samuelcolvin/pydantic/issues/2816
    _subscribers: Dict[str, List[tractor.Context]] = {}

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = False

    async def cancel_all(self) -> None:
        for sym, (cs, msg, quote) in self.feeds.items():
            log.debug(f'Cancelling cached feed for {self.brokername}:{sym}')
            cs.cancel()


_bus: _FeedsBus = None


def get_feed_bus(
    brokername: str,
    nursery: Optional[trio.Nursery] = None,
) -> _FeedsBus:
    """
    Retreive broker-daemon-local data feeds bus from process global
    scope. Serialize task access to lock.

    """

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
    brokername: str
) -> None:
    """Allocate a actor-wide service nursery in ``brokerd``
    such that feeds can be run in the background persistently by
    the broker backend as needed.

    """
    try:
        async with trio.open_nursery() as service_nursery:

            # assign a nursery to the feeds bus for spawning
            # background tasks from clients
            bus = get_feed_bus(brokername, service_nursery)

            # unblock caller
            await ctx.started()

            # we pin this task to keep the feeds manager active until the
            # parent actor decides to tear it down
            await trio.sleep_forever()
    finally:
        # TODO: this needs to be shielded?
        await bus.cancel_all()


async def allocate_persistent_feed(
    ctx: tractor.Context,
    bus: _FeedsBus,
    brokername: str,
    symbol: str,
    loglevel: str,
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
) -> None:

    try:
        mod = get_brokermod(brokername)
    except ImportError:
        mod = get_ingestormod(brokername)

    # allocate shm array for this broker/symbol
    # XXX: we should get an error here if one already exists

    shm, opened = maybe_open_shm_array(
        key=sym_to_shm_key(brokername, symbol),

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', base_iohlc_dtype),

        # we expect the sub-actor to write
        readonly=False,
    )

    # do history validation?
    # assert opened, f'Persistent shm for {symbol} was already open?!'
    # if not opened:
    #     raise RuntimeError("Persistent shm for sym was already open?!")

    send, quote_stream = trio.open_memory_channel(10)
    feed_is_live = trio.Event()

    # establish broker backend quote stream
    # ``stream_quotes()`` is a required backend func
    init_msg, first_quote = await bus.nursery.start(
        partial(
            mod.stream_quotes,
            send_chan=send,
            feed_is_live=feed_is_live,
            symbols=[symbol],
            shm=shm,
            loglevel=loglevel,
        )
    )

    init_msg[symbol]['shm_token'] = shm.token
    cs = bus.nursery.cancel_scope

    # TODO: make this into a composed type which also
    # contains the backfiller cs for individual super-based
    # resspawns when needed.
    bus.feeds[symbol] = (cs, init_msg, first_quote)

    if opened:

        # start history backfill task ``backfill_bars()`` is
        # a required backend func this must block until shm is
        # filled with first set of ohlc bars
        await bus.nursery.start(mod.backfill_bars, symbol, shm)

    times = shm.array['time']
    delay_s = times[-1] - times[times != times[-1]][-1]

    # pass OHLC sample rate in seconds
    init_msg[symbol]['sample_rate'] = delay_s

    # yield back control to starting nursery
    task_status.started((init_msg,  first_quote))

    await feed_is_live.wait()

    if opened:
        _shms.setdefault(delay_s, []).append(shm)

        # start shm incrementing for OHLC sampling
        if _incrementers.get(delay_s) is None:
            cs = await bus.nursery.start(increment_ohlc_buffer, delay_s)

    sum_tick_vlm: bool = init_msg.get(
        'shm_write_opts', {}
    ).get('sum_tick_vlm', True)

    # start sample loop
    await sample_and_broadcast(bus, shm, quote_stream, sum_tick_vlm)


@tractor.stream
async def attach_feed_bus(

    ctx: tractor.Context,
    brokername: str,
    symbol: str,
    loglevel: str,
) -> None:

    # try:
    if loglevel is None:
        loglevel = tractor.current_actor().loglevel

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    # ensure we are who we think we are
    assert 'brokerd' in tractor.current_actor().name

    bus = get_feed_bus(brokername)

    async with bus.task_lock:
        task_cs = bus.feeds.get(symbol)
        sub_only: bool = False

        # if no cached feed for this symbol has been created for this
        # brokerd yet, start persistent stream and shm writer task in
        # service nursery
        if task_cs is None:
            init_msg, first_quote = await bus.nursery.start(
                partial(
                    allocate_persistent_feed,
                    ctx=ctx,
                    bus=bus,
                    brokername=brokername,
                    symbol=symbol,
                    loglevel=loglevel,
                )
            )
            bus._subscribers.setdefault(symbol, []).append(ctx)
        else:
            sub_only = True

    # XXX: ``first_quote`` may be outdated here if this is secondary
    # subscriber
    cs, init_msg, first_quote = bus.feeds[symbol]

    # send this even to subscribers to existing feed?
    await ctx.send_yield(init_msg)

    # deliver a first quote asap
    await ctx.send_yield(first_quote)

    if sub_only:
        bus._subscribers[symbol].append(ctx)

    try:
        await trio.sleep_forever()
    finally:
        bus._subscribers[symbol].remove(ctx)


@dataclass
class Feed:
    """A data feed for client-side interaction with far-process# }}}
    real-time data sources.

    This is an thin abstraction on top of ``tractor``'s portals for
    interacting with IPC streams and conducting automatic
    memory buffer orchestration.
    """
    name: str
    stream: AsyncIterator[Dict[str, Any]]
    shm: ShmArray
    mod: ModuleType

    _brokerd_portal: tractor._portal.Portal
    _index_stream: Optional[AsyncIterator[int]] = None
    _trade_stream: Optional[AsyncIterator[Dict[str, Any]]] = None
    _max_sample_rate: int = 0

    search: Callable[..., Awaitable] = None

    # cache of symbol info messages received as first message when
    # a stream startsc.
    symbols: Dict[str, Symbol] = field(default_factory=dict)

    async def receive(self) -> dict:
        return await self.stream.__anext__()

    @asynccontextmanager
    async def index_stream(
        self,
        delay_s: Optional[int] = None

    ) -> AsyncIterator[int]:

        if not self._index_stream:
            # XXX: this should be singleton on a host,
            # a lone broker-daemon per provider should be
            # created for all practical purposes
            async with self._brokerd_portal.open_stream_from(
                iter_ohlc_periods,
                delay_s=delay_s or self._max_sample_rate,
            ) as self._index_stream:

                yield self._index_stream
        else:
            yield self._index_stream

    @asynccontextmanager
    async def receive_trades_data(self) -> AsyncIterator[dict]:

        if not getattr(self.mod, 'stream_trades', False):
            log.warning(
                f"{self.mod.name} doesn't have trade data support yet :(")

            if not self._trade_stream:
                raise RuntimeError(
                    f'Can not stream trade data from {self.mod.name}')

        # NOTE: this can be faked by setting a rx chan
        # using the ``_.set_fake_trades_stream()`` method
        if self._trade_stream is None:

            async with self._brokerd_portal.open_stream_from(

                self.mod.stream_trades,

                # do we need this? -> yes
                # the broker side must declare this key
                # in messages, though we could probably use
                # more then one?
                topics=['local_trades'],
            ) as self._trade_stream:
                yield self._trade_stream
        else:

            yield self._trade_stream


def sym_to_shm_key(
    broker: str,
    symbol: str,
) -> str:
    return f'{broker}.{symbol}'


@asynccontextmanager
async def install_brokerd_search(
    portal: tractor._portal.Portal,
    brokermod: ModuleType,
) -> None:
    async with portal.open_context(
        brokermod.open_symbol_search
    ) as (ctx, cache):

        # shield here since we expect the search rpc to be
        # cancellable by the user as they see fit.
        async with ctx.open_stream() as stream:

            async def search(text: str) -> Dict[str, Any]:
                await stream.send(text)
                return await stream.receive()

            async with _search.register_symbol_search(
                provider_name=brokermod.name,
                search_routine=search,
                pause_period=brokermod._search_conf.get('pause_period'),

            ):
                yield


@asynccontextmanager
async def open_feed(
    brokername: str,
    symbols: Sequence[str],
    loglevel: Optional[str] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Open a "data feed" which provides streamed real-time quotes.

    """

    sym = symbols[0].lower()

    # TODO: feed cache locking, right now this is causing
    # issues when reconncting to a long running emsd?
    # global _searcher_cache

    # async with _cache_lock:
    #     feed = _searcher_cache.get((brokername, sym))

    #     # if feed is not None and sym in feed.symbols:
    #     if feed is not None:
    #         yield feed
    #         # short circuit
    #         return

    try:
        mod = get_brokermod(brokername)
    except ImportError:
        mod = get_ingestormod(brokername)

    # no feed for broker exists so maybe spawn a data brokerd
    async with maybe_spawn_brokerd(
        brokername,
        loglevel=loglevel
    ) as portal:

        async with portal.open_stream_from(

            attach_feed_bus,
            brokername=brokername,
            symbol=sym,
            loglevel=loglevel

        ) as stream:

            # TODO: can we make this work better with the proposed
            # context based bidirectional streaming style api proposed in:
            # https://github.com/goodboy/tractor/issues/53
            init_msg = await stream.receive()

            # we can only read from shm
            shm = attach_shm_array(
                token=init_msg[sym]['shm_token'],
                readonly=True,
            )

            feed = Feed(
                name=brokername,
                stream=stream,
                shm=shm,
                mod=mod,
                _brokerd_portal=portal,
            )
            ohlc_sample_rates = []

            for sym, data in init_msg.items():

                si = data['symbol_info']
                ohlc_sample_rates.append(data['sample_rate'])

                symbol = Symbol(
                    key=sym,
                    type_key=si.get('asset_type', 'forex'),
                    tick_size=si.get('price_tick_size', 0.01),
                    lot_tick_size=si.get('lot_tick_size', 0.0),
                )
                symbol.broker_info[brokername] = si

                feed.symbols[sym] = symbol

                # cast shm dtype to list... can't member why we need this
                shm_token = data['shm_token']
                shm_token['dtype_descr'] = list(shm_token['dtype_descr'])
                assert shm_token == shm.token  # sanity

            feed._max_sample_rate = max(ohlc_sample_rates)

            if brokername in _search._searcher_cache:
                yield feed

            else:
                async with install_brokerd_search(
                    feed._brokerd_portal,
                    mod,
                ):
                    yield feed
