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

We provide tsdb integrations for retrieving
and storing data from your brokers as well as
sharing your feeds with other fellow pikers.

"""
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from functools import partial
from importlib import import_module
from types import ModuleType
from typing import (
    Dict, Any, Sequence,
    AsyncIterator, Optional,
    List
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
from ._normalize import iterticks
from ._sharedmem import (
    maybe_open_shm_array,
    attach_shm_array,
    open_shm_array,
    ShmArray,
    get_shm_token,
)
from ._source import base_iohlc_dtype, Symbol
from ._buffer import (
    _shms,
    _incrementers,
    increment_ohlc_buffer,
    iter_ohlc_periods,
)

__all__ = [
    'iterticks',
    'maybe_open_shm_array',
    'attach_shm_array',
    'open_shm_array',
    'get_shm_token',
    # 'subscribe_ohlc_for_increment',
]


log = get_logger(__name__)

__ingestors__ = [
    'marketstore',
]


def get_ingestormod(name: str) -> ModuleType:
    """Return the imported ingestor module by name.
    """
    module = import_module('.' + name, 'piker.data')
    # we only allow monkeying because it's for internal keying
    module.name = module.__name__.split('.')[-1]
    return module


class _FeedsBus(BaseModel):
    """Data feeds broadcaster and persistence management.

    This is a brokerd side api used to manager persistent real-time
    streams that can be allocated and left alive indefinitely.

    """
    brokername: str
    nursery: trio.Nursery
    feeds: Dict[str, trio.CancelScope] = {}
    subscribers: Dict[str, List[tractor.Context]] = {}
    task_lock: trio.StrictFIFOLock = trio.StrictFIFOLock()

    class Config:
        arbitrary_types_allowed = True

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


async def _setup_persistent_brokerd(brokername:  str) -> None:
    """Allocate a actor-wide service nursery in ``brokerd``
    such that feeds can be run in the background persistently by
    the broker backend as needed.

    """
    try:
        async with trio.open_nursery() as service_nursery:

            # assign a nursery to the feeds bus for spawning
            # background tasks from clients
            bus = get_feed_bus(brokername, service_nursery)

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
    assert opened, "Persistent shm for sym was already open?!"
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

    log.info("Started shared mem bar writer")

    # iterate stream delivered by broker
    async for quotes in quote_stream:
        for sym, quote in quotes.items():

            # TODO: in theory you can send the IPC msg *before*
            # writing to the sharedmem array to decrease latency,
            # however, that will require `tractor.msg.pub` support
            # here or at least some way to prevent task switching
            # at the yield such that the array write isn't delayed
            # while another consumer is serviced..

            # start writing the shm buffer with appropriate
            # trade data
            for tick in quote['ticks']:

                # if tick['type'] in ('utrade',):
                #     print(tick)

                # write trade events to shm last OHLC sample
                if tick['type'] in ('trade', 'utrade'):

                    last = tick['price']

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
                        'bar_wap',  # can be optionally provided
                        'volume',
                    ]][-1] = (
                        o,
                        max(high, last),
                        min(low, last),
                        last,
                        quote.get('bar_wap', 0),
                        volume,
                    )

            # XXX: we need to be very cautious here that no
            # context-channel is left lingering which doesn't have
            # a far end receiver actor-task. In such a case you can
            # end up triggering backpressure which which will
            # eventually block this producer end of the feed and
            # thus other consumers still attached.
            subs = bus.subscribers[sym]
            for ctx in subs:
                # print(f'sub is {ctx.chan.uid}')
                try:
                    await ctx.send_yield({sym: quote})
                except (
                    trio.BrokenResourceError,
                    trio.ClosedResourceError
                ):
                    subs.remove(ctx)
                    log.error(f'{ctx.chan.uid} dropped connection')


@tractor.stream
async def attach_feed_bus(
    ctx: tractor.Context,
    brokername: str,
    symbol: str,
    loglevel: str,
):

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
            bus.subscribers.setdefault(symbol, []).append(ctx)
        else:
            sub_only = True

    # XXX: ``first_quote`` may be outdated here if this is secondary
    # subscriber
    cs, init_msg, first_quote = bus.feeds[symbol]

    # send this even to subscribers to existing feed?
    await ctx.send_yield(init_msg)
    await ctx.send_yield(first_quote)

    if sub_only:
        bus.subscribers[symbol].append(ctx)

    try:
        await trio.sleep_forever()
    finally:
        bus.subscribers[symbol].remove(ctx)


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

    # cache of symbol info messages received as first message when
    # a stream startsc.
    symbols: Dict[str, Symbol] = field(default_factory=dict)

    async def receive(self) -> dict:
        return await self.stream.__anext__()

    async def index_stream(
        self,
        delay_s: Optional[int] = None

    ) -> AsyncIterator[int]:

        if not self._index_stream:
            # XXX: this should be singleton on a host,
            # a lone broker-daemon per provider should be
            # created for all practical purposes
            self._index_stream = await self._brokerd_portal.run(
                iter_ohlc_periods,
                delay_s=delay_s or self._max_sample_rate,
            )

        return self._index_stream

    async def recv_trades_data(self) -> AsyncIterator[dict]:

        if not getattr(self.mod, 'stream_trades', False):
            log.warning(
                f"{self.mod.name} doesn't have trade data support yet :(")

            if not self._trade_stream:
                raise RuntimeError(
                    f'Can not stream trade data from {self.mod.name}')

        # NOTE: this can be faked by setting a rx chan
        # using the ``_.set_fake_trades_stream()`` method
        if self._trade_stream is None:

            self._trade_stream = await self._brokerd_portal.run(

                self.mod.stream_trades,

                # do we need this? -> yes
                # the broker side must declare this key
                # in messages, though we could probably use
                # more then one?
                topics=['local_trades'],
            )

        return self._trade_stream


def sym_to_shm_key(
    broker: str,
    symbol: str,
) -> str:
    return f'{broker}.{symbol}'


@asynccontextmanager
async def open_feed(
    brokername: str,
    symbols: Sequence[str],
    loglevel: Optional[str] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Open a "data feed" which provides streamed real-time quotes.
    """
    try:
        mod = get_brokermod(brokername)
    except ImportError:
        mod = get_ingestormod(brokername)

    if loglevel is None:
        loglevel = tractor.current_actor().loglevel

    # TODO: do all!
    sym = symbols[0]

    async with maybe_spawn_brokerd(
        brokername,
        loglevel=loglevel,
    ) as portal:

        stream = await portal.run(
            attach_feed_bus,
            brokername=brokername,
            symbol=sym,
            loglevel=loglevel,
        )

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

        try:
            yield feed

        finally:
            # always cancel the far end producer task
            with trio.CancelScope(shield=True):
                await stream.aclose()
