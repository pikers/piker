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
from importlib import import_module
from types import ModuleType
from typing import (
    Dict, Any, Sequence,
    AsyncIterator, Optional,
    Callable, Awaitable
)

import trio
import tractor
from pydantic import BaseModel

from ..brokers import get_brokermod
from ..log import get_logger, get_console_log
from .._daemon import (
    spawn_brokerd,
    maybe_open_pikerd,
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
    increment_ohlc_buffer,
    subscribe_ohlc_for_increment
)

__all__ = [
    'iterticks',
    'maybe_open_shm_array',
    'attach_shm_array',
    'open_shm_array',
    'get_shm_token',
    'subscribe_ohlc_for_increment',
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


# @dataclass
class _FeedsCache(BaseModel):
    """Data feeds manager.

    This is a brokerd side api used to manager persistent real-time
    streams that can be allocated and left alive indefinitely.

    """
    brokername: str
    nursery: trio.Nursery
    tasks: Dict[str, trio.CancelScope] = {}

    class Config:
        arbitrary_types_allowed = True

    # tasks: Dict[str, trio.CancelScope] = field(default_factory=dict)

    async def start_feed(
        symbol: str,
        func: Callable[[int], Awaitable[None]],
    ) -> None:
        """Start a bg feed task and register a surrouding cancel scope
        for it.

        """
        with trio.CancelCscope() as cs:
            pass

    async def cancel_all(self) -> None:
        for name, cs in self.tasks.item():
            log.debug(f'Cancelling cached feed for {name}')
            cs.cancel()


_feeds: _FeedsCache = None


def get_feeds_manager(
    brokername: str,
    nursery: Optional[trio.Nursery] = None,
) -> _FeedsCache:
    """
    Retreive data feeds manager from process global scope.

    """

    global _feeds

    if nursery is not None:
        assert _feeds is None, "Feeds manager is already setup?"

        # this is initial setup by parent actor
        _feeds = _FeedsCache(
            brokername=brokername,
            nursery=nursery,
        )
        assert not _feeds.tasks

    assert _feeds.brokername == brokername, "Uhhh wtf"
    return _feeds


async def _setup_persistent_feeds(brokername:  str) -> None:
    """Allocate a actor-wide service nursery in ``brokerd``
    such that feeds can be run in the background persistently by
    the broker backend as needed.

    """
    async with trio.open_nursery() as service_nursery:
        _feeds = get_feeds_manager(brokername, service_nursery)

        # we pin this task to keep the feeds manager active until the
        # parent actor decides to tear it down
        await trio.sleep_forever()


@tractor.stream
async def allocate_cached_feed(
    ctx: tractor.Context,
    symbol: str
):
    _feeds = get_feeds_manager(brokername, service_nursery)

    # setup shared mem buffer
    pass



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
    # ticks: ShmArray
    _brokerd_portal: tractor._portal.Portal
    _index_stream: Optional[AsyncIterator[int]] = None
    _trade_stream: Optional[AsyncIterator[Dict[str, Any]]] = None

    # cache of symbol info messages received as first message when
    # a stream startsc.
    symbols: Dict[str, Symbol] = field(default_factory=dict)

    async def receive(self) -> dict:
        return await self.stream.__anext__()

    async def index_stream(self) -> AsyncIterator[int]:
        if not self._index_stream:
            # XXX: this should be singleton on a host,
            # a lone broker-daemon per provider should be
            # created for all practical purposes
            self._index_stream = await self._brokerd_portal.run(
                increment_ohlc_buffer,
                shm_token=self.shm.token,
                topics=['index'],
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

    # Attempt to allocate (or attach to) shm array for this broker/symbol
    shm, opened = maybe_open_shm_array(
        key=sym_to_shm_key(brokername, sym),

        # use any broker defined ohlc dtype:
        dtype=getattr(mod, '_ohlc_dtype', base_iohlc_dtype),

        # we expect the sub-actor to write
        readonly=True,
    )

    async with maybe_spawn_brokerd(

        brokername,
        loglevel=loglevel,

        # TODO: add a cli flag for this
        # debug_mode=False,

    ) as portal:

        stream = await portal.run(
            mod.stream_quotes,

            # TODO: actually handy multiple symbols...
            symbols=symbols,

            shm_token=shm.token,

            # compat with eventual ``tractor.msg.pub``
            topics=symbols,
            loglevel=loglevel,
        )

        feed = Feed(
            name=brokername,
            stream=stream,
            shm=shm,
            mod=mod,
            _brokerd_portal=portal,
        )

        # TODO: we can't do this **and** be compate with
        # ``tractor.msg.pub``, should we maybe just drop this after
        # tests are in?
        init_msg = await stream.receive()

        for sym, data in init_msg.items():

            si = data['symbol_info']

            symbol = Symbol(
                key=sym,
                type_key=si.get('asset_type', 'forex'),
                tick_size=si.get('price_tick_size', 0.01),
                lot_tick_size=si.get('lot_tick_size', 0.0),
            )
            symbol.broker_info[brokername] = si

            feed.symbols[sym] = symbol

            shm_token = data['shm_token']
            if opened:
                assert data['is_shm_writer']
                log.info("Started shared mem bar writer")
            else:
                s = attach_shm_array(shm_token)

        shm_token['dtype_descr'] = list(shm_token['dtype_descr'])
        assert shm_token == shm.token  # sanity

        yield feed
