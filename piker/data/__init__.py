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
    Dict, Any, Sequence, AsyncIterator, Optional
)

import trio
import tractor

from ..brokers import get_brokermod
from ..log import get_logger, get_console_log
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


# capable rpc modules
_data_mods = [
    'piker.brokers.core',
    'piker.brokers.data',
    'piker.data',
    'piker.data._buffer',
]


@asynccontextmanager
async def maybe_spawn_brokerd(
    brokername: str,
    sleep: float = 0.5,
    loglevel: Optional[str] = None,
    **tractor_kwargs,
) -> tractor._portal.Portal:
    """If no ``brokerd.{brokername}`` daemon-actor can be found,
    spawn one in a local subactor and return a portal to it.
    """
    if loglevel:
        get_console_log(loglevel)

    # disable debugger in brokerd?
    # tractor._state._runtime_vars['_debug_mode'] = False

    tractor_kwargs['loglevel'] = loglevel

    brokermod = get_brokermod(brokername)
    dname = f'brokerd.{brokername}'
    async with tractor.find_actor(dname) as portal:

        # WTF: why doesn't this work?
        if portal is not None:
            yield portal

        else:  # no daemon has been spawned yet

            log.info(f"Spawning {brokername} broker daemon")
            tractor_kwargs = getattr(brokermod, '_spawn_kwargs', {})
            async with tractor.open_nursery() as nursery:
                try:
                    # spawn new daemon
                    portal = await nursery.start_actor(
                        dname,
                        enable_modules=_data_mods + [brokermod.__name__],
                        loglevel=loglevel,
                        **tractor_kwargs
                    )
                    async with tractor.wait_for_actor(dname) as portal:
                        yield portal
                finally:
                    # client code may block indefinitely so cancel when
                    # teardown is invoked
                    await nursery.cancel()


@dataclass
class Feed:
    """A data feed for client-side interaction with far-process
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

    def _set_fake_trades_stream(
        self,
        recv_chan: trio.abc.ReceiveChannel,
    ) -> None:
        self._trade_stream = recv_chan

    async def recv_trades_data(self) -> AsyncIterator[dict]:

        if not getattr(self.mod, 'stream_trades', False):
            log.warning(
                f"{self.mod.name} doesn't have trade data support yet :(")

            if not self._trade_stream:
                raise RuntimeError(
                    f'Can not stream trade data from {self.mod.name}')

        # NOTE: this can be faked by setting a rx chan
        # using the ``_.set_fake_trades_stream()`` method
        if not self._trade_stream:

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
    ) as portal:
        stream = await portal.run(
            mod.stream_quotes,

            # TODO: actually handy multiple symbols...
            symbols=symbols,

            shm_token=shm.token,

            # compat with eventual ``tractor.msg.pub``
            topics=symbols,
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
                sym,
                min_tick=si.get('minTick', 0.01),
            )
            symbol.broker_info[brokername] = si

            feed.symbols[sym] = symbol

            shm_token = data['shm_token']
            if opened:
                assert data['is_shm_writer']
                log.info("Started shared mem bar writer")

        shm_token['dtype_descr'] = list(shm_token['dtype_descr'])
        assert shm_token == shm.token  # sanity

        yield feed
