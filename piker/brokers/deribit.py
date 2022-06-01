# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for piker0)

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
Deribit backend

"""
from contextlib import asynccontextmanager as acm
from datetime import datetime
from typing import (
    Any, Union, Optional,
    AsyncGenerator, Callable,
)
import time

import trio
from trio_typing import TaskStatus
import pendulum
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor
from pydantic.dataclasses import dataclass
from pydantic import BaseModel
import wsproto

from .._cacheables import open_cached_client
from ._util import resproc, SymbolNotFound
from ..log import get_logger, get_console_log
from ..data import ShmArray
from ..data._web_bs import open_autorecon_ws, NoBsWs


from cryptofeed import FeedHandler

from cryptofeed.callback import (
    L1BookCallback,
    TradeCallback
)
from cryptofeed.defines import DERIBIT, L1_BOOK, TRADES 


log = get_logger(__name__)


_url = 'https://www.deribit.com'


# Broker specific ohlc schema (rest)
_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('bar_wap', float),  # will be zeroed by sampler if not filled
]

class KLinesResult(BaseModel):
    close: List[float]
    cost: List[float]
    high: List[float]
    low: List[float]
    open: List[float]
    status: str
    ticks: List[int]
    volume: List[float]

class KLines(BaseModel):
    id: int
    jsonrpc: str = '2.0'
    result: KLinesResult 


# convert datetime obj timestamp to unixtime in milliseconds
def deribit_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url
        self._pairs: dict[str, Any] = {}

    async def _api(
        self,
        method: str,
        params: dict,
    ) -> dict[str, Any]:
        resp = await self._sesh.get(
            path=f'/api/v2/public/{method}',
            params=params,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def symbol_info(
        self,
        instrument: Optional[str] = None,
        currency: str = 'btc',  # BTC, ETH, SOL, USDC
        kind: str = 'option',
        expired: bool = False
    ) -> dict[str, Any]:
        '''Get symbol info for the exchange.

        '''
        # TODO: we can load from our self._pairs cache
        # on repeat calls...

        # will retrieve all symbols by default
        params = {
            'currency': currency.to_upper(),
            'kind': kind,
            'expired': expired
        }

        resp = await self._api(
            'get_instrument', params=params)

        if 'result' in resp:
            raise SymbolNotFound

        results = resp['result']

        instruments = {
            item['instrument_name']: item for item in results}

        if instrument is not None:
            return instruments[instrument]
        else:
            return instruments

    async def cache_symbols(
        self,
    ) -> dict:
        if not self._pairs:
            self._pairs = await self.symbol_info()

        return self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,
    ) -> dict[str, Any]:
        if self._pairs is not None:
            data = self._pairs
        else:
            data = await self.symbol_info()

        matches = fuzzy.extractBests(
            pattern,
            data,
            score_cutoff=50,
        )
        # repack in dict form
        return {item[0]['instrument_name']: item[0]
                for item in matches}

    async def bars(
        self,
        instrument: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        as_np: bool = True,
    ) -> dict:

        if end_dt is None:
            end_dt = pendulum.now('UTC')

        if start_dt is None:
            start_dt = end_dt.start_of(
                'minute').subtract(minutes=limit)

        start_time = deribit_timestamp(start_dt)
        end_time = deribit_timestamp(end_dt)

        # https://docs.deribit.com/#public-get_tradingview_chart_data
        response = await self._api(
            'get_tradingview_chart_data',
            params={
                'instrument_name': instrument.upper(),
                'start_timestamp': start_time,
                'end_timestamp': end_time,
                'resolution': '1'
            }
        )

        klines = KLines(**response)
    
        result = klines.result
        new_bars = []
        for i in range(len(result.close)):

            _open = result.open[i]
            high = result.high[i]
            low = result.low[i]
            close = result.close[i]
            volume = result.volume[i]

            row = [
                (start_time + (i * (60 * 1000))) / 1000.0,  # time
                result.open[i],
                result.high[i],
                result.low[i],
                result.close[i],
                result.volume[i]
            ]

            new_bars.append((i,) + tuple(row))

        array = np.array(
            [i, ], dtype=_ohlc_dtype) if as_np else klines
        return array


@acm
async def get_client() -> Client:
    client = Client()
    await client.cache_symbols()
    yield client


# inside here we are in an asyncio context
async def open_aio_cryptofeed_relay(
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,
    event_consumers: dict[str, trio.Event],
    instruments: List[str] = []
) -> None:

    async def trade_cb(feed, instrument, data: dict, receipt_timestamp):
        to_trio.send_nowait({
            'type': 'trade',
            instrument: data,
            'receipt': receipt_timestamp}) 

    async def l1_book_cb(feed, instrument, data: dict, receipt_timestamp):
        to_trio.send_nowait({
            'type': 'l1_book',
            instrument: data,
            'receipt': receipt_timestamp}) 


    fh = FeedHandler()
    fh.run(start_loop=False)

    fh.add_feed(
        DERIBIT,
        channels=[TRADES],
        symbols=instruments,
        callbacks={TRADES: TradeCallback(trade_cb)})

    fh.add_feed(
        DERIBIT,
        channels=[L1_BOOK],
        symbols=instruments,
        callbacks={L1_BOOK: L1BookCallback(l1_book_cb)})

    # sync with trio
    to_trio.send_nowait(None)

    await from_trio.get()


@acm
async def open_cryptofeeds():

    # try:
    event_table = {}

    async with (
        to_asyncio.open_channel_from(
            open_aio_cryptofeed_relay,
            event_consumers=event_table,
            instruments=['BTC-10JUN22-30000-C']
        ) as (first, chan),
        trio.open_nursery() as n,
    ):
        assert first is None

        async def relay_events():
            async with chan.subscribe() as msg_stream:
                async for msg in msg_stream:
                    print(msg)

        n.start_soon(relay_events)

        yield None
        
        await chan.send(None)


@acm
async def open_history_client(
    instrument: str,
) -> tuple[Callable, int]:

    # TODO implement history getter for the new storage layer.
    async with open_cached_client('deribit') as client:

        async def get_ohlc(
            end_dt: Optional[datetime] = None,
            start_dt: Optional[datetime] = None,

        ) -> tuple[
            np.ndarray,
            datetime,  # start
            datetime,  # end
        ]:

            array = await client.bars(
                instrument,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            start_dt = pendulum.from_timestamp(array[0]['time'])
            end_dt = pendulum.from_timestamp(array[-1]['time'])
            return array, start_dt, end_dt

        yield get_ohlc, {'erlangs': 3, 'rate': 3}


async def backfill_bars(
    instrument: str,
    shm: ShmArray,  # type: ignore # noqa
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Fill historical bars into shared mem / storage afap.
    """
    with trio.CancelScope() as cs:
        async with open_cached_client('deribit') as client:
            bars = await client.bars(instrument)
            shm.push(bars)
            task_status.started(cs)


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    sym_infos = {}
    uid = 0

    async with (
        open_cached_client('deribit') as client,
        send_chan as send_chan,
    ):

        # keep client cached for real-time section
        cache = await client.cache_symbols()

        breakpoint() 


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> Client:
    async with open_cached_client('deribit') as client:

        # load all symbols locally for fast search
        cache = await client.cache_symbols()
        await ctx.started()

        async with ctx.open_stream() as stream:

            async for pattern in stream:
                # results = await client.symbol_info(sym=pattern.upper())

                matches = fuzzy.extractBests(
                    pattern,
                    cache,
                    score_cutoff=50,
                )
                # repack in dict form
                await stream.send(
                    {item[0]['instrument_name']: item[0]
                     for item in matches}
                )
