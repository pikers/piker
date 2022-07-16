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
Deribit backend.

'''

from contextlib import asynccontextmanager as acm
from datetime import datetime
from typing import Any, Optional, List

import pendulum
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
from pydantic import BaseModel

from .._util import resproc

from piker import config
from piker.log import get_logger

from cryptofeed.symbols import Symbol

_spawn_kwargs = {
    'infect_asyncio': True,
}

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


class JSONRPCResult(BaseModel):
    jsonrpc: str = '2.0'
    result: dict 
    usIn: int 
    usOut: int 
    usDiff: int 
    testnet: bool


class KLinesResult(BaseModel):
    close: List[float]
    cost: List[float]
    high: List[float]
    low: List[float]
    open: List[float]
    status: str
    ticks: List[int]
    volume: List[float]


class KLines(JSONRPCResult):
    result: KLinesResult


class Trade(BaseModel):
    trade_seq: int
    trade_id: str
    timestamp: int
    tick_direction: int
    price: float
    mark_price: float
    iv: float
    instrument_name: str
    index_price: float
    direction: str
    amount: float

class LastTradesResult(BaseModel):
    trades: List[Trade]
    has_more: bool

class LastTrades(JSONRPCResult):
    result: LastTradesResult


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
            'currency': currency.upper(),
            'kind': kind,
            'expired': str(expired).lower()
        }

        resp = await self._api(
            'get_instruments', params=params)

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
        symbol: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        limit: int = 1000,
        as_np: bool = True,
    ) -> dict:
        instrument = symbol

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
                result.volume[i],
                0
            ]

            new_bars.append((i,) + tuple(row))

        array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else klines
        return array

    async def last_trades(
        self,
        instrument: str,
        count: int = 10
    ):
        response = await self._api(
            'get_last_trades_by_instrument',
            params={
                'instrument_name': instrument,
                'count': count
            }
        )

        return LastTrades(**response)


@acm
async def get_client() -> Client:
    client = Client()
    await client.cache_symbols()
    yield client
