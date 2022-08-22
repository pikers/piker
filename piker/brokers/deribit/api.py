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

'''
Deribit backend.

'''
import time

from contextlib import asynccontextmanager as acm, AsyncExitStack
from itertools import count
from datetime import datetime
from typing import Any, List, Dict, Optional, Iterable

import pendulum
import asks
import trio
from trio_typing import Nursery, TaskStatus
from fuzzywuzzy import process as fuzzy
import numpy as np

from piker.data.types import Struct
from piker.data._web_bs import NoBsWs, open_autorecon_ws

from .._util import resproc

from piker import config
from piker.log import get_logger

from cryptofeed.symbols import Symbol

log = get_logger(__name__)


_url = 'https://www.deribit.com'
_ws_url = 'wss://www.deribit.com/ws/api/v2'


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


class JSONRPCResult(Struct):
    jsonrpc: str = '2.0'
    id: int
    result: dict 
    usIn: int 
    usOut: int 
    usDiff: int 
    testnet: bool

class JSONRPCHTTPResult(Struct):
    jsonrpc: str = '2.0'
    result: dict 
    usIn: int 
    usOut: int 
    usDiff: int 
    testnet: bool


class KLinesResult(Struct):
    close: List[float]
    cost: List[float]
    high: List[float]
    low: List[float]
    open: List[float]
    status: str
    ticks: List[int]
    volume: List[float]

class Trade(Struct):
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

class LastTradesResult(Struct):
    trades: List[Trade]
    has_more: bool


# convert datetime obj timestamp to unixtime in milliseconds
def deribit_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


def get_config() -> dict[str, Any]:

    conf, path = config.load()

    section = conf.get('deribit')

    conf['log'] = {}
    conf['log']['disabled'] = True

    if section is None:
        log.warning(f'No config section found for deribit in {path}')

    return conf 


class Client:

    def __init__(self, n: Nursery, ws: NoBsWs) -> None:
        self._pairs: dict[str, Any] = {}

        config = get_config().get('deribit', {})

        if ('key_id' in config) and ('key_secret' in config):
            self._key_id = config['key_id']
            self._key_secret = config['key_secret']

        else:
            self._key_id = None
            self._key_secret = None

        self._ws = ws 
        self._n = n 

        self._rpc_id: Iterable = count(0)
        self._rpc_results: Dict[int, Dict] = {}

        self._expiry_time: int = float('inf')
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None

    def _next_json_body(self, method: str, params: Dict):
        return {
            'jsonrpc': '2.0',
            'id': next(self._rpc_id),
            'method': method,
            'params': params
        }

    async def start_rpc(self):
        self._n.start_soon(self._recv_task)

        if self._key_id is not None:
            await self._n.start(self._auth_loop)

    async def _recv_task(self):
        while True:
            msg = JSONRPCResult(**(await self._ws.recv_msg()))

            if msg.id not in self._rpc_results:
                self._rpc_results[msg.id] = {
                    'result': None,
                    'event': trio.Event()
                }

            self._rpc_results[msg.id]['result'] = msg
            self._rpc_results[msg.id]['event'].set()

    async def json_rpc(self, method: str, params: Dict) -> Dict:
        msg = self._next_json_body(method, params)
        _id = msg['id']

        self._rpc_results[_id] = {
            'result': None,
            'event': trio.Event()
        }

        await self._ws.send_msg(msg)

        await self._rpc_results[_id]['event'].wait()

        ret = self._rpc_results[_id]['result']

        del self._rpc_results[_id]

        return ret

    async def _auth_loop(
        self,
        task_status: TaskStatus = trio.TASK_STATUS_IGNORED
    ):
        '''https://docs.deribit.com/?python#authentication-2
        '''
        renew_time = 10
        access_scope = 'trade:read_write'
        self._expiry_time = time.time()
        got_access = False

        while True:
            if time.time() - self._expiry_time < renew_time:
                if self._refresh_token != None:
                    params = {
                        'grant_type': 'refresh_token',
                        'refresh_token': self._refresh_token,
                        'scope': access_scope
                    }

                else:
                    params = {
                        'grant_type': 'client_credentials',
                        'client_id': self._key_id,
                        'client_secret': self._key_secret,
                        'scope': access_scope
                    }
               
                resp = await self.json_rpc('public/auth', params)
                result = resp.result

                self._expiry_time = time.time() + result['expires_in']
                self._refresh_token = result['refresh_token']

                if 'access_token' in result:
                    self._access_token = result['access_token']

                if not got_access:
                    got_access = True
                    task_status.started()

            else:
                await trio.sleep(renew_time / 2)

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

        resp = await self.json_rpc('public/get_instruments', params)
        results = resp.result

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
        resp = await self.json_rpc(
            'public/get_tradingview_chart_data',
            params={
                'instrument_name': instrument.upper(),
                'start_timestamp': start_time,
                'end_timestamp': end_time,
                'resolution': '1'
            })

        result = KLinesResult(**resp.result)
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
        resp = await self.json_rpc(
            'public/get_last_trades_by_instrument',
            params={
                'instrument_name': instrument,
                'count': count
            })

        return LastTradesResult(**resp.result)


@acm
async def get_client() -> Client:
    async with (
        trio.open_nursery() as n,
        open_autorecon_ws(_ws_url) as ws
    ):

        client = Client(n, ws)
        await client.start_rpc()
        await client.cache_symbols()
        yield client
