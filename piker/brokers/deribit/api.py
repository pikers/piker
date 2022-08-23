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
import json
import time
import asyncio

from contextlib import asynccontextmanager as acm, AsyncExitStack
from itertools import count
from functools import partial
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

from tractor.trionics import broadcast_receiver, BroadcastReceiver
from tractor import to_asyncio

from cryptofeed import FeedHandler

from cryptofeed.defines import (
    DERIBIT, L1_BOOK, TRADES, OPTION, CALL, PUT
)
from cryptofeed.symbols import Symbol

log = get_logger(__name__)


_spawn_kwargs = {
    'infect_asyncio': True,
}


_url = 'https://www.deribit.com'
_ws_url = 'wss://www.deribit.com/ws/api/v2'
_testnet_ws_url = 'wss://test.deribit.com/ws/api/v2'


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
    result: Optional[dict] = None
    error: Optional[dict] = None
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
    combo_trade_id: Optional[int] = 0,
    combo_id: Optional[str] = '',
    amount: float

class LastTradesResult(Struct):
    trades: List[Trade]
    has_more: bool


# convert datetime obj timestamp to unixtime in milliseconds
def deribit_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


def str_to_cb_sym(name: str) -> Symbol:
    base, strike_price, expiry_date, option_type = name.split('-')

    quote = base

    if option_type == 'put':
        option_type = PUT 
    elif option_type  == 'call':
        option_type = CALL
    else:
        raise Exception("Couldn\'t parse option type")

    return Symbol(
        base, quote,
        type=OPTION,
        strike_price=strike_price,
        option_type=option_type,
        expiry_date=expiry_date,
        expiry_normalize=False)


def piker_sym_to_cb_sym(name: str) -> Symbol:
    base, expiry_date, strike_price, option_type = tuple(
        name.upper().split('-'))

    quote = base

    if option_type == 'P':
        option_type = PUT 
    elif option_type  == 'C':
        option_type = CALL
    else:
        raise Exception("Couldn\'t parse option type")

    return Symbol(
        base, quote,
        type=OPTION,
        strike_price=strike_price,
        option_type=option_type,
        expiry_date=expiry_date.upper())


def cb_sym_to_deribit_inst(sym: Symbol):
    # cryptofeed normalized
    cb_norm = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

    # deribit specific 
    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

    exp = sym.expiry_date

    # YYMDD
    # 01234
    year, month, day = (
        exp[:2], months[cb_norm.index(exp[2:3])], exp[3:])

    otype = 'C' if sym.option_type == CALL else 'P'

    return f'{sym.base}-{day}{month}{year}-{sym.strike_price}-{otype}'


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
        self._pairs: dict[str, Any] = None

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

        self.feeds = CryptoFeedRelay()

    @property
    def currencies(self):
        return ['btc', 'eth', 'sol', 'usd']

    def _next_json_body(self, method: str, params: Dict):
        """get the typical json rpc 2.0 msg body and increment the req id
        """
        return {
            'jsonrpc': '2.0',
            'id': next(self._rpc_id),
            'method': method,
            'params': params
        }

    async def start_rpc(self):
        """launch message receiver
        """
        self._n.start_soon(self._recv_task)

        # if we have client creds launch auth loop
        if self._key_id is not None:
            await self._n.start(self._auth_loop)

    async def _recv_task(self):
        """receives every ws message and stores it in its corresponding result
        field, then sets the event to wakeup original sender tasks.
        """
        while True:
            msg = JSONRPCResult(**(await self._ws.recv_msg()))

            if msg.id not in self._rpc_results:
                # in case this message wasn't beign accounted for store it 
                self._rpc_results[msg.id] = {
                    'result': None,
                    'event': trio.Event()
                }

            self._rpc_results[msg.id]['result'] = msg
            self._rpc_results[msg.id]['event'].set()

    async def json_rpc(self, method: str, params: Dict) -> Dict:
        """perform a json rpc call and wait for the result, raise exception in
        case of error field present on response
        """
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

        if ret.error is not None:
            raise Exception(json.dumps(ret.error, indent=4))

        return ret

    async def _auth_loop(
        self,
        task_status: TaskStatus = trio.TASK_STATUS_IGNORED
    ):
        """Background task that adquires a first access token and then will
        refresh the access token while the nursery isn't cancelled.

        https://docs.deribit.com/?python#authentication-2
        """
        renew_time = 10
        access_scope = 'trade:read_write'
        self._expiry_time = time.time()
        got_access = False

        while True:
            if time.time() - self._expiry_time < renew_time:
                # if we are close to token expiry time

                if self._refresh_token != None:
                    # if we have a refresh token already dont need to send
                    # secret
                    params = {
                        'grant_type': 'refresh_token',
                        'refresh_token': self._refresh_token,
                        'scope': access_scope
                    }

                else:
                    # we don't have refresh token, send secret to initialize
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
                    # first time this loop runs we must indicate task is
                    # started, we have auth
                    got_access = True
                    task_status.started()

            else:
                await trio.sleep(renew_time / 2)

    async def get_balances(self, kind: str = 'option') -> dict[str, float]:
        """Return the set of positions for this account
        by symbol.
        """
        balances = {}

        for currency in self.currencies:
            resp = await self.json_rpc(
                'private/get_positions', params={
                    'currency': currency.upper(),
                    'kind': kind})

            balances[currency] = resp.result

        return balances

    async def get_assets(self) -> dict[str, float]:
        """Return the set of asset balances for this account
        by symbol.
        """
        balances = {}

        for currency in self.currencies:
            resp = await self.json_rpc(
                'private/get_account_summary', params={
                    'currency': currency.upper()})

            balances[currency] = resp.result['balance']

        return balances

    async def submit_limit(
        self,
        symbol: str,
        price: float,
        action: str,
        size: float
    ) -> dict:
        """Place an order
        """
        params = {
            'instrument_name': symbol.upper(),
            'amount': size,
            'type': 'limit',
            'price': price,
        }
        resp = await self.json_rpc(
            f'private/{action}', params)

        return resp.result

    async def submit_cancel(self, oid: str):
        """Send cancel request for order id
        """
        resp = await self.json_rpc(
            'private/cancel', {'order_id': oid})
        return resp.result

    async def symbol_info(
        self,
        instrument: Optional[str] = None,
        currency: str = 'btc',  # BTC, ETH, SOL, USDC
        kind: str = 'option',
        expired: bool = False
    ) -> dict[str, Any]:
        """Get symbol info for the exchange.

        """
        if self._pairs:
            return self._pairs

        # will retrieve all symbols by default
        params = {
            'currency': currency.upper(),
            'kind': kind,
            'expired': str(expired).lower()
        }

        resp = await self.json_rpc('public/get_instruments', params)
        results = resp.result

        instruments = {
            item['instrument_name'].lower(): item
            for item in results
        }

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
        limit: int = 30,
    ) -> dict[str, Any]:
        data = await self.symbol_info()

        matches = fuzzy.extractBests(
            pattern,
            data,
            score_cutoff=35,
            limit=limit
        )
        # repack in dict form
        return {item[0]['instrument_name'].lower(): item[0]
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
        open_autorecon_ws(_testnet_ws_url) as ws
    ):
        client = Client(n, ws)
        await client.start_rpc()
        await client.cache_symbols()
        yield client
        await client.feeds.stop()


class CryptoFeedRelay:

    def __init__(self):
        self._fh = FeedHandler(config=get_config())

        self._price_streams: dict[str, BroadcastReceiver] = {}
        self._order_stream: Optional[BroadcastReceiver] = None

        self._loop = None 

    async def stop(self):
        await to_asyncio.run_task(
            partial(self._fh.stop_async, loop=self._loop))

    @acm
    async def open_price_feed(
        self,
        instruments: List[str]
    ) -> trio.abc.ReceiveStream:
        inst_str = ','.join(instruments)
        instruments = [piker_sym_to_cb_sym(i) for i in instruments]

        if inst_str in self._price_streams:
            # TODO: a good value for maxlen?
            yield broadcast_receiver(self._price_streams[inst_str], 10)

        else:
            async def relay(
                from_trio: asyncio.Queue,
                to_trio: trio.abc.SendChannel,
            ) -> None:
                async def _trade(data: dict, receipt_timestamp):
                    to_trio.send_nowait(('trade', {
                        'symbol': cb_sym_to_deribit_inst(
                            str_to_cb_sym(data.symbol)).lower(),
                        'last': data,
                        'broker_ts': time.time(),
                        'data': data.to_dict(),
                        'receipt': receipt_timestamp
                    }))

                async def _l1(data: dict, receipt_timestamp):
                    to_trio.send_nowait(('l1', {
                        'symbol': cb_sym_to_deribit_inst(
                            str_to_cb_sym(data.symbol)).lower(),
                        'ticks': [
                            {'type': 'bid',
                                'price': float(data.bid_price), 'size': float(data.bid_size)},
                            {'type': 'bsize',
                                'price': float(data.bid_price), 'size': float(data.bid_size)},
                            {'type': 'ask',
                                'price': float(data.ask_price), 'size': float(data.ask_size)},
                            {'type': 'asize',
                                'price': float(data.ask_price), 'size': float(data.ask_size)}
                        ]
                    }))

                self._fh.add_feed(
                    DERIBIT,
                    channels=[TRADES, L1_BOOK],
                    symbols=instruments,
                    callbacks={
                        TRADES: _trade,
                        L1_BOOK: _l1
                    })

                if not self._fh.running:
                    self._fh.run(start_loop=False)
                    self._loop = asyncio.get_event_loop()
            
                # sync with trio
                to_trio.send_nowait(None)

                try:
                    await asyncio.sleep(float('inf'))

                except asyncio.exceptions.CancelledError:
                    ...

            async with to_asyncio.open_channel_from(
                relay
            ) as (first, chan):
                self._price_streams[inst_str] = chan 
                yield self._price_streams[inst_str]

    @acm
    async def open_order_feed(
        self,
        instruments: List[str]
    ) -> trio.abc.ReceiveStream:

        inst_str = ','.join(instruments)
        instruments = [piker_sym_to_cb_sym(i) for i in instruments]

        if self._order_stream:
            yield broadcast_receiver(self._order_streams[inst_str], 10)

        else:
            async def relay(
                from_trio: asyncio.Queue,
                to_trio: trio.abc.SendChannel,
            ) -> None:
                async def _fill(data: dict, receipt_timestamp):
                    breakpoint()

                async def _order_info(data: dict, receipt_timestamp):
                    breakpoint()

                self._fh.add_feed(
                    DERIBIT,
                    channels=[FILLS, ORDER_INFO],
                    symbols=instruments,
                    callbacks={
                        FILLS: _fill,
                        ORDER_INFO: _order_info,
                    })

                if not self._fh.running:
                    self._fh.run(start_loop=False)
                    self._loop = asyncio.get_event_loop()
            
                # sync with trio
                to_trio.send_nowait(None)

                try:
                    await asyncio.sleep(float('inf'))

                except asyncio.exceptions.CancelledError:
                    ...

            async with to_asyncio.open_channel_from(
                relay
            ) as (first, chan):
                self._order_stream = chan 
                yield self._order_stream
