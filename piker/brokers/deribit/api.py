# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for pikers)

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
from __future__ import annotations
import time

from contextlib import asynccontextmanager as acm
from functools import partial
from datetime import datetime
from typing import (
    Any,
    Optional,
    Callable,
)

import pendulum
import asks
import trio
from trio_typing import TaskStatus
from fuzzywuzzy import process as fuzzy
import numpy as np
from tractor.trionics import (
    broadcast_receiver,
    maybe_open_context
)

from piker.data.types import Struct
from piker.data._web_bs import (
    open_jsonrpc_session
)

from piker import config
from piker.log import get_logger
from piker._cacheables import open_cached_client


log = get_logger(__name__)


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
    id: int
    usIn: int
    usOut: int
    usDiff: int
    testnet: bool
    jsonrpc: str = '2.0'
    result: Optional[dict] = None
    error: Optional[dict] = None


class JSONRPCChannel(Struct):
    method: str
    params: dict
    jsonrpc: str = '2.0'


class KLinesResult(Struct):
    close: list[float]
    cost: list[float]
    high: list[float]
    low: list[float]
    open: list[float]
    status: str
    ticks: list[int]
    volume: list[float]


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
    combo_trade_id: Optional[int] = 0
    combo_id: Optional[str] = ''
    block_trade_id: str | None = ''


class LastTradesResult(Struct):
    trades: list[Trade]
    has_more: bool


# convert datetime obj timestamp to unixtime in milliseconds
def deribit_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


def sym_fmt_piker_to_deribit(sym: str) -> str:
    return sym.upper()


def sym_fmt_deribit_to_piker(sym: str):
    return sym.lower()


def get_config() -> dict[str, Any]:

    conf, path = config.load()

    section = conf.get('deribit')

    if section is None:
        log.warning(f'No config section found for deribit in {path}')

    return conf


class Client:

    def __init__(
        self,
        json_rpc: Callable,
        append_hooks: Callable,
        update_types: Callable,
    ) -> None:

        self._pairs: dict[str, Any] = None

        config = get_config().get('deribit', {})

        if ('key_id' in config) and ('key_secret' in config):
            self._key_id = config['key_id']
            self._key_secret = config['key_secret']

        else:
            self._key_id = None
            self._key_secret = None

        self.json_rpc = json_rpc
        self.append_hooks = append_hooks
        self.update_types = update_types

    @property
    def currencies(self):
        return ['btc', 'eth', 'sol', 'usd']

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
            'instrument_name': sym_fmt_piker_to_deribit(symbol),
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
            sym_fmt_deribit_to_piker(item['instrument_name']): item
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
        return {
            sym_fmt_deribit_to_piker(item[0]['instrument_name']): item[0]
            for item in matches
        }

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
                'instrument_name': sym_fmt_piker_to_deribit(instrument),
                'start_timestamp': start_time,
                'end_timestamp': end_time,
                'resolution': '1'
            })

        result = KLinesResult(**resp.result)
        new_bars = []

        for i in range(len(result.close)):
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

        array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else new_bars
        return array

    async def last_trades(
        self,
        instrument: str,
        count: int = 10
    ):
        resp = await self.json_rpc(
            'public/get_last_trades_by_instrument',
            params={
                'instrument_name': sym_fmt_piker_to_deribit(instrument),
                'count': count
            })

        return LastTradesResult(**resp.result)

    async def get_book_summary(
        self,
        currency: str,
        kind: str = 'option'
    ):
        return await self.json_rpc(
            'public/get_book_summary_by_currency',
            params={
                'currency': currency,
                'kind': kind
            })



class JSONRPCSubRequest(Struct):
    method: str
    params: dict
    jsonrpc: str = '2.0'


@acm
async def get_client(
    is_brokercheck: bool = False
) -> Client:

    async with (
        trio.open_nursery() as n,
        open_jsonrpc_session(
            _ws_url,
            response_type=JSONRPCResult
        ) as control_functions
    ):
        client = Client(*control_functions)

        _refresh_token: Optional[str] = None
        _access_token: Optional[str] = None

        async def _auth_loop(
            task_status: TaskStatus = trio.TASK_STATUS_IGNORED
        ):
            """Background task that adquires a first access token and then will
            refresh the access token while the nursery isn't cancelled.

            https://docs.deribit.com/?python#authentication-2
            """
            renew_time = 240
            access_scope = 'trade:read_write'
            _expiry_time = time.time()
            got_access = False
            nonlocal _refresh_token
            nonlocal _access_token

            while True:
                if time.time() - _expiry_time < renew_time:
                    # if we are close to token expiry time

                    if _refresh_token is not None:
                        # if we have a refresh token already dont need to send
                        # secret
                        params = {
                            'grant_type': 'refresh_token',
                            'refresh_token': _refresh_token,
                            'scope': access_scope
                        }

                    else:
                        # we don't have refresh token, send secret to
                        # initialize
                        params = {
                            'grant_type': 'client_credentials',
                            'client_id': client._key_id,
                            'client_secret': client._key_secret,
                            'scope': access_scope
                        }

                    resp = await client.json_rpc('public/auth', params)
                    result = resp.result

                    _expiry_time = time.time() + result['expires_in']
                    _refresh_token = result['refresh_token']

                    if 'access_token' in result:
                        _access_token = result['access_token']

                    if not got_access:
                        # first time this loop runs we must indicate task is
                        # started, we have auth
                        got_access = True
                        task_status.started()

                else:
                    await trio.sleep(renew_time / 2)

        # if we have client creds launch auth loop
        if client._key_id is not None:
            await n.start(_auth_loop)

        await client.cache_symbols()
        yield client
        n.cancel_scope.cancel()


@acm
async def open_price_feed(
    instrument: str
) -> trio.abc.ReceiveStream:

    instrument_db = sym_fmt_piker_to_deribit(instrument)

    trades_chan = f'trades.{instrument_db}.raw'
    book_chan = f'book.{instrument_db}.none.1.100ms'

    channels = [trades_chan, book_chan]

    send_chann, recv_chann = trio.open_memory_channel(0)
    async def sub_hook(msg):
        chan = msg.params['channel']
        data = msg.params['data']
        if chan == trades_chan:
            await send_chann.send((
                'trade', {
                    'symbol': instrument,
                    'last': data['price'],
                    'brokerd_ts': time.time(),
                    'ticks': [{
                        'type': 'trade',
                        'price': data['price'],
                        'size': data['amount'],
                        'broker_ts': data['timestamp']
                    }]
                }
            ))
            return True

        elif chan == book_chan:
            bid, bsize = data['bids'][0]
            ask, asize = data['asks'][0]
            await send_chann.send((
                'l1', {
                'symbol': instrument,
                'ticks': [
                    {'type': 'bid', 'price': bid, 'size': bsize},
                    {'type': 'bsize', 'price': bid, 'size': bsize},
                    {'type': 'ask', 'price': ask, 'size': asize},
                    {'type': 'asize', 'price': ask, 'size': asize}
                ]}
            ))
            return True

        return False

    async with open_cached_client('deribit') as client:

        client.append_hooks({
            'request': [sub_hook]
        })
        client.update_types({
            'request': JSONRPCSubRequest
        })

        resp = await client.json_rpc(
            'private/subscribe', {'channels': channels})

        assert not resp.error

        log.info(f'Subscribed to {channels}')

        yield recv_chann

        resp = await client.json_rpc('private/unsubscribe', {'channels': channels})

        assert not resp.error

@acm
async def maybe_open_price_feed(
    instrument: str
) -> trio.abc.ReceiveStream:

    # TODO: add a predicate to maybe_open_context
    async with maybe_open_context(
        acm_func=open_price_feed,
        kwargs={
            'instrument': instrument
        },
        key=f'{instrument}-price',
    ) as (cache_hit, feed):
        if cache_hit:
            yield broadcast_receiver(feed, 10)
        else:
            yield feed


@acm
async def open_ticker_feed(
    instrument: str
) -> trio.abc.ReceiveStream:

    instrument_db = sym_fmt_piker_to_deribit(instrument)

    ticker_chan = f'incremental_ticker.{instrument_db}'

    channels = [ticker_chan]

    send_chann, recv_chann = trio.open_memory_channel(0)
    async def sub_hook(msg):
        chann = msg.params['channel']
        if chann == ticker_chan:
            data = msg.params['data']
            await send_chann.send((
                'ticker', {
                    'symbol': instrument,
                    'data': data
                }
            ))
            return True

        return False

    async with open_cached_client('deribit') as client:

        client.append_hooks({
            'request': [sub_hook]
        })

        resp = await client.json_rpc(
            'private/subscribe', {'channels': channels})

        assert not resp.error

        log.info(f'Subscribed to {channels}')

        yield recv_chann

        resp = await client.json_rpc('private/unsubscribe', {'channels': channels})

        assert not resp.error

@acm
async def maybe_open_ticker_feed(
    instrument: str
) -> trio.abc.ReceiveStream:

    async with maybe_open_context(
        acm_func=open_ticker_feed,
        kwargs={
            'instrument': instrument
        },
        key=f'{instrument}-ticker',
    ) as (cache_hit, feed):
        if cache_hit:
            yield broadcast_receiver(feed, 10)
        else:
            yield feed
