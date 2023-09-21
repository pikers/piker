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
import asyncio
from contextlib import (
    asynccontextmanager as acm,
)
from datetime import datetime
from functools import partial
import time
from typing import (
    Any,
    Optional,
    Callable,
)

import pendulum
import trio
from trio_typing import TaskStatus
from rapidfuzz import process as fuzzy
import numpy as np
from tractor.trionics import (
    broadcast_receiver,
    maybe_open_context
)
from tractor import to_asyncio
# XXX WOOPS XD
# yeah you'll need to install it since it was removed in #489 by
# accident; well i thought we had removed all usage..
from cryptofeed import FeedHandler
from cryptofeed.defines import (
    DERIBIT,
    L1_BOOK, TRADES,
    OPTION, CALL, PUT
)
from cryptofeed.symbols import Symbol

from piker.data import (
    def_iohlcv_fields,
    match_from_pairs,
    Struct,
)
from piker.data._web_bs import (
    open_jsonrpc_session
)


from piker import config
from piker.log import get_logger


log = get_logger(__name__)


_spawn_kwargs = {
    'infect_asyncio': True,
}


_url = 'https://www.deribit.com'
_ws_url = 'wss://www.deribit.com/ws/api/v2'
_testnet_ws_url = 'wss://test.deribit.com/ws/api/v2'


class JSONRPCResult(Struct):
    jsonrpc: str = '2.0'
    id: int
    result: Optional[list[dict]] = None
    error: Optional[dict] = None
    usIn: int
    usOut: int
    usDiff: int
    testnet: bool

class JSONRPCChannel(Struct):
    jsonrpc: str = '2.0'
    method: str
    params: dict


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
    combo_trade_id: Optional[int] = 0,
    combo_id: Optional[str] = '',
    amount: float

class LastTradesResult(Struct):
    trades: list[Trade]
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

    # TODO: document why we send this, basically because logging params for cryptofeed
    conf['log'] = {}
    conf['log']['disabled'] = True

    if section is None:
        log.warning(f'No config section found for deribit in {path}')

    return conf 


class Client:

    def __init__(self, json_rpc: Callable) -> None:
        self._pairs: dict[str, Any] = None

        config = get_config().get('deribit', {})

        if ('key_id' in config) and ('key_secret' in config):
            self._key_id = config['key_id']
            self._key_secret = config['key_secret']

        else:
            self._key_id = None
            self._key_secret = None

        self.json_rpc = json_rpc

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

    ) -> dict[str, dict]:
        '''
        Get symbol infos.

        '''
        if self._pairs:
            return self._pairs

        # will retrieve all symbols by default
        params: dict[str, str] = {
            'currency': currency.upper(),
            'kind': kind,
            'expired': str(expired).lower()
        }

        resp: JSONRPCResult = await self.json_rpc(
            'public/get_instruments',
            params,
        )
        # convert to symbol-keyed table
        results: list[dict] | None = resp.result
        instruments: dict[str, dict] = {
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
        '''
        Fuzzy search symbology set for pairs matching `pattern`.

        '''
        pairs: dict[str, Any] = await self.symbol_info()
        matches: dict[str, Pair] = match_from_pairs(
            pairs=pairs,
            query=pattern.upper(),
            score_cutoff=35,
            limit=limit
        )

       # repack in name-keyed table
        return {
            pair['instrument_name'].lower(): pair
            for pair in matches.values()
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

        array = np.array(new_bars, dtype=def_iohlcv_fields) if as_np else klines
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
async def get_client(
    is_brokercheck: bool = False
) -> Client:

    async with (
        trio.open_nursery() as n,
        open_jsonrpc_session(
            _testnet_ws_url, dtype=JSONRPCResult) as json_rpc
    ):
        client = Client(json_rpc)

        _refresh_token: Optional[str] = None
        _access_token: Optional[str] = None

        async def _auth_loop(
            task_status: TaskStatus = trio.TASK_STATUS_IGNORED
        ):
            """Background task that adquires a first access token and then will
            refresh the access token while the nursery isn't cancelled.

            https://docs.deribit.com/?python#authentication-2
            """
            renew_time = 10
            access_scope = 'trade:read_write'
            _expiry_time = time.time()
            got_access = False
            nonlocal _refresh_token
            nonlocal _access_token

            while True:
                if time.time() - _expiry_time < renew_time:
                    # if we are close to token expiry time

                    if _refresh_token != None:
                        # if we have a refresh token already dont need to send
                        # secret
                        params = {
                            'grant_type': 'refresh_token',
                            'refresh_token': _refresh_token,
                            'scope': access_scope
                        }

                    else:
                        # we don't have refresh token, send secret to initialize
                        params = {
                            'grant_type': 'client_credentials',
                            'client_id': client._key_id,
                            'client_secret': client._key_secret,
                            'scope': access_scope
                        }

                    resp = await json_rpc('public/auth', params)
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
async def open_feed_handler():
    fh = FeedHandler(config=get_config())
    yield fh
    await to_asyncio.run_task(fh.stop_async)


@acm
async def maybe_open_feed_handler() -> trio.abc.ReceiveStream:
    async with maybe_open_context(
        acm_func=open_feed_handler,
        key='feedhandler',
    ) as (cache_hit, fh):
        yield fh


async def aio_price_feed_relay(
    fh: FeedHandler,
    instrument: Symbol,
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

    fh.add_feed(
        DERIBIT,
        channels=[TRADES, L1_BOOK],
        symbols=[piker_sym_to_cb_sym(instrument)],
        callbacks={
            TRADES: _trade,
            L1_BOOK: _l1
        })

    if not fh.running:
        fh.run(
            start_loop=False,
            install_signal_handlers=False)

    # sync with trio
    to_trio.send_nowait(None)

    await asyncio.sleep(float('inf'))


@acm
async def open_price_feed(
    instrument: str
) -> trio.abc.ReceiveStream:
    async with maybe_open_feed_handler() as fh:
        async with to_asyncio.open_channel_from(
            partial(
                aio_price_feed_relay,
                fh,
                instrument
            )
        ) as (first, chan):
            yield chan


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



async def aio_order_feed_relay(
    fh: FeedHandler,
    instrument: Symbol,
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,
) -> None:
    async def _fill(data: dict, receipt_timestamp):
        breakpoint()

    async def _order_info(data: dict, receipt_timestamp):
        breakpoint()

    fh.add_feed(
        DERIBIT,
        channels=[FILLS, ORDER_INFO],
        symbols=[instrument.upper()],
        callbacks={
            FILLS: _fill,
            ORDER_INFO: _order_info,
        })

    if not fh.running:
        fh.run(
            start_loop=False,
            install_signal_handlers=False)

    # sync with trio
    to_trio.send_nowait(None)

    await asyncio.sleep(float('inf'))


@acm
async def open_order_feed(
    instrument: list[str]
) -> trio.abc.ReceiveStream:
    async with maybe_open_feed_handler() as fh:
        async with to_asyncio.open_channel_from(
            partial(
                aio_order_feed_relay,
                fh,
                instrument
            )
        ) as (first, chan):
            yield chan


@acm
async def maybe_open_order_feed(
    instrument: str
) -> trio.abc.ReceiveStream:

    # TODO: add a predicate to maybe_open_context
    async with maybe_open_context(
        acm_func=open_order_feed,
        kwargs={
            'instrument': instrument,
            'fh': fh
        },
        key=f'{instrument}-order',
    ) as (cache_hit, feed):
        if cache_hit:
            yield broadcast_receiver(feed, 10)
        else:
            yield feed
