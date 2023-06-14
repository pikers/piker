# piker: trading gear for hackers
# Copyright (C)
#   Guillermo Rodriguez (aka ze jefe)
#   Tyler Goodlet
#   (in stewardship for pikers)

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
Binance clients for http and ws APIs.

"""
from __future__ import annotations
from collections import (
    OrderedDict,
    ChainMap,
)
from contextlib import (
    asynccontextmanager as acm,
)
from datetime import datetime
from typing import (
    Any,
    Callable,
    Type,
)
import hmac
import hashlib
from pathlib import Path

import trio
from pendulum import (
    now,
)
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np

from piker import config
from piker.data.types import Struct
from piker.data import def_iohlcv_fields
from piker.brokers._util import (
    resproc,
    SymbolNotFound,
    get_logger,
)
from .schemas import (
    PAIRTYPES,
    Pair,
    MarketType,
)

log = get_logger('piker.brokers.binance')


def get_config() -> dict:

    conf: dict
    path: Path
    conf, path = config.load()

    section = conf.get('binance')

    if not section:
        log.warning(f'No config section found for binance in {path}')
        return {}

    return section


log = get_logger(__name__)


_domain: str = 'binance.com'
_spot_url = _url = f'https://api.{_domain}'
_futes_url = f'https://fapi.{_domain}'

# test nets
_testnet_futes_url = 'https://testnet.binancefuture.com'

# WEBsocketz
# NOTE XXX: see api docs which show diff addr?
# https://developers.binance.com/docs/binance-trading-api/websocket_api#general-api-information
_spot_ws: str = 'wss://stream.binance.com/ws'
# 'wss://ws-api.binance.com:443/ws-api/v3',

# NOTE: spot test network only allows certain ep sets:
# https://testnet.binance.vision/
_testnet_spot_ws: str = 'wss://testnet.binance.vision/ws-api/v3'

# https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams
_futes_ws: str = f'wss://fstream.{_domain}/ws/'
_auth_futes_ws: str = 'wss://fstream-auth.{_domain}/ws/'


# Broker specific ohlc schema (rest)
# XXX TODO? some additional fields are defined in the docs:
# https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

# _ohlc_dtype = [
    # ('close_time', int),
    # ('quote_vol', float),
    # ('num_trades', int),
    # ('buy_base_vol', float),
    # ('buy_quote_vol', float),
    # ('ignore', float),
# ]


class OHLC(Struct):
    '''
    Description of the flattened OHLC quote format.

    For schema details see:
    https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams

    '''
    time: int

    open: float
    high: float
    low: float
    close: float
    volume: float

    close_time: int

    quote_vol: float
    num_trades: int
    buy_base_vol: float
    buy_quote_vol: float
    ignore: int


# convert datetime obj timestamp to unixtime in milliseconds
def binance_timestamp(
    when: datetime
) -> int:
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


class Client:
    '''
    Async ReST API client using ``trio`` + ``asks`` B)

    Supports all of the spot, margin and futures endpoints depending
    on method.

    '''
    def __init__(
        self,

        # TODO: change this to `Client.[mkt_]venue: MarketType`?
        mkt_mode: MarketType = 'spot',

    ) -> None:

        # build out pair info tables for each market type
        # and wrap in a chain-map view for search / query.
        self._spot_pairs: dict[str, Pair] = {}  # spot info table
        self._ufutes_pairs: dict[str, Pair] = {}  # usd-futures table
        self._mkt2pairs: dict[str, dict] = {
            'spot': self._spot_pairs,
            'usdtm_futes': self._ufutes_pairs,
        }
        # NOTE: only stick in the spot table for now until exchange info
        # is loaded, since at that point we'll suffix all the futes
        # market symbols for use by search. See `.exch_info()`.
        self._pairs: ChainMap[str, Pair] = ChainMap()

        # spot EPs sesh
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location: str = _url

        # margin and extended spot endpoints session.
        self._sapi_sesh = asks.Session(connections=4)
        self._sapi_sesh.base_location: str = _url

        # futes EPs sesh
        self._fapi_sesh = asks.Session(connections=4)
        self._fapi_sesh.base_location: str = _futes_url

        # for creating API keys see,
        # https://www.binance.com/en/support/faq/how-to-create-api-keys-on-binance-360002502072
        conf: dict = get_config()
        self.api_key: str = conf.get('api_key', '')
        self.api_secret: str = conf.get('api_secret', '')

        self.watchlist = conf.get('watchlist', [])

        if self.api_key:
            api_key_header = {'X-MBX-APIKEY': self.api_key}
            self._sesh.headers.update(api_key_header)
            self._sapi_sesh.headers.update(api_key_header)
            self._fapi_sesh.headers.update(api_key_header)

        self.mkt_mode: MarketType = mkt_mode
        self.mkt_mode_req: dict[str, Callable] = {
            'spot': self._api,
            'margin': self._sapi,
            'usdtm_futes': self._fapi,
            # 'futes_coin': self._dapi,  # TODO
        }

    def _get_signature(self, data: OrderedDict) -> str:

        # XXX: Info on security and authentification
        # https://binance-docs.github.io/apidocs/#endpoint-security-type

        if not self.api_secret:
            raise config.NoSignature(
                "Can't generate a signature without setting up credentials"
            )

        query_str = '&'.join([
            f'{_key}={value}'
            for _key, value in data.items()])
        log.info(query_str)
        msg_auth = hmac.new(
            self.api_secret.encode('utf-8'),
            query_str.encode('utf-8'),
            hashlib.sha256
        )
        return msg_auth.hexdigest()

    # TODO: factor all these _api methods into a single impl
    # which looks up the parent path for eps depending on a
    # mkt_mode: MarketType input!
    async def _api(
        self,
        method: str,
        params: dict | OrderedDict,
        signed: bool = False,
        action: str = 'get'

    ) -> dict[str, Any]:
        '''
        Make a /api/v3/ SPOT account/market endpoint request.

        For eg. rest market-data and spot-account-trade eps use
        this endpoing parent path:
        - https://binance-docs.github.io/apidocs/spot/en/#market-data-endpoints
        - https://binance-docs.github.io/apidocs/spot/en/#spot-account-trade

        '''
        if signed:
            params['signature'] = self._get_signature(params)

        resp = await getattr(self._sesh, action)(
            path=f'/api/v3/{method}',
            params=params,
            timeout=float('inf'),
        )

        return resproc(resp, log)

    async def _fapi(
        self,
        method: str,
        params: dict | OrderedDict,
        signed: bool = False,
        action: str = 'get'

    ) -> dict[str, Any]:
        '''
        Make a /fapi/v3/ USD-M FUTURES account/market endpoint
        request.

        For all USD-M futures endpoints use this parent path:
        https://binance-docs.github.io/apidocs/futures/en/#market-data-endpoints

        '''
        if signed:
            params['signature'] = self._get_signature(params)

        resp = await getattr(self._fapi_sesh, action)(
            path=f'/fapi/v1/{method}',
            params=params,
            timeout=float('inf')
        )

        return resproc(resp, log)

    async def _sapi(
        self,
        method: str,
        params: dict | OrderedDict,
        signed: bool = False,
        action: str = 'get'

    ) -> dict[str, Any]:
        '''
        Make a /api/v3/ SPOT/MARGIN account/market endpoint request.

        For eg. all margin and advancecd spot account eps use this
        endpoing parent path:
        - https://binance-docs.github.io/apidocs/spot/en/#margin-account-trade
        - https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        - https://binance-docs.github.io/apidocs/spot/en/#spot-algo-endpoints

        '''
        if signed:
            params['signature'] = self._get_signature(params)

        resp = await getattr(self._sapi_sesh, action)(
            path=f'/sapi/v1/{method}',
            params=params,
            timeout=float('inf')
        )

        return resproc(resp, log)

    async def _cache_pairs(
        self,
        mkt_type: str,

    ) -> None:
        # lookup internal mkt-specific pair table to update
        pair_table: dict[str, Pair] = self._mkt2pairs[mkt_type]

        # make API request(s)
        resp = await self.mkt_mode_req[mkt_type](
            'exchangeInfo',
            params={},  # NOTE: retrieve all symbols by default
        )
        entries = resp['symbols']
        if not entries:
            raise SymbolNotFound(f'No market pairs found!?:\n{resp}')

        for item in entries:
            filters_ls: list = item.pop('filters', False)
            if filters_ls:
                filters = {}
                for entry in filters_ls:
                    ftype = entry['filterType']
                    filters[ftype] = entry

                item['filters'] = filters

            pair_type: Type = PAIRTYPES[mkt_type]
            pair: Pair = pair_type(**item)
            pair_table[pair.symbol.upper()] = pair

    async def exch_info(
        self,
        sym: str | None = None,

        mkt_type: MarketType | None = None,

    ) -> dict[str, Pair] | Pair:
        '''
        Fresh exchange-pairs info query for symbol ``sym: str``.

        Depending on `mkt_type` different api eps are used:
        - spot:
          https://binance-docs.github.io/apidocs/spot/en/#exchange-information
        - usd futes:
          https://binance-docs.github.io/apidocs/futures/en/#check-server-time
        - coin futes:
          https://binance-docs.github.io/apidocs/delivery/en/#exchange-information

        '''
        pair_table: dict[str, Pair] = self._mkt2pairs[
            mkt_type or self.mkt_mode
        ]
        if cached_pair := pair_table.get(sym):
            return cached_pair

        # params = {}
        # if sym is not None:
        #     params = {'symbol': sym}

        mkts: list[str] = ['spot', 'usdtm_futes']
        if mkt_type:
            mkts: list[str] = [mkt_type]

        async with trio.open_nursery() as rn:
            for mkt_type in mkts:
                rn.start_soon(
                    self._cache_pairs,
                    mkt_type,
                )

        # make merged view of all market-type pairs but
        # use market specific `Pair.bs_fqme` for keys!
        for venue, venue_pairs_table in self._mkt2pairs.items():
            self._pairs.maps.append(
                {pair.bs_fqme: pair
                 for pair in venue_pairs_table.values()}
            )

        return pair_table[sym] if sym else self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,
    ) -> dict[str, Any]:

        # if self._spot_pairs is not None:
        #     data = self._spot_pairs
        # else:
        fq_pairs: dict = await self.exch_info()

        matches = fuzzy.extractBests(
            pattern,
            fq_pairs,
            score_cutoff=50,
        )
        # repack in dict form
        return {item[0]['symbol']: item[0]
                for item in matches}

    async def bars(
        self,
        symbol: str,

        start_dt: datetime | None = None,
        end_dt: datetime | None = None,

        as_np: bool = True,

    ) -> list[tuple] | np.ndarray:

        # NOTE: diff market-venues have diff datums limits:
        # - spot max is 1k
        #   https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        # - usdm futes max is 1500
        #   https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
        limits: dict[str, int] = {
            'spot': 1000,
            'usdtm_futes': 1500,
        }
        limit = limits[self.mkt_mode]

        if end_dt is None:
            end_dt = now('UTC').add(minutes=1)

        if start_dt is None:
            start_dt = end_dt.start_of(
                'minute').subtract(minutes=limit)

        start_time = binance_timestamp(start_dt)
        end_time = binance_timestamp(end_dt)

        # https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        bars = await self.mkt_mode_req[self.mkt_mode](
        # bars = await self._api(
            'klines',
            params={
                'symbol': symbol.upper(),
                'interval': '1m',
                'startTime': start_time,
                'endTime': end_time,
                'limit': limit
            }
        )
        new_bars: list[tuple] = []
        for i, bar in enumerate(bars):

            bar = OHLC(*bar)
            bar.typecast()

            row = []
            for j, (name, ftype) in enumerate(def_iohlcv_fields[1:]):

                # TODO: maybe we should go nanoseconds on all
                # history time stamps?
                if name == 'time':
                    # convert to epoch seconds: float
                    row.append(bar.time / 1000.0)

                else:
                    row.append(getattr(bar, name))

            new_bars.append((i,) + tuple(row))

        if not as_np:
            return bars

        return np.array(
            new_bars,
            dtype=def_iohlcv_fields,
        )

    async def get_positions(
        self,
        recv_window: int = 60000

    ) -> tuple:
        positions = {}
        volumes = {}

        for sym in self.watchlist:
            log.info(f'doing {sym}...')
            params = OrderedDict([
                ('symbol', sym),
                ('recvWindow', recv_window),
                ('timestamp', binance_timestamp(now()))
            ])
            resp = await self._api(
                'allOrders',
                params=params,
                signed=True
            )
            log.info(f'done. len {len(resp)}')
            # await trio.sleep(3)

        return positions, volumes

    async def get_deposits(
        self,
        recv_window: int = 60000
    ) -> list:

        # TODO: can't we drop this since normal dicts are
        # ordered implicitly in mordern python?
        params = OrderedDict([
            ('recvWindow', recv_window),
            ('timestamp', binance_timestamp(now()))
        ])
        return await self._sapi(
            'capital/deposit/hisrec',
            params=params,
            signed=True,
        )

    async def get_withdrawls(
        self,
        recv_window: int = 60000
    ) -> list:

        params = OrderedDict([
            ('recvWindow', recv_window),
            ('timestamp', binance_timestamp(now()))
        ])
        return await self._sapi(
            'capital/withdraw/history',
            params=params,
            signed=True,
        )

    async def submit_limit(
        self,
        symbol: str,
        side: str,  # SELL / BUY
        quantity: float,
        price: float,
        # time_in_force: str = 'GTC',
        oid: int | None = None,
        # iceberg_quantity: float | None = None,
        # order_resp_type: str | None = None,
        recv_window: int = 60000

    ) -> int:
        symbol = symbol.upper()

        await self.cache_symbols()

        # asset_precision = self._spot_pairs[symbol]['baseAssetPrecision']
        # quote_precision = self._pairs[symbol]['quoteAssetPrecision']

        params = OrderedDict([
            ('symbol', symbol),
            ('side', side.upper()),
            ('type', 'LIMIT'),
            ('timeInForce', 'GTC'),
            ('quantity', quantity),
            ('price', price),
            ('recvWindow', recv_window),
            ('newOrderRespType', 'ACK'),
            ('timestamp', binance_timestamp(now()))
        ])

        if oid:
            params['newClientOrderId'] = oid

        resp = await self._api(
            'order',
            params=params,
            signed=True,
            action='post'
        )
        log.info(resp)
        # return resp['orderId']
        return resp['orderId']

    async def submit_cancel(
        self,
        symbol: str,
        oid: str,
        recv_window: int = 60000
    ) -> None:
        symbol = symbol.upper()

        params = OrderedDict([
            ('symbol', symbol),
            ('orderId', oid),
            ('recvWindow', recv_window),
            ('timestamp', binance_timestamp(now()))
        ])

        return await self._api(
            'order',
            params=params,
            signed=True,
            action='delete'
        )

    async def get_listen_key(self) -> str:
        return (await self._api(
            'userDataStream',
            params={},
            action='post'
        ))['listenKey']

    async def keep_alive_key(self, listen_key: str) -> None:
        await self._fapi(
            'userDataStream',
            params={'listenKey': listen_key},
            action='put'
        )

    async def close_listen_key(self, listen_key: str) -> None:
        await self._fapi(
            'userDataStream',
            params={'listenKey': listen_key},
            action='delete'
        )

    @acm
    async def manage_listen_key(self):

        async def periodic_keep_alive(
            self,
            listen_key: str,
            timeout=60 * 29  # 29 minutes
        ):
            while True:
                await trio.sleep(timeout)
                await self.keep_alive_key(listen_key)

        key = await self.get_listen_key()

        async with trio.open_nursery() as n:
            n.start_soon(periodic_keep_alive, self, key)
            yield key
            n.cancel_scope.cancel()

        await self.close_listen_key(key)


@acm
async def get_client() -> Client:

    client = Client()
    await client.exch_info()
    log.info(
        f'{client} in {client.mkt_mode} mode: caching exchange infos..\n'
        'Cached multi-market pairs:\n'
        f'spot: {len(client._spot_pairs)}\n'
        f'usdtm_futes: {len(client._ufutes_pairs)}\n'
        f'Total: {len(client._pairs)}\n'
    )

    yield client
