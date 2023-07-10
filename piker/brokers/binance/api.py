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
from collections import ChainMap
from contextlib import (
    asynccontextmanager as acm,
)
from datetime import datetime
from pprint import pformat
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
from piker.clearing._messages import (
    Order,
)
from piker.accounting import (
    Asset,
    digits_to_dec,
)
from piker.data.types import Struct
from piker.data import def_iohlcv_fields
from piker.brokers import (
    resproc,
    SymbolNotFound,
    get_logger,
)
from .venues import (
    PAIRTYPES,
    Pair,
    MarketType,

    _spot_url,
    _futes_url,
    _testnet_futes_url,
    _testnet_spot_url,
)

log = get_logger('piker.brokers.binance')


def get_config() -> dict:

    conf: dict
    path: Path
    conf, path = config.load(touch_if_dne=True)

    section = conf.get('binance')

    if not section:
        log.warning(f'No config section found for binance in {path}')
        return {}

    return section


log = get_logger(__name__)

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
    time: int  # epoch in ms

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
        self._venue2pairs: dict[str, dict] = {
            'spot': self._spot_pairs,
            'usdtm_futes': self._ufutes_pairs,
        }

        self._venue2assets: dict[
            str,
            dict[str, dict] | None,
        ] = {
            # NOTE: only the spot table contains a dict[str, Asset]
            # since others (like futes, opts) can just do lookups
            # from a list of names to the spot equivalent.
            'spot': {},
            'usdtm_futes': {},
            # 'coinm_futes': {},
        }

        # NOTE: only stick in the spot table for now until exchange info
        # is loaded, since at that point we'll suffix all the futes
        # market symbols for use by search. See `.exch_info()`.
        self._pairs: ChainMap[str, Pair] = ChainMap()

        # spot EPs sesh
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location: str = _spot_url
        # spot testnet
        self._test_sesh: asks.Session = asks.Session(connections=4)
        self._test_sesh.base_location: str = _testnet_spot_url

        # margin and extended spot endpoints session.
        self._sapi_sesh = asks.Session(connections=4)
        self._sapi_sesh.base_location: str = _spot_url

        # futes EPs sesh
        self._fapi_sesh = asks.Session(connections=4)
        self._fapi_sesh.base_location: str = _futes_url
        # futes testnet
        self._test_fapi_sesh: asks.Session = asks.Session(connections=4)
        self._test_fapi_sesh.base_location: str = _testnet_futes_url

        # global client "venue selection" mode.
        # set this when you want to switch venues and not have to
        # specify the venue for the next request.
        self.mkt_mode: MarketType = mkt_mode

        # per 8
        self.venue_sesh: dict[
            str,  # venue key
            tuple[asks.Session, str]  # session, eps path
        ] = {
            'spot': (self._sesh, '/api/v3/'),
            'spot_testnet': (self._test_sesh, '/fapi/v1/'),

            'margin': (self._sapi_sesh, '/sapi/v1/'),

            'usdtm_futes': (self._fapi_sesh, '/fapi/v1/'),
            'usdtm_futes_testnet': (self._test_fapi_sesh, '/fapi/v1/'),

            # 'futes_coin': self._dapi,  # TODO
        }

        # lookup for going from `.mkt_mode: str` to the config
        # subsection `key: str`
        self.venue2configkey: dict[str, str] = {
            'spot': 'spot',
            'margin': 'spot',
            'usdtm_futes': 'futes',
            # 'coinm_futes': 'futes',
        }
        self.confkey2venuekeys: dict[str, list[str]] = {
            'spot': ['spot'], # 'margin'],
            'futes': ['usdtm_futes'],
        }

        # for creating API keys see,
        # https://www.binance.com/en/support/faq/how-to-create-api-keys-on-binance-360002502072
        self.conf: dict = get_config()

        for key, subconf in self.conf.items():
            if api_key := subconf.get('api_key', ''):
                venue_keys: list[str] = self.confkey2venuekeys[key]

                venue_key: str
                sesh: asks.Session
                for venue_key in venue_keys:
                    sesh, _ = self.venue_sesh[venue_key]

                    api_key_header: dict = {
                        # taken from official:
                        # https://github.com/binance/binance-futures-connector-python/blob/main/binance/api.py#L47
                        "Content-Type": "application/json;charset=utf-8",

                        # TODO: prolly should just always query and copy
                        # in the real latest ver?
                        "User-Agent": "binance-connector/6.1.6smbz6",
                        "X-MBX-APIKEY": api_key,
                    }
                    sesh.headers.update(api_key_header)

                    # if `.use_tesnet = true` in the config then
                    # also add headers for the testnet session which
                    # will be used for all order control
                    if subconf.get('use_testnet', False):
                        testnet_sesh, _ = self.venue_sesh[
                            venue_key + '_testnet'
                        ]
                        testnet_sesh.headers.update(api_key_header)

    def _mk_sig(
        self,
        data: dict,
        venue: str,

    ) -> str:

        # look up subconfig (spot or futes) section using 
        # venue specific key lookup to figure out which mkt
        # we need a key for.
        section_name: str = self.venue2configkey[venue]
        subconf: dict | None = self.conf.get(section_name)
        if subconf is None:
            raise config.ConfigurationError(
                f'binance configuration is missing a `{section_name}` section '
                'to define the creds for auth-ed endpoints!?'
            )


        # XXX: Info on security and authentification
        # https://binance-docs.github.io/apidocs/#endpoint-security-type
        if not (api_secret := subconf.get('api_secret')):
            raise config.NoSignature(
                "Can't generate a signature without setting up credentials"
            )

        query_str: str = '&'.join([
            f'{key}={value}'
            for key, value in data.items()
        ])

        msg_auth = hmac.new(
            api_secret.encode('utf-8'),
            query_str.encode('utf-8'),
            hashlib.sha256
        )
        return msg_auth.hexdigest()

    # TODO: factor all these _api methods into a single impl
    # which looks up the parent path for eps depending on a
    # mkt_mode: MarketType input!
    async def _api(
        self,
        endpoint: str,  # ReST endpoint key
        params: dict,

        method: str = 'get',
        venue: str | None = None,  # if None use `.mkt_mode` state
        signed: bool = False,
        allow_testnet: bool = False,

    ) -> dict[str, Any]:
        '''
        Make a ReST API request via
        - a /api/v3/ SPOT, or
        - /fapi/v3/ USD-M FUTURES, or
        - /api/v3/ SPOT/MARGIN

        account/market endpoint request depending on either passed in `venue: str`
        or the current setting `.mkt_mode: str` setting, default `'spot'`.


        Docs per venue API:

        SPOT: market-data and spot-account-trade eps use this
        ----  endpoing parent path:
        - https://binance-docs.github.io/apidocs/spot/en/#market-data-endpoints
        - https://binance-docs.github.io/apidocs/spot/en/#spot-account-trade

        MARGIN: and advancecd spot account eps:
        ------
        - https://binance-docs.github.io/apidocs/spot/en/#margin-account-trade
        - https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        - https://binance-docs.github.io/apidocs/spot/en/#spot-algo-endpoints

        USD-M FUTES:
        -----------
        - https://binance-docs.github.io/apidocs/futures/en/#market-data-endpoints

        '''
        venue_key: str = venue or self.mkt_mode

        if signed:
            params['signature'] = self._mk_sig(
                params,
                venue=venue_key,
            )

        sesh: asks.Session
        path: str

        # Check if we're configured to route order requests to the
        # venue equivalent's testnet.
        use_testnet: bool = False
        section_name: str = self.venue2configkey[venue_key]
        if subconf := self.conf.get(section_name):
            use_testnet = (
                subconf.get('use_testnet', False)
                and allow_testnet
            )

        if (
            use_testnet
            and method not in {
                'klines',
                'exchangeInfo',
            }
        ):
            # NOTE: only use testnet if user set brokers.toml config
            # var to true **and** it's not one of the market data
            # endpoints since we basically never want to display the
            # test net feeds, we only are using it for testing order
            # ctl machinery B)
            venue_key += '_testnet'

        sesh, path = self.venue_sesh[venue_key]

        meth: Callable = getattr(sesh, method)
        resp = await meth(
            path=path + endpoint,
            params=params,
            timeout=float('inf'),
        )
        return resproc(resp, log)

    async def _cache_pairs(
        self,
        venue: str,

    ) -> None:
        # lookup internal mkt-specific pair table to update
        pair_table: dict[str, Pair] = self._venue2pairs[venue]

        # make API request(s)
        resp = await self._api(
            'exchangeInfo',
            params={},  # NOTE: retrieve all symbols by default
            # XXX: MUST explicitly pass the routing venue since we
            # don't know the routing mode but want to cache market
            # infos across all venues
            venue=venue,
            allow_testnet=False,  # XXX: never use testnet for symbol lookups
        )

        mkt_pairs = resp['symbols']
        if not mkt_pairs:
            raise SymbolNotFound(f'No market pairs found!?:\n{resp}')

        pairs_view_subtable: dict[str, Pair] = {}

        for item in mkt_pairs:
            filters_ls: list = item.pop('filters', False)
            if filters_ls:
                filters = {}
                for entry in filters_ls:
                    ftype = entry['filterType']
                    filters[ftype] = entry

                item['filters'] = filters

            pair_type: Type = PAIRTYPES[venue]
            pair: Pair = pair_type(**item)
            pair_table[pair.symbol.upper()] = pair

            # update an additional top-level-cross-venue-table
            # `._pairs: ChainMap` for search B0
            pairs_view_subtable[pair.bs_fqme] = pair

            # XXX WOW: TURNS OUT THIS ISN'T TRUE !?
            # > (populate `Asset` table for spot mkts only since it
            # > should be a superset of any other venues such as
            # > futes or margin)
            if venue == 'spot':
                dst_sectype: str = 'crypto_currency'

            elif venue in {'usdtm_futes'}:
                dst_sectype: str = 'future'
                if pair.contractType == 'PERPETUAL':
                    dst_sectype: str = 'perpetual_future'

            spot_asset_table: dict[str, Asset] = self._venue2assets['spot']
            ven_asset_table: dict[str, Asset] = self._venue2assets[venue]

            if (
                (name := pair.quoteAsset) not in spot_asset_table
            ):
                spot_asset_table[pair.bs_src_asset] = Asset(
                    name=name,
                    atype='crypto_currency',
                    tx_tick=digits_to_dec(pair.quoteAssetPrecision),
                )

            if (
                (name := pair.baseAsset) not in ven_asset_table
            ):
                if venue != 'spot':
                    assert dst_sectype != 'crypto_currency'

                ven_asset_table[pair.bs_dst_asset] = Asset(
                    name=name,
                    atype=dst_sectype,
                    tx_tick=digits_to_dec(pair.baseAssetPrecision),
                )

        # log.warning(
        #     f'Assets not YET found in spot set: `{pformat(dne)}`!?'
        # )
        # NOTE: make merged view of all market-type pairs but
        # use market specific `Pair.bs_fqme` for keys!
        # this allows searching for market pairs with different
        # suffixes easily, for ex. `BTCUSDT.USDTM.PERP` will show
        # up when a user uses the search endpoint with pattern
        # `btc` B)
        self._pairs.maps.append(pairs_view_subtable)

        if venue == 'spot':
            return

        # TODO: maybe use this assets response for non-spot venues?
        # -> issue is we do the exch_info queries conc, so we can't
        # guarantee order for inter-table lookups..
        # if venue ep delivers an explicit set of assets copy just
        # ensure they are also already listed in the spot equivs.
        # assets: list[dict] = resp.get('assets', ())
        # for entry in assets:
        #     name: str = entry['asset']
        #     spot_asset_table: dict[str, Asset] = self._venue2assets['spot']
        #     if name not in spot_asset_table:
        #         log.warning(
        #             f'COULDNT FIND ASSET {name}\n{entry}\n'
        #             f'ADDING AS FUTES ONLY!?'
        #         )
        #     asset_table: dict[str, Asset] = self._venue2assets[venue]
        #     asset_table[name] = spot_asset_table.get(name)

    async def exch_info(
        self,
        sym: str | None = None,

        venue: MarketType | None = None,
        expiry: str | None = None,

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
        pair_table: dict[str, Pair] = self._venue2pairs[
            venue or self.mkt_mode
        ]
        if (
            expiry
            and 'perp' not in expiry.lower()
        ):
            sym: str = f'{sym}_{expiry}'

        if (
            sym
            and (cached_pair := pair_table.get(sym))
        ):
            return cached_pair

        venues: list[str] = ['spot', 'usdtm_futes']
        if venue:
            venues: list[str] = [venue]

        # batch per-venue download of all exchange infos
        async with trio.open_nursery() as rn:
            for ven in venues:
                rn.start_soon(
                    self._cache_pairs,
                    ven,
                )

        if sym:
            return pair_table[sym]
        else:
            self._pairs

    async def get_assets(
        self,
        venue: str | None = None,

    ) -> dict[str, Asset]:
        if (
            venue
            and venue != 'spot'
        ):
            venues = [venue]
        else:
            venues = ['usdtm_futes']

        ass_table: dict[str, Asset] = self._venue2assets['spot']

        # merge in futes contracts with a sectype suffix
        for venue in venues:
            ass_table |= self._venue2assets[venue]

        return ass_table


    async def get_mkt_pairs(self) -> dict[str, Pair]:
        '''
        Flatten the multi-venue (chain) map of market pairs
        to a fqme indexed table for data layer caching.

        '''
        flat: dict[str, Pair] = {}
        for venmap in self._pairs.maps:
            for bs_fqme, pair in venmap.items():
                flat[pair.bs_fqme] = pair

        return flat

    # TODO: unused except by `brokers.core.search_symbols()`?
    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,

    ) -> dict[str, Any]:

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
        bars = await self._api(
            'klines',
            params={
                'symbol': symbol.upper(),
                'interval': '1m',
                'startTime': start_time,
                'endTime': end_time,
                'limit': limit
            },
            allow_testnet=False,
        )
        new_bars: list[tuple] = []
        for i, bar_list in enumerate(bars):

            bar = OHLC(*bar_list)
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

    # TODO: maybe drop? Do we need this if we can simply request it
    # over the user stream wss?
    # async def get_positions(
    #     self,
    #     symbol: str,
    #     recv_window: int = 60000

    # ) -> tuple:

    #     positions = {}
    #     volumes = {}

    #     params = dict([
    #         ('symbol', symbol),
    #         ('recvWindow', recv_window),
    #         ('timestamp', binance_timestamp(now()))
    #     ])
    #     resp = await self._api(
    #         'allOrders',
    #         params=params,
    #         signed=True
    #     )
    #     log.info(f'done. len {len(resp)}')

    #     return positions, volumes

    async def get_deposits(
        self,
        recv_window: int = 60000
    ) -> list:

        # TODO: can't we drop this since normal dicts are
        # ordered implicitly in mordern python?
        params = dict([
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

        params = dict([
            ('recvWindow', recv_window),
            ('timestamp', binance_timestamp(now()))
        ])
        return await self._sapi(
            'capital/withdraw/history',
            params=params,
            signed=True,
        )

    async def get_open_orders(
        self,
        symbol: str | None = None,

    ) -> list[Order]:
        '''
        Get all open orders for venue-account.

        WARNING: apparently not specifying the symbol is given
        a much heavier API "weight" meaning you shouldn't call it
        often to avoid getting throttled as per:

        'https://binance-docs.github.io/apidocs/futures/en/#current-all-open-orders-user_data


        '''
        params: dict[str, Any] = {
            'timestamp': binance_timestamp(now()),
        }
        if symbol is not None:
            params['symbol'] = symbol

        resp = await self._api(
            'openOrders',
            params=params,
            signed=True,
            method='get',
            allow_testnet=True,
        )
        # figure out which venue (in FQME terms) we're using
        # since that normally maps 1-to-1 with the account (right?)
        venue: str = self.mkt_mode.rstrip('_futes')

        orders: list[Order] = []
        for entry in resp:
            oid: str = entry['clientOrderId']
            symbol: str = entry['symbol']

            # build out a fqme-styled key that should map to a pair
            # entry in `._pairs` cross-venue table.
            bs_mktid, _, expiry = entry['symbol'].partition('_')
            bs_mktid += f'.{venue.upper()}'

            if expiry:
                bs_mktid += f'.{expiry}'
            else:
                bs_mktid += '.PERP'

            # should never key error if we've got it right B)
            pair: Pair = self._pairs[bs_mktid]

            orders.append(
                Order(
                    oid=oid,
                    symbol=pair.bs_fqme.lower(),

                    action=entry['side'].lower(),
                    price=float(entry['price']),
                    size=float(entry['origQty']),

                    exec_mode='live',
                    account=f'binance.{venue}',
                )
            )
        return orders

    async def submit_limit(
        self,
        symbol: str,
        side: str,  # sell / buy
        quantity: float,
        price: float,

        oid: int | None = None,
        tif: str = 'GTC',
        recv_window: int = 60000,

        # iceberg_quantity: float | None = None,
        resp_type: str = 'ACK',

        # TODO: this is probably useful for doing stops, maybe we
        # can set it only on dark-stops?
        # close_all: bool = False,

        modify: bool = False,

    ) -> str:
        '''
        Submit or modify a live limit order to ze binance.

        For modify see:
        - spot: 
        - futes https://binance-docs.github.io/apidocs/futures/en/#modify-order-trade

        '''
        # lookup the binance-native symbol from search table
        bs_mktid: str = self._pairs[symbol.upper()].symbol
        params: dict = dict([
            ('symbol', bs_mktid),
            ('side', side.upper()),
            ('type', 'LIMIT'),
            ('timeInForce', tif),
            ('quantity', quantity),
            ('price', price),
            ('recvWindow', recv_window),
            ('newOrderRespType', resp_type),
            ('timestamp', binance_timestamp(now()))

            # ('closeAll', close_all),
        ])

        method: str = 'post'

        # NOTE: modifies only require diff key for user oid:
        # https://binance-docs.github.io/apidocs/futures/en/#modify-order-trade
        if modify:
            assert oid
            params['origClientOrderId'] = oid
            method: str = 'put'

        elif oid:
            params['newClientOrderId'] = oid

        log.info(
            'Submitting ReST order request:\n'
            f'{pformat(params)}'
        )
        resp = await self._api(
            'order',
            params=params,
            signed=True,
            method=method,
            venue=self.mkt_mode,
            allow_testnet=True,
        )

        # ensure our id is tracked by them
        if (
            oid
            and not modify
        ):
            assert oid == resp['clientOrderId']

        reqid: str = resp['orderId']
        return reqid

    async def submit_cancel(
        self,
        symbol: str,
        oid: str,

        recv_window: int = 60000

    ) -> None:
        bs_mktid: str = self._pairs[symbol.upper()].symbol
        params = dict([
            ('symbol', bs_mktid),
            # ('orderId', oid),
            ('origClientOrderId', oid),
            ('recvWindow', recv_window),
            ('timestamp', binance_timestamp(now()))
        ])

        log.cancel(
            'Submitting ReST order cancel: {oid}\n'
            f'{pformat(params)}'
        )
        await self._api(
            'order',
            params=params,
            signed=True,
            method='delete',
            allow_testnet=True,
        )

    async def get_listen_key(self) -> str:

        resp = await self._api(
            # 'userDataStream',  # spot
            'listenKey',
            params={},
            method='post',
            signed=True,
            allow_testnet=True,
        )
        return resp['listenKey']

    async def keep_alive_key(self, listen_key: str) -> None:
        await self._api(
            # 'userDataStream',
            'listenKey',
            params={'listenKey': listen_key},
            method='put',
            allow_testnet=True,
        )

    async def close_listen_key(self, listen_key: str) -> None:
        await self._api(
            # 'userDataStream',
            'listenKey',
            params={'listenKey': listen_key},
            method='delete',
            allow_testnet=True,
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
