# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
Per market data-type definitions and schemas types.

"""
from __future__ import annotations
from typing import (
    Literal,
)
from decimal import Decimal

from msgspec import field

from piker.types import Struct


# API endpoint paths by venue / sub-API
_domain: str = 'binance.com'
_spot_url = f'https://api.{_domain}'
_futes_url = f'https://fapi.{_domain}'

# WEBsocketz
# NOTE XXX: see api docs which show diff addr?
# https://developers.binance.com/docs/binance-trading-api/websocket_api#general-api-information
_spot_ws: str = 'wss://stream.binance.com/ws'
# or this one? ..
# 'wss://ws-api.binance.com:443/ws-api/v3',

# https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams
_futes_ws: str = f'wss://fstream.{_domain}/ws'
_auth_futes_ws: str = 'wss://fstream-auth.{_domain}/ws'

# test nets
# NOTE: spot test network only allows certain ep sets:
# https://testnet.binance.vision/
# https://www.binance.com/en/support/faq/how-to-test-my-functions-on-binance-testnet-ab78f9a1b8824cf0a106b4229c76496d
_testnet_spot_url: str = 'https://testnet.binance.vision/api'
_testnet_spot_ws: str = 'wss://testnet.binance.vision/ws'
# or this one? ..
# 'wss://testnet.binance.vision/ws-api/v3'

_testnet_futes_url: str = 'https://testnet.binancefuture.com'
_testnet_futes_ws: str = 'wss://stream.binancefuture.com/ws'


MarketType = Literal[
    'spot',
    # 'margin',
    'usdtm_futes',
    # 'coinm_futes',
]


def get_api_eps(venue: MarketType) -> tuple[str, str]:
    '''
    Return API ep root paths per venue.

    '''
    return {
        'spot': (
            _spot_url,
            _spot_ws,
        ),
        'usdtm_futes': (
            _futes_url,
            _futes_ws,
        ),
    }[venue]


class Pair(Struct, frozen=True, kw_only=True):

    symbol: str
    status: str
    orderTypes: list[str]

    # src
    quoteAsset: str
    quotePrecision: int

    # dst
    baseAsset: str
    baseAssetPrecision: int

    filters: dict[
        str,
        str | int | float,
    ] = field(default_factory=dict)

    @property
    def price_tick(self) -> Decimal:
        # XXX: lul, after manually inspecting the response format we
        # just directly pick out the info we need
        step_size: str = self.filters['PRICE_FILTER']['tickSize'].rstrip('0')
        return Decimal(step_size)

    @property
    def size_tick(self) -> Decimal:
        step_size: str = self.filters['LOT_SIZE']['stepSize'].rstrip('0')
        return Decimal(step_size)

    @property
    def bs_fqme(self) -> str:
        return self.symbol

    @property
    def bs_mktid(self) -> str:
        return f'{self.symbol}.{self.venue}'


class SpotPair(Pair, frozen=True):

    cancelReplaceAllowed: bool
    allowTrailingStop: bool
    quoteAssetPrecision: int

    baseCommissionPrecision: int
    quoteCommissionPrecision: int

    icebergAllowed: bool
    ocoAllowed: bool
    quoteOrderQtyMarketAllowed: bool
    isSpotTradingAllowed: bool
    isMarginTradingAllowed: bool

    defaultSelfTradePreventionMode: str
    allowedSelfTradePreventionModes: list[str]
    permissions: list[str]

    # NOTE: see `.data._symcache.SymbologyCache.load()` for why
    ns_path: str = 'piker.brokers.binance:SpotPair'

    @property
    def venue(self) -> str:
        return 'SPOT'

    @property
    def bs_fqme(self) -> str:
        return f'{self.symbol}.SPOT'

    @property
    def bs_src_asset(self) -> str:
        return f'{self.quoteAsset}'

    @property
    def bs_dst_asset(self) -> str:
        return f'{self.baseAsset}'


class FutesPair(Pair):
    symbol: str  # 'BTCUSDT',
    pair: str  # 'BTCUSDT',
    baseAssetPrecision: int # 8,
    contractType: str  # 'PERPETUAL',
    deliveryDate: int   # 4133404800000,
    liquidationFee: float  # '0.012500',
    maintMarginPercent: float  # '2.5000',
    marginAsset: str  # 'USDT',
    marketTakeBound: float  # '0.05',
    maxMoveOrderLimit: int  # 10000,
    onboardDate: int  # 1569398400000,
    pricePrecision: int  # 2,
    quantityPrecision: int  # 3,
    quoteAsset: str  # 'USDT',
    quotePrecision: int  # 8,
    requiredMarginPercent: float  # '5.0000',
    settlePlan: int  # 0,
    timeInForce: list[str]  # ['GTC', 'IOC', 'FOK', 'GTX'],
    triggerProtect: float  # '0.0500',
    underlyingSubType: list[str]  # ['PoW'],
    underlyingType: str  # 'COIN'

    # NOTE: see `.data._symcache.SymbologyCache.load()` for why
    ns_path: str = 'piker.brokers.binance:FutesPair'

    # NOTE: for compat with spot pairs and `MktPair.src: Asset`
    # processing..
    @property
    def quoteAssetPrecision(self) -> int:
        return self.quotePrecision

    @property
    def venue(self) -> str:
        symbol: str = self.symbol
        ctype: str = self.contractType
        margin: str = self.marginAsset

        match ctype:
            case 'PERPETUAL':
                return f'{margin}M.PERP'

            case 'CURRENT_QUARTER':
                _, _, expiry = symbol.partition('_')
                return f'{margin}M.{expiry}'

            case '':
                subtype: list[str] = self.underlyingSubType
                if not subtype:
                    if self.status == 'PENDING_TRADING':
                        return f'{margin}M.PENDING'

                match subtype:
                    case ['DEFI']:
                        return f'{subtype[0]}.PERP'

        # XXX: yeah no clue then..
        return 'WTF.PWNED.BBQ'

    @property
    def bs_fqme(self) -> str:
        symbol: str = self.symbol
        ctype: str = self.contractType
        venue: str = self.venue

        match ctype:
            case 'CURRENT_QUARTER':
                symbol, _, expiry = symbol.partition('_')

        return f'{symbol}.{venue}'

    @property
    def bs_src_asset(self) -> str:
        return f'{self.quoteAsset}'

    @property
    def bs_dst_asset(self) -> str:
        return f'{self.baseAsset}.{self.venue}'


PAIRTYPES: dict[MarketType, Pair] = {
    'spot': SpotPair,
    'usdtm_futes': FutesPair,

    # TODO: support coin-margined venue:
    # https://binance-docs.github.io/apidocs/delivery/en/#change-log
    # 'coinm_futes': CoinFutesPair,
}
