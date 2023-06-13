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
Per market data-type definitions and schemas types.

"""
from __future__ import annotations
from typing import (
    Literal,
)
from decimal import Decimal

from piker.data.types import Struct


class Pair(Struct, frozen=True):
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
    ]

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

    # NOTE: for compat with spot pairs and `MktPair.src: Asset`
    # processing..
    @property
    def quoteAssetPrecision(self) -> int:
        return self.quotePrecision


MarketType = Literal[
    'spot',
    'margin',
    'usd_futes',
    'coin_futes',
]


PAIRTYPES: dict[MarketType, Pair] = {
    'spot': SpotPair,
    'usd_futes': FutesPair,
}
