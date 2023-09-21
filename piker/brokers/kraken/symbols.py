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

'''
Symbology defs and search.

'''
from decimal import Decimal

import tractor
from rapidfuzz import process as fuzzy

from piker._cacheables import (
    async_lifo_cache,
)
from piker.accounting._mktinfo import (
    digits_to_dec,
)
from piker.brokers import (
    open_cached_client,
    SymbolNotFound,
)
from piker.types import Struct
from piker.accounting._mktinfo import (
    Asset,
    MktPair,
    unpack_fqme,
)


# https://www.kraken.com/features/api#get-tradable-pairs
class Pair(Struct):
    xname: str  # idiotic bs_mktid equiv i guess?
    altname: str  # alternate pair name
    wsname: str  # WebSocket pair name (if available)
    aclass_base: str  # asset class of base component
    base: str  # asset id of base component
    aclass_quote: str  # asset class of quote component
    quote: str  # asset id of quote component
    lot: str  # volume lot size

    cost_decimals: int
    costmin: float
    pair_decimals: int  # scaling decimal places for pair
    lot_decimals: int  # scaling decimal places for volume

    # amount to multiply lot volume by to get currency volume
    lot_multiplier: float

    # array of leverage amounts available when buying
    leverage_buy: list[int]
    # array of leverage amounts available when selling
    leverage_sell: list[int]

    # fee schedule array in [volume, percent fee] tuples
    fees: list[tuple[int, float]]

    # maker fee schedule array in [volume, percent fee] tuples (if on
    # maker/taker)
    fees_maker: list[tuple[int, float]]

    fee_volume_currency: str  # volume discount currency
    margin_call: str  # margin call level
    margin_stop: str  # stop-out/liquidation margin level
    ordermin: float  # minimum order volume for pair
    tick_size: float  # min price step size
    status: str

    short_position_limit: float = 0
    long_position_limit: float = float('inf')

    # TODO: should we make this a literal NamespacePath ref?
    ns_path: str = 'piker.brokers.kraken:Pair'

    @property
    def bs_mktid(self) -> str:
        '''
        Kraken seems to index it's market symbol sets in
        transaction ledgers using the key returned from rest
        queries.. so use that since apparently they can't
        make up their minds on a better key set XD

        '''
        return self.xname

    @property
    def price_tick(self) -> Decimal:
        return digits_to_dec(self.pair_decimals)

    @property
    def size_tick(self) -> Decimal:
        return digits_to_dec(self.lot_decimals)

    @property
    def bs_dst_asset(self) -> str:
        dst, _ = self.wsname.split('/')
        return dst

    @property
    def bs_src_asset(self) -> str:
        _, src = self.wsname.split('/')
        return src

    @property
    def bs_fqme(self) -> str:
        '''
        Basically the `.altname` but with special '.' handling and
        `.SPOT` suffix appending (for future multi-venue support).

        '''
        dst, src = self.wsname.split('/')
        # XXX: omg for stupid shite like ETH2.S/ETH..
        dst = dst.replace('.', '-')
        return f'{dst}{src}.SPOT'


@tractor.context
async def open_symbol_search(ctx: tractor.Context) -> None:
    async with open_cached_client('kraken') as client:

        # load all symbols locally for fast search
        cache = await client.get_mkt_pairs()
        await ctx.started(cache)

        async with ctx.open_stream() as stream:
            async for pattern in stream:
                await stream.send(
                    await client.search_symbols(pattern)
                )


@async_lifo_cache()
async def get_mkt_info(
    fqme: str,

) -> tuple[MktPair, Pair]:
    '''
    Query for and return a `MktPair` and backend-native `Pair` (or
    wtv else) info.

    If more then one fqme is provided return a ``dict`` of native
    key-strs to `MktPair`s.

    '''
    venue: str = 'spot'
    expiry: str = ''
    if '.kraken' not in fqme:
        fqme += '.kraken'

    broker, pair, venue, expiry = unpack_fqme(fqme)
    venue: str = venue or 'spot'

    if venue.lower() != 'spot':
        raise SymbolNotFound(
            'kraken only supports spot markets right now!\n'
            f'{fqme}\n'
        )

    async with open_cached_client('kraken') as client:

        # uppercase since kraken bs_mktid is always upper
        # bs_fqme, _, broker = fqme.partition('.')
        # pair_str: str = bs_fqme.upper()
        pair_str: str = f'{pair}.{venue}'

        pair: Pair | None = client._pairs.get(pair_str.upper())
        if not pair:
            bs_fqme: str = client.to_bs_fqme(pair_str)
            pair: Pair = client._pairs[bs_fqme]

        if not (assets := client._assets):
            assets: dict[str, Asset] = await client.get_assets()

        dst_asset: Asset = assets[pair.bs_dst_asset]
        src_asset: Asset = assets[pair.bs_src_asset]

        mkt = MktPair(
            dst=dst_asset,
            src=src_asset,

            price_tick=pair.price_tick,
            size_tick=pair.size_tick,
            bs_mktid=pair.bs_mktid,

            expiry=expiry,
            venue=venue or 'spot',

            # TODO: futes
            # _atype=_atype,

            broker='kraken',
        )
        return mkt, pair
