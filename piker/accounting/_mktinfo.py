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
Market (pair) meta-info layer: sane addressing semantics and meta-data
for cross-provider marketplaces.

We intoduce the concept of,

- a FQMA: fully qualified market address,
- a sane schema for FQMAs including derivatives,
- a msg-serializeable description of markets for
  easy sharing with other pikers B)

'''
from __future__ import annotations
from decimal import (
    Decimal,
    ROUND_HALF_EVEN,
)
from typing import (
    Any,
    Literal,
)

from ..data.types import Struct


_underlyings: list[str] = [
    'stock',
    'bond',
    'crypto',
    'fiat',
    'commodity',
]


_derivs: list[str] = [
    'swap',
    'future',
    'continuous_future',
    'option',
    'futures_option',
]

# NOTE: a tag for other subsystems to try
# and do default settings for certain things:
# - allocator does unit vs. dolla size limiting.
AssetTypeName: Literal[
    _underlyings
    +
    _derivs
]

# egs. stock, futer, option, bond etc.


def float_digits(
    value: float,
) -> int:
    '''
    Return the number of precision digits read from a decimal or float
    value.

    '''
    if value == 0:
        return 0

    return int(
        -Decimal(str(value)).as_tuple().exponent
    )


def digits_to_dec(
    ndigits: int,
) -> Decimal:
    '''
    Return the minimum float value for an input integer value.

    eg. 3 -> 0.001

    '''
    if ndigits == 0:
        return Decimal('0')

    return Decimal('0.' + '0'*(ndigits-1) + '1')


class Asset(Struct, frozen=True):
    '''
    Container type describing any transactable asset and its
    contract-like and/or underlying technology meta-info.

    '''
    name: str
    atype: AssetTypeName

    # minimum transaction size / precision.
    # eg. for buttcoin this is a "satoshi".
    tx_tick: Decimal

    # NOTE: additional info optionally packed in by the backend, but
    # should not be explicitly required in our generic API.
    info: dict = {}  # make it frozen?

    # TODO?
    # _to_dict_skip = {'info'}

    def __str__(self) -> str:
        return self.name

    def quantize(
        self,
        size: float,

    ) -> Decimal:
        '''
        Truncate input ``size: float`` using ``Decimal``
        quantized form of the digit precision defined
        by ``self.lot_tick_size``.

        '''
        digits = float_digits(self.tx_tick)
        return Decimal(size).quantize(
            Decimal(f'1.{"0".ljust(digits, "0")}'),
            rounding=ROUND_HALF_EVEN
        )


def maybe_cons_tokens(
    tokens: list[Any],
    delim_char: str = '.',
) -> str:
    '''
    Construct `str` output from a maybe-concatenation of input
    sequence of elements in ``tokens``.

    '''
    return '.'.join(filter(bool, tokens)).lower()


class MktPair(Struct, frozen=True):
    '''
    Market description for a pair of assets which are tradeable:
    a market which enables transactions of the form,
        buy: source asset -> destination asset
        sell: destination asset -> source asset

    The main intention of this type is for a **simple** cross-asset
    venue/broker normalized descrption type from which all
    market-auctions can be mapped from FQME identifiers.

    TODO: our eventual target fqme format/schema is:
    <dst>/<src>.<expiry>.<con_info_1>.<con_info_2>. -> .<venue>.<broker>
          ^ -- optional tokens ------------------------------- ^

    '''
    dst: str | Asset
    # "destination asset" (name) used to buy *to*
    # (or used to sell *from*)

    price_tick: Decimal  # minimum price increment
    size_tick: Decimal  # minimum size (aka vlm) increment
    # the tick size is the number describing the smallest step in value
    # available in this market between the source and destination
    # assets.
    # https://en.wikipedia.org/wiki/Tick_size
    # https://en.wikipedia.org/wiki/Commodity_tick
    # https://en.wikipedia.org/wiki/Percentage_in_point

    # unique "broker id" since every market endpoint provider
    # has their own nomenclature and schema for market maps.
    bs_mktid: str
    broker: str  # the middle man giving access

    # NOTE: to start this field is optional but should eventually be
    # required; the reason is for backward compat since more positioning
    # calculations were not originally stored with a src asset..

    src: str | Asset | None = None
    # "source asset" (name) used to buy *from*
    # (or used to sell *to*).

    venue: str = ''  # market venue provider name
    expiry: str = ''  # for derivs, expiry datetime parseable str

    # destination asset's financial type/classification name
    # NOTE: this is required for the order size allocator system,
    # since we use different default settings based on the type
    # of the destination asset, eg. futes use a units limits vs.
    # equities a $limit.
    # dst_type: AssetTypeName | None = None

    # source asset's financial type/classification name
    # TODO: is a src type required for trading?
    # there's no reason to need any more then the one-way alloc-limiter
    # config right?
    # src_type: AssetTypeName

    # for derivs, info describing contract, egs.
    # strike price, call or put, swap type, exercise model, etc.
    contract_info: str | None = None

    @classmethod
    def from_msg(
        self,
        msg: dict[str, Any],

    ) -> MktPair:
        '''
        Constructor for a received msg-dict normally received over IPC.

        '''
        raise NotImplementedError

    @property
    def resolved(self) -> bool:
        return isinstance(self.dst, Asset)

    @classmethod
    def from_fqme(
        cls,
        fqme: str,
        price_tick: float | str,
        size_tick: float | str,
        bs_mktid: str,

    ) -> MktPair:

        broker, key, suffix = unpack_fqme(fqme)

        # XXX: loading from a fqme string will
        # leave this pair as "un resolved" meaning
        # we don't yet have `.dst` set as an `Asset`
        # which we expect to be filled in by some
        # backend client with access to that data-info.
        return cls(
            dst=key,  # not resolved
            price_tick=price_tick,
            size_tick=size_tick,
            bs_mktid=bs_mktid,
            broker=broker,
        )

    @property
    def key(self) -> str:
        '''
        The "endpoint key" for this market.

        Eg. mnq/usd or btc/usdt or xmr/btc

        In most other tina platforms this is referred to as the
        "symbol".

        '''
        return maybe_cons_tokens([self.dst, self.src])

    # NOTE: the main idea behind an fqme is to map a "market address"
    # to some endpoint from a transaction provider (eg. a broker) such
    # that we build a table of `fqme: str -> bs_mktid: Any` where any "piker
    # market address" maps 1-to-1 to some broker trading endpoint.
    # @cached_property
    @property
    def fqme(self) -> str:
        '''
        Return the fully qualified market endpoint-address for the
        pair of transacting assets.

        fqme = "fully qualified market endpoint"

        And yes, you pronounce it colloquially as read..

        Basically the idea here is for all client code (consumers of piker's
        APIs which query the data/broker-provider agnostic layer(s)) should be
        able to tell which backend / venue / derivative each data feed/flow is
        from by an explicit string-key of the current form:

        <market-instrument-name>
            .<venue>
            .<expiry>
            .<derivative-suffix-info>
            .<brokerbackendname>

        eg. for an explicit daq mini futes contract: mnq.cme.20230317.ib

        TODO: I have thoughts that we should actually change this to be
        more like an "attr lookup" (like how the web should have done
        urls, but marketting peeps ruined it etc. etc.)

        <broker>.<venue>.<instrumentname>.<suffixwithmetadata>

        TODO:
        See community discussion on naming and nomenclature, order
        of addressing hierarchy, general schema, internal representation:

        https://github.com/pikers/piker/issues/467

        '''
        return maybe_cons_tokens([
            self.key,  # final "pair name" (eg. qqq[/usd], btcusdt)
            self.venue,
            self.expiry,
            self.broker,
        ])

    @property
    def fqsn(self) -> str:
        return self.fqme

    def quantize(
        self,
        size: float,

        quantity_type: Literal['price', 'size'] = 'size',

    ) -> Decimal:
        '''
        Truncate input ``size: float`` using ``Decimal``
        and ``.size_tick``'s # of digits.

        '''
        match quantity_type:
            case 'price':
                digits = float_digits(self.price_tick)
            case 'size':
                digits = float_digits(self.size_tick)

        return Decimal(size).quantize(
            Decimal(f'1.{"0".ljust(digits, "0")}'),
            rounding=ROUND_HALF_EVEN
        )

    # @property
    # def size_tick_digits(self) -> int:
    #     return float_digits(self.size_tick)

    # TODO: BACKWARD COMPAT, TO REMOVE?
    @property
    def type_key(self) -> str:
        return str(self.dst.atype)

    @property
    def tick_size_digits(self) -> int:
        return float_digits(self.price_tick)

    @property
    def lot_size_digits(self) -> int:
        return float_digits(self.size_tick)


def unpack_fqme(
    fqme: str,
) -> tuple[str, str, str]:
    '''
    Unpack a fully-qualified-symbol-name to ``tuple``.

    '''
    venue = ''
    suffix = ''

    # TODO: probably reverse the order of all this XD
    tokens = fqme.split('.')

    match tokens:
        case [mkt_ep, broker]:
            # probably crypto
            # mkt_ep, broker = tokens
            return (
                broker,
                mkt_ep,
                '',
            )

        # TODO: swap venue and suffix/deriv-info here?
        case [mkt_ep, venue, suffix, broker]:
            pass

        case [mkt_ep, venue, broker]:
            suffix = ''

        case _:
            raise ValueError(f'Invalid fqme: {fqme}')

    return (
        broker,
        '.'.join([mkt_ep, venue]),
        suffix,
    )


unpack_fqsn = unpack_fqme


class Symbol(Struct):
    '''
    I guess this is some kinda container thing for dealing with
    all the different meta-data formats from brokers?

    '''
    key: str
    tick_size: float = 0.01
    lot_tick_size: float = 0.0  # "volume" precision as min step value
    suffix: str = ''
    broker_info: dict[str, dict[str, Any]] = {}

    @classmethod
    def from_fqsn(
        cls,
        fqsn: str,
        info: dict[str, Any],

    ) -> Symbol:
        broker, key, suffix = unpack_fqsn(fqsn)
        tick_size = info.get('price_tick_size', 0.01)
        lot_size = info.get('lot_tick_size', 0.0)

        return Symbol(
            key=key,
            tick_size=tick_size,
            lot_tick_size=lot_size,
            suffix=suffix,
            broker_info={broker: info},
        )

    # compat name mapping
    from_fqme = from_fqsn

    @property
    def type_key(self) -> str:
        return list(self.broker_info.values())[0]['asset_type']

    @property
    def tick_size_digits(self) -> int:
        return float_digits(self.lot_tick_size)

    @property
    def lot_size_digits(self) -> int:
        return float_digits(self.lot_tick_size)

    @property
    def broker(self) -> str:
        return list(self.broker_info.keys())[0]

    @property
    def fqsn(self) -> str:
        broker = self.broker
        key = self.key
        if self.suffix:
            tokens = (key, self.suffix, broker)
        else:
            tokens = (key, broker)

        return '.'.join(tokens).lower()

    fqme = fqsn

    def quantize(
        self,
        size: float,
    ) -> Decimal:
        digits = float_digits(self.lot_tick_size)
        return Decimal(size).quantize(
            Decimal(f'1.{"0".ljust(digits, "0")}'),
            rounding=ROUND_HALF_EVEN
        )