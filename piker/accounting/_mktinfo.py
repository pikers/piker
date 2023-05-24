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

    # if we can't figure it out, presume the worst XD
    'unknown',
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


def dec_digits(
    value: float | str | Decimal,

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


float_digits = dec_digits


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
    atype: str  # AssetTypeName

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

    @classmethod
    def guess_from_mkt_ep_key(
        cls,
        mkt_ep_key: str,
        atype: str | None = None,

    ) -> Asset:
        '''
        A hacky guess method for presuming a (target) asset's properties
        based on either the actualy market endpoint key, or config settings
        from the user.

        '''
        atype = atype or 'unknown'

        # attempt to strip off any source asset
        # via presumed syntax of:
        # - <dst>/<src>
        # - <dst>.<src>
        # - etc.
        for char in ['/', '.']:
            dst, _, src = mkt_ep_key.partition(char)
            if src:
                if not atype:
                    atype = 'fiat'
                break

        return Asset(
            name=dst,
            atype=atype,
            tx_tick=Decimal('0.01'),
        )


def maybe_cons_tokens(
    tokens: list[Any],
    delim_char: str = '.',
) -> str:
    '''
    Construct `str` output from a maybe-concatenation of input
    sequence of elements in ``tokens``.

    '''
    return delim_char.join(filter(bool, tokens)).lower()


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

    src: str | Asset = ''
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
    contract_info: list[str] | None = None

    _atype: str = ''

    # NOTE: when cast to `str` return fqme
    def __str__(self) -> str:
        return self.fqme

    @classmethod
    def from_msg(
        cls,
        msg: dict[str, Any],

    ) -> MktPair:
        '''
        Constructor for a received msg-dict normally received over IPC.

        '''
        dst_asset_msg = msg.pop('dst')
        src_asset_msg = msg.pop('src')

        if isinstance(dst_asset_msg, str):
            src: str = str(src_asset_msg)
            assert isinstance(src, str)
            return cls.from_fqme(
                dst_asset_msg,
                src=src,
                **msg,
            )

        else:
            # NOTE: we call `.copy()` here to ensure
            # type casting!
            dst = Asset(**dst_asset_msg).copy()
            if not isinstance(src_asset_msg, str):
                src = Asset(**src_asset_msg).copy()
            else:
                src = str(src_asset_msg)

        return cls(
            dst=dst,
            src=src,
            **msg,
        # XXX NOTE: ``msgspec`` can encode `Decimal`
        # but it doesn't decide to it by default since
        # we aren't spec-cing these msgs as structs, SO
        # we have to ensure we do a struct type case (which `.copy()`
        # does) to ensure we get the right type!
        ).copy()

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

        broker: str | None = None,
        **kwargs,

    ) -> MktPair:

        _fqme: str = fqme
        if (
            broker
            and broker not in fqme
        ):
            _fqme = f'{fqme}.{broker}'

        broker, mkt_ep_key, venue, suffix = unpack_fqme(_fqme)
        dst: Asset = Asset.guess_from_mkt_ep_key(
            mkt_ep_key,
            atype=kwargs.get('_atype'),
        )

        # XXX: loading from a fqme string will
        # leave this pair as "un resolved" meaning
        # we don't yet have `.dst` set as an `Asset`
        # which we expect to be filled in by some
        # backend client with access to that data-info.
        return cls(
            # XXX: not resolved to ``Asset`` :(
            dst=dst,

            broker=broker,
            venue=venue,
            # XXX NOTE: we presume this token
            # if the expiry for now!
            expiry=suffix,

            price_tick=price_tick,
            size_tick=size_tick,
            bs_mktid=bs_mktid,

            **kwargs,

        ).copy()

    @property
    def key(self) -> str:
        '''
        The "endpoint key" for this market.

        '''
        return self.pair

    def pair(
        self,
        delim_char: str | None = None,
    ) -> str:
        '''
        The "endpoint asset pair key" for this market.
        Eg. mnq/usd or btc/usdt or xmr/btc

        In most other tina platforms this is referred to as the
        "symbol".

        '''
        return maybe_cons_tokens(
            [str(self.dst),
             str(self.src)],
            # TODO: make the default '/'
            delim_char=delim_char or '',
        )

    @property
    def suffix(self) -> str:
        '''
        The "contract suffix" for this market.

        Eg. mnq/usd.20230616.cme.ib
                    ^ ----- ^
        or tsla/usd.20230324.200c.cboe.ib
                    ^ ---------- ^

        In most other tina platforms they only show you these details in
        some kinda "meta data" format, we have FQMEs so we do this up
        front and explicit.

        '''
        field_strs = [self.expiry]
        con_info = self.contract_info
        if con_info is not None:
            field_strs.extend(con_info)

        return maybe_cons_tokens(field_strs)

    def get_fqme(
        self,

        # NOTE: allow dropping the source asset from the
        # market endpoint's pair key. Eg. to change
        # mnq/usd.<> -> mnq.<> which is useful when
        # searching (legacy) stock exchanges.
        without_src: bool = False,
        delim_char: str | None = None,

    ) -> str:
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
        key: str = (
            self.pair(delim_char=delim_char)
            if not without_src
            else str(self.dst)
        )

        return maybe_cons_tokens([
            key,  # final "pair name" (eg. qqq[/usd], btcusdt)
            self.venue,
            self.suffix,  # includes expiry and other con info
            self.broker,
        ])

    # NOTE: the main idea behind an fqme is to map a "market address"
    # to some endpoint from a transaction provider (eg. a broker) such
    # that we build a table of `fqme: str -> bs_mktid: Any` where any "piker
    # market address" maps 1-to-1 to some broker trading endpoint.
    # @cached_property
    fqme = property(get_fqme)

    def get_bs_fqme(
        self,
        **kwargs,
    ) -> str:
        '''
        FQME sin broker part XD

        '''
        sin_broker, *_ = self.get_fqme(**kwargs).rpartition('.')
        return sin_broker

    bs_fqme = property(get_bs_fqme)

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

    # TODO: BACKWARD COMPAT, TO REMOVE?
    @property
    def type_key(self) -> str:
        if isinstance(self.dst, Asset):
            return str(self.dst.atype)

        return self._atype

    @property
    def price_tick_digits(self) -> int:
        return float_digits(self.price_tick)

    @property
    def size_tick_digits(self) -> int:
        return float_digits(self.size_tick)


def unpack_fqme(
    fqme: str,

    broker: str | None = None

) -> tuple[str, ...]:
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
            return (
                broker,
                mkt_ep,
                '',
                '',
            )

        # TODO: swap venue and suffix/deriv-info here?
        case [mkt_ep, venue, suffix, broker]:
            pass

        # handle `bs_mktid` + `broker` input case
        case [
            mkt_ep, venue, suffix
        ] if (
            broker
            and suffix != broker
        ):
            pass

        case [mkt_ep, venue, broker]:
            suffix = ''

        case _:
            raise ValueError(f'Invalid fqme: {fqme}')

    return (
        broker,
        mkt_ep,
        venue,
        # '.'.join([mkt_ep, venue]),
        suffix,
    )


class Symbol(Struct):
    '''
    I guess this is some kinda container thing for dealing with
    all the different meta-data formats from brokers?

    '''
    key: str

    broker: str = ''
    venue: str = ''

    # precision descriptors for price and vlm
    tick_size: Decimal = Decimal('0.01')
    lot_tick_size: Decimal = Decimal('0.0')

    suffix: str = ''
    broker_info: dict[str, dict[str, Any]] = {}

    @classmethod
    def from_fqme(
        cls,
        fqsn: str,
        info: dict[str, Any],

    ) -> Symbol:
        broker, mktep, venue, suffix = unpack_fqme(fqsn)
        tick_size = info.get('price_tick_size', 0.01)
        lot_size = info.get('lot_tick_size', 0.0)

        return Symbol(
            broker=broker,
            key=mktep,
            tick_size=tick_size,
            lot_tick_size=lot_size,
            venue=venue,
            suffix=suffix,
            broker_info={broker: info},
        )

    @property
    def type_key(self) -> str:
        return list(self.broker_info.values())[0]['asset_type']

    @property
    def tick_size_digits(self) -> int:
        return float_digits(self.tick_size)

    @property
    def lot_size_digits(self) -> int:
        return float_digits(self.lot_tick_size)

    @property
    def price_tick(self) -> Decimal:
        return Decimal(str(self.tick_size))

    @property
    def size_tick(self) -> Decimal:
        return Decimal(str(self.lot_tick_size))

    @property
    def broker(self) -> str:
        return list(self.broker_info.keys())[0]

    @property
    def fqme(self) -> str:
        return maybe_cons_tokens([
            self.key,  # final "pair name" (eg. qqq[/usd], btcusdt)
            self.venue,
            self.suffix,  # includes expiry and other con info
            self.broker,
        ])

    def quantize(
        self,
        size: float,
    ) -> Decimal:
        digits = float_digits(self.lot_tick_size)
        return Decimal(size).quantize(
            Decimal(f'1.{"0".ljust(digits, "0")}'),
            rounding=ROUND_HALF_EVEN
        )

    # NOTE: when cast to `str` return fqme
    def __str__(self) -> str:
        return self.fqme
