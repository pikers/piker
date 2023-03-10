# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship for piker0)

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
numpy data source coversion helpers.
"""
from __future__ import annotations
from decimal import (
    Decimal,
    ROUND_HALF_EVEN,
)
from typing import Any

from bidict import bidict
import numpy as np

from .types import Struct
# from numba import from_dtype


ohlc_fields = [
    ('time', float),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('bar_wap', float),
]

ohlc_with_index = ohlc_fields.copy()
ohlc_with_index.insert(0, ('index', int))

# our minimum structured array layout for ohlc data
base_iohlc_dtype = np.dtype(ohlc_with_index)
base_ohlc_dtype = np.dtype(ohlc_fields)

# TODO: for now need to construct this manually for readonly arrays, see
# https://github.com/numba/numba/issues/4511
# numba_ohlc_dtype = from_dtype(base_ohlc_dtype)

# map time frame "keys" to seconds values
tf_in_1s = bidict({
    1: '1s',
    60: '1m',
    60*5: '5m',
    60*15: '15m',
    60*30: '30m',
    60*60: '1h',
    60*60*24: '1d',
})


def mk_fqsn(
    provider: str,
    symbol: str,

) -> str:
    '''
    Generate a "fully qualified symbol name" which is
    a reverse-hierarchical cross broker/provider symbol

    '''
    return '.'.join([symbol, provider]).lower()


def float_digits(
    value: float,
) -> int:
    '''
    Return the number of precision digits read from a float value.

    '''
    if value == 0:
        return 0

    return int(-Decimal(str(value)).as_tuple().exponent)


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


def ohlc_zeros(length: int) -> np.ndarray:
    """Construct an OHLC field formatted structarray.

    For "why a structarray" see here: https://stackoverflow.com/a/52443038
    Bottom line, they're faster then ``np.recarray``.

    """
    return np.zeros(length, dtype=base_ohlc_dtype)


def unpack_fqsn(fqsn: str) -> tuple[str, str, str]:
    '''
    Unpack a fully-qualified-symbol-name to ``tuple``.

    '''
    venue = ''
    suffix = ''

    # TODO: probably reverse the order of all this XD
    tokens = fqsn.split('.')
    if len(tokens) < 3:
        # probably crypto
        symbol, broker = tokens
        return (
            broker,
            symbol,
            '',
        )

    elif len(tokens) > 3:
        symbol, venue, suffix, broker = tokens
    else:
        symbol, venue, broker = tokens
        suffix = ''

    # head, _, broker = fqsn.rpartition('.')
    # symbol, _, suffix = head.rpartition('.')
    return (
        broker,
        '.'.join([symbol, venue]),
        suffix,
    )


class MktPair(Struct, frozen=True):

    src: str  # source asset name being used to buy
    src_type: str  # source asset's financial type/classification name
    # ^ specifies a "class" of financial instrument
    # egs. stock, futer, option, bond etc.

    dst: str  # destination asset name being bought
    dst_type: str  # destination asset's financial type/classification name

    price_tick: float  # minimum price increment value increment
    price_tick_digits: int  # required decimal digits for above

    size_tick: float  # minimum size (aka vlm) increment value increment
    size_tick_digits: int  # required decimal digits for above

    venue: str | None = None  # market venue provider name
    expiry: str | None = None  # for derivs, expiry datetime parseable str

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
        ...

    # fqa, fqma, .. etc. see issue:
    # https://github.com/pikers/piker/issues/467
    @property
    def fqsn(self) -> str:
        '''
        Return the fully qualified market (endpoint) name for the
        pair of transacting assets.

        '''
        ...


# TODO: rework the below `Symbol` (which was originally inspired and
# derived from stuff in quantdom) into a simpler, ipc msg ready, market
# endpoint meta-data container type as per the drafted interace above.
class Symbol(Struct):
    '''
    I guess this is some kinda container thing for dealing with
    all the different meta-data formats from brokers?

    '''
    key: str
    tick_size: float = 0.01
    lot_tick_size: float = 0.0  # "volume" precision as min step value
    tick_size_digits: int = 2
    lot_size_digits: int = 0
    suffix: str = ''
    broker_info: dict[str, dict[str, Any]] = {}

    @classmethod
    def from_broker_info(
        cls,
        broker: str,
        symbol: str,
        info: dict[str, Any],
        suffix: str = '',

    ) -> Symbol:

        tick_size = info.get('price_tick_size', 0.01)
        lot_size = info.get('lot_tick_size', 0.0)

        return Symbol(
            key=symbol,

            tick_size=tick_size,
            lot_tick_size=lot_size,

            tick_size_digits=float_digits(tick_size),
            lot_size_digits=float_digits(lot_size),

            suffix=suffix,
            broker_info={broker: info},
        )

    @classmethod
    def from_fqsn(
        cls,
        fqsn: str,
        info: dict[str, Any],

    ) -> Symbol:
        broker, key, suffix = unpack_fqsn(fqsn)
        return cls.from_broker_info(
            broker,
            key,
            info=info,
            suffix=suffix,
        )

    @property
    def type_key(self) -> str:
        return list(self.broker_info.values())[0]['asset_type']

    @property
    def brokers(self) -> list[str]:
        return list(self.broker_info.keys())

    def nearest_tick(self, value: float) -> float:
        '''
        Return the nearest tick value based on mininum increment.

        '''
        mult = 1 / self.tick_size
        return round(value * mult) / mult

    def front_feed(self) -> tuple[str, str]:
        '''
        Return the "current" feed key for this symbol.

        (i.e. the broker + symbol key in a tuple).

        '''
        return (
            list(self.broker_info.keys())[0],
            self.key,
        )

    def tokens(self) -> tuple[str]:
        broker, key = self.front_feed()
        if self.suffix:
            return (key, self.suffix, broker)
        else:
            return (key, broker)

    @property
    def fqsn(self) -> str:
        return '.'.join(self.tokens()).lower()

    def front_fqsn(self) -> str:
        '''
        fqsn = "fully qualified symbol name"

        Basically the idea here is for all client-ish code (aka programs/actors
        that ask the provider agnostic layers in the stack for data) should be
        able to tell which backend / venue / derivative each data feed/flow is
        from by an explicit string key of the current form:

        <instrumentname>.<venue>.<suffixwithmetadata>.<brokerbackendname>

        TODO: I have thoughts that we should actually change this to be
        more like an "attr lookup" (like how the web should have done
        urls, but marketting peeps ruined it etc. etc.):

        <broker>.<venue>.<instrumentname>.<suffixwithmetadata>

        '''
        tokens = self.tokens()
        fqsn = '.'.join(map(str.lower, tokens))
        return fqsn

    def quantize_size(
        self,
        size: float,

    ) -> Decimal:
        '''
        Truncate input ``size: float`` using ``Decimal``
        and ``.lot_size_digits``.

        '''
        digits = self.lot_size_digits
        return Decimal(size).quantize(
            Decimal(f'1.{"0".ljust(digits, "0")}'),
            rounding=ROUND_HALF_EVEN
        )


def _nan_to_closest_num(array: np.ndarray):
    """Return interpolated values instead of NaN.

    """
    for col in ['open', 'high', 'low', 'close']:
        mask = np.isnan(array[col])
        if not mask.size:
            continue
        array[col][mask] = np.interp(
            np.flatnonzero(mask), np.flatnonzero(~mask), array[col][~mask]
        )
