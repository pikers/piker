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
from ..accounting._mktinfo import (
    # mkfqsn,
    unpack_fqsn,
    # digits_to_dec,
    float_digits,
)

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
# from numba import from_dtype
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


def ohlc_zeros(length: int) -> np.ndarray:
    """Construct an OHLC field formatted structarray.

    For "why a structarray" see here: https://stackoverflow.com/a/52443038
    Bottom line, they're faster then ``np.recarray``.

    """
    return np.zeros(length, dtype=base_ohlc_dtype)


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
