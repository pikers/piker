# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
import decimal
from dataclasses import dataclass

import numpy as np
import pandas as pd
# from numba import from_dtype


ohlc_fields = [
    ('time', float),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', int),
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

# map time frame "keys" to minutes values
tf_in_1m = {
    '1m': 1,
    '5m':  5,
    '15m': 15,
    '30m':  30,
    '1h': 60,
    '4h': 240,
    '1d': 1440,
}


def float_digits(
    value: float,
) -> int:
    return int(-decimal.Decimal(str(value)).as_tuple().exponent)


def ohlc_zeros(length: int) -> np.ndarray:
    """Construct an OHLC field formatted structarray.

    For "why a structarray" see here: https://stackoverflow.com/a/52443038
    Bottom line, they're faster then ``np.recarray``.

    """
    return np.zeros(length, dtype=base_ohlc_dtype)


@dataclass
class Symbol:
    """I guess this is some kinda container thing for dealing with
    all the different meta-data formats from brokers?

    """
    key: str = ''
    min_tick: float = 0.01
    contract: str = ''

    def digits(self) -> int:
        """Return the trailing number of digits specified by the
        min tick size for the instrument.

        """
        return float_digits(self.min_tick)


def from_df(
    df: pd.DataFrame,
    source=None,
    default_tf=None
) -> np.recarray:
    """Convert OHLC formatted ``pandas.DataFrame`` to ``numpy.recarray``.

    """
    df.reset_index(inplace=True)

    # hackery to convert field names
    date = 'Date'
    if 'date' in df.columns:
        date = 'date'

    # convert to POSIX time
    df[date] = [d.timestamp() for d in df[date]]

    # try to rename from some camel case
    columns = {
        'Date': 'time',
        'date': 'time',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',

        # most feeds are providing this over sesssion anchored
        'vwap': 'bar_wap',

        # XXX: ib_insync calls this the "wap of the bar"
        # but no clue what is actually is...
        # https://github.com/pikers/piker/issues/119#issuecomment-729120988
        'average': 'bar_wap',
    }

    df = df.rename(columns=columns)

    for name in df.columns:
        # if name not in base_ohlc_dtype.names[1:]:
        if name not in base_ohlc_dtype.names:
            del df[name]

    # TODO: it turns out column access on recarrays is actually slower:
    # https://jakevdp.github.io/PythonDataScienceHandbook/02.09-structured-data-numpy.html#RecordArrays:-Structured-Arrays-with-a-Twist
    # it might make sense to make these structured arrays?
    array = df.to_records(index=False)
    _nan_to_closest_num(array)

    return array


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
