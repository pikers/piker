"""
Numpy data source machinery.
"""
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


OHLC_dtype = np.dtype(
    [
        ('id', int),
        ('time', float),
        ('open', float),
        ('high', float),
        ('low', float),
        ('close', float),
        ('volume', int),
    ]
)

# tf = {
#     1: TimeFrame.M1,
#     5: TimeFrame.M5,
#     15: TimeFrame.M15,
#     30: TimeFrame.M30,
#     60: TimeFrame.H1,
#     240: TimeFrame.H4,
#     1440: TimeFrame.D1,
# }


def ohlc_zeros(length: int) -> np.ndarray:
    """Construct an OHLC field formatted structarray.

    For "why a structarray" see here: https://stackoverflow.com/a/52443038
    Bottom line, they're faster then ``np.recarray``.
    """
    return np.zeros(length, dtype=OHLC_dtype)


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
        return int(math.log(self.min_tick, 0.1))


def from_df(
    df: pd.DataFrame,
    source=None,
    default_tf=None
) -> np.recarray:
    """Convert OHLC formatted ``pandas.DataFrame`` to ``numpy.recarray``.
    """
    df.reset_index(inplace=True)
    df['Date'] = [d.timestamp() for d in df.Date]

    # try to rename from some camel case
    columns={
        'Date': 'time',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
    }
    for name in df.columns:
        if name not in columns:
            del df[name]
    df = df.rename(columns=columns)
    array = df.to_records()
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
