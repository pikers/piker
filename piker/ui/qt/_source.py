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
):
    """Cast OHLC ``pandas.DataFrame`` to ``numpy.structarray``.
    """
    # shape = (len(df),)
    # array.resize(shape, refcheck=False)
    array = np.array([], dtype=OHLC_dtype)

    df.reset_index(inplace=True)
    df.insert(0, 'id', df.index)
    array['time'] = np.array([d.timestamp().time for d in df.Date])

    # try to rename from some camel case
    df = df.rename(
        columns={
            # 'Date': 'time',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
        }
    )
    for name in array.dtype.names:
        array[name] = df[name]

    array[:] = df[:]

    _nan_to_closest_num(array)
    # self._set_time_frame(default_tf)

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
