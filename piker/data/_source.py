# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship for pikers)

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

from bidict import bidict
import numpy as np


def_iohlcv_fields: list[tuple[str, type]] = [

    # YES WE KNOW, this isn't needed in polars but we use it for doing
    # ring-buffer like pre/append ops our our `ShmArray` real-time
    # numpy-array buffering system such that there is a master index
    # that can be used for index-arithmetic when write data to the
    # "middle" of the array. See the ``tractor.ipc.shm`` pkg for more
    # details.
    ('index', int),

    # presume int for epoch stamps since it's most common
    # and makes the most sense to avoid float rounding issues.
    # TODO: if we want higher reso we should use the new
    # ``time.time_ns()`` in python 3.10+
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),

    # TODO: can we elim this from default field set to save on mem?
    # i think only kraken really uses this in terms of what we get from
    # their ohlc history API?
    ('bar_wap', float),  # shouldn't be default right?
]

# remove index field
def_ohlcv_fields: list[tuple[str, type]] = def_iohlcv_fields.copy()
def_ohlcv_fields.pop(0)
assert (len(def_iohlcv_fields) - len(def_ohlcv_fields)) == 1

# TODO: for now need to construct this manually for readonly arrays, see
# https://github.com/numba/numba/issues/4511
# from numba import from_dtype
# base_ohlc_dtype = np.dtype(def_ohlc_fields)
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
