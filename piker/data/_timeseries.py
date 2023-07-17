# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of pikers)

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
Financial time series processing utilities usually
pertaining to OHLCV style sampled data.

Routines are generally implemented in either ``numpy`` or ``polars`` B)

'''
from __future__ import annotations
from typing import Literal
from math import (
    ceil,
    floor,
)

import numpy as np
import polars as pl

from ._sharedmem import ShmArray
from .._profile import (
    Profiler,
    pg_profile_enabled,
    ms_slower_then,
)


def slice_from_time(
    arr: np.ndarray,
    start_t: float,
    stop_t: float,
    step: float,  # sampler period step-diff

) -> slice:
    '''
    Calculate array indices mapped from a time range and return them in
    a slice.

    Given an input array with an epoch `'time'` series entry, calculate
    the indices which span the time range and return in a slice. Presume
    each `'time'` step increment is uniform and when the time stamp
    series contains gaps (the uniform presumption is untrue) use
    ``np.searchsorted()`` binary search to look up the appropriate
    index.

    '''
    profiler = Profiler(
        msg='slice_from_time()',
        disabled=not pg_profile_enabled(),
        ms_threshold=ms_slower_then,
    )

    times = arr['time']
    t_first = floor(times[0])
    t_last = ceil(times[-1])

    # the greatest index we can return which slices to the
    # end of the input array.
    read_i_max = arr.shape[0]

    # compute (presumed) uniform-time-step index offsets
    i_start_t = floor(start_t)
    read_i_start = floor(((i_start_t - t_first) // step)) - 1

    i_stop_t = ceil(stop_t)

    # XXX: edge case -> always set stop index to last in array whenever
    # the input stop time is detected to be greater then the equiv time
    # stamp at that last entry.
    if i_stop_t >= t_last:
        read_i_stop = read_i_max
    else:
        read_i_stop = ceil((i_stop_t - t_first) // step) + 1

    # always clip outputs to array support
    # for read start:
    # - never allow a start < the 0 index
    # - never allow an end index > the read array len
    read_i_start = min(
        max(0, read_i_start),
        read_i_max - 1,
    )
    read_i_stop = max(
        0,
        min(read_i_stop, read_i_max),
    )

    # check for larger-then-latest calculated index for given start
    # time, in which case we do a binary search for the correct index.
    # NOTE: this is usually the result of a time series with time gaps
    # where it is expected that each index step maps to a uniform step
    # in the time stamp series.
    t_iv_start = times[read_i_start]
    if (
        t_iv_start > i_start_t
    ):
        # do a binary search for the best index mapping to ``start_t``
        # given we measured an overshoot using the uniform-time-step
        # calculation from above.

        # TODO: once we start caching these per source-array,
        # we can just overwrite ``read_i_start`` directly.
        new_read_i_start = np.searchsorted(
            times,
            i_start_t,
            side='left',
        )

        # TODO: minimize binary search work as much as possible:
        # - cache these remap values which compensate for gaps in the
        #   uniform time step basis where we calc a later start
        #   index for the given input ``start_t``.
        # - can we shorten the input search sequence by heuristic?
        #   up_to_arith_start = index[:read_i_start]

        if (
            new_read_i_start <= read_i_start
        ):
            # t_diff = t_iv_start - start_t
            # print(
            #     f"WE'RE CUTTING OUT TIME - STEP:{step}\n"
            #     f'start_t:{start_t} -> 0index start_t:{t_iv_start}\n'
            #     f'diff: {t_diff}\n'
            #     f'REMAPPED START i: {read_i_start} -> {new_read_i_start}\n'
            # )
            read_i_start = new_read_i_start

    t_iv_stop = times[read_i_stop - 1]
    if (
        t_iv_stop > i_stop_t
    ):
        # t_diff = stop_t - t_iv_stop
        # print(
        #     f"WE'RE CUTTING OUT TIME - STEP:{step}\n"
        #     f'calced iv stop:{t_iv_stop} -> stop_t:{stop_t}\n'
        #     f'diff: {t_diff}\n'
        #     # f'SHOULD REMAP STOP: {read_i_start} -> {new_read_i_start}\n'
        # )
        new_read_i_stop = np.searchsorted(
            times[read_i_start:],
            # times,
            i_stop_t,
            side='right',
        )

        if (
            new_read_i_stop <= read_i_stop
        ):
            read_i_stop = read_i_start + new_read_i_stop + 1

    # sanity checks for range size
    # samples = (i_stop_t - i_start_t) // step
    # index_diff = read_i_stop - read_i_start + 1
    # if index_diff > (samples + 3):
    #     breakpoint()

    # read-relative indexes: gives a slice where `shm.array[read_slc]`
    # will be the data spanning the input time range `start_t` ->
    # `stop_t`
    read_slc = slice(
        int(read_i_start),
        int(read_i_stop),
    )

    profiler(
        'slicing complete'
        # f'{start_t} -> {abs_slc.start} | {read_slc.start}\n'
        # f'{stop_t} -> {abs_slc.stop} | {read_slc.stop}\n'
    )

    # NOTE: if caller needs absolute buffer indices they can
    # slice the buffer abs index like so:
    # index = arr['index']
    # abs_indx = index[read_slc]
    # abs_slc = slice(
    #     int(abs_indx[0]),
    #     int(abs_indx[-1]),
    # )

    return read_slc


def detect_null_time_gap(
    shm: ShmArray,
    imargin: int = 1,

) -> tuple[float, float] | None:
    '''
    Detect if there are any zero-epoch stamped rows in
    the presumed 'time' field-column.

    Filter to the gap and return a surrounding index range.

    NOTE: for now presumes only ONE gap XD

    '''
    zero_pred: np.ndarray = shm.array['time'] == 0
    zero_t: np.ndarray = shm.array[zero_pred]
    if zero_t.size:
        istart, iend = zero_t['index'][[0, -1]]
        start, end = shm._array['time'][
            [istart - imargin, iend + imargin]
        ]
        return (
            istart - imargin,
            start,
            end,
            iend + imargin,
        )

    return None


t_unit: Literal = Literal[
    'days',
    'hours',
    'minutes',
    'seconds',
    'miliseconds',
    'microseconds',
    'nanoseconds',
]


def with_dts(
    df: pl.DataFrame,
    time_col: str = 'time',
) -> pl.DataFrame:
    '''
    Insert datetime (casted) columns to a (presumably) OHLC sampled
    time series with an epoch-time column keyed by ``time_col``.

    '''
    return df.with_columns([
        pl.col(time_col).shift(1).suffix('_prev'),
        pl.col(time_col).diff().alias('s_diff'),
        pl.from_epoch(pl.col(time_col)).alias('dt'),
    ]).with_columns([
        pl.from_epoch(pl.col(f'{time_col}_prev')).alias('dt_prev'),
        pl.col('dt').diff().alias('dt_diff'),
    ]) #.with_columns(
        # pl.col('dt').diff().dt.days().alias('days_dt_diff'),
    # )


def detect_time_gaps(
    df: pl.DataFrame,

    time_col: str = 'time',
    # epoch sampling step diff
    expect_period: float = 60,

    # datetime diff unit and gap value
    # crypto mkts
    # gap_dt_unit: t_unit = 'minutes',
    # gap_thresh: int = 1,

    # legacy stock mkts
    gap_dt_unit: t_unit = 'days',
    gap_thresh: int = 2,

) -> pl.DataFrame:
    '''
    Filter to OHLC datums which contain sample step gaps.

    For eg. legacy markets which have venue close gaps and/or
    actual missing data segments.

    '''
    dt_gap_col: str = f'{gap_dt_unit}_diff'
    return with_dts(
        df
    ).filter(
        pl.col('s_diff').abs() > expect_period
    ).with_columns(
        getattr(
            pl.col('dt_diff').dt,
            gap_dt_unit,  # NOTE: must be valid ``Expr.dt.<name>``
        )().alias(dt_gap_col)
    ).filter(
        pl.col(dt_gap_col).abs() > gap_thresh
    )


def detect_price_gaps(
    df: pl.DataFrame,
    gt_multiplier: float = 2.,
    price_fields: list[str] = ['high', 'low'],

) -> pl.DataFrame:
    '''
    Detect gaps in clearing price over an OHLC series.

    2 types of gaps generally exist; up gaps and down gaps:

    - UP gap: when any next sample's lo price is strictly greater
      then the current sample's hi price.

    - DOWN gap: when any next sample's hi price is strictly
      less then the current samples lo price.

    '''
    # return df.filter(
    #     pl.col('high') - ) > expect_period,
    # ).select([
    #     pl.dt.datetime(pl.col(time_col).shift(1)).suffix('_previous'),
    #     pl.all(),
    # ]).select([
    #     pl.all(),
    #     (pl.col(time_col) - pl.col(f'{time_col}_previous')).alias('diff'),
    # ])
    ...
