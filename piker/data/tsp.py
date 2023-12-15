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

Routines are generally implemented in either ``numpy`` or
``polars`` B)

'''
from __future__ import annotations
from functools import partial
from math import (
    ceil,
    floor,
)
import time
from typing import (
    Literal,
    # AsyncGenerator,
    Generator,
)

import numpy as np
import polars as pl
from pendulum import (
    DateTime,
    from_timestamp,
)

from ..toolz.profile import (
    Profiler,
    pg_profile_enabled,
    ms_slower_then,
)
from ..log import (
    get_logger,
    get_console_log,
)
# for "time series processing"
subsys: str = 'piker.tsp'

log = get_logger(subsys)
get_console_log = partial(
    get_console_log,
    name=subsys,
)

# NOTE: union type-defs to handle generic `numpy` and `polars` types
# side-by-side Bo
# |_ TODO: schema spec typing?
#   -[ ] nptyping!
#   -[ ] wtv we can with polars?
Frame = pl.DataFrame | np.ndarray
Seq = pl.Series | np.ndarray


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


def get_null_segs(
    frame: Frame,
    period: float,  # sampling step in seconds
    imargin: int = 1,
    col: str = 'time',

) -> tuple[
    # Seq,  # TODO: can we make it an array-type instead?
    list[
        list[int, int],
    ],
    Seq,
    Frame
] | None:
    '''
    Detect if there are any zero(-epoch stamped) valued
    rows in for the provided `col: str` column; by default
    presume the 'time' field/column.

    Filter to all such zero (time) segments and return
    the corresponding frame zeroed segment's,

      - gap absolute (in buffer terms) indices-endpoints as
        `absi_zsegs`
      - abs indices of all rows with zeroed `col` values as `absi_zeros`
      - the corresponding frame's row-entries (view) which are
        zeroed for the `col` as `zero_t`

    '''
    times: Seq = frame['time']
    zero_pred: Seq = (times == 0)

    if isinstance(frame, np.ndarray):
        tis_zeros: int = zero_pred.any()
    else:
        tis_zeros: int = zero_pred.any()

    if not tis_zeros:
        return None

    # TODO: use ndarray for this?!
    absi_zsegs: list[list[int, int]] = []

    if isinstance(frame, np.ndarray):
        # view of ONLY the zero segments as one continuous chunk
        zero_t: np.ndarray = frame[zero_pred]
        # abs indices of said zeroed rows
        absi_zeros = zero_t['index']
        # diff of abs index steps between each zeroed row
        absi_zdiff: np.ndarray = np.diff(absi_zeros)

        # scan for all frame-indices where the
        # zeroed-row-abs-index-step-diff is greater then the
        # expected increment of 1.
        # data  1st zero seg  data  zeros
        # ----  ------------  ----  -----  ------  ----
        # ||||..000000000000..||||..00000..||||||..0000
        # ----  ------------  ----  -----  ------  ----
        #       ^zero_t[0]                            ^zero_t[-1]
        #                           ^fi_zgaps[0]   ^fi_zgaps[1]
        #       ^absi_zsegs[0][0]   ^---^ => absi_zsegs[1]: tuple
        #  absi_zsegs[0][1]^
        #
        # NOTE: the first entry in `fi_zgaps` is where
        # the first (absolute) index step diff is > 1.
        # and it is a frame-relative index into `zero_t`.
        fi_zgaps = np.argwhere(
            absi_zdiff > 1
            # NOTE: +1 here is ensure we index to the "start" of each
            # segment (if we didn't the below loop needs to be
            # re-written to expect `fi_end_rows`!
        ) + 1
        # the rows from the contiguous zeroed segments which have
        # abs-index steps >1 compared to the previous zero row
        # (indicating an end of zeroed segment).
        fi_zseg_start_rows = zero_t[fi_zgaps]

    # TODO: equiv for pl.DataFrame case!
    else:
        izeros: pl.Series = zero_pred.arg_true()
        zero_t: pl.DataFrame = frame[izeros]

        absi_zeros = zero_t['index']
        absi_zdiff: pl.Series = absi_zeros.diff()
        fi_zgaps = (absi_zdiff > 1).arg_true()

    # XXX: our goal (in this func) is to select out slice index
    # pairs (zseg0_start, zseg_end) in abs index units for each
    # null-segment portion detected throughout entire input frame.

    # only up to one null-segment in entire frame?
    num_gaps: int = fi_zgaps.size + 1
    if num_gaps < 1:
        if absi_zeros.size > 1:
            absi_zsegs = [[
                # see `get_hist()` in backend, should ALWAYS be
                # able to handle a `start_dt=None`!
                # None,
                max(
                    absi_zeros[0] - 1,
                    0,
                ),
                # NOTE: need the + 1 to guarantee we index "up to"
                # the next non-null row-datum.
                min(
                    absi_zeros[-1] + 1,
                    frame['index'][-1],
                ),
            ]]
        else:
            # XXX EDGE CASE: only one null-datum found so
            # mark the start abs index as None to trigger
            # a full frame-len query to the respective backend?
            absi_zsegs = [[
                # see `get_hist()` in backend, should ALWAYS be
                # able to handle a `start_dt=None`!
                # None,
                None,
                absi_zeros[0] + 1,
            ]]

    # XXX NOTE XXX: if >= 2 zeroed segments are found, there should
    # ALWAYS be more then one zero-segment-abs-index-step-diff row
    # in `absi_zdiff`, so loop through all such
    # abs-index-step-diffs >1 (i.e. the entries of `absi_zdiff`)
    # and add them as the "end index" entries for each segment.
    # Then, iif NOT iterating the first such segment end, look back
    # for the prior segments zero-segment start indext by relative
    # indexing the `zero_t` frame by -1 and grabbing the abs index
    # of what should be the prior zero-segment abs start index.
    else:
        # NOTE: since `absi_zdiff` will never have a row
        # corresponding to the first zero-segment's row, we add it
        # manually here.
        absi_zsegs.append([
            absi_zeros[0] - 1,
            None,
        ])

        # TODO: can we do it with vec ops?
        for i, (
            fi,  # frame index of zero-seg start
            zseg_start_row,  # full row for ^
        ) in enumerate(zip(
            fi_zgaps,
            fi_zseg_start_rows,
        )):
            assert (zseg_start_row == zero_t[fi]).all()
            iabs: int = zseg_start_row['index'][0]
            absi_zsegs.append([
                iabs - 1,
                None,  # backfilled on next iter
            ])

            # row = zero_t[fi]
            # absi_pre_zseg = row['index'][0] - 1
            # absi_pre_zseg = absi - 1

            # final iter case, backfill FINAL end iabs!
            if (i + 1) == fi_zgaps.size:
                absi_zsegs[-1][1] = absi_zeros[-1] + 1

            # NOTE: only after the first segment (due to `.diff()`
            # usage above) can we do a lookback to the prior
            # segment's end row and determine it's abs index to
            # retroactively insert to the prior
            # `absi_zsegs[i-1][1]` entry Bo
            last_end: int = absi_zsegs[i][1]
            if last_end is None:
                prev_zseg_row = zero_t[fi - 1]
                absi_post_zseg = prev_zseg_row['index'][0] + 1
                # XXX: MUST BACKFILL previous end iabs!
                absi_zsegs[i][1] = absi_post_zseg

        else:
            if 0 < num_gaps < 2:
                absi_zsegs[-1][1] = absi_zeros[-1] + 1

            iabs_first: int = frame['index'][0]
            for start, end in absi_zsegs:
                ts_start: float = times[start - iabs_first]
                ts_end: float = times[end - iabs_first]
                if (
                    ts_start == 0
                    or
                    ts_end == 0
                ):
                    import pdbp
                    pdbp.set_trace()

                assert end
                assert start < end

    log.warning(
        f'Frame has {len(absi_zsegs)} NULL GAPS!?\n'
        f'period: {period}\n'
        f'total null samples: {len(zero_t)}\n'
    )

    return (
        absi_zsegs,  # [start, end] abs slice indices of seg
        absi_zeros,  # all abs indices within all null-segs
        zero_t,  # sliced-view of all null-segment rows-datums
    )


def iter_null_segs(
    timeframe: float,
    frame: Frame | None = None,
    null_segs: tuple | None = None,

) -> Generator[
    tuple[
        int, int,
        int, int,
        float, float,
        float, float,

        # Seq,  # TODO: can we make it an array-type instead?
        # list[
        #     list[int, int],
        # ],
        # Seq,
        # Frame
    ],
    None,
]:
    if null_segs is None:
        null_segs: tuple = get_null_segs(
            frame,
            period=timeframe,
        )

    absi_pairs_zsegs: list[list[float, float]]
    izeros: Seq
    zero_t: Frame
    (
        absi_pairs_zsegs,
        izeros,
        zero_t,
    ) = null_segs

    absi_first: int = frame[0]['index']
    for (
        absi_start,
        absi_end,
    ) in absi_pairs_zsegs:

        fi_end: int = absi_end - absi_first
        end_row: Seq = frame[fi_end]
        end_t: float = end_row['time']
        end_dt: DateTime = from_timestamp(end_t)

        fi_start = None
        start_row = None
        start_t = None
        start_dt = None
        if (
            absi_start is not None
            and start_t != 0
        ):
            fi_start: int = absi_start - absi_first
            start_row: Seq = frame[fi_start]
            start_t: float = start_row['time']
            start_dt: DateTime = from_timestamp(start_t)

        if absi_start < 0:
            import pdbp
            pdbp.set_trace()

        yield (
            absi_start, absi_end,  # abs indices
            fi_start, fi_end,  # relative "frame" indices
            start_t, end_t,
            start_dt, end_dt,
        )


def with_dts(
    df: pl.DataFrame,
    time_col: str = 'time',
) -> pl.DataFrame:
    '''
    Insert datetime (casted) columns to a (presumably) OHLC sampled
    time series with an epoch-time column keyed by `time_col: str`.

    '''
    return df.with_columns([
        pl.col(time_col).shift(1).suffix('_prev'),
        pl.col(time_col).diff().alias('s_diff'),
        pl.from_epoch(pl.col(time_col)).alias('dt'),
    ]).with_columns([
        pl.from_epoch(
            pl.col(f'{time_col}_prev')
        ).alias('dt_prev'),
        pl.col('dt').diff().alias('dt_diff'),
    ]) #.with_columns(
        # pl.col('dt').diff().dt.days().alias('days_dt_diff'),
    # )


def dedup_dt(
    df: pl.DataFrame,
) -> pl.DataFrame:
    '''
    Drop duplicate date-time rows (normally from an OHLC frame).

    '''
    return df.unique(
        subset=['dt'],
        maintain_order=True,
    )


t_unit: Literal = Literal[
    'days',
    'hours',
    'minutes',
    'seconds',
    'miliseconds',
    'microseconds',
    'nanoseconds',
]


def detect_time_gaps(
    df: pl.DataFrame,

    time_col: str = 'time',
    # epoch sampling step diff
    expect_period: float = 60,

    # datetime diff unit and gap value
    # crypto mkts
    # gap_dt_unit: t_unit = 'minutes',
    # gap_thresh: int = 1,

    # NOTE: legacy stock mkts have venue operating hours
    # and thus gaps normally no more then 1-2 days at
    # a time.
    # XXX -> must be valid ``polars.Expr.dt.<name>``
    # TODO: allow passing in a frame of operating hours
    # durations/ranges for faster legit gap checks.
    gap_dt_unit: t_unit = 'days',
    gap_thresh: int = 1,

) -> pl.DataFrame:
    '''
    Filter to OHLC datums which contain sample step gaps.

    For eg. legacy markets which have venue close gaps and/or
    actual missing data segments.

    '''
    return (
        with_dts(df)
        # First by a seconds unit step size
        .filter(
            pl.col('s_diff').abs() > expect_period
        )
        .filter(
            # Second by an arbitrary dt-unit step size
            getattr(
                pl.col('dt_diff').dt,
                gap_dt_unit,
            )().abs() > gap_thresh
        )
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


def dedupe(src_df: pl.DataFrame) -> tuple[
    pl.DataFrame,  # with dts
    pl.DataFrame,  # gaps
    pl.DataFrame,  # with deduplicated dts (aka gap/repeat removal)
    int,  # len diff between input and deduped
]:
    '''
    Check for time series gaps and if found
    de-duplicate any datetime entries, check for
    a frame height diff and return the newly
    dt-deduplicated frame.

    '''
    df: pl.DataFrame = with_dts(src_df)
    gaps: pl.DataFrame = detect_time_gaps(df)

    # if no gaps detected just return carbon copies
    # and no len diff.
    if gaps.is_empty():
        return (
            df,
            gaps,
            df,
            0,
        )

    # remove duplicated datetime samples/sections
    deduped: pl.DataFrame = dedup_dt(df)
    deduped_gaps = detect_time_gaps(deduped)

    diff: int = (
        df.height
        -
        deduped.height
    )
    log.warning(
        f'Gaps found:\n{gaps}\n'
        f'deduped Gaps found:\n{deduped_gaps}'
    )
    return (
        df,
        gaps,
        deduped,
        diff,
    )


def sort_diff(
    src_df: pl.DataFrame,
    col: str = 'time',

) -> tuple[
    pl.DataFrame,  # with dts
    pl.DataFrame,  # sorted
    list[int],  # indices of segments that are out-of-order
]:
    ser: pl.Series = src_df[col]
    sortd: pl.DataFrame = ser.sort()
    diff: pl.Series = ser.diff()

    sortd_diff: pl.Series = sortd.diff()
    i_step_diff = (diff != sortd_diff).arg_true()
    frame_reorders: int = i_step_diff.len()
    if frame_reorders:
        log.warn(
            f'Resorted frame on col: {col}\n'
            f'{frame_reorders}'

        )
        # import pdbp; pdbp.set_trace()

# NOTE: thanks to this SO answer for the below conversion routines
# to go from numpy struct-arrays to polars dataframes and back:
# https://stackoverflow.com/a/72054819
def np2pl(array: np.ndarray) -> pl.DataFrame:
    start = time.time()

    # XXX: thanks to this SO answer for this conversion tip:
    # https://stackoverflow.com/a/72054819
    df = pl.DataFrame({
        field_name: array[field_name]
        for field_name in array.dtype.fields
    })
    delay: float = round(
        time.time() - start,
        ndigits=6,
    )
    log.info(
        f'numpy -> polars conversion took {delay} secs\n'
        f'polars df: {df}'
    )
    return df


def pl2np(
    df: pl.DataFrame,
    dtype: np.dtype,

) -> np.ndarray:

    # Create numpy struct array of the correct size and dtype
    # and loop through df columns to fill in array fields.
    array = np.empty(
        df.height,
        dtype,
    )
    for field, col in zip(
        dtype.fields,
        df.columns,
    ):
        array[field] = df.get_column(col).to_numpy()

    return array
