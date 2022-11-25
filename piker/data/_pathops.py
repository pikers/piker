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
Super fast ``QPainterPath`` generation related operator routines.

"""
from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
)

import msgspec
from msgspec import field
import numpy as np
from numpy.lib import recfunctions as rfn
from numba import (
    types,
    njit,
    float64,
    int64,
    optional,
)
from numba.core.types.misc import StringLiteral
# from numba.extending import as_numba_type

from ._sharedmem import (
    ShmArray,
)
# from ._source import numba_ohlc_dtype
from ._compression import (
    ds_m4,
)

if TYPE_CHECKING:
    from ._render import (
        Viz,
    )
    from .._profile import Profiler


class IncrementalFormatter(msgspec.Struct):
    '''
    Incrementally updating, pre-path-graphics tracking, formatter.

    Allows tracking source data state in an updateable pre-graphics
    ``np.ndarray`` format (in local process memory) as well as
    incrementally rendering from that format **to** 1d x/y for path
    generation using ``pg.functions.arrayToQPath()``.

    '''
    shm: ShmArray
    viz: Viz
    index_field: str = 'index'

    # last read from shm (usually due to an update call)
    _last_read: tuple[
        int,
        int,
        np.ndarray

    ]

    @property
    def last_read(self) -> tuple | None:
        return self._last_read

    def __repr__(self) -> str:
        msg = (
            f'{type(self)}: ->\n\n'
            f'fqsn={self.viz.name}\n'
            f'shm_name={self.shm.token["shm_name"]}\n\n'

            f'last_vr={self._last_vr}\n'
            f'last_ivdr={self._last_ivdr}\n\n'

            f'xy_nd_start={self.xy_nd_start}\n'
            f'xy_nd_stop={self.xy_nd_stop}\n\n'
        )

        x_nd_len = 0
        y_nd_len = 0
        if self.x_nd is not None:
            x_nd_len = len(self.x_nd)
            y_nd_len = len(self.y_nd)

        msg += (
            f'x_nd_len={x_nd_len}\n'
            f'y_nd_len={y_nd_len}\n'
        )

        return msg

    def diff(
        self,
        new_read: tuple[np.ndarray],

    ) -> tuple[
        np.ndarray,
        np.ndarray,
    ]:
        (
            last_xfirst,
            last_xlast,
            last_array,
            last_ivl,
            last_ivr,
            last_in_view,
        ) = self.last_read

        # TODO: can the renderer just call ``Viz.read()`` directly?
        # unpack latest source data read
        (
            xfirst,
            xlast,
            array,
            ivl,
            ivr,
            in_view,
        ) = new_read

        # compute the length diffs between the first/last index entry in
        # the input data and the last indexes we have on record from the
        # last time we updated the curve index.
        prepend_length = int(last_xfirst - xfirst)
        append_length = int(xlast - last_xlast)

        # blah blah blah
        # do diffing for prepend, append and last entry
        return (
            slice(xfirst, last_xfirst),
            prepend_length,
            append_length,
            slice(last_xlast, xlast),
        )

    # Incrementally updated xy ndarray formatted data, a pre-1d
    # format which is updated and cached independently of the final
    # pre-graphics-path 1d format.
    x_nd: Optional[np.ndarray] = None
    y_nd: Optional[np.ndarray] = None

    # indexes which slice into the above arrays (which are allocated
    # based on source data shm input size) and allow retrieving
    # incrementally updated data.
    xy_nd_start: int = 0
    xy_nd_stop: int = 0

    # TODO: eventually incrementally update 1d-pre-graphics path data?
    # x_1d: Optional[np.ndarray] = None
    # y_1d: Optional[np.ndarray] = None

    # incremental view-change state(s) tracking
    _last_vr: tuple[float, float] | None = None
    _last_ivdr: tuple[float, float] | None = None

    def _track_inview_range(
        self,
        view_range: tuple[int, int],

    ) -> bool:
        # if a view range is passed, plan to draw the
        # source ouput that's "in view" of the chart.
        vl, vr = view_range
        zoom_or_append = False
        last_vr = self._last_vr

        # incremental in-view data update.
        if last_vr:
            lvl, lvr = last_vr  # relative slice indices

            # TODO: detecting more specifically the interaction changes
            # last_ivr = self._last_ivdr or (vl, vr)
            # al, ar = last_ivr  # abs slice indices
            # left_change = abs(x_iv[0] - al) >= 1
            # right_change = abs(x_iv[-1] - ar) >= 1

            # likely a zoom/pan view change or data append update
            if (
                (vr - lvr) > 2
                or vl < lvl

                # append / prepend update
                # we had an append update where the view range
                # didn't change but the data-viewed (shifted)
                # underneath, so we need to redraw.
                # or left_change and right_change and last_vr == view_range

                # not (left_change and right_change) and ivr
                # (
                # or abs(x_iv[ivr] - livr) > 1
            ):
                zoom_or_append = True

        self._last_vr = view_range

        return zoom_or_append

    def format_to_1d(
        self,
        new_read: tuple,
        array_key: str,
        profiler: Profiler,

        slice_to_head: int = -1,
        read_src_from_key: bool = True,
        slice_to_inview: bool = True,

    ) -> tuple[
        np.ndarray,
        np.ndarray,
    ]:
        shm = self.shm

        (
            _,
            _,
            array,
            ivl,
            ivr,
            in_view,

        ) = new_read

        (
            pre_slice,
            prepend_len,
            append_len,
            post_slice,
        ) = self.diff(new_read)

        if self.y_nd is None:
            # we first need to allocate xy data arrays
            # from the source data.
            self.x_nd, self.y_nd = self.allocate_xy_nd(
                shm,
                array_key,
            )
            self.xy_nd_start = shm._first.value
            self.xy_nd_stop = shm._last.value
            profiler('allocated xy history')

        if prepend_len:
            y_prepend = shm._array[pre_slice]
            if read_src_from_key:
                y_prepend = y_prepend[array_key]

            (
                new_y_nd,
                y_nd_slc,

            ) = self.incr_update_xy_nd(
                shm,
                array_key,

                # this is the pre-sliced, "normally expected"
                # new data that an updater would normally be
                # expected to process, however in some cases (like
                # step curves) the updater routine may want to do
                # the source history-data reading itself, so we pass
                # both here.
                y_prepend,
                pre_slice,
                prepend_len,

                self.xy_nd_start,
                self.xy_nd_stop,
                is_append=False,
            )

            # y_nd_view = self.y_nd[y_nd_slc]
            self.y_nd[y_nd_slc] = new_y_nd
            # if read_src_from_key:
            #     y_nd_view[:][array_key] = new_y_nd
            # else:
            #     y_nd_view[:] = new_y_nd

            self.xy_nd_start = shm._first.value
            profiler('prepended xy history: {prepend_length}')

        if append_len:
            y_append = shm._array[post_slice]
            if read_src_from_key:
                y_append = y_append[array_key]

            (
                new_y_nd,
                y_nd_slc,

            ) = self.incr_update_xy_nd(
                shm,
                array_key,

                y_append,
                post_slice,
                append_len,

                self.xy_nd_start,
                self.xy_nd_stop,
                is_append=True,
            )
            # self.y_nd[post_slice] = new_y_nd
            # self.y_nd[xy_slice or post_slice] = xy_data
            self.y_nd[y_nd_slc] = new_y_nd
            # if read_src_from_key:
            #     y_nd_view[:][array_key] = new_y_nd
            # else:
            #     y_nd_view[:] = new_y_nd

            self.xy_nd_stop = shm._last.value
            profiler('appened xy history: {append_length}')

        view_changed: bool = False
        view_range: tuple[int, int] = (ivl, ivr)
        if slice_to_inview:
            view_changed = self._track_inview_range(view_range)
            array = in_view
            profiler(f'{self.viz.name} view range slice {view_range}')

        hist = array[:slice_to_head]

        # xy-path data transform: convert source data to a format
        # able to be passed to a `QPainterPath` rendering routine.
        if not len(hist):
            # XXX: this might be why the profiler only has exits?
            return

        # TODO: hist here should be the pre-sliced
        # x/y_data in the case where allocate_xy is
        # defined?
        x_1d, y_1d, connect = self.format_xy_nd_to_1d(
            hist,
            array_key,
            view_range,
        )

        # app_tres = None
        # if append_len:
        #     appended = array[-append_len-1:slice_to_head]
        #     app_tres = self.format_xy_nd_to_1d(
        #         appended,
        #         array_key,
        #         (
        #             view_range[1] - append_len + slice_to_head,
        #             view_range[1]
        #         ),
        #     )
        #     # assert (len(appended) - 1) == append_len
        #     # assert len(appended) == append_len
        #     print(
        #         f'{self.viz.name} APPEND LEN: {append_len}\n'
        #         f'{self.viz.name} APPENDED: {appended}\n'
        #         f'{self.viz.name} app_tres: {app_tres}\n'
        #     )

        # update the last "in view data range"
        if len(x_1d):
            self._last_ivdr = x_1d[0], x_1d[slice_to_head]

        # TODO: eventually maybe we can implement some kind of
        # transform on the ``QPainterPath`` that will more or less
        # detect the diff in "elements" terms?
        # update diff state since we've now rendered paths.
        self._last_read = new_read

        profiler('.format_to_1d()')
        return (
            x_1d,
            y_1d,
            connect,
            prepend_len,
            append_len,
            view_changed,
            # app_tres,
        )

    ###############################
    # Sub-type override interface #
    ###############################

    # optional pre-graphics xy formatted data which
    # is incrementally updated in sync with the source data.
    # XXX: was ``.allocate_xy()``
    def allocate_xy_nd(
        self,
        src_shm: ShmArray,
        data_field: str,

    ) -> tuple[
        np.ndarray,  # x
        np.nd.array  # y
    ]:
        '''
        Convert the structured-array ``src_shm`` format to
        a equivalently shaped (and field-less) ``np.ndarray``.

        Eg. a 4 field x N struct-array => (N, 4)

        '''
        y_nd = src_shm._array[data_field].copy()
        x_nd = src_shm._array[self.index_field].copy()
        return x_nd, y_nd

    # XXX: was ``.update_xy()``
    def incr_update_xy_nd(
        self,

        src_shm: ShmArray,
        data_field: str,

        new_from_src: np.ndarray,  # portion of source that was updated

        read_slc: slice,
        ln: int,  # len of updated

        nd_start: int,
        nd_stop: int,

        is_append: bool,
        index_field: str = 'index',

    ) -> tuple[
        np.ndarray,
        slice,
    ]:
        # write pushed data to flattened copy
        new_y_nd = new_from_src

        # XXX
        # TODO: this should be returned and written by caller!
        # XXX
        # generate same-valued-per-row x support based on y shape
        if index_field != 'index':
            self.x_nd[read_slc, :] = new_from_src[index_field]

        return new_y_nd, read_slc

    # XXX: was ``.format_xy()``
    def format_xy_nd_to_1d(
        self,

        array: np.ndarray,
        array_key: str,
        vr: tuple[int, int],

    ) -> tuple[
        np.ndarray,  # 1d x
        np.ndarray,  # 1d y
        np.ndarray | str,  # connection array/style
    ]:
        '''
        Default xy-nd array to 1d pre-graphics-path render routine.

        Return single field column data verbatim

        '''
        return (
            array[self.index_field],
            array[array_key],

            # 1d connection array or style-key to
            # ``pg.functions.arrayToQPath()``
            'all',
        )


class OHLCBarsFmtr(IncrementalFormatter):

    fields: list[str] = field(
        default_factory=lambda: ['open', 'high', 'low', 'close']
    )

    def allocate_xy_nd(
        self,

        ohlc_shm: ShmArray,
        data_field: str,

    ) -> tuple[
        np.ndarray,  # x
        np.nd.array  # y
    ]:
        '''
        Convert an input struct-array holding OHLC samples into a pair of
        flattened x, y arrays with the same size (datums wise) as the source
        data.

        '''
        y_nd = ohlc_shm.ustruct(self.fields)

        # generate an flat-interpolated x-domain
        x_nd = (
            np.broadcast_to(
                ohlc_shm._array[self.index_field][:, None],
                (
                    ohlc_shm._array.size,
                    # 4,  # only ohlc
                    y_nd.shape[1],
                ),
            ) + np.array([-0.5, 0, 0, 0.5])
        )
        assert y_nd.any()

        # write pushed data to flattened copy
        return (
            x_nd,
            y_nd,
        )

    @staticmethod
    @njit(
        # NOTE: need to construct this manually for readonly
        # arrays, see https://github.com/numba/numba/issues/4511
        # (
        #     types.Array(
        #         numba_ohlc_dtype,
        #         1,
        #         'C',
        #         readonly=True,
        #     ),
        #     int64,
        #     types.unicode_type,
        #     optional(float64),
        # ),
        nogil=True
    )
    def path_arrays_from_ohlc(
        data: np.ndarray,
        start: int64,
        bar_gap: float64 = 0.43,
        # index_field: str,

    ) -> tuple[
        np.ndarray,
        np.ndarray,
        np.ndarray,
    ]:
        '''
        Generate an array of lines objects from input ohlc data.

        '''
        size = int(data.shape[0] * 6)

        # XXX: see this for why the dtype might have to be defined outside
        # the routine.
        # https://github.com/numba/numba/issues/4098#issuecomment-493914533
        x = np.zeros(
            shape=size,
            dtype=float64,
        )
        y, c = x.copy(), x.copy()

        # TODO: report bug for assert @
        # /home/goodboy/repos/piker/env/lib/python3.8/site-packages/numba/core/typing/builtins.py:991
        for i, q in enumerate(data[start:], start):

            # TODO: ask numba why this doesn't work..
            # open, high, low, close, index = q[
            #     ['open', 'high', 'low', 'close', 'index']]

            open = q['open']
            high = q['high']
            low = q['low']
            close = q['close']
            # index = float64(q[index_field])
            index = float64(q['index'])

            istart = i * 6
            istop = istart + 6

            # x,y detail the 6 points which connect all vertexes of a ohlc bar
            x[istart:istop] = (
                index - bar_gap,
                index,
                index,
                index,
                index,
                index + bar_gap,
            )
            y[istart:istop] = (
                open,
                open,
                low,
                high,
                close,
                close,
            )

            # specifies that the first edge is never connected to the
            # prior bars last edge thus providing a small "gap"/"space"
            # between bars determined by ``bar_gap``.
            c[istart:istop] = (1, 1, 1, 1, 1, 0)

        return x, y, c

    # TODO: can we drop this frame and just use the above?
    def format_xy_nd_to_1d(
        self,

        array: np.ndarray,
        array_key: str,
        vr: tuple[int, int],

        start: int = 0,  # XXX: do we need this?
        # 0.5 is no overlap between arms, 1.0 is full overlap
        w: float = 0.43,

    ) -> tuple[
        np.ndarray,
        np.ndarray,
        np.ndarray,
    ]:
        '''
        More or less direct proxy to the ``numba``-fied
        ``path_arrays_from_ohlc()`` (above) but with closed in kwargs
        for line spacing.

        '''
        x, y, c = self.path_arrays_from_ohlc(
            array,
            start,
            # self.index_field,
            bar_gap=w,
        )
        return x, y, c

    def incr_update_xy_nd(
        self,

        src_shm: ShmArray,
        data_field: str,

        new_from_src: np.ndarray,  # portion of source that was updated

        read_slc: slice,
        ln: int,  # len of updated

        nd_start: int,
        nd_stop: int,

        is_append: bool,
        index_field: str = 'index',

    ) -> tuple[
        np.ndarray,
        slice,
    ]:
        # write newly pushed data to flattened copy
        # a struct-arr is always passed in.
        new_y_nd = rfn.structured_to_unstructured(
            new_from_src[self.fields]
        )

        # XXX
        # TODO: this should be returned and written by caller!
        # XXX
        # generate same-valued-per-row x support based on y shape
        if index_field != 'index':
            self.x_nd[read_slc, :] = new_from_src[index_field]

        return new_y_nd, read_slc


class OHLCBarsAsCurveFmtr(OHLCBarsFmtr):

    def format_xy_nd_to_1d(
        self,

        array: np.ndarray,
        array_key: str,
        vr: tuple[int, int],

    ) -> tuple[
        np.ndarray,
        np.ndarray,
        str,
    ]:
        # TODO: in the case of an existing ``.update_xy()``
        # should we be passing in array as an xy arrays tuple?

        # 2 more datum-indexes to capture zero at end
        x_flat = self.x_nd[self.xy_nd_start:self.xy_nd_stop]
        y_flat = self.y_nd[self.xy_nd_start:self.xy_nd_stop]

        # slice to view
        ivl, ivr = vr
        x_iv_flat = x_flat[ivl:ivr]
        y_iv_flat = y_flat[ivl:ivr]

        # reshape to 1d for graphics rendering
        y_iv = y_iv_flat.reshape(-1)
        x_iv = x_iv_flat.reshape(-1)

        return x_iv, y_iv, 'all'


class StepCurveFmtr(IncrementalFormatter):

    def allocate_xy_nd(
        self,

        shm: ShmArray,
        data_field: str,

        index_field: str = 'index',

    ) -> tuple[
        np.ndarray,  # x
        np.nd.array  # y
    ]:
        '''
        Convert an input 1d shm array to a "step array" format
        for use by path graphics generation.

        '''
        i = shm._array[self.index_field].copy()
        out = shm._array[data_field].copy()

        x_out = np.broadcast_to(
            i[:, None],
            (i.size, 2),
        ) + np.array([-0.5, 0.5])

        y_out = np.empty((len(out), 2), dtype=out.dtype)
        y_out[:] = out[:, np.newaxis]

        # start y at origin level
        y_out[0, 0] = 0
        return x_out, y_out

    def incr_update_xy_nd(
        self,

        src_shm: ShmArray,
        array_key: str,

        src_update: np.ndarray,  # portion of source that was updated
        slc: slice,
        ln: int,  # len of updated

        first: int,
        last: int,

        is_append: bool,

    ) -> tuple[
        np.ndarray,
        slice,
    ]:
        # for a step curve we slice from one datum prior
        # to the current "update slice" to get the previous
        # "level".
        if is_append:
            start = max(last - 1, 0)
            end = src_shm._last.value
            new_y = src_shm._array[start:end][array_key]
            slc = slice(start, end)

        else:
            new_y = src_update

        return (
            np.broadcast_to(
                new_y[:, None], (new_y.size, 2),
            ),
            slc,
        )

    def format_xy_nd_to_1d(
        self,

        array: np.ndarray,
        array_key: str,
        vr: tuple[int, int],

    ) -> tuple[
        np.ndarray,
        np.ndarray,
        str,
    ]:
        lasts = array[['index', array_key]]
        last = lasts[array_key][-1]

        # 2 more datum-indexes to capture zero at end
        x_step = self.x_nd[self.xy_nd_start:self.xy_nd_stop+2]
        y_step = self.y_nd[self.xy_nd_start:self.xy_nd_stop+2]
        y_step[-1] = last

        # slice out in-view data
        ivl, ivr = vr
        ys_iv = y_step[ivl:ivr+1]
        xs_iv = x_step[ivl:ivr+1]

        # flatten to 1d
        y_iv = ys_iv.reshape(ys_iv.size)
        x_iv = xs_iv.reshape(xs_iv.size)

        # print(
        #     f'ys_iv : {ys_iv[-s:]}\n'
        #     f'y_iv: {y_iv[-s:]}\n'
        #     f'xs_iv: {xs_iv[-s:]}\n'
        #     f'x_iv: {x_iv[-s:]}\n'
        # )

        return x_iv, y_iv, 'all'


def xy_downsample(
    x,
    y,
    uppx,

    x_spacer: float = 0.5,

) -> tuple[
    np.ndarray,
    np.ndarray,
    float,
    float,
]:
    '''
    Downsample 1D (flat ``numpy.ndarray``) arrays using M4 given an input
    ``uppx`` (units-per-pixel) and add space between discreet datums.

    '''
    # downsample whenever more then 1 pixels per datum can be shown.
    # always refresh data bounds until we get diffing
    # working properly, see above..
    bins, x, y, ymn, ymx = ds_m4(
        x,
        y,
        uppx,
    )

    # flatten output to 1d arrays suitable for path-graphics generation.
    x = np.broadcast_to(x[:, None], y.shape)
    x = (x + np.array(
        [-x_spacer, 0, 0, x_spacer]
    )).flatten()
    y = y.flatten()

    return x, y, ymn, ymx
