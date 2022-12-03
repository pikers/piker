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
Pre-(path)-graphics formatted x/y nd/1d rendering subsystem.

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

from ._sharedmem import (
    ShmArray,
)
from ._pathops import (
    path_arrays_from_ohlc,
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

    @property
    def index_field(self) -> 'str':
        '''
        Value (``str``) used to look up the "index series" from the
        underlying source ``numpy`` struct-array; delegate directly to
        the managing ``Viz``.

        '''
        return self.viz.index_field

    # Incrementally updated xy ndarray formatted data, a pre-1d
    # format which is updated and cached independently of the final
    # pre-graphics-path 1d format.
    x_nd: Optional[np.ndarray] = None
    y_nd: Optional[np.ndarray] = None

    @property
    def xy_nd(self) -> tuple[np.ndarray, np.ndarray]:
        return (
            self.x_nd[self.xy_slice],
            self.y_nd[self.xy_slice],
        )

    @property
    def xy_slice(self) -> slice:
        return slice(
            self.xy_nd_start,
            self.xy_nd_stop,
        )

    # indexes which slice into the above arrays (which are allocated
    # based on source data shm input size) and allow retrieving
    # incrementally updated data.
    xy_nd_start: int | None = None
    xy_nd_stop: int | None = None

    # TODO: eventually incrementally update 1d-pre-graphics path data?
    # x_1d: Optional[np.ndarray] = None
    # y_1d: Optional[np.ndarray] = None

    # incremental view-change state(s) tracking
    _last_vr: tuple[float, float] | None = None
    _last_ivdr: tuple[float, float] | None = None

    def __repr__(self) -> str:
        msg = (
            f'{type(self)}: ->\n\n'
            f'fqsn={self.viz.name}\n'
            f'shm_name={self.shm.token["shm_name"]}\n\n'

            f'last_vr={self._last_vr}\n'
            f'last_ivdr={self._last_ivdr}\n\n'

            f'xy_slice={self.xy_slice}\n'
            # f'xy_nd_stop={self.xy_nd_stop}\n\n'
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
        # TODO:
        # - can the renderer just call ``Viz.read()`` directly? unpack
        #   latest source data read
        # - eventually maybe we can implement some kind of
        #   transform on the ``QPainterPath`` that will more or less
        #   detect the diff in "elements" terms? update diff state since
        #   we've now rendered paths.
        (
            xfirst,
            xlast,
            array,
            ivl,
            ivr,
            in_view,
        ) = new_read

        index = array['index']

        # if the first index in the read array is 0 then
        # it means the source buffer has bee completely backfilled to
        # available space.
        src_start = index[0]
        src_stop = index[-1] + 1

        # these are the "formatted output data" indices
        # for the pre-graphics arrays.
        nd_start = self.xy_nd_start
        nd_stop = self.xy_nd_stop

        if (
            nd_start is None
        ):
            assert nd_stop is None

            # setup to do a prepend of all existing src history
            nd_start = self.xy_nd_start = src_stop
            # set us in a zero-to-append state
            nd_stop = self.xy_nd_stop = src_stop

        # compute the length diffs between the first/last index entry in
        # the input data and the last indexes we have on record from the
        # last time we updated the curve index.
        prepend_length = int(nd_start - src_start)
        append_length = int(src_stop - nd_stop)

        # blah blah blah
        # do diffing for prepend, append and last entry
        return (
            slice(src_start, nd_start),
            prepend_length,
            append_length,
            slice(nd_stop, src_stop),
        )

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
            self.xy_nd_start = shm._first.value
            self.xy_nd_stop = shm._last.value
            self.x_nd, self.y_nd = self.allocate_xy_nd(
                shm,
                array_key,
            )
            profiler('allocated xy history')

        if prepend_len:
            self.incr_update_xy_nd(
                shm,
                array_key,

                # this is the pre-sliced, "normally expected"
                # new data that an updater would normally be
                # expected to process, however in some cases (like
                # step curves) the updater routine may want to do
                # the source history-data reading itself, so we pass
                # both here.
                shm._array[pre_slice],
                pre_slice,
                prepend_len,

                self.xy_nd_start,
                self.xy_nd_stop,
                is_append=False,
            )

            # self.y_nd[y_nd_slc] = new_y_nd
            self.xy_nd_start = shm._first.value
            profiler('prepended xy history: {prepend_length}')

        if append_len:
            self.incr_update_xy_nd(
                shm,
                array_key,

                shm._array[post_slice],
                post_slice,
                append_len,

                self.xy_nd_start,
                self.xy_nd_stop,
                is_append=True,
            )
            self.xy_nd_stop = shm._last.value
            profiler('appened xy history: {append_length}')

        view_changed: bool = False
        view_range: tuple[int, int] = (ivl, ivr)
        if slice_to_inview:
            view_changed = self._track_inview_range(view_range)
            array = in_view
            profiler(f'{self.viz.name} view range slice {view_range}')

        # hist = array[:slice_to_head]

        # XXX: WOA WTF TRACTOR DEBUGGING BUGGG
        # assert 0

        # xy-path data transform: convert source data to a format
        # able to be passed to a `QPainterPath` rendering routine.
        if not len(array):
            # XXX: this might be why the profiler only has exits?
            return

        # TODO: hist here should be the pre-sliced
        # x/y_data in the case where allocate_xy is
        # defined?
        x_1d, y_1d, connect = self.format_xy_nd_to_1d(
            array,
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
            self._last_ivdr = x_1d[0], x_1d[-1]
            if (x_1d[-1] == 0.5).any():
                breakpoint()

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

    ) -> None:
        # write pushed data to flattened copy
        new_y_nd = new_from_src[data_field]
        self.y_nd[read_slc] = new_y_nd

        x_nd_new = self.x_nd[read_slc]
        x_nd_new[:] = new_from_src[self.index_field]

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
        # NOTE: we don't include the very last datum which is filled in
        # normally by another graphics object.
        return (
            array[self.index_field][:-1],
            array[array_key][:-1],

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

    ) -> None:
        # write newly pushed data to flattened copy
        # a struct-arr is always passed in.
        new_y_nd = rfn.structured_to_unstructured(
            new_from_src[self.fields]
        )
        self.y_nd[read_slc] = new_y_nd

        # generate same-valued-per-row x support based on y shape
        x_nd_new = self.x_nd[read_slc]
        x_nd_new[:] = np.broadcast_to(
            new_from_src[self.index_field][:, None],
            new_y_nd.shape,
        ) + np.array([-0.5, 0, 0, 0.5])

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
        x, y, c = path_arrays_from_ohlc(
            array,
            start,
            # self.index_field,
            bar_gap=w,
        )
        return x, y, c


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
        x_flat = self.x_nd[self.xy_nd_start:self.xy_nd_stop-1]
        y_flat = self.y_nd[self.xy_nd_start:self.xy_nd_stop-1]

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

        # fill out Nx2 array to hold each step's left + right vertices.
        y_out = np.empty(
            # (len(out), 2),
            x_out.shape,
            dtype=out.dtype,
        )
        # fill in (current) values from source shm buffer
        y_out[:] = out[:, np.newaxis]

        # TODO: pretty sure we can drop this?
        # start y at origin level
        # y_out[0, 0] = 0
        # y_out[self.xy_nd_start] = 0
        return x_out, y_out

    def incr_update_xy_nd(
        self,

        src_shm: ShmArray,
        array_key: str,

        new_from_src: np.ndarray,  # portion of source that was updated
        read_slc: slice,
        ln: int,  # len of updated

        nd_start: int,
        nd_stop: int,

        is_append: bool,

    ) -> tuple[
        np.ndarray,
        slice,
    ]:
        # NOTE: for a step curve we slice from one datum prior
        # to the current "update slice" to get the previous
        # "level".
        #
        # why this is needed,
        # - the current new append slice will often have a zero
        #   value in the latest datum-step (at least for zero-on-new
        #   cases like vlm in the) as per configuration of the FSP
        #   engine.
        # - we need to look back a datum to get the last level which
        #   will be used to terminate/complete the last step x-width
        #   which will be set to pair with the last x-index THIS MEANS
        #
        # XXX: this means WE CAN'T USE the append slice since we need to
        # "look backward" one step to get the needed back-to-zero level
        # and the update data in ``new_from_src`` will only contain the
        # latest new data.
        back_1 = slice(
            read_slc.start - 1,
            read_slc.stop,
        )

        to_write = src_shm._array[back_1]
        y_nd_new = self.y_nd[back_1]
        y_nd_new[:] = to_write[array_key][:, None]

        x_nd_new = self.x_nd[read_slc]
        x_nd_new[:] = (
            new_from_src[self.index_field][:, None]
            +
            np.array([-0.5, 0.5])
        )

        # XXX: uncomment for debugging
        # x_nd = self.x_nd[self.xy_slice]
        # y_nd = self.y_nd[self.xy_slice]
        # name = self.viz.name
        # if 'dolla_vlm' in name:
        #     s = 4
        #     print(
        #         f'{name}:\n'
        #         'NEW_FROM_SRC:\n'
        #         f'new_from_src: {new_from_src}\n\n'

        #         f'PRE self.x_nd:'
        #         f'\n{x_nd[-s:]}\n'
        #         f'PRE self.y_nd:\n'
        #         f'{y_nd[-s:]}\n\n'

        #         f'TO WRITE:\n'
        #         f'x_nd_new:\n'
        #         f'{x_nd_new}\n'
        #         f'y_nd_new:\n'
        #         f'{y_nd_new}\n'
        #     )

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
        last_t, last = array[-1][[self.index_field, array_key]]

        start = self.xy_nd_start
        stop = self.xy_nd_stop

        x_step = self.x_nd[start:stop]
        y_step = self.y_nd[start:stop]

        # pack in duplicate final value to complete last step level
        y_step[-1, 1] = last

        # debugging
        # if y_step.any():
        #     s = 3
        #     print(
        #         f'x_step:\n{x_step[-s:]}\n'
        #         f'y_step:\n{y_step[-s:]}\n\n'
        #     )

        # slice out in-view data
        ivl, ivr = vr

        # NOTE: add an extra step to get the vertical-line-down-to-zero
        # adjacent to the last-datum graphic (filled rect).
        x_step_iv = x_step[ivl:ivr+1]
        y_step_iv = y_step[ivl:ivr+1]

        # flatten to 1d
        x_1d = x_step_iv.reshape(x_step_iv.size)
        y_1d = y_step_iv.reshape(y_step_iv.size)

        if not x_1d.size == y_1d.size:
            breakpoint()

        if x_1d.any() and (x_1d[-1] == 0.5).any():
            breakpoint()

        # debugging
        # if y_1d.any():
        #     s = 6
        #     print(
        #         f'x_step_iv:\n{x_step_iv[-s:]}\n'
        #         f'y_step_iv:\n{y_step_iv[-s:]}\n\n'
        #         f'x_1d:\n{x_1d[-s:]}\n'
        #         f'y_1d:\n{y_1d[-s:]}\n'
        #     )

        return x_1d, y_1d, 'all'
