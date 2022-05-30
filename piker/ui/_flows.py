# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
High level streaming graphics primitives.

This is an intermediate layer which associates real-time low latency
graphics primitives with underlying FSP related data structures for fast
incremental update.

'''
from __future__ import annotations
from functools import partial
from typing import (
    Optional,
    Callable,
    Union,
)

import msgspec
import numpy as np
from numpy.lib import recfunctions as rfn
import pyqtgraph as pg
from PyQt5.QtGui import QPainterPath
from PyQt5.QtCore import (
    # Qt,
    QLineF,
    # QSizeF,
    QRectF,
    # QPointF,
)

from ..data._sharedmem import (
    ShmArray,
)
from .._profile import (
    pg_profile_enabled,
    # ms_slower_then,
)
from ._pathops import (
    gen_ohlc_qpath,
    ohlc_to_line,
    to_step_format,
    xy_downsample,
)
from ._ohlc import (
    BarItems,
)
from ._curve import (
    FastAppendCurve,
)
from ..log import get_logger


log = get_logger(__name__)


# class FlowsTable(msgspec.Struct):
#     '''
#     Data-AGGRegate: high level API onto multiple (categorized)
#     ``Flow``s with high level processing routines for
#     multi-graphics computations and display.

#     '''
#     flows: dict[str, np.ndarray] = {}


def update_ohlc_to_line(
    src_shm: ShmArray,
    array_key: str,
    src_update: np.ndarray,
    slc: slice,
    ln: int,
    first: int,
    last: int,
    is_append: bool,

) -> np.ndarray:

    fields = ['open', 'high', 'low', 'close']
    return (
        rfn.structured_to_unstructured(src_update[fields]),
        slc,
    )


def ohlc_flat_to_xy(
    r: Renderer,
    array: np.ndarray,
    array_key: str,
    vr: tuple[int, int],

) -> tuple[
    np.ndarray,
    np.nd.array,
    str,
]:
    # TODO: in the case of an existing ``.update_xy()``
    # should we be passing in array as an xy arrays tuple?

    # 2 more datum-indexes to capture zero at end
    x_flat = r.x_data[r._xy_first:r._xy_last]
    y_flat = r.y_data[r._xy_first:r._xy_last]

    # slice to view
    ivl, ivr = vr
    x_iv_flat = x_flat[ivl:ivr]
    y_iv_flat = y_flat[ivl:ivr]

    # reshape to 1d for graphics rendering
    y_iv = y_iv_flat.reshape(-1)
    x_iv = x_iv_flat.reshape(-1)

    return x_iv, y_iv, 'all'


def render_baritems(
    flow: Flow,
    graphics: BarItems,
    read: tuple[
        int, int, np.ndarray,
        int, int, np.ndarray,
    ],
    profiler: pg.debug.Profiler,
    **kwargs,

) -> None:
    '''
    Graphics management logic for a ``BarItems`` object.

    Mostly just logic to determine when and how to downsample an OHLC
    lines curve into a flattened line graphic and when to display one
    graphic or the other.

    TODO: this should likely be moved into some kind of better abstraction
    layer, if not a `Renderer` then something just above it?

    '''
    bars = graphics

    # if no source data renderer exists create one.
    self = flow
    show_bars: bool = False

    r = self._src_r
    if not r:
        show_bars = True
        # OHLC bars path renderer
        r = self._src_r = Renderer(
            flow=self,
            format_xy=gen_ohlc_qpath,
            last_read=read,
        )

        ds_curve_r = Renderer(
            flow=self,
            last_read=read,

            # incr update routines
            allocate_xy=ohlc_to_line,
            update_xy=update_ohlc_to_line,
            format_xy=ohlc_flat_to_xy,
        )

        curve = FastAppendCurve(
            name=f'{flow.name}_ds_ohlc',
            color=bars._color,
        )
        curve.hide()
        self.plot.addItem(curve)

        # baseline "line" downsampled OHLC curve that should
        # kick on only when we reach a certain uppx threshold.
        self._render_table = (ds_curve_r, curve)

    ds_r, curve = self._render_table

    # do checks for whether or not we require downsampling:
    # - if we're **not** downsampling then we simply want to
    #   render the bars graphics curve and update..
    # - if instead we are in a downsamplig state then we to
    x_gt = 6
    uppx = curve.x_uppx()
    in_line = should_line = curve.isVisible()
    if (
        should_line
        and uppx < x_gt
    ):
        # print('FLIPPING TO BARS')
        should_line = False

    elif (
        not should_line
        and uppx >= x_gt
    ):
        # print('FLIPPING TO LINE')
        should_line = True

    profiler(f'ds logic complete line={should_line}')

    # do graphics updates
    if should_line:
        r = ds_r
        graphics = curve
        profiler('updated ds curve')

    else:
        graphics = bars

    if show_bars:
        bars.show()

    changed_to_line = False
    if (
        not in_line
        and should_line
    ):
        # change to line graphic
        log.info(
            f'downsampling to line graphic {self.name}'
        )
        bars.hide()
        curve.show()
        curve.update()
        changed_to_line = True

    elif in_line and not should_line:
        # change to bars graphic
        log.info(f'showing bars graphic {self.name}')
        curve.hide()
        bars.show()
        bars.update()

    draw_last = False
    lasts = self.shm.array[-2:]
    last = lasts[-1]

    if should_line:
        def draw_last():
            x, y = lasts['index'], lasts['close']
            curve.draw_last(x, y)
    else:
        draw_last = partial(
            bars.draw_last,
            last,
        )

    return (
        graphics,
        r,
        {'read_from_key': False},
        draw_last,
        should_line,
        changed_to_line,
    )


def update_step_xy(
    src_shm: ShmArray,
    array_key: str,
    y_update: np.ndarray,
    slc: slice,
    ln: int,
    first: int,
    last: int,
    is_append: bool,

) -> np.ndarray:

    # for a step curve we slice from one datum prior
    # to the current "update slice" to get the previous
    # "level".
    if is_append:
        start = max(last - 1, 0)
        end = src_shm._last.value
        new_y = src_shm._array[start:end][array_key]
        slc = slice(start, end)

    else:
        new_y = y_update

    return (
        np.broadcast_to(
            new_y[:, None], (new_y.size, 2),
        ),
        slc,
    )


def step_to_xy(
    r: Renderer,
    array: np.ndarray,
    array_key: str,
    vr: tuple[int, int],

) -> tuple[
    np.ndarray,
    np.nd.array,
    str,
]:

    # 2 more datum-indexes to capture zero at end
    x_step = r.x_data[r._xy_first:r._xy_last+2]
    y_step = r.y_data[r._xy_first:r._xy_last+2]

    lasts = array[['index', array_key]]
    last = lasts[array_key][-1]
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


class Flow(msgspec.Struct):  # , frozen=True):
    '''
    (Financial Signal-)Flow compound type which wraps a real-time
    shm array stream with displayed graphics (curves, charts)
    for high level access and control as well as efficient incremental
    update.

    The intention is for this type to eventually be capable of shm-passing
    of incrementally updated graphics stream data between actors.

    '''
    name: str
    plot: pg.PlotItem
    graphics: pg.GraphicsObject
    _shm: ShmArray

    is_ohlc: bool = False
    render: bool = True  # toggle for display loop

    # pre-graphics formatted data
    gy: Optional[np.ndarray] = None
    gx: Optional[np.ndarray] = None

    # pre-graphics update indices
    _iflat_last: int = 0
    _iflat_first: int = 0

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False

    _graphics_tranform_fn: Optional[Callable[ShmArray, np.ndarray]] = None

    # map from uppx -> (downsampled data, incremental graphics)
    _src_r: Optional[Renderer] = None
    _render_table: dict[
        Optional[int],
        tuple[Renderer, pg.GraphicsItem],
    ] = (None, None)

    # TODO: hackery to be able to set a shm later
    # but whilst also allowing this type to hashable,
    # likely will require serializable token that is used to attach
    # to the underlying shm ref after startup?
    # _shm: Optional[ShmArray] = None  # currently, may be filled in "later"

    # last read from shm (usually due to an update call)
    _last_read: Optional[np.ndarray] = None

    # cache of y-range values per x-range input.
    _mxmns: dict[tuple[int, int], tuple[float, float]] = {}

    @property
    def shm(self) -> ShmArray:
        return self._shm

    # TODO: remove this and only allow setting through
    # private ``._shm`` attr?
    @shm.setter
    def shm(self, shm: ShmArray) -> ShmArray:
        self._shm = shm

    def maxmin(
        self,
        lbar: int,
        rbar: int,

    ) -> tuple[float, float]:
        '''
        Compute the cached max and min y-range values for a given
        x-range determined by ``lbar`` and ``rbar``.

        '''
        rkey = (lbar, rbar)
        cached_result = self._mxmns.get(rkey)
        if cached_result:
            return cached_result

        shm = self.shm
        if shm is None:
            mxmn = None

        else:  # new block for profiling?..
            arr = shm.array

            # build relative indexes into shm array
            # TODO: should we just add/use a method
            # on the shm to do this?
            ifirst = arr[0]['index']
            slice_view = arr[
                lbar - ifirst:
                (rbar - ifirst) + 1
            ]

            if not slice_view.size:
                mxmn = None

            else:
                if self.is_ohlc:
                    ylow = np.min(slice_view['low'])
                    yhigh = np.max(slice_view['high'])

                else:
                    view = slice_view[self.name]
                    ylow = np.min(view)
                    yhigh = np.max(view)

                mxmn = ylow, yhigh

            if mxmn is not None:
                # cache new mxmn result
                self._mxmns[rkey] = mxmn

            return mxmn

    def view_range(self) -> tuple[int, int]:
        '''
        Return the indexes in view for the associated
        plot displaying this flow's data.

        '''
        vr = self.plot.viewRect()
        return int(vr.left()), int(vr.right())

    def datums_range(self) -> tuple[
        int, int, int, int, int, int
    ]:
        '''
        Return a range tuple for the datums present in view.

        '''
        l, r = self.view_range()

        # TODO: avoid this and have shm passed
        # in earlier.
        if self.shm is None:
            # haven't initialized the flow yet
            return (0, l, 0, 0, r, 0)

        array = self.shm.array
        index = array['index']
        start = index[0]
        end = index[-1]
        lbar = max(l, start)
        rbar = min(r, end)
        return (
            start, l, lbar, rbar, r, end,
        )

    def read(
        self,
        array_field: Optional[str] = None,

    ) -> tuple[
            int, int, np.ndarray,
            int, int, np.ndarray,
    ]:
        # read call
        array = self.shm.array

        indexes = array['index']
        ifirst = indexes[0]
        ilast = indexes[-1]

        ifirst, l, lbar, rbar, r, ilast = self.datums_range()

        # get read-relative indices adjusting
        # for master shm index.
        lbar_i = max(l, ifirst) - ifirst
        rbar_i = min(r, ilast) - ifirst

        if array_field:
            array = array[array_field]

        # TODO: we could do it this way as well no?
        # to_draw = array[lbar - ifirst:(rbar - ifirst) + 1]
        in_view = array[lbar_i: rbar_i + 1]

        return (
            # abs indices + full data set
            ifirst, ilast, array,

            # relative indices + in view datums
            lbar_i, rbar_i, in_view,
        )

    def update_graphics(
        self,
        use_vr: bool = True,
        render: bool = True,
        array_key: Optional[str] = None,

        profiler: Optional[pg.debug.Profiler] = None,
        do_append: bool = True,

        **kwargs,

    ) -> pg.GraphicsObject:
        '''
        Read latest datums from shm and render to (incrementally)
        render to graphics.

        '''

        # profiler = profiler or pg.debug.Profiler(
        profiler = pg.debug.Profiler(
            msg=f'Flow.update_graphics() for {self.name}',
            disabled=not pg_profile_enabled(),
            # disabled=False,
            ms_threshold=4,
            # ms_threshold=ms_slower_then,
        )
        # shm read and slice to view
        read = (
            xfirst, xlast, array,
            ivl, ivr, in_view,
        ) = self.read()

        profiler('read src shm data')

        graphics = self.graphics

        if (
            not in_view.size
            or not render
        ):
            # print('exiting early')
            return graphics

        draw_last: bool = True
        slice_to_head: int = -1

        should_redraw: bool = False

        rkwargs = {}
        bars = False

        if isinstance(graphics, BarItems):
            # XXX: special case where we change out graphics
            # to a line after a certain uppx threshold.
            (
                graphics,
                r,
                rkwargs,
                draw_last,
                should_line,
                changed_to_line,
            ) = render_baritems(
                self,
                graphics,
                read,
                profiler,
                **kwargs,
            )
            bars = True
            should_redraw = changed_to_line or not should_line

        else:
            r = self._src_r
            if not r:
                # just using for ``.diff()`` atm..
                r = self._src_r = Renderer(
                    flow=self,
                    # TODO: rename this to something with ohlc
                    last_read=read,
                )

        # ``FastAppendCurve`` case:
        array_key = array_key or self.name
        # print(array_key)

        # ds update config
        new_sample_rate: bool = False
        should_ds: bool = r._in_ds
        showing_src_data: bool = not r._in_ds

        step_mode = getattr(graphics, '_step_mode', False)
        if step_mode:

            r.allocate_xy = to_step_format
            r.update_xy = update_step_xy
            r.format_xy = step_to_xy

            slice_to_head = -2

            # TODO: append logic inside ``.render()`` isn't
            # corrent yet for step curves.. remove this to see it.
            should_redraw = True

            # TODO: remove this and instead place all step curve
            # updating into pre-path data render callbacks.
            # full input data
            x = array['index']
            y = array[array_key]
            x_last = x[-1]
            y_last = y[-1]

            draw_last = True

        # downsampling incremental state checking
        # check for and set std m4 downsample conditions
        uppx = graphics.x_uppx()
        uppx_diff = (uppx - self._last_uppx)
        profiler(f'diffed uppx {uppx}')
        if (
            uppx > 1
            and abs(uppx_diff) >= 1
        ):
            log.debug(
                f'{array_key} sampler change: {self._last_uppx} -> {uppx}'
            )
            self._last_uppx = uppx

            new_sample_rate = True
            showing_src_data = False
            should_ds = True
            should_redraw = True

        elif (
            uppx <= 2
            and self._in_ds
        ):
            # we should de-downsample back to our original
            # source data so we clear our path data in prep
            # to generate a new one from original source data.
            new_sample_rate = True
            showing_src_data = True
            should_ds = False
            should_redraw = True

        # MAIN RENDER LOGIC:
        # - determine in view data and redraw on range change
        # - determine downsampling ops if needed
        # - (incrementally) update ``QPainterPath``

        path, data, reset = r.render(
            read,
            array_key,
            profiler,
            uppx=uppx,
            # use_vr=True,

            # TODO: better way to detect and pass this?
            # if we want to eventually cache renderers for a given uppx
            # we should probably use this as a key + state?
            should_redraw=should_redraw,
            new_sample_rate=new_sample_rate,
            should_ds=should_ds,
            showing_src_data=showing_src_data,

            slice_to_head=slice_to_head,
            do_append=do_append,

            **rkwargs,
        )

        # XXX: SUPER UGGGHHH... without this we get stale cache
        # graphics that don't update until you downsampler again..
        if reset:
            with graphics.reset_cache():
                # assign output paths to graphicis obj
                graphics.path = r.path
                graphics.fast_path = r.fast_path
        else:
            # assign output paths to graphicis obj
            graphics.path = r.path
            graphics.fast_path = r.fast_path

        if draw_last and not bars:
            # default line draw last call
            if not step_mode:
                with graphics.reset_cache():
                    x = data['index']
                    y = data[array_key]
                    graphics.draw_last(x, y)

            else:
                w = 0.5
                graphics._last_line = QLineF(
                    x_last - w, 0,
                    x_last + w, 0,
                )
                graphics._last_step_rect = QRectF(
                    x_last - w, 0,
                    x_last + w, y_last,
                )

                # TODO: does this actuallly help us in any way (prolly should
                # look at the source / ask ogi). I think it avoid artifacts on
                # wheel-scroll downsampling curve updates?
                graphics.update()
                profiler('.prepareGeometryChange()')

        elif bars and draw_last:
            draw_last()
            graphics.update()
            profiler('.update()')

        return graphics


def by_index_and_key(
    renderer: Renderer,
    array: np.ndarray,
    array_key: str,
    vr: tuple[int, int],

) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    return array['index'], array[array_key], 'all'


class Renderer(msgspec.Struct):

    flow: Flow
    # last array view read
    last_read: Optional[tuple] = None

    # default just returns index, and named array from data
    format_xy: Callable[
        [np.ndarray, str],
        tuple[np.ndarray]
    ] = by_index_and_key

    # optional pre-graphics xy formatted data which
    # is incrementally updated in sync with the source data.
    allocate_xy: Optional[Callable[
        [int, slice],
        tuple[np.ndarray, np.nd.array]
    ]] = None

    update_xy: Optional[Callable[
        [int, slice], None]
    ] = None

    x_data: Optional[np.ndarray] = None
    y_data: Optional[np.ndarray] = None

    # indexes which slice into the above arrays (which are allocated
    # based on source data shm input size) and allow retrieving
    # incrementally updated data.
    _xy_first: int = 0
    _xy_last: int = 0

    # output graphics rendering, the main object
    # processed in ``QGraphicsObject.paint()``
    path: Optional[QPainterPath] = None
    fast_path: Optional[QPainterPath] = None

    # XXX: just ideas..
    # called on the final data (transform) output to convert
    # to "graphical data form" a format that can be passed to
    # the ``.draw()`` implementation.
    # graphics_t: Optional[Callable[ShmArray, np.ndarray]] = None
    # graphics_t_shm: Optional[ShmArray] = None

    # path graphics update implementation methods
    # prepend_fn: Optional[Callable[QPainterPath, QPainterPath]] = None
    # append_fn: Optional[Callable[QPainterPath, QPainterPath]] = None

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False

    # incremental update state(s)
    _last_vr: Optional[tuple[float, float]] = None
    _last_ivr: Optional[tuple[float, float]] = None

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

        # TODO: can the renderer just call ``Flow.read()`` directly?
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

    def draw_path(
        self,
        x: np.ndarray,
        y: np.ndarray,
        connect: Union[str, np.ndarray] = 'all',
        path: Optional[QPainterPath] = None,
        redraw: bool = False,

    ) -> QPainterPath:

        path_was_none = path is None

        if redraw and path:
            path.clear()

            # TODO: avoid this?
            if self.fast_path:
                self.fast_path.clear()

            # profiler('cleared paths due to `should_redraw=True`')

        path = pg.functions.arrayToQPath(
            x,
            y,
            connect=connect,
            finiteCheck=False,

            # reserve mem allocs see:
            # - https://doc.qt.io/qt-5/qpainterpath.html#reserve
            # - https://doc.qt.io/qt-5/qpainterpath.html#capacity
            # - https://doc.qt.io/qt-5/qpainterpath.html#clear
            # XXX: right now this is based on had hoc checks on a
            # hidpi 3840x2160 4k monitor but we should optimize for
            # the target display(s) on the sys.
            # if no_path_yet:
            #     graphics.path.reserve(int(500e3))
            path=path,  # path re-use / reserving
        )

        # avoid mem allocs if possible
        if path_was_none:
            path.reserve(path.capacity())

        return path

    def render(
        self,

        new_read,
        array_key: str,
        profiler: pg.debug.Profiler,
        uppx: float = 1,

        # redraw and ds flags
        should_redraw: bool = False,
        new_sample_rate: bool = False,
        should_ds: bool = False,
        showing_src_data: bool = True,

        do_append: bool = True,
        slice_to_head: int = -1,
        use_fpath: bool = True,

        # only render datums "in view" of the ``ChartView``
        use_vr: bool = True,
        read_from_key: bool = True,

    ) -> list[QPainterPath]:
        '''
        Render the current graphics path(s)

        There are (at least) 3 stages from source data to graphics data:
        - a data transform (which can be stored in additional shm)
        - a graphics transform which converts discrete basis data to
          a `float`-basis view-coords graphics basis. (eg. ``ohlc_flatten()``,
          ``step_path_arrays_from_1d()``, etc.)

        - blah blah blah (from notes)

        '''
        # TODO: can the renderer just call ``Flow.read()`` directly?
        # unpack latest source data read
        (
            xfirst,
            xlast,
            array,
            ivl,
            ivr,
            in_view,
        ) = new_read

        (
            pre_slice,
            prepend_length,
            append_length,
            post_slice,
        ) = self.diff(new_read)

        if self.update_xy:

            shm = self.flow.shm

            if self.y_data is None:
                # we first need to allocate xy data arrays
                # from the source data.
                assert self.allocate_xy
                self.x_data, self.y_data = self.allocate_xy(
                    shm,
                    array_key,
                )
                self._xy_first = shm._first.value
                self._xy_last = shm._last.value
                profiler('allocated xy history')

            if prepend_length:
                y_prepend = shm._array[pre_slice]

                if read_from_key:
                    y_prepend = y_prepend[array_key]

                xy_data, xy_slice = self.update_xy(
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
                    prepend_length,
                    self._xy_first,
                    self._xy_last,
                    is_append=False,
                )
                self.y_data[xy_slice] = xy_data
                self._xy_first = shm._first.value
                profiler('prepended xy history: {prepend_length}')

            if append_length:
                y_append = shm._array[post_slice]

                if read_from_key:
                    y_append = y_append[array_key]

                xy_data, xy_slice = self.update_xy(
                    shm,
                    array_key,

                    y_append,
                    post_slice,
                    append_length,

                    self._xy_first,
                    self._xy_last,
                    is_append=True,
                )
                # self.y_data[post_slice] = xy_data
                # self.y_data[xy_slice or post_slice] = xy_data
                self.y_data[xy_slice] = xy_data
                self._xy_last = shm._last.value
                profiler('appened xy history: {append_length}')

        if use_vr:
            array = in_view
        # else:
        #     ivl, ivr = xfirst, xlast

        hist = array[:slice_to_head]

        # xy-path data transform: convert source data to a format
        # able to be passed to a `QPainterPath` rendering routine.
        x_out, y_out, connect = self.format_xy(
            self,
            # TODO: hist here should be the pre-sliced
            # x/y_data in the case where allocate_xy is
            # defined?
            hist,
            array_key,
            (ivl, ivr),
        )

        profiler('sliced input arrays')

        if (
            use_vr
        ):
            # if a view range is passed, plan to draw the
            # source ouput that's "in view" of the chart.
            view_range = (ivl, ivr)
            # print(f'{self._name} vr: {view_range}')

            profiler(f'view range slice {view_range}')

            vl, vr = view_range

            zoom_or_append = False
            last_vr = self._last_vr
            last_ivr = self._last_ivr or vl, vr

            # incremental in-view data update.
            if last_vr:
                # relative slice indices
                lvl, lvr = last_vr
                # abs slice indices
                al, ar = last_ivr

                # left_change = abs(x_iv[0] - al) >= 1
                # right_change = abs(x_iv[-1] - ar) >= 1

                if (
                    # likely a zoom view change
                    (vr - lvr) > 2 or vl < lvl
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
            if len(x_out):
                self._last_ivr = x_out[0], x_out[slice_to_head]

        # redraw conditions
        if (
            prepend_length > 0
            or new_sample_rate
            or append_length > 0
            or zoom_or_append
        ):
            should_redraw = True

        path = self.path
        fast_path = self.fast_path
        reset = False

        # redraw the entire source data if we have either of:
        # - no prior path graphic rendered or,
        # - we always intend to re-render the data only in view
        if (
            path is None
            or should_redraw
        ):
            # print(f"{self.flow.name} -> REDRAWING BRUH")
            if new_sample_rate and showing_src_data:
                log.info(f'DEDOWN -> {array_key}')
                self._in_ds = False

            elif should_ds and uppx > 1:

                x_out, y_out = xy_downsample(
                    x_out,
                    y_out,
                    uppx,
                )
                reset = True
                profiler(f'FULL PATH downsample redraw={should_ds}')
                self._in_ds = True

            path = self.draw_path(
                x=x_out,
                y=y_out,
                connect=connect,
                path=path,
                redraw=True,
            )

            profiler(
                'generated fresh path. '
                f'(should_redraw: {should_redraw} '
                f'should_ds: {should_ds} new_sample_rate: {new_sample_rate})'
            )

        # TODO: get this piecewise prepend working - right now it's
        # giving heck on vwap...
        # elif prepend_length:
        #     breakpoint()

        #     prepend_path = pg.functions.arrayToQPath(
        #         x[0:prepend_length],
        #         y[0:prepend_length],
        #         connect='all'
        #     )

        #     # swap prepend path in "front"
        #     old_path = graphics.path
        #     graphics.path = prepend_path
        #     # graphics.path.moveTo(new_x[0], new_y[0])
        #     graphics.path.connectPath(old_path)

        elif (
            append_length > 0
            and do_append
            and not should_redraw
        ):
            # print(f'{array_key} append len: {append_length}')
            new_x = x_out[-append_length - 2:]  # slice_to_head]
            new_y = y_out[-append_length - 2:]  # slice_to_head]
            profiler('sliced append path')

            profiler(
                f'diffed array input, append_length={append_length}'
            )

            # if should_ds:
            #     new_x, new_y = xy_downsample(
            #         new_x,
            #         new_y,
            #         uppx,
            #     )
            #     profiler(f'fast path downsample redraw={should_ds}')

            append_path = self.draw_path(
                x=new_x,
                y=new_y,
                connect=connect,
                path=fast_path,
            )
            profiler('generated append qpath')

            if use_fpath:
                # an attempt at trying to make append-updates faster..
                if fast_path is None:
                    fast_path = append_path
                    # fast_path.reserve(int(6e3))
                else:
                    fast_path.connectPath(append_path)
                    size = fast_path.capacity()
                    profiler(f'connected fast path w size: {size}')

                    # print(f"append_path br: {append_path.boundingRect()}")
                    # graphics.path.moveTo(new_x[0], new_y[0])
                    # path.connectPath(append_path)

                    # XXX: lol this causes a hang..
                    # graphics.path = graphics.path.simplified()
            else:
                size = path.capacity()
                profiler(f'connected history path w size: {size}')
                path.connectPath(append_path)

        self.path = path
        self.fast_path = fast_path

        # TODO: eventually maybe we can implement some kind of
        # transform on the ``QPainterPath`` that will more or less
        # detect the diff in "elements" terms?
        # update diff state since we've now rendered paths.
        self.last_read = new_read

        return self.path, array, reset
