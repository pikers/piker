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
# from functools import partial
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
    # open_shm_array,
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

# @classmethod
# def from_token(
#     cls,
#     shm_token: tuple[
#         str,
#         str,
#         tuple[str, str],
#     ],

# ) -> Renderer:

#     shm = attach_shm_array(token)
#     return cls(shm)


def rowarr_to_path(
    rows_array: np.ndarray,
    x_basis: np.ndarray,
    flow: Flow,

) -> QPainterPath:

    # TODO: we could in theory use ``numba`` to flatten
    # if needed?

    # to 1d
    y = rows_array.flatten()

    return pg.functions.arrayToQPath(
        # these get passed at render call time
        x=x_basis[:y.size],
        y=y,
        connect='all',
        finiteCheck=False,
        path=flow.path,
    )


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
    (
        xfirst, xlast, array,
        ivl, ivr, in_view,
    ) = read

    # if no source data renderer exists create one.
    self = flow
    r = self._src_r
    show_bars: bool = False
    if not r:
        show_bars = True
        # OHLC bars path renderer
        r = self._src_r = Renderer(
            flow=self,
            format_xy=gen_ohlc_qpath,
            last_read=read,
        )

        # ds_curve_r = Renderer(
        #     flow=self,

        #     # just swap in the flat view
        #     # data_t=lambda array: self.gy.array,
        #     last_read=read,
        #     draw_path=partial(
        #         rowarr_to_path,
        #         x_basis=None,
        #     ),

        # )
        curve = FastAppendCurve(
            name='OHLC',
            color=graphics._color,
        )
        curve.hide()
        self.plot.addItem(curve)

        # baseline "line" downsampled OHLC curve that should
        # kick on only when we reach a certain uppx threshold.
        self._render_table[0] = curve
        # (
        #     # ds_curve_r,
        #     curve,
        # )

    curve = self._render_table[0]
    # dsc_r, curve = self._render_table[0]

    # do checks for whether or not we require downsampling:
    # - if we're **not** downsampling then we simply want to
    #   render the bars graphics curve and update..
    # - if insteam we are in a downsamplig state then we to
    x_gt = 6
    uppx = curve.x_uppx()
    in_line = should_line = curve.isVisible()
    if (
        should_line
        and uppx < x_gt
    ):
        print('FLIPPING TO BARS')
        should_line = False

    elif (
        not should_line
        and uppx >= x_gt
    ):
        print('FLIPPING TO LINE')
        should_line = True

    profiler(f'ds logic complete line={should_line}')

    # do graphics updates
    if should_line:

        fields = ['open', 'high', 'low', 'close']
        if self.gy is None:
            # create a flattened view onto the OHLC array
            # which can be read as a line-style format
            shm = self.shm
            (
                self._iflat_first,
                self._iflat_last,
                self.gx,
                self.gy,
            ) = ohlc_to_line(
                shm,
                fields=fields,
            )

        # print(f'unstruct diff: {time.time() - start}')

        gy = self.gy

        # update flatted ohlc copy
        (
            iflat_first,
            iflat,
            ishm_last,
            ishm_first,
        ) = (
            self._iflat_first,
            self._iflat_last,
            self.shm._last.value,
            self.shm._first.value
        )

        # check for shm prepend updates since last read.
        if iflat_first != ishm_first:

            # write newly prepended data to flattened copy
            gy[
                ishm_first:iflat_first
            ] = rfn.structured_to_unstructured(
                self.shm._array[fields][ishm_first:iflat_first]
            )
            self._iflat_first = ishm_first

        to_update = rfn.structured_to_unstructured(
            self.shm._array[iflat:ishm_last][fields]
        )

        gy[iflat:ishm_last][:] = to_update
        profiler('updated ustruct OHLC data')

        # slice out up-to-last step contents
        y_flat = gy[ishm_first:ishm_last]
        x_flat = self.gx[ishm_first:ishm_last]

        # update local last-index tracking
        self._iflat_last = ishm_last

        # reshape to 1d for graphics rendering
        y = y_flat.reshape(-1)
        x = x_flat.reshape(-1)
        profiler('flattened ustruct OHLC data')

        # do all the same for only in-view data
        y_iv_flat = y_flat[ivl:ivr]
        x_iv_flat = x_flat[ivl:ivr]
        y_iv = y_iv_flat.reshape(-1)
        x_iv = x_iv_flat.reshape(-1)
        profiler('flattened ustruct in-view OHLC data')

        # pass into curve graphics processing
        # curve.update_from_array(
        #     x,
        #     y,
        #     x_iv=x_iv,
        #     y_iv=y_iv,
        #     view_range=(ivl, ivr),  # hack
        #     profiler=profiler,
        #     # should_redraw=False,

        #     # NOTE: already passed through by display loop?
        #     # do_append=uppx < 16,
        #     **kwargs,
        # )
        curve.draw_last(x, y)
        curve.show()
        profiler('updated ds curve')

    else:
        # render incremental or in-view update
        # and apply ouput (path) to graphics.
        path, data = r.render(
            read,
            'ohlc',
            profiler=profiler,
            # uppx=1,
            use_vr=True,
            # graphics=graphics,
            # should_redraw=True,  # always
        )
        assert path

        graphics.path = path
        graphics.draw_last(data[-1])
        if show_bars:
            graphics.show()

        # NOTE: on appends we used to have to flip the coords
        # cache thought it doesn't seem to be required any more?
        # graphics.setCacheMode(QtWidgets.QGraphicsItem.NoCache)
        # graphics.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # graphics.prepareGeometryChange()
        graphics.update()

    if (
        not in_line
        and should_line
    ):
        # change to line graphic

        log.info(
            f'downsampling to line graphic {self.name}'
        )
        graphics.hide()
        # graphics.update()
        curve.show()
        curve.update()

    elif in_line and not should_line:
        log.info(f'showing bars graphic {self.name}')
        curve.hide()
        graphics.show()
        graphics.update()

    #   update our pre-downsample-ready data and then pass that
    #   new data the downsampler algo for incremental update.

        # graphics.update_from_array(
        #     array,
        #     in_view,
        #     view_range=(ivl, ivr) if use_vr else None,

        #     **kwargs,
        # )

        # generate and apply path to graphics obj
        # graphics.path, last = r.render(
        #     read,
        #     only_in_view=True,
        # )
        # graphics.draw_last(last)

    if should_line:
        return (
            curve,
            x,
            y,
            x_iv,
            y_iv,
        )


def update_step_data(
    flow: Flow,
    shm: ShmArray,
    ivl: int,
    ivr: int,
    array_key: str,
    iflat_first: int,
    iflat: int,
    profiler: pg.debug.Profiler,

) -> tuple:

    self = flow
    (
        # iflat_first,
        # iflat,
        ishm_last,
        ishm_first,
    ) = (
        # self._iflat_first,
        # self._iflat_last,
        shm._last.value,
        shm._first.value
    )
    il = max(iflat - 1, 0)
    profiler('read step mode incr update indices')

    # check for shm prepend updates since last read.
    if iflat_first != ishm_first:

        print(f'prepend {array_key}')

        # i_prepend = self.shm._array['index'][
        #   ishm_first:iflat_first]
        y_prepend = self.shm._array[array_key][
            ishm_first:iflat_first
        ]

        y2_prepend = np.broadcast_to(
            y_prepend[:, None], (y_prepend.size, 2),
        )

        # write newly prepended data to flattened copy
        self.gy[ishm_first:iflat_first] = y2_prepend
        self._iflat_first = ishm_first
        profiler('prepended step mode history')

    append_diff = ishm_last - iflat
    if append_diff:

        # slice up to the last datum since last index/append update
        # new_x = self.shm._array[il:ishm_last]['index']
        new_y = self.shm._array[il:ishm_last][array_key]

        new_y2 = np.broadcast_to(
            new_y[:, None], (new_y.size, 2),
        )
        self.gy[il:ishm_last] = new_y2
        profiler('updated step curve data')

        # print(
        #     f'append size: {append_diff}\n'
        #     f'new_x: {new_x}\n'
        #     f'new_y: {new_y}\n'
        #     f'new_y2: {new_y2}\n'
        #     f'new gy: {gy}\n'
        # )

        # update local last-index tracking
        self._iflat_last = ishm_last

    # slice out up-to-last step contents
    x_step = self.gx[ishm_first:ishm_last+2]
    # shape to 1d
    x = x_step.reshape(-1)
    profiler('sliced step x')

    y_step = self.gy[ishm_first:ishm_last+2]
    lasts = self.shm.array[['index', array_key]]
    last = lasts[array_key][-1]
    y_step[-1] = last
    # shape to 1d
    y = y_step.reshape(-1)

    # s = 6
    # print(f'lasts: {x[-2*s:]}, {y[-2*s:]}')

    profiler('sliced step y')

    # do all the same for only in-view data
    ys_iv = y_step[ivl:ivr+1]
    xs_iv = x_step[ivl:ivr+1]
    y_iv = ys_iv.reshape(ys_iv.size)
    x_iv = xs_iv.reshape(xs_iv.size)
    # print(
    #     f'ys_iv : {ys_iv[-s:]}\n'
    #     f'y_iv: {y_iv[-s:]}\n'
    #     f'xs_iv: {xs_iv[-s:]}\n'
    #     f'x_iv: {x_iv[-s:]}\n'
    # )
    profiler('sliced in view step data')

    # legacy full-recompute-everytime method
    # x, y = ohlc_flatten(array)
    # x_iv, y_iv = ohlc_flatten(in_view)
    # profiler('flattened OHLC data')
    return (
        x,
        y,
        x_iv,
        y_iv,
        append_diff,
    )


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
    gy: Optional[ShmArray] = None
    gx: Optional[np.ndarray] = None
    # pre-graphics update indices
    _iflat_last: int = 0
    _iflat_first: int = 0

    # view-range incremental state
    _vr: Optional[tuple] = None
    _avr: Optional[tuple] = None

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False

    _graphics_tranform_fn: Optional[Callable[ShmArray, np.ndarray]] = None

    # map from uppx -> (downsampled data, incremental graphics)
    _src_r: Optional[Renderer] = None
    _render_table: dict[
        Optional[int],
        tuple[Renderer, pg.GraphicsItem],
    ] = {}

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
        print(f'{self.name} DO NOT SET SHM THIS WAY!?')
        self._shm = shm

    def maxmin(
        self,
        lbar,
        rbar,

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
            return graphics

        draw_last: bool = True
        slice_to_head: int = -1
        input_data = None

        out: Optional[tuple] = None
        if isinstance(graphics, BarItems):
            draw_last = False
            # XXX: special case where we change out graphics
            # to a line after a certain uppx threshold.
            # render_baritems(
            out = render_baritems(
                self,
                graphics,
                read,
                profiler,
                **kwargs,
            )

            if out is None:
                return graphics

            # return graphics

        r = self._src_r
        if not r:
            # just using for ``.diff()`` atm..
            r = self._src_r = Renderer(
                flow=self,
                # TODO: rename this to something with ohlc
                # draw_path=gen_ohlc_qpath,
                last_read=read,
            )

        # ``FastAppendCurve`` case:
        array_key = array_key or self.name
        shm = self.shm

        if out is not None:
            # hack to handle ds curve from bars above
            (
                graphics,  # curve
                x,
                y,
                x_iv,
                y_iv,
            ) = out
            input_data = out[1:]
            # breakpoint()

        # ds update config
        new_sample_rate: bool = False
        should_redraw: bool = False
        should_ds: bool = r._in_ds
        showing_src_data: bool = not r._in_ds

        # downsampling incremental state checking
        # check for and set std m4 downsample conditions
        uppx = graphics.x_uppx()
        uppx_diff = (uppx - self._last_uppx)
        profiler(f'diffed uppx {uppx}')
        if (
            uppx > 1
            and abs(uppx_diff) >= 1
        ):
            log.info(
                f'{array_key} sampler change: {self._last_uppx} -> {uppx}'
            )
            self._last_uppx = uppx
            new_sample_rate = True
            showing_src_data = False
            should_redraw = True
            should_ds = True

        elif (
            uppx <= 2
            and self._in_ds
        ):
            # we should de-downsample back to our original
            # source data so we clear our path data in prep
            # to generate a new one from original source data.
            should_redraw = True
            new_sample_rate = True
            should_ds = False
            showing_src_data = True

        if graphics._step_mode:
            slice_to_head = -2

            # TODO: remove this and instead place all step curve
            # updating into pre-path data render callbacks.
            # full input data
            x = array['index']
            y = array[array_key]
            x_last = x[-1]
            y_last = y[-1]

            # inview data
            x_iv = in_view['index']
            y_iv = in_view[array_key]

            if self.gy is None:
                (
                    self._iflat_first,
                    self.gx,
                    self.gy,
                ) = to_step_format(
                    shm,
                    array_key,
                )
                profiler('generated step mode data')

            (
                x,
                y,
                x_iv,
                y_iv,
                append_diff,

            ) = update_step_data(
                self,
                shm,
                ivl,
                ivr,
                array_key,
                self._iflat_first,
                self._iflat_last,
                profiler,
            )

            graphics._last_line = QLineF(
                x_last - 0.5, 0,
                x_last + 0.5, 0,
            )
            graphics._last_step_rect = QRectF(
                x_last - 0.5, 0,
                x_last + 0.5, y_last,
            )

            should_redraw = bool(append_diff)
            draw_last = False
            input_data = (
                x,
                y,
                x_iv,
                y_iv,
            )

        # compute the length diffs between the first/last index entry in
        # the input data and the last indexes we have on record from the
        # last time we updated the curve index.
        # prepend_length, append_length = r.diff(read)

        # MAIN RENDER LOGIC:
        # - determine in view data and redraw on range change
        # - determine downsampling ops if needed
        # - (incrementally) update ``QPainterPath``

        # path = graphics.path
        # fast_path = graphics.fast_path

        path, data = r.render(
            read,
            array_key,
            profiler,
            uppx=uppx,
            input_data=input_data,
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
            graphics=graphics,
        )
        # graphics.prepareGeometryChange()
        # assign output paths to graphicis obj
        graphics.path = r.path
        graphics.fast_path = r.fast_path

        if draw_last:
            x = data['index']
            y = data[array_key]
            graphics.draw_last(x, y)
            profiler('draw last segment')

        graphics.update()
        profiler('.update()')

        profiler('`graphics.update_from_array()` complete')
        return graphics


def by_index_and_key(
    array: np.ndarray,
    array_key: str,

) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    # full input data
    x = array['index']
    y = array[array_key]

    # # inview data
    # x_iv = in_view['index']
    # y_iv = in_view[array_key]

    return tuple({
        'x': x,
        'y': y,
        # 'x_iv': x_iv,
        # 'y_iv': y_iv,
        'connect': 'all',
    }.values())


class Renderer(msgspec.Struct):

    flow: Flow
    # last array view read
    last_read: Optional[tuple] = None
    format_xy: Callable[np.ndarray, tuple[np.ndarray]] = by_index_and_key

    # called to render path graphics
    # draw_path: Optional[Callable[np.ndarray, QPainterPath]] = None

    # output graphics rendering, the main object
    # processed in ``QGraphicsObject.paint()``
    path: Optional[QPainterPath] = None
    fast_path: Optional[QPainterPath] = None

    # called on input data but before any graphics format
    # conversions or processing.
    data_t: Optional[Callable[ShmArray, np.ndarray]] = None
    data_t_shm: Optional[ShmArray] = None

    # called on the final data (transform) output to convert
    # to "graphical data form" a format that can be passed to
    # the ``.draw()`` implementation.
    graphics_t: Optional[Callable[ShmArray, np.ndarray]] = None
    graphics_t_shm: Optional[ShmArray] = None

    # path graphics update implementation methods
    prepend_fn: Optional[Callable[QPainterPath, QPainterPath]] = None
    append_fn: Optional[Callable[QPainterPath, QPainterPath]] = None

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False

    # incremental update state(s)
    _last_vr: Optional[tuple[float, float]] = None
    _last_ivr: Optional[tuple[float, float]] = None

    # view-range incremental state
    _vr: Optional[tuple] = None
    _avr: Optional[tuple] = None

    def diff(
        self,
        new_read: tuple[np.ndarray],

    ) -> tuple[np.ndarray]:

        (
            last_xfirst,
            last_xlast,
            last_array,
            last_ivl, last_ivr,
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

        # TODO: eventually maybe we can implement some kind of
        # transform on the ``QPainterPath`` that will more or less
        # detect the diff in "elements" terms?
        # update state
        self.last_read = new_read

        # blah blah blah
        # do diffing for prepend, append and last entry
        return (
            prepend_length,
            append_length,
            # last,
        )

    # def gen_path_data(
    #     self,
    #     redraw: bool = False,
    # ) -> np.ndarray:
    #     ...

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

        input_data: Optional[tuple[np.ndarray]] = None,

        # redraw and ds flags
        should_redraw: bool = True,
        new_sample_rate: bool = False,
        should_ds: bool = False,
        showing_src_data: bool = True,

        do_append: bool = True,
        slice_to_head: int = -1,
        use_fpath: bool = True,

        # only render datums "in view" of the ``ChartView``
        use_vr: bool = True,
        graphics: Optional[pg.GraphicObject] = None,

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

        if use_vr:
            array = in_view

        if input_data:
            # allow input data passing for now from alt curve updaters.
            (
                x_out,
                y_out,
                x_iv,
                y_iv,
            ) = input_data
            connect = 'all'

            if use_vr:
                x_out = x_iv
                y_out = y_iv

            # last = y_out[slice_to_head]

        else:
            hist = array[:slice_to_head]
            # last = array[slice_to_head]

            (
                x_out,
                y_out,
                # x_iv,
                # y_iv,
                connect,
            ) = self.format_xy(hist, array_key)

            # print(f'{array_key} len x,y: {(len(x_out), len(y_out))}')
#             # full input data
#             x = array['index']
#             y = array[array_key]

#             # inview data
#             x_iv = in_view['index']
#             y_iv = in_view[array_key]

        profiler('sliced input arrays')

        (
            prepend_length,
            append_length,
        ) = self.diff(new_read)

        if (
            use_vr
        ):
            # if a view range is passed, plan to draw the
            # source ouput that's "in view" of the chart.
            view_range = (ivl, ivr)
            # print(f'{self._name} vr: {view_range}')

            # by default we only pull data up to the last (current) index
            # x_out = x_iv[:slice_to_head]
            # y_out = y_iv[:slice_to_head]

            profiler(f'view range slice {view_range}')

            vl, vr = view_range

            zoom_or_append = False
            last_vr = self._vr
            last_ivr = self._avr

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

            if (
                view_range != last_vr
                and (
                    append_length > 1
                    or zoom_or_append
                )
            ):
                should_redraw = True
                # print("REDRAWING BRUH")

            self._vr = view_range
            if len(x_out):
                self._avr = x_out[0], x_out[slice_to_head]

        if prepend_length > 0:
            should_redraw = True

        # # last datums
        # x_last = x_out[-1]
        # y_last = y_out[-1]

        path = self.path
        fast_path = self.fast_path

        if (
            path is None
            or should_redraw
            or new_sample_rate
            or prepend_length > 0
        ):
            # if should_redraw:
            #     if path:
            #         path.clear()
            #         profiler('cleared paths due to `should_redraw=True`')

            #     if fast_path:
            #         fast_path.clear()

            #     profiler('cleared paths due to `should_redraw` set')

            if new_sample_rate and showing_src_data:
                # if self._in_ds:
                log.info(f'DEDOWN -> {array_key}')

                self._in_ds = False

            elif should_ds and uppx > 1:

                x_out, y_out = xy_downsample(
                    x_out,
                    y_out,
                    uppx,
                )
                profiler(f'FULL PATH downsample redraw={should_ds}')
                self._in_ds = True
            # else:
            #     print(f"NOT DOWNSAMPLING {array_key}")

            path = self.draw_path(
                x=x_out,
                y=y_out,
                connect=connect,
                path=path,
                redraw=True,
            )
            # path = pg.functions.arrayToQPath(
            #     x_out,
            #     y_out,
            #     connect='all',
            #     finiteCheck=False,
            #     path=path,
            # )
            if graphics:
                graphics.prepareGeometryChange()

            profiler(
                'generated fresh path. '
                f'(should_redraw: {should_redraw} '
                f'should_ds: {should_ds} new_sample_rate: {new_sample_rate})'
            )
            # profiler(f'DRAW PATH IN VIEW -> {self.name}')

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
            # print(f'{self.name} append len: {append_length}')
            print(f'{array_key} append len: {append_length}')
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
                # path=fast_path,
            )

            # append_path = pg.functions.arrayToQPath(
            #     connect='all',
            #     finiteCheck=False,
            #     path=fast_path,
            # )
            profiler('generated append qpath')

            # if graphics.use_fpath:
            if use_fpath:
                print("USING FPATH")
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

        # if use_vr:
        #     array = in_view
            # # get latest data from flow shm
            # self.last_read = (
            #     xfirst, xlast, array, ivl, ivr, in_view
            # ) = new_read

        # if (
        #     self.path is None
        #     or use_vr
        # ):
            # redraw the entire source data if we have either of:
            # - no prior path graphic rendered or,
            # - we always intend to re-render the data only in view

            # data transform: convert source data to a format
            # expected to be incrementally updates and later rendered
            # to a more graphics native format.
            # if self.data_t:
            #     array = self.data_t(array)

                # maybe allocate shm for data transform output
                # if self.data_t_shm is None:
                #     fshm = self.flow.shm

                #     shm, opened = maybe_open_shm_array(
                #         f'{self.flow.name}_data_t',
                #         # TODO: create entry for each time frame
                #         dtype=array.dtype,
                #         readonly=False,
                #     )
                #     assert opened
                #     shm.push(array)
                #     self.data_t_shm = shm

        # elif self.path:
        #     print(f'inremental update not supported yet {self.flow.name}')
            # TODO: do incremental update
            # prepend, append, last = self.diff(self.flow.read())

            # do path generation for each segment
            # and then push into graphics object.

        # call path render func on history
        # self.path = self.draw_path(hist)
        self.path = path
        self.fast_path = fast_path

        self.last_read = new_read
        return self.path, array
