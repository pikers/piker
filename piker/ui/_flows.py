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
from .._profile import pg_profile_enabled, ms_slower_then
from ._ohlc import (
    BarItems,
    gen_qpath,
)
from ._curve import (
    FastAppendCurve,
    # step_path_arrays_from_1d,
)
from ._compression import (
    # ohlc_flatten,
    ds_m4,
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


def mk_ohlc_flat_copy(
    ohlc_shm: ShmArray,

    # XXX: we bind this in currently..
    # x_basis: np.ndarray,

    # vr: Optional[slice] = None,

) -> tuple[np.ndarray, np.ndarray]:
    '''
    Return flattened-non-copy view into an OHLC shm array.

    '''
    ohlc = ohlc_shm._array[['open', 'high', 'low', 'close']]
    # if vr:
    #     ohlc = ohlc[vr]
    #     x = x_basis[vr]

    unstructured = rfn.structured_to_unstructured(
        ohlc,
        copy=False,
    )
    # breakpoint()
    y = unstructured.flatten()
    # x = x_basis[:y.size]
    return y


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
    gy: Optional[ShmArray] = None
    gx: Optional[np.ndarray] = None
    _iflat_last: int = 0
    _iflat_first: int = 0

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

    def read(self) -> tuple[
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

        **kwargs,

    ) -> pg.GraphicsObject:
        '''
        Read latest datums from shm and render to (incrementally)
        render to graphics.

        '''

        profiler = profiler or pg.debug.Profiler(
            msg=f'Flow.update_graphics() for {self.name}',
            disabled=not pg_profile_enabled(),
            gt=ms_slower_then,
            delayed=True,
        )
        # shm read and slice to view
        read = (
            xfirst, xlast, array,
            ivl, ivr, in_view,
        ) = self.read()
        profiler('read src shm data')

        if (
            not in_view.size
            or not render
        ):
            return self.graphics

        graphics = self.graphics
        if isinstance(graphics, BarItems):

            # if no source data renderer exists create one.
            r = self._src_r
            if not r:
                # OHLC bars path renderer
                r = self._src_r = Renderer(
                    flow=self,
                    # TODO: rename this to something with ohlc
                    draw_path=gen_qpath,
                    last_read=read,
                )

                ds_curve_r = Renderer(
                    flow=self,

                    # just swap in the flat view
                    # data_t=lambda array: self.gy.array,
                    last_read=read,
                    draw_path=partial(
                        rowarr_to_path,
                        x_basis=None,
                    ),

                )
                curve = FastAppendCurve(
                    name='OHLC',
                    color=graphics._color,
                )
                curve.hide()
                self.plot.addItem(curve)

                # baseline "line" downsampled OHLC curve that should
                # kick on only when we reach a certain uppx threshold.
                self._render_table[0] = (
                    ds_curve_r,
                    curve,
                )

            dsc_r, curve = self._render_table[0]

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

                    # flat = self.gy = self.shm.unstruct_view(fields)
                    self.gy = self.shm.ustruct(fields)
                    first = self._iflat_first = self.shm._first.value
                    last = self._iflat_last = self.shm._last.value

                    # write pushed data to flattened copy
                    self.gy[first:last] = rfn.structured_to_unstructured(
                        self.shm.array[fields]
                    )

                    # generate an flat-interpolated x-domain
                    self.gx = (
                        np.broadcast_to(
                            shm._array['index'][:, None],
                            (
                                shm._array.size,
                                # 4,  # only ohlc
                                self.gy.shape[1],
                            ),
                        ) + np.array([-0.5, 0, 0, 0.5])
                    )
                    assert self.gy.any()

                # print(f'unstruct diff: {time.time() - start}')
                # profiler('read unstr view bars to line')
                # start = self.gy._first.value
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
                    self.gy[
                        ishm_first:iflat_first
                    ] = rfn.structured_to_unstructured(
                        self.shm._array[fields][ishm_first:iflat_first]
                    )
                    self._iflat_first = ishm_first

                #     # flat = self.gy = self.shm.unstruct_view(fields)
                #     self.gy = self.shm.ustruct(fields)
                #     # self._iflat_last = self.shm._last.value

                #     # self._iflat_first = self.shm._first.value
                #     # do an update for the most recent prepend
                #     # index
                #     iflat = ishm_first

                to_update = rfn.structured_to_unstructured(
                    self.shm._array[iflat:ishm_last][fields]
                )

                self.gy[iflat:ishm_last][:] = to_update
                profiler('updated ustruct OHLC data')

                # slice out up-to-last step contents
                y_flat = self.gy[ishm_first:ishm_last]
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

                # legacy full-recompute-everytime method
                # x, y = ohlc_flatten(array)
                # x_iv, y_iv = ohlc_flatten(in_view)
                # profiler('flattened OHLC data')

                curve.update_from_array(
                    x,
                    y,
                    x_iv=x_iv,
                    y_iv=y_iv,
                    view_range=(ivl, ivr),  # hack
                    profiler=profiler,
                    # should_redraw=False,

                    # NOTE: already passed through by display loop?
                    # do_append=uppx < 16,
                    **kwargs,
                )
                curve.show()
                profiler('updated ds curve')

            else:
                # render incremental or in-view update
                # and apply ouput (path) to graphics.
                path, last = r.render(
                    read,
                    only_in_view=True,
                )

                graphics.path = path
                graphics.draw_last(last)

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

        else:
            # ``FastAppendCurve`` case:
            array_key = array_key or self.name
            uppx = graphics.x_uppx()

            if graphics._step_mode and self.gy is None:
                self._iflat_first = self.shm._first.value

                # create a flattened view onto the OHLC array
                # which can be read as a line-style format
                shm = self.shm

                # fields = ['index', array_key]
                i = shm._array['index'].copy()
                out = shm._array[array_key].copy()

                self.gx = np.broadcast_to(
                    i[:, None],
                    (i.size, 2),
                ) + np.array([-0.5, 0.5])

                # self.gy = np.broadcast_to(
                #     out[:, None], (out.size, 2),
                # )
                self.gy = np.empty((len(out), 2), dtype=out.dtype)
                self.gy[:] = out[:, np.newaxis]

                # start y at origin level
                self.gy[0, 0] = 0

            if graphics._step_mode:
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

                il = max(iflat - 1, 0)

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

                y_step = self.gy[ishm_first:ishm_last+2]
                lasts = self.shm.array[['index', array_key]]
                last = lasts[array_key][-1]
                y_step[-1] = last
                # shape to 1d
                y = y_step.reshape(-1)

                # s = 6
                # print(f'lasts: {x[-2*s:]}, {y[-2*s:]}')

                profiler('sliced step data')

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
                profiler('flattened ustruct in-view OHLC data')

                # legacy full-recompute-everytime method
                # x, y = ohlc_flatten(array)
                # x_iv, y_iv = ohlc_flatten(in_view)
                # profiler('flattened OHLC data')

                x_last = array['index'][-1]
                y_last = array[array_key][-1]
                graphics._last_line = QLineF(
                    x_last - 0.5, 0,
                    x_last + 0.5, 0,
                )
                graphics._last_step_rect = QRectF(
                    x_last - 0.5, 0,
                    x_last + 0.5, y_last,
                )
                # graphics.update()

                graphics.update_from_array(
                    x=x,
                    y=y,

                    x_iv=x_iv,
                    y_iv=y_iv,

                    view_range=(ivl, ivr) if use_vr else None,

                    draw_last=False,
                    slice_to_head=-2,

                    should_redraw=bool(append_diff),

                    # NOTE: already passed through by display loop?
                    # do_append=uppx < 16,

                    **kwargs
                )
                # graphics.reset_cache()
                # print(
                #     f"path br: {graphics.path.boundingRect()}\n",
                #     # f"fast path br: {graphics.fast_path.boundingRect()}",
                #     f"last rect br: {graphics._last_step_rect}\n",
                #     f"full br: {graphics._br}\n",
                # )

            else:
                x = array['index']
                y = array[array_key]
                x_iv = in_view['index']
                y_iv = in_view[array_key]

                # graphics.draw_last(x, y)
                profiler(f'draw last segment {array_key}')

                graphics.update_from_array(
                    x=x,
                    y=y,

                    x_iv=x_iv,
                    y_iv=y_iv,

                    view_range=(ivl, ivr) if use_vr else None,

                    # NOTE: already passed through by display loop?
                    # do_append=uppx < 16,
                    **kwargs
                )

        return graphics


def xy_downsample(
    x,
    y,
    px_width,
    uppx,

    x_spacer: float = 0.5,

) -> tuple[np.ndarray, np.ndarray]:

    # downsample whenever more then 1 pixels per datum can be shown.
    # always refresh data bounds until we get diffing
    # working properly, see above..
    bins, x, y = ds_m4(
        x,
        y,
        px_width=px_width,
        uppx=uppx,
        log_scale=bool(uppx)
    )

    # flatten output to 1d arrays suitable for path-graphics generation.
    x = np.broadcast_to(x[:, None], y.shape)
    x = (x + np.array(
        [-x_spacer, 0, 0, x_spacer]
    )).flatten()
    y = y.flatten()

    return x, y


class Renderer(msgspec.Struct):

    flow: Flow

    # called to render path graphics
    draw_path: Callable[np.ndarray, QPainterPath]

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

    # last array view read
    last_read: Optional[np.ndarray] = None

    # output graphics rendering, the main object
    # processed in ``QGraphicsObject.paint()``
    path: Optional[QPainterPath] = None

    # def diff(
    #     self,
    #     latest_read: tuple[np.ndarray],

    # ) -> tuple[np.ndarray]:
    #     # blah blah blah
    #     # do diffing for prepend, append and last entry
    #     return (
    #         to_prepend
    #         to_append
    #         last,
    #     )

    def render(
        self,

        new_read,

        # only render datums "in view" of the ``ChartView``
        only_in_view: bool = False,

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
        # do full source data render to path
        (
            xfirst, xlast, array,
            ivl, ivr, in_view,
        ) = self.last_read

        if only_in_view:
            array = in_view
            # # get latest data from flow shm
            # self.last_read = (
            #     xfirst, xlast, array, ivl, ivr, in_view
            # ) = new_read

        if self.path is None or only_in_view:
            # redraw the entire source data if we have either of:
            # - no prior path graphic rendered or,
            # - we always intend to re-render the data only in view

            # data transform: convert source data to a format
            # expected to be incrementally updates and later rendered
            # to a more graphics native format.
            if self.data_t:
                array = self.data_t(array)

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

        elif self.path:
            print(f'inremental update not supported yet {self.flow.name}')
            # TODO: do incremental update
            # prepend, append, last = self.diff(self.flow.read())

            # do path generation for each segment
            # and then push into graphics object.

        hist, last = array[:-1], array[-1]

        # call path render func on history
        self.path = self.draw_path(hist)

        self.last_read = new_read
        return self.path, last
