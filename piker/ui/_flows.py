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

from ..data._sharedmem import (
    ShmArray,
    open_shm_array,
)
from .._profile import pg_profile_enabled, ms_slower_then
from ._ohlc import (
    BarItems,
    gen_qpath,
)
from ._curve import (
    FastAppendCurve,
)
from ._compression import (
    ohlc_flatten,
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


def ohlc_flat_view(
    ohlc_shm: ShmArray,

    # XXX: we bind this in currently..
    x_basis: np.ndarray,

    # vr: Optional[slice] = None,

) -> np.ndarray:
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
    x = x_basis[:y.size]
    return x, y


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
    flat: Optional[ShmArray] = None
    x_basis: Optional[np.ndarray] = None
    _iflat: int = 0

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

        profiler=None,

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

            fields = ['open', 'high', 'low', 'close']
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

                # create a flattened view onto the OHLC array
                # which can be read as a line-style format
                shm = self.shm

                # flat = self.flat = self.shm.unstruct_view(fields)
                self.flat = self.shm.ustruct(fields)
                self._iflat = self.shm._last.value

                # import pdbpp
                # pdbpp.set_trace()
                # assert len(flat._array) == len(self.shm._array[fields])

                x = self.x_basis = (
                    np.broadcast_to(
                        shm._array['index'][:, None],
                        (
                            shm._array.size,
                            # 4,  # only ohlc
                            self.flat.shape[1],
                        ),
                    ) + np.array([-0.5, 0, 0, 0.5])
                )

                # fshm = self.flat = open_shm_array(
                #     f'{self.name}_flat',
                #     dtype=flattened.dtype,
                #     size=flattened.size,
                # )
                # fshm.push(flattened)

                # print(f'unstruct diff: {time.time() - start}')
                # profiler('read unstr view bars to line')
                # start = self.flat._first.value

                ds_curve_r = Renderer(
                    flow=self,

                    # just swap in the flat view
                    data_t=lambda array: self.flat.array,
                    # data_t=partial(
                    #     ohlc_flat_view,
                    #     self.shm,
                    # ),
                    last_read=read,
                    draw_path=partial(
                        rowarr_to_path,
                        x_basis=None,
                    ),

                )
                curve = FastAppendCurve(
                    # y=y,
                    # x=x,
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
                should_line = False

            elif (
                not should_line
                and uppx >= x_gt
            ):
                should_line = True

            profiler(f'ds logic complete line={should_line}')

            # do graphics updates
            if should_line:

                # update flatted ohlc copy
                iflat, ishm = self._iflat, self.shm._last.value
                to_update = rfn.structured_to_unstructured(
                    self.shm._array[iflat:ishm][fields]
                )

                # print(to_update)
                self.flat[iflat:ishm][:] = to_update
                profiler('updated ustruct OHLC data')

                y_flat = self.flat[:ishm]
                x_flat = self.x_basis[:ishm]

                self._iflat = ishm

                y = y_flat.reshape(-1)
                x = x_flat.reshape(-1)
                profiler('flattened ustruct OHLC data')

                y_iv_flat = y_flat[ivl:ivr]
                x_iv_flat = x_flat[ivl:ivr]

                y_iv = y_iv_flat.reshape(-1)
                x_iv = x_iv_flat.reshape(-1)
                profiler('flattened ustruct in-view OHLC data')

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
                )
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

            graphics.update_from_array(
                x=array['index'],
                y=array[array_key],

                x_iv=in_view['index'],
                y_iv=in_view[array_key],
                view_range=(ivl, ivr) if use_vr else None,

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
