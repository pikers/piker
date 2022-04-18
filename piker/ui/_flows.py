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
from typing import (
    Optional,
    Callable,
)

import msgspec
import numpy as np
import pyqtgraph as pg
from PyQt5.QtGui import QPainterPath

from ..data._sharedmem import (
    ShmArray,
    # attach_shm_array
)
from ._ohlc import (
    BarItems,
    gen_qpath,
)
from ._curve import (
    FastAppendCurve,
)
from ._compression import (
    ohlc_flatten,
)

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


class Flow(msgspec.Struct):  # , frozen=True):
    '''
    (FinancialSignal-)Flow compound type which wraps a real-time
    graphics (curve) and its backing data stream together for high level
    access and control.

    The intention is for this type to eventually be capable of shm-passing
    of incrementally updated graphics stream data between actors.

    '''
    name: str
    plot: pg.PlotItem
    graphics: pg.GraphicsObject
    _shm: ShmArray

    is_ohlc: bool = False
    render: bool = True  # toggle for display loop

    _last_uppx: float = 0
    _in_ds: bool = False

    _graphics_tranform_fn: Optional[Callable[ShmArray, np.ndarray]] = None

    # map from uppx -> (downsampled data, incremental graphics)
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

        **kwargs,

    ) -> pg.GraphicsObject:
        '''
        Read latest datums from shm and render to (incrementally)
        render to graphics.

        '''
        # shm read and slice to view
        read = xfirst, xlast, array, ivl, ivr, in_view = self.read()

        if (
            not in_view.size
            or not render
        ):
            return self.graphics

        graphics = self.graphics
        if isinstance(graphics, BarItems):

            # ugh, not luvin dis, should we have just a designated
            # instance var?
            r = self._render_table.get('src')
            if not r:
                r = Renderer(
                    flow=self,
                    draw=gen_qpath,  # TODO: rename this to something with ohlc
                    last_read=read,
                )
                self._render_table['src'] = (r, graphics)

                ds_curve_r = Renderer(
                    flow=self,
                    draw=gen_qpath,  # TODO: rename this to something with ohlc
                    last_read=read,
                    prerender_fn=ohlc_flatten,
                )

                # baseline "line" downsampled OHLC curve that should
                # kick on only when we reach a certain uppx threshold.
                self._render_table[0] = (
                    ds_curve_r,
                    FastAppendCurve(
                        y=y,
                        x=x,
                        name='OHLC',
                        color=self._color,
                    ),
                )

            # do checks for whether or not we require downsampling:
            # - if we're **not** downsampling then we simply want to
            #   render the bars graphics curve and update..
            # - if insteam we are in a downsamplig state then we to
            #   update our pre-downsample-ready data and then pass that
            #   new data the downsampler algo for incremental update.
            else:
                pass
                # do incremental update

            graphics.update_from_array(
                array,
                in_view,
                view_range=(ivl, ivr) if use_vr else None,

                **kwargs,
            )

            # generate and apply path to graphics obj
            graphics.path, last = r.render(only_in_view=True)
            graphics.draw_last(last)

        else:
            # should_ds = False
            # should_redraw = False

            # # downsampling incremental state checking
            # uppx = bars.x_uppx()
            # px_width = bars.px_width()
            # uppx_diff = (uppx - self._last_uppx)

            # if self.renderer is None:
            #     self.renderer = Renderer(
            #         flow=self,

            # if not self._in_ds:
            #     # in not currently marked as downsampling graphics
            #     # then only draw the full bars graphic for datums "in
            #     # view".

            # # check for downsampling conditions
            # if (
            #     # std m4 downsample conditions
            #     px_width
            #     and uppx_diff >= 4
            #     or uppx_diff <= -3
            #     or self._step_mode and abs(uppx_diff) >= 4

            # ):
            #     log.info(
            #         f'{self._name} sampler change: {self._last_uppx} -> {uppx}'
            #     )
            #     self._last_uppx = uppx
            #     should_ds = True

            # elif (
            #     uppx <= 2
            #     and self._in_ds
            # ):
            #     # we should de-downsample back to our original
            #     # source data so we clear our path data in prep
            #     # to generate a new one from original source data.
            #     should_redraw = True
            #     should_ds = False

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


class Renderer(msgspec.Struct):

    flow: Flow

    # called to render path graphics
    draw: Callable[np.ndarray, QPainterPath]

    # called on input data but before
    prerender_fn: Optional[Callable[ShmArray, np.ndarray]] = None

    prepend_fn: Optional[Callable[QPainterPath, QPainterPath]] = None
    append_fn: Optional[Callable[QPainterPath, QPainterPath]] = None

    # last array view read
    last_read: Optional[np.ndarray] = None

    # output graphics rendering
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

        # only render datums "in view" of the ``ChartView``
        only_in_view: bool = True,

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
        xfirst, xlast, array, ivl, ivr, in_view = self.last_read

        if only_in_view:
            # get latest data from flow shm
            self.last_read = (
                xfirst, xlast, array, ivl, ivr, in_view
            ) = self.flow.read()

            array = in_view

        if self.path is None or in_view:
            # redraw the entire source data if we have either of:
            # - no prior path graphic rendered or,
            # - we always intend to re-render the data only in view

            if self.prerender_fn:
                array = self.prerender_fn(array)

            hist, last = array[:-1], array[-1]

            # call path render func on history
            self.path = self.draw(hist)

        elif self.path:
            print(f'inremental update not supported yet {self.flow.name}')
            # TODO: do incremental update
            # prepend, append, last = self.diff(self.flow.read())

            # do path generation for each segment
            # and then push into graphics object.

        return self.path, last
