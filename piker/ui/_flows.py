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
from ._ohlc import BarItems


# class FlowsTable(msgspec.Struct):
#     '''
#     Data-AGGRegate: high level API onto multiple (categorized)
#     ``Flow``s with high level processing routines for
#     multi-graphics computations and display.

#     '''
#     flows: dict[str, np.ndarray] = {}


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
    is_ohlc: bool = False
    render: bool = True  # toggle for display loop

    graphics: pg.GraphicsObject

    # TODO: hackery to be able to set a shm later
    # but whilst also allowing this type to hashable,
    # likely will require serializable token that is used to attach
    # to the underlying shm ref after startup?
    _shm: Optional[ShmArray] = None  # currently, may be filled in "later"

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
        xfirst, xlast, array, ivl, ivr, in_view = self.read()

        if (
            not in_view.size
            or not render
        ):
            return self.graphics

        array_key = array_key or self.name

        graphics = self.graphics
        if isinstance(graphics, BarItems):
            graphics.update_from_array(
                array,
                in_view,
                view_range=(ivl, ivr) if use_vr else None,

                **kwargs,
            )

        else:
            graphics.update_from_array(
                x=array['index'],
                y=array[array_key],

                x_iv=in_view['index'],
                y_iv=in_view[array_key],
                view_range=(ivl, ivr) if use_vr else None,

                **kwargs
            )

        return graphics

        # @classmethod
        # def from_token(
        #     cls,
        #     shm_token: tuple[
        #         str,
        #         str,
        #         tuple[str, str],
        #     ],

        # ) -> PathRenderer:

        #     shm = attach_shm_array(token)
        #     return cls(shm)


class PathRenderer(msgspec.Struct):

    # output graphics rendering
    path: Optional[QPainterPath] = None

    last_read_src_array: np.ndarray
    # called on input data but before
    prerender_fn: Callable[ShmArray, np.ndarray]

    def diff(
        self,
    ) -> dict[str, np.ndarray]:
        ...

    def update(self) -> QPainterPath:
        '''
        Incrementally update the internal path graphics from
        updates in shm data and deliver the new (sub)-path
        generated.

        '''
        ...


    def render(
        self,

    ) -> list[QPainterPath]:
        '''
        Render the current graphics path(s)

        '''
        ...
