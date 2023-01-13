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
)

import msgspec
import numpy as np
import pyqtgraph as pg
from PyQt5.QtGui import QPainterPath
from PyQt5.QtCore import QLineF

from ..data._sharedmem import (
    ShmArray,
)
from .._profile import (
    pg_profile_enabled,
    # ms_slower_then,
)
from ._pathops import (
    IncrementalFormatter,
    OHLCBarsFmtr,  # Plain OHLC renderer
    OHLCBarsAsCurveFmtr,  # OHLC converted to line
    StepCurveFmtr,  # "step" curve (like for vlm)
    xy_downsample,
)
from ._ohlc import (
    BarItems,
    # bar_from_ohlc_row,
)
from ._curve import (
    Curve,
    StepCurve,
    FlattenedOHLC,
)
from ..log import get_logger
from .._profile import Profiler


log = get_logger(__name__)


def render_baritems(
    flow: Flow,
    graphics: BarItems,
    read: tuple[
        int, int, np.ndarray,
        int, int, np.ndarray,
    ],
    profiler: Profiler,
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
            fmtr=OHLCBarsFmtr(
                shm=flow.shm,
                flow=flow,
                _last_read=read,
            ),
        )

        ds_curve_r = Renderer(
            flow=self,
            fmtr=OHLCBarsAsCurveFmtr(
                shm=flow.shm,
                flow=flow,
                _last_read=read,
            ),
        )

        curve = FlattenedOHLC(
            name=f'{flow.name}_ds_ohlc',
            color=bars._color,
        )
        flow.ds_graphics = curve
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
        in_line
        and uppx < x_gt
    ):
        # print('FLIPPING TO BARS')
        should_line = False
        flow._in_ds = False

    elif (
        not in_line
        and uppx >= x_gt
    ):
        # print('FLIPPING TO LINE')
        should_line = True
        flow._in_ds = True

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

    return (
        graphics,
        r,
        {'read_from_key': False},
        should_line,
        changed_to_line,
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
    graphics: Curve | BarItems
    _shm: ShmArray
    yrange: tuple[float, float] = None

    # in some cases a flow may want to change its
    # graphical "type" or, "form" when downsampling,
    # normally this is just a plain line.
    ds_graphics: Optional[Curve] = None

    is_ohlc: bool = False
    render: bool = True  # toggle for display loop

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False

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

    ) -> Optional[tuple[float, float]]:
        '''
        Compute the cached max and min y-range values for a given
        x-range determined by ``lbar`` and ``rbar`` or ``None``
        if no range can be determined (yet).

        '''
        rkey = (lbar, rbar)
        cached_result = self._mxmns.get(rkey)
        if cached_result:
            return cached_result

        shm = self.shm
        if shm is None:
            return None

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
            return None

        elif self.yrange:
            mxmn = self.yrange
            # print(f'{self.name} M4 maxmin: {mxmn}')

        else:
            if self.is_ohlc:
                ylow = np.min(slice_view['low'])
                yhigh = np.max(slice_view['high'])

            else:
                view = slice_view[self.name]
                ylow = np.min(view)
                yhigh = np.max(view)

            mxmn = ylow, yhigh
            # print(f'{self.name} MANUAL maxmin: {mxmin}')

        # cache result for input range
        assert mxmn
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

        profiler: Optional[Profiler] = None,
        do_append: bool = True,

        **kwargs,

    ) -> pg.GraphicsObject:
        '''
        Read latest datums from shm and render to (incrementally)
        render to graphics.

        '''
        profiler = Profiler(
            msg=f'Flow.update_graphics() for {self.name}',
            disabled=not pg_profile_enabled(),
            ms_threshold=4,
            # ms_threshold=ms_slower_then,
        )
        # shm read and slice to view
        read = (
            xfirst, xlast, src_array,
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

        slice_to_head: int = -1
        should_redraw: bool = False
        should_line: bool = False
        rkwargs = {}

        # TODO: probably specialize ``Renderer`` types instead of
        # these logic checks?
        # - put these blocks into a `.load_renderer()` meth?
        # - consider a OHLCRenderer, StepCurveRenderer, Renderer?
        r = self._src_r
        if isinstance(graphics, BarItems):
            # XXX: special case where we change out graphics
            # to a line after a certain uppx threshold.
            (
                graphics,
                r,
                rkwargs,
                should_line,
                changed_to_line,
            ) = render_baritems(
                self,
                graphics,
                read,
                profiler,
                **kwargs,
            )
            should_redraw = changed_to_line or not should_line
            self._in_ds = should_line

        elif not r:
            if isinstance(graphics, StepCurve):

                r = self._src_r = Renderer(
                    flow=self,
                    fmtr=StepCurveFmtr(
                        shm=self.shm,
                        flow=self,
                        _last_read=read,
                    ),
                )

                # TODO: append logic inside ``.render()`` isn't
                # correct yet for step curves.. remove this to see it.
                should_redraw = True
                slice_to_head = -2

            else:
                r = self._src_r
                if not r:
                    # just using for ``.diff()`` atm..
                    r = self._src_r = Renderer(
                        flow=self,
                        fmtr=IncrementalFormatter(
                            shm=self.shm,
                            flow=self,
                            _last_read=read,
                        ),
                    )

        # ``Curve`` derivative case(s):
        array_key = array_key or self.name
        # print(array_key)

        # ds update config
        new_sample_rate: bool = False
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
            should_ds = False
            should_redraw = True

            showing_src_data = True
            # reset yrange to be computed from source data
            self.yrange = None

        # MAIN RENDER LOGIC:
        # - determine in view data and redraw on range change
        # - determine downsampling ops if needed
        # - (incrementally) update ``QPainterPath``

        out = r.render(
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
        if showing_src_data:
            # print(f"{self.name} SHOWING SOURCE")
            # reset yrange to be computed from source data
            self.yrange = None

        if not out:
            log.warning(f'{self.name} failed to render!?')
            return graphics

        path, data, reset = out

        # if self.yrange:
        #     print(f'flow {self.name} yrange from m4: {self.yrange}')

        # XXX: SUPER UGGGHHH... without this we get stale cache
        # graphics that don't update until you downsampler again..
        if reset:
            with graphics.reset_cache():
                # assign output paths to graphicis obj
                graphics.path = r.path
                graphics.fast_path = r.fast_path

                # XXX: we don't need this right?
                # graphics.draw_last_datum(
                #     path,
                #     src_array,
                #     data,
                #     reset,
                #     array_key,
                # )
                # graphics.update()
                # profiler('.update()')
        else:
            # assign output paths to graphicis obj
            graphics.path = r.path
            graphics.fast_path = r.fast_path

        graphics.draw_last_datum(
            path,
            src_array,
            data,
            reset,
            array_key,
        )
        graphics.update()
        profiler('.update()')

        # TODO: does this actuallly help us in any way (prolly should
        # look at the source / ask ogi). I think it avoid artifacts on
        # wheel-scroll downsampling curve updates?
        # TODO: is this ever better?
        # graphics.prepareGeometryChange()
        # profiler('.prepareGeometryChange()')

        # track downsampled state
        self._in_ds = r._in_ds

        return graphics

    def draw_last(
        self,
        array_key: Optional[str] = None,
        only_last_uppx: bool = False,

    ) -> None:

        # shm read and slice to view
        (
            xfirst, xlast, src_array,
            ivl, ivr, in_view,
        ) = self.read()

        g = self.graphics
        array_key = array_key or self.name
        x, y = g.draw_last_datum(
            g.path,
            src_array,
            src_array,
            False,  # never reset path
            array_key,
        )

        # the renderer is downsampling we choose
        # to always try and updadte a single (interpolating)
        # line segment that spans and tries to display
        # the las uppx's worth of datums.
        # we only care about the last pixel's
        # worth of data since that's all the screen
        # can represent on the last column where
        # the most recent datum is being drawn.
        if self._in_ds or only_last_uppx:
            dsg = self.ds_graphics or self.graphics

            # XXX: pretty sure we don't need this?
            # if isinstance(g, Curve):
            #     with dsg.reset_cache():
            uppx = self._last_uppx
            y = y[-uppx:]
            ymn, ymx = y.min(), y.max()
            # print(f'drawing uppx={uppx} mxmn line: {ymn}, {ymx}')
            try:
                iuppx = x[-uppx]
            except IndexError:
                # we're less then an x-px wide so just grab the start
                # datum index.
                iuppx = x[0]

            dsg._last_line = QLineF(
                iuppx, ymn,
                x[-1], ymx,
            )
            # print(f'updating DS curve {self.name}')
            dsg.update()

        else:
            # print(f'updating NOT DS curve {self.name}')
            g.update()


class Renderer(msgspec.Struct):

    flow: Flow
    fmtr: IncrementalFormatter

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

    def draw_path(
        self,
        x: np.ndarray,
        y: np.ndarray,
        connect: str | np.ndarray = 'all',
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
            # path=path,  # path re-use / reserving
        )

        # avoid mem allocs if possible
        if path_was_none:
            path.reserve(path.capacity())

        return path

    def render(
        self,

        new_read,
        array_key: str,
        profiler: Profiler,
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
        fmtr = self.fmtr

        (
            _,
            _,
            array,
            ivl,
            ivr,
            in_view,
        ) = new_read

        # xy-path data transform: convert source data to a format
        # able to be passed to a `QPainterPath` rendering routine.
        fmt_out = fmtr.format_to_1d(
            new_read,
            array_key,
            profiler,

            slice_to_head=slice_to_head,
            read_src_from_key=read_from_key,
            slice_to_inview=use_vr,
        )

        # no history in view case
        if not fmt_out:
            # XXX: this might be why the profiler only has exits?
            return

        (
            x_1d,
            y_1d,
            connect,
            prepend_length,
            append_length,
            view_changed,

        ) = fmt_out

        # redraw conditions
        if (
            prepend_length > 0
            or new_sample_rate
            or append_length > 0
            or view_changed
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

                x_1d, y_1d, ymn, ymx = xy_downsample(
                    x_1d,
                    y_1d,
                    uppx,
                )
                self.flow.yrange = ymn, ymx
                # print(f'{self.flow.name} post ds: ymn, ymx: {ymn},{ymx}')

                reset = True
                profiler(f'FULL PATH downsample redraw={should_ds}')
                self._in_ds = True

            path = self.draw_path(
                x=x_1d,
                y=y_1d,
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
            print(f'{array_key} append len: {append_length}')
            new_x = x_1d[-append_length - 2:]  # slice_to_head]
            new_y = y_1d[-append_length - 2:]  # slice_to_head]
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
                print(f'{self.flow.name}: FAST PATH')
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

        return self.path, array, reset
