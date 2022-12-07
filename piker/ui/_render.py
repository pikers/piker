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
    TYPE_CHECKING,
)

import msgspec
import numpy as np
import pyqtgraph as pg
from PyQt5.QtGui import QPainterPath
from PyQt5.QtCore import QLineF

from ..data._sharedmem import (
    ShmArray,
)
from ..data.feed import Flume
from ..data._formatters import (
    IncrementalFormatter,
    OHLCBarsFmtr,  # Plain OHLC renderer
    OHLCBarsAsCurveFmtr,  # OHLC converted to line
    StepCurveFmtr,  # "step" curve (like for vlm)
)
from ..data._pathops import (
    xy_downsample,
    slice_from_time,
)
from ._ohlc import (
    BarItems,
)
from ._curve import (
    Curve,
    StepCurve,
    FlattenedOHLC,
)
from ..log import get_logger
from .._profile import (
    Profiler,
    pg_profile_enabled,
)


if TYPE_CHECKING:
    from ._interaction import ChartView
    from ._chart import ChartPlotWidget


log = get_logger(__name__)


def render_baritems(
    viz: Viz,
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
    self = viz
    show_bars: bool = False

    r = self._src_r
    if not r:
        show_bars = True

        # OHLC bars path renderer
        r = self._src_r = Renderer(
            viz=self,
            fmtr=OHLCBarsFmtr(
                shm=viz.shm,
                viz=viz,
            ),
        )

        ds_curve_r = Renderer(
            viz=self,
            fmtr=OHLCBarsAsCurveFmtr(
                shm=viz.shm,
                viz=viz,
            ),
        )

        curve = FlattenedOHLC(
            name=f'{viz.name}_ds_ohlc',
            color=bars._color,
        )
        viz.ds_graphics = curve
        curve.hide()
        self.plot.addItem(curve)

        # baseline "line" downsampled OHLC curve that should
        # kick on only when we reach a certain uppx threshold.
        self._render_table = (ds_curve_r, curve)

    ds_r, curve = self._render_table

    # print(
    #     f'r: {r.fmtr.xy_slice}\n'
    #     f'ds_r: {ds_r.fmtr.xy_slice}\n'
    # )

    # do checks for whether or not we require downsampling:
    # - if we're **not** downsampling then we simply want to
    #   render the bars graphics curve and update..
    # - if instead we are in a downsamplig state then we to
    x_gt = 6 * (r.fmtr.index_step_size or 1)
    uppx = curve.x_uppx()
    # print(f'BARS UPPX: {uppx}')
    in_line = should_line = curve.isVisible()
    if (
        in_line
        and uppx < x_gt
    ):
        # print('FLIPPING TO BARS')
        should_line = False
        viz._in_ds = False

    elif (
        not in_line
        and uppx >= x_gt
    ):
        # print('FLIPPING TO LINE')
        should_line = True
        viz._in_ds = True

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
        # breakpoint()

    return (
        graphics,
        r,
        {'read_from_key': False},
        should_line,
        changed_to_line,
    )


class Viz(msgspec.Struct):  # , frozen=True):
    '''
    (Data) "Visualization" compound type which wraps a real-time
    shm array stream with displayed graphics (curves, charts)
    for high level access and control as well as efficient incremental
    update.

    The intention is for this type to eventually be capable of shm-passing
    of incrementally updated graphics stream data between actors.

    '''
    name: str
    plot: pg.PlotItem
    _shm: ShmArray
    flume: Flume
    graphics: Curve | BarItems

    # for tracking y-mn/mx for y-axis auto-ranging
    yrange: tuple[float, float] = None

    # in some cases a viz may want to change its
    # graphical "type" or, "form" when downsampling, to
    # start this is only ever an interpolation line.
    ds_graphics: Optional[Curve] = None

    is_ohlc: bool = False
    render: bool = True  # toggle for display loop

    # _index_field: str = 'index'
    _index_field: str = 'time'

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False
    _index_step: float | None = None

    # map from uppx -> (downsampled data, incremental graphics)
    _src_r: Optional[Renderer] = None
    _render_table: dict[
        Optional[int],
        tuple[Renderer, pg.GraphicsItem],
    ] = (None, None)

    # cache of y-range values per x-range input.
    _mxmns: dict[tuple[int, int], tuple[float, float]] = {}

    @property
    def shm(self) -> ShmArray:
        return self._shm

    @property
    def index_field(self) -> str:
        return self._index_field

    def index_step(
        self,
        reset: bool = False,

    ) -> float:
        if self._index_step is None:
            index = self.shm.array[self.index_field]
            self._index_step = index[-1] - index[-2]

        return self._index_step

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
        # TODO: hash the slice instead maybe?
        # https://stackoverflow.com/a/29980872
        rkey = (round(lbar), round(rbar))
        cached_result = self._mxmns.get(rkey)
        do_print = 'btc' in self.name
        if cached_result:

            # if do_print:
            #     print(
            #         f'{self.name} CACHED maxmin\n'
            #         f'{rkey} -> {cached_result}'
            #     )
            return cached_result

        shm = self.shm
        if shm is None:
            breakpoint()
            return None

        arr = shm.array
        # times = arr['time']
        # step = round(times[-1] - times[-2])
        # if (
        #     do_print
        #     and step == 60
        # ):
        #     breakpoint()

        # get relative slice indexes into array
        if self.index_field == 'time':
            read_slc = slice_from_time(
                arr,
                start_t=lbar,
                stop_t=rbar,
            )
            slice_view = arr[read_slc]

        else:
            ifirst = arr[0]['index']
            slice_view = arr[
                lbar - ifirst:
                (rbar - ifirst) + 1
            ]

        if not slice_view.size:
            log.warning(f'{self.name} no maxmin in view?')
            # breakpoint()
            return None

        elif self.yrange:
            mxmn = self.yrange
            if do_print:
                print(
                    f'{self.name} M4 maxmin:\n'
                    f'{rkey} -> {mxmn}'
                )

        else:
            if self.is_ohlc:
                ylow = np.min(slice_view['low'])
                yhigh = np.max(slice_view['high'])

            else:
                view = slice_view[self.name]
                ylow = np.min(view)
                yhigh = np.max(view)

            mxmn = ylow, yhigh
            if do_print:
                print(
                    f'{self.name} MANUAL ohlc={self.is_ohlc} maxmin:\n'
                    f'{rkey} -> {mxmn}'
                )

        # cache result for input range
        assert mxmn
        self._mxmns[rkey] = mxmn

        return mxmn

    def view_range(self) -> tuple[int, int]:
        '''
        Return the start and stop x-indexes for the managed ``ViewBox``.

        '''
        vr = self.plot.viewRect()
        return (
            vr.left(),
            vr.right(),
        )

    def bars_range(self) -> tuple[int, int, int, int]:
        '''
        Return a range tuple for the left-view, left-datum, right-datum
        and right-view x-indices.

        '''
        l, start, datum_start, datum_stop, stop, r = self.datums_range()
        return l, datum_start, datum_stop, r

    def datums_range(
        self,
        view_range: None | tuple[float, float] = None,
        index_field: str | None = None,
        array: None | np.ndarray = None,

    ) -> tuple[
        int, int, int, int, int, int
    ]:
        '''
        Return a range tuple for the datums present in view.

        '''
        l, r = view_range or self.view_range()

        index_field: str = index_field or self.index_field
        if index_field == 'index':
            l, r = round(l), round(r)

        if array is None:
            array = self.shm.array

        index = array[index_field]
        first = round(index[0])
        last = round(index[-1])

        # first and last datums in view determined by
        # l / r view range.
        leftmost = round(l)
        rightmost = round(r)

        # invalid view state
        if (
            r < l
            or l < 0
            or r < 0
            or (l > last and r > last)
        ):
            leftmost = first
            rightmost = last
        else:
            rightmost = max(
                min(last, rightmost),
                first,
            )

            leftmost = min(
                max(first, leftmost),
                last,
                rightmost - 1,
            )

            assert leftmost < rightmost

        return (
            l,  # left x-in-view
            first,  # first datum
            leftmost,
            rightmost,
            last,  # last_datum
            r,  # right-x-in-view
        )

    def read(
        self,
        array_field: Optional[str] = None,
        index_field: str | None = None,
        profiler: None | Profiler = None,

    ) -> tuple[
        int, int, np.ndarray,
        int, int, np.ndarray,
    ]:
        '''
        Read the underlying shm array buffer and
        return the data plus indexes for the first
        and last
        which has been written to.

        '''
        index_field: str = index_field or self.index_field
        vr = l, r = self.view_range()

        # readable data
        array = self.shm.array

        if profiler:
            profiler('self.shm.array READ')

        (
            l,
            ifirst,
            lbar,
            rbar,
            ilast,
            r,
        ) = self.datums_range(
            view_range=vr,
            index_field=index_field,
            array=array,
        )
        # if rbar < lbar:
        #     breakpoint()

        if profiler:
            profiler('self.datums_range()')

        abs_slc = slice(ifirst, ilast)

        # TODO: support time slicing
        if index_field == 'time':
            read_slc = slice_from_time(
                array,
                start_t=lbar,
                stop_t=rbar,
            )

            # TODO: maybe we should return this from the slicer call
            # above?
            in_view = array[read_slc]
            if in_view.size:
                abs_indx = in_view['index']
                abs_slc = slice(
                    int(abs_indx[0]),
                    int(abs_indx[-1]),
                )

            if profiler:
                profiler(
                    '`slice_from_time('
                    f'start_t={lbar}'
                    f'stop_t={rbar})'
                )

        # array-index slicing
        # TODO: can we do time based indexing using arithmetic presuming
        # a uniform time stamp step size?
        else:
            # get read-relative indices adjusting for master shm index.
            lbar_i = max(l, ifirst) - ifirst
            rbar_i = min(r, ilast) - ifirst

            # NOTE: the slice here does NOT include the extra ``+ 1``
            # BUT the ``in_view`` slice DOES..
            read_slc = slice(lbar_i, rbar_i)
            in_view = array[lbar_i: rbar_i + 1]

            # XXX: same as ^
            # to_draw = array[lbar - ifirst:(rbar - ifirst) + 1]
            if profiler:
                profiler('index arithmetic for slicing')

        if array_field:
            array = array[array_field]

        return (
            # abs indices + full data set
            abs_slc.start,
            abs_slc.stop,
            array,

            # relative (read) indices + in view data
            read_slc.start,
            read_slc.stop,
            in_view,
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
            msg=f'Viz.update_graphics() for {self.name}',
            disabled=not pg_profile_enabled(),
            ms_threshold=4,
            # ms_threshold=ms_slower_then,
        )
        # shm read and slice to view
        read = (
            xfirst, xlast, src_array,
            ivl, ivr, in_view,
        ) = self.read(profiler=profiler)

        profiler('read src shm data')

        graphics = self.graphics

        if (
            not in_view.size
            or not render
        ):
            # print('exiting early')
            return graphics

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
                    viz=self,
                    fmtr=StepCurveFmtr(
                        shm=self.shm,
                        viz=self,
                    ),
                )

                # TODO: append logic inside ``.render()`` isn't
                # correct yet for step curves.. remove this to see it.
                should_redraw = True

            else:
                r = self._src_r
                if not r:
                    # just using for ``.diff()`` atm..
                    r = self._src_r = Renderer(
                        viz=self,
                        fmtr=IncrementalFormatter(
                            shm=self.shm,
                            viz=self,
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

            do_append=do_append,

            **rkwargs,
        )

        if not out:
            log.warning(f'{self.name} failed to render!?')
            return graphics

        path, data, reset = out

        # if self.yrange:
        #     print(f'viz {self.name} yrange from m4: {self.yrange}')

        # XXX: SUPER UGGGHHH... without this we get stale cache
        # graphics that don't update until you downsampler again..
        # if reset:
        #     with graphics.reset_cache():
        #         # assign output paths to graphicis obj
        #         graphics.path = r.path
        #         graphics.fast_path = r.fast_path

        #         # XXX: we don't need this right?
        #         # graphics.draw_last_datum(
        #         #     path,
        #         #     src_array,
        #         #     data,
        #         #     reset,
        #         #     array_key,
        #         # )
        #         # graphics.update()
        #         # profiler('.update()')
        # else:
        # assign output paths to graphicis obj
        graphics.path = r.path
        graphics.fast_path = r.fast_path

        graphics.draw_last_datum(
            path,
            src_array,
            data,
            reset,
            array_key,
            index_field=self.index_field,
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
            self.index_field,
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

    def curve_width_pxs(
        self,
    ) -> float:
        '''

        Return the width of the current datums in view in pixel units.
        '''
        _, lbar, rbar, _ = self.bars_range()
        return self.view.mapViewToDevice(
            QLineF(
                lbar, 0,
                rbar, 0
            )
        ).length()

    def default_view(
        self,
        bars_from_y: int = int(616 * 3/8),
        y_offset: int = 0,
        do_ds: bool = True,

    ) -> None:
        '''
        Set the plot's viewbox to a "default" startup setting where
        we try to show the underlying data range sanely.

        '''
        shm: ShmArray = self.shm
        array: np.ndarray = shm.array
        view: ChartView = self.plot.vb
        (
            vl,
            first_datum,
            datum_start,
            datum_stop,
            last_datum,
            vr,
        ) = self.datums_range(array=array)

        # invalid case: view is not ordered correctly
        # return and expect caller to sort it out.
        if (
            vl > vr
        ):
            log.warning(
                'Skipping `.default_view()` viewbox not initialized..\n'
                f'l -> r: {vl} -> {vr}\n'
                f'datum_start -> datum_stop: {datum_start} -> {datum_stop}\n'
            )
            return

        chartw: ChartPlotWidget = self.plot.getViewWidget()
        index_field = self.index_field
        step = self.index_step()

        if index_field == 'time':
            # transform l -> r view range values into
            # data index domain to determine how view
            # should be reset to better match data.
            read_slc = slice_from_time(
                array,
                start_t=vl,
                stop_t=vr,
                step=step,
            )

        index_iv = array[index_field][read_slc]
        uppx: float = self.graphics.x_uppx() or 1

        # l->r distance in scene units, no larger then data span
        data_diff = last_datum - first_datum
        rl_diff = min(vr - vl, data_diff)

        # orient by offset from the y-axis including
        # space to compensate for the L1 labels.
        if not y_offset:

            # we get the L1 spread label "length" in view coords and
            # make sure it doesn't colide with the right-most datum in
            # view.
            _, l1_len = chartw.pre_l1_xs()
            offset = l1_len/(uppx*step)

            # if no L1 label is present just offset by a few datums
            # from the y-axis.
            if chartw._max_l1_line_len == 0:
                offset += 3*step
        else:
            offset = (y_offset * step) + uppx*step

        # align right side of view to the rightmost datum + the selected
        # offset from above.
        r_reset = last_datum + offset

        # no data is in view so check for the only 2 sane cases:
        # - entire view is LEFT of data
        # - entire view is RIGHT of data
        if index_iv.size == 0:
            log.warning(f'No data in view for {vl} -> {vr}')

            # 2 cases either the view is to the left or right of the
            #   data set.
            if (
                vl <= first_datum
                and vr <= first_datum
            ):
                l_reset = first_datum

            elif (
                vl >= last_datum
                and vr >= last_datum
            ):
                l_reset = r_reset - rl_diff

            else:
                raise RuntimeError(f'Unknown view state {vl} -> {vr}')

        else:
            # maintain the l->r view distance
            l_reset = r_reset - rl_diff

        # remove any custom user yrange setttings
        if chartw._static_yrange == 'axis':
            chartw._static_yrange = None

        view.setXRange(
            min=l_reset,
            max=r_reset,
            padding=0,
        )

        if do_ds:
            view.maybe_downsample_graphics()
            view._set_yrange()

            # caller should do this!
            # self.linked.graphics_cycle()


class Renderer(msgspec.Struct):

    viz: Viz
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
        # TODO: can the renderer just call ``Viz.read()`` directly?
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
            # append_tres,

        ) = fmt_out

        # redraw conditions
        if (
            prepend_length > 0
            or new_sample_rate
            or view_changed

            # NOTE: comment this to try and make "append paths"
            # work below..
            or append_length > 0
        ):
            should_redraw = True

        path = self.path
        fast_path = self.fast_path
        reset = False

        self.viz.yrange = None

        # redraw the entire source data if we have either of:
        # - no prior path graphic rendered or,
        # - we always intend to re-render the data only in view
        if (
            path is None
            or should_redraw
        ):
            # print(f"{self.viz.name} -> REDRAWING BRUH")
            if new_sample_rate and showing_src_data:
                log.info(f'DEDOWN -> {array_key}')
                self._in_ds = False

            elif should_ds and uppx > 1:

                x_1d, y_1d, ymn, ymx = xy_downsample(
                    x_1d,
                    y_1d,
                    uppx,
                )
                self.viz.yrange = ymn, ymx
                # print(f'{self.viz.name} post ds: ymn, ymx: {ymn},{ymx}')

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
        ):
            print(f'{array_key} append len: {append_length}')
            # new_x = x_1d[-append_length - 2:]  # slice_to_head]
            # new_y = y_1d[-append_length - 2:]  # slice_to_head]
            profiler('sliced append path')
            # (
            #     x_1d,
            #     y_1d,
            #     connect,
            # ) = append_tres

            profiler(
                f'diffed array input, append_length={append_length}'
            )

            # if should_ds and uppx > 1:
            #     new_x, new_y = xy_downsample(
            #         new_x,
            #         new_y,
            #         uppx,
            #     )
            #     profiler(f'fast path downsample redraw={should_ds}')

            append_path = self.draw_path(
                x=x_1d,
                y=y_1d,
                connect=connect,
                path=fast_path,
            )
            profiler('generated append qpath')

            if use_fpath:
                # print(f'{self.viz.name}: FAST PATH')
                # an attempt at trying to make append-updates faster..
                if fast_path is None:
                    fast_path = append_path
                    # fast_path.reserve(int(6e3))
                else:
                    fast_path.connectPath(append_path)
                    size = fast_path.capacity()
                    profiler(f'connected fast path w size: {size}')

                    print(
                        f"append_path br: {append_path.boundingRect()}\n"
                        f"path size: {size}\n"
                        f"append_path len: {append_path.length()}\n"
                        f"fast_path len: {fast_path.length()}\n"
                    )
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
