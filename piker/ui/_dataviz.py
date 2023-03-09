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
Data vizualization APIs

'''
from __future__ import annotations
from functools import lru_cache
from math import (
    ceil,
    floor,
)
from typing import (
    Literal,
    TYPE_CHECKING,
)

from msgspec import (
    Struct,
    field,
)
import numpy as np
import pyqtgraph as pg
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
from ._render import Renderer
from ..log import get_logger
from .._profile import (
    Profiler,
    pg_profile_enabled,
    ms_slower_then,
)


if TYPE_CHECKING:
    from ._interaction import ChartView
    from ._chart import ChartPlotWidget
    from ._display import DisplayState


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

    self = viz  # TODO: make this a ``Viz`` method?
    r = self._src_r
    first_render: bool = False

    # if no source data renderer exists create one.
    if not r:
        first_render = True

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
        self._alt_r = (ds_curve_r, curve)

    ds_r, curve = self._alt_r

    # print(
    #     f'r: {r.fmtr.xy_slice}\n'
    #     f'ds_r: {ds_r.fmtr.xy_slice}\n'
    # )

    # do checks for whether or not we require downsampling:
    # - if we're **not** downsampling then we simply want to
    #   render the bars graphics curve and update..
    # - if instead we are in a downsamplig state then we to
    x_gt = 6 * (self.index_step() or 1)
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

    if first_render:
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

    elif (
        in_line
        and not should_line
    ):
        # change to bars graphic
        log.info(
            f'showing bars graphic {self.name}\n'
            f'first bars render?: {first_render}'
        )
        curve.hide()
        bars.show()
        bars.update()

    # XXX: is this required?
    viz._in_ds = should_line

    should_redraw = (
        changed_to_line
        or not should_line
    )
    return (
        graphics,
        r,
        should_redraw,
        should_line,
    )


_sample_rates: set[float] = {1, 60}


class ViewState(Struct):
    '''
    Indexing objects representing the current view x-range -> y-range.

    '''
    # (xl, xr) "input" view range in x-domain
    xrange: tuple[
        float | int,
        float | int
    ] | None = None

    # TODO: cache the (ixl, ixr) read_slc-into-.array style slice index?

    # (ymn, ymx) "output" min and max in viewed y-codomain
    yrange: tuple[
        float | int,
        float | int
    ] | None = None

    # last in view ``ShmArray.array[read_slc]`` data
    in_view: np.ndarray | None = None


class Viz(Struct):
    '''
    (Data) "Visualization" compound type which wraps a real-time
    shm array stream with displayed graphics (curves, charts)
    for high level access and control as well as efficient incremental
    update, oriented around the idea of a "view state".

    The (backend) intention is for this interface and type is to
    eventually be capable of shm-passing of incrementally updated
    graphics stream data, thus providing a cross-actor solution to
    sharing UI-related update state potentionally in a (compressed)
    binary-interchange format.

    Further, from an interaction-triggers-view-in-UI perspective, this type
    operates as a transform:
        (x_left, x_right) -> output metrics {ymn, ymx, uppx, ...}

    wherein each x-domain range maps to some output set of (graphics
    related) vizualization metrics. In further documentation we often
    refer to this abstraction as a vizualization curve: Ci. Each Ci is
    considered a function which maps an x-range (input view range) to
    a multi-variate (metrics) output.

    '''
    name: str
    plot: pg.PlotItem
    _shm: ShmArray
    flume: Flume
    graphics: Curve | BarItems

    vs: ViewState = field(default_factory=ViewState)

    # last calculated y-mn/mx from m4 downsample code, this
    # is updated in the body of `Renderer.render()`.
    ds_yrange: tuple[float, float] | None = None
    yrange: tuple[float, float] | None = None

    # in some cases a viz may want to change its
    # graphical "type" or, "form" when downsampling, to
    # start this is only ever an interpolation line.
    ds_graphics: Curve | None = None

    is_ohlc: bool = False
    render: bool = True  # toggle for display loop

    _index_field: Literal[
        'index',
        'time',

        # TODO: idea is to re-index all time series to a common
        # longest-len-int-index where we avoid gaps and instead
        # graph on the 0 -> N domain of the array index super set.
        # 'gapless',

    ] = 'time'

    # TODO: maybe compound this into a downsampling state type?
    _last_uppx: float = 0
    _in_ds: bool = False
    _index_step: float | None = None

    # map from uppx -> (downsampled data, incremental graphics)
    _src_r: Renderer | None = None
    _alt_r: tuple[
        Renderer,
        pg.GraphicsItem
    ] | None = None

    # cache of y-range values per x-range input.
    _mxmns: dict[
        tuple[int, int],
        tuple[float, float],
    ] = {}

    # cache of median calcs from input read slice hashes
    # see `.median()`
    _meds: dict[
        int,
        float,
    ] = {}

    # to make lru_cache-ing work, see
    # https://docs.python.org/3/faq/programming.html#how-do-i-cache-method-calls
    def __eq__(self, other):
        return self._shm._token == other._shm._token

    def __hash__(self):
        return hash(self._shm._token)

    @property
    def shm(self) -> ShmArray:
        return self._shm

    @property
    def index_field(self) -> str:
        '''
        The column name as ``str`` in the underlying ``._shm: ShmArray``
        which will deliver the "index" array.

        '''
        return self._index_field

    def index_step(
        self,
        reset: bool = False,
    ) -> float:
        '''
        Return the size between sample steps in the units of the
        x-domain, normally either an ``int`` array index size or an
        epoch time in seconds.

        '''
        # attempt to dectect the best step size by scanning a sample of
        # the source data.
        if self._index_step is None:

            index = self.shm.array[self.index_field]
            isample = index[:16]

            mxdiff: None | float = None
            for step in np.diff(isample):
                if step in _sample_rates:
                    if (
                        mxdiff is not None
                        and step != mxdiff
                    ):
                        raise ValueError(
                            f'Multiple step sizes detected? {mxdiff}, {step}'
                        )
                    mxdiff = step

            self._index_step = max(mxdiff, 1)
            if (
                mxdiff < 1
                or 1 < mxdiff < 60
            ):
                # TODO: remove this once we're sure the above scan loop
                # is rock solid.
                breakpoint()

        return self._index_step

    def maxmin(
        self,

        x_range: slice | tuple[int, int] | None = None,
        i_read_range: tuple[int, int] | None = None,
        use_caching: bool = True,

    ) -> tuple[float, float] | None:
        '''
        Compute the cached max and min y-range values for a given
        x-range determined by ``lbar`` and ``rbar`` or ``None``
        if no range can be determined (yet).

        '''
        name = self.name
        profiler = Profiler(
            msg=f'`Viz[{name}].maxmin()`',
            disabled=not pg_profile_enabled(),
            ms_threshold=4,
            delayed=True,
        )

        shm = self.shm
        if shm is None:
            return None

        do_print: bool = False
        arr = shm.array

        if i_read_range is not None:
            read_slc = slice(*i_read_range)
            index = arr[read_slc][self.index_field]
            if not index.size:
                return None
            ixrng = (index[0], index[-1])

        else:
            if x_range is None:
                (
                    l,
                    _,
                    lbar,
                    rbar,
                    _,
                    r,
                ) = self.datums_range()

                profiler(f'{self.name} got bars range')
                x_range = lbar, rbar

            # TODO: hash the slice instead maybe?
            # https://stackoverflow.com/a/29980872
            ixrng = lbar, rbar = round(x_range[0]), round(x_range[1])

        if use_caching:
            cached_result = self._mxmns.get(ixrng)
            if cached_result:
                if do_print:
                    print(
                        f'{self.name} CACHED maxmin\n'
                        f'{ixrng} -> {cached_result}'
                    )
                read_slc, mxmn = cached_result
                self.vs.yrange = mxmn
                return (
                    ixrng,
                    read_slc,
                    mxmn,
                )

        if i_read_range is None:
            # get relative slice indexes into array
            if self.index_field == 'time':
                read_slc = slice_from_time(
                    arr,
                    start_t=lbar,
                    stop_t=rbar,
                    step=self.index_step(),
                )

            else:
                ifirst = arr[0]['index']
                read_slc = slice(
                    lbar - ifirst,
                    (rbar - ifirst) + 1
                )

        slice_view = arr[read_slc]

        if not slice_view.size:
            log.warning(
                f'{self.name} no maxmin in view?\n'
                f"{name} no mxmn for bars_range => {ixrng} !?"
            )
            return None

        elif self.ds_yrange:
            mxmn = self.ds_yrange
            if do_print:
                print(
                    f'{self.name} M4 maxmin:\n'
                    f'{ixrng} -> {mxmn}'
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
            if (
                do_print
            ):
                s = 3
                print(
                    f'{self.name} MANUAL ohlc={self.is_ohlc} maxmin:\n'
                    f'{ixrng} -> {mxmn}\n'
                    f'read_slc: {read_slc}\n'
                    # f'abs_slc: {slice_view["index"]}\n'
                    f'first {s}:\n{slice_view[:s]}\n'
                    f'last {s}:\n{slice_view[-s:]}\n'
                )

        # cache result for input range
        assert mxmn
        self._mxmns[ixrng] = (read_slc, mxmn)
        self.vs.yrange = mxmn
        profiler(f'yrange mxmn cacheing: {x_range} -> {mxmn}')
        return (
            ixrng,
            read_slc,
            mxmn,
        )

    def view_range(self) -> tuple[int, int]:
        '''
        Return the start and stop x-indexes for the managed ``ViewBox``.

        '''
        vr = self.plot.viewRect()
        return (
            vr.left(),
            vr.right(),
        )

    def datums_range(
        self,
        view_range: None | tuple[float, float] = None,
        index_field: str | None = None,
        array: np.ndarray | None = None,

    ) -> tuple[
        int, int, int, int, int, int
    ]:
        '''
        Return a range tuple for the datums present in view.

        '''
        l, r = view_range or self.view_range()

        index_field: str = index_field or self.index_field
        if index_field == 'index':
            l: int = round(l)
            r: int = round(r)

        if array is None:
            array = self.shm.array

        index = array[index_field]
        first: int = floor(index[0])
        last: int = ceil(index[-1])

        # invalid view state
        if (
            r < l
            or l < 0
            or r < 0
            or (
                l > last
                and r > last
            )
        ):
            leftmost: int = first
            rightmost: int = last

        else:
            # determine first and last datums in view determined by
            # l -> r view range.
            rightmost = max(
                min(last, ceil(r)),
                first,
            )

            leftmost = min(
                max(first, floor(l)),
                last,
                rightmost - 1,
            )

            # sanity
            # assert leftmost < rightmost

        self.vs.xrange = leftmost, rightmost

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
        array_field: str | None = None,
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
            index_field=index_field,
            array=array,
        )

        if profiler:
            profiler('self.datums_range()')

        abs_slc = slice(ifirst, ilast)

        # TODO: support time slicing
        if index_field == 'time':
            read_slc = slice_from_time(
                array,
                start_t=lbar,
                stop_t=rbar,
                step=self.index_step(),
            )

            # TODO: maybe we should return this from the slicer call
            # above?
            in_view = array[read_slc]
            if in_view.size:
                self.vs.in_view = in_view
                abs_indx = in_view['index']
                abs_slc = slice(
                    int(abs_indx[0]),
                    int(abs_indx[-1]),
                )
            else:
                self.vs.in_view = None

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
            self.vs.in_view = in_view
            # in_view = array[lbar_i-1: rbar_i+1]
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
        render: bool = True,
        array_key: str | None = None,

        profiler: Profiler | None = None,
        do_append: bool = True,

        **kwargs,

    ) -> tuple[
        bool,
        tuple[int, int],
        pg.GraphicsObject,
    ]:
        '''
        Read latest datums from shm and (incrementally) render to
        graphics.

        '''
        profiler = Profiler(
            msg=f'Viz.update_graphics() for {self.name}',
            disabled=not pg_profile_enabled(),
            ms_threshold=ms_slower_then,
            # ms_threshold=4,
        )
        # shm read and slice to view
        read = (
            xfirst,
            xlast,
            src_array,
            ivl,
            ivr,
            in_view,
        ) = self.read(profiler=profiler)

        profiler('read src shm data')

        graphics = self.graphics

        if (
            not in_view.size
            or not render
        ):
            # print(f'{self.name} not in view (exiting early)')
            return (
                False,
                (ivl, ivr),
                graphics,
            )

        should_redraw: bool = False
        ds_allowed: bool = True  # guard for m4 activation

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
                should_redraw,
                ds_allowed,  # in line mode?
            ) = render_baritems(
                self,
                graphics,
                read,
                profiler,
                **kwargs,
            )

        elif not r:
            if isinstance(graphics, StepCurve):

                r = self._src_r = Renderer(
                    viz=self,
                    fmtr=StepCurveFmtr(
                        shm=self.shm,
                        viz=self,
                    ),
                )

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
            and ds_allowed
        ):
            log.debug(
                f'{array_key} sampler change: {self._last_uppx} -> {uppx}'
            )
            self._last_uppx = uppx

            new_sample_rate = True
            showing_src_data = False
            should_ds = True
            should_redraw = True

        # "back to source" case:
        # this more or less skips use of the m4 downsampler
        # inside ``Renderer.render()`` which results in a path
        # drawn verbatim to match the xy source data.
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

            # TODO: better way to detect and pass this?
            # if we want to eventually cache renderers for a given uppx
            # we should probably use this as a key + state?
            should_redraw=should_redraw,
            new_sample_rate=new_sample_rate,
            should_ds=should_ds,
            showing_src_data=showing_src_data,

            do_append=do_append,
        )

        if not out:
            log.warning(f'{self.name} failed to render!?')
            return (
                False,
                (ivl, ivr),
                graphics,
            )

        path, reset_cache = out

        # XXX: SUPER UGGGHHH... without this we get stale cache
        # graphics that "smear" across the view horizontally
        # when panning and the first datum is out of view..
        reset_cache = False
        if (
            reset_cache
        ):
            # assign output paths to graphicis obj but
            # after a coords-cache reset.
            with graphics.reset_cache():
                graphics.path = r.path
                graphics.fast_path = r.fast_path

                self.draw_last(
                    array_key=array_key,
                    last_read=read,
                    reset_cache=reset_cache,
                )
        else:
            # assign output paths to graphicis obj
            graphics.path = r.path
            graphics.fast_path = r.fast_path

            self.draw_last(
                array_key=array_key,
                last_read=read,
                reset_cache=reset_cache,
            )
        # graphics.draw_last_datum(
        #     path,
        #     src_array,
        #     reset_cache,
        #     array_key,
        #     index_field=self.index_field,
        # )
        # TODO: does this actuallly help us in any way (prolly should
        # look at the source / ask ogi). I think it avoid artifacts on
        # wheel-scroll downsampling curve updates?
        # TODO: is this ever better?
        graphics.prepareGeometryChange()
        profiler('.prepareGeometryChange()')

        graphics.update()
        profiler('.update()')

        # track downsampled state
        self._in_ds = r._in_ds

        return (
            True,
            (ivl, ivr),
            graphics,
        )

    def draw_last(
        self,
        array_key: str | None = None,
        last_read: tuple | None = None,
        reset_cache: bool = False,
        only_last_uppx: bool = False,

    ) -> None:

        # shm read and slice to view
        (
            xfirst, xlast, src_array,
            ivl, ivr, in_view,
        ) = last_read or self.read()

        array_key = array_key or self.name

        gfx = self.graphics

        # the renderer is downsampling we choose
        # to always try and update a single (interpolating)
        # line segment that spans and tries to display
        # the last uppx's worth of datums.
        # we only care about the last pixel's
        # worth of data since that's all the screen
        # can represent on the last column where
        # the most recent datum is being drawn.
        uppx = ceil(gfx.x_uppx())

        if (
            (self._in_ds or only_last_uppx)
            and uppx > 0
        ):
            alt_renderer = self._alt_r
            if alt_renderer:
                renderer, gfx = alt_renderer
            else:
                renderer = self._src_r

            fmtr = renderer.fmtr
            x = fmtr.x_1d
            y = fmtr.y_1d

            iuppx = ceil(uppx)
            if alt_renderer:
                iuppx = ceil(uppx / fmtr.flat_index_ratio)

            y = y[-iuppx:]
            ymn, ymx = y.min(), y.max()
            try:
                x_start = x[-iuppx]
            except IndexError:
                # we're less then an x-px wide so just grab the start
                # datum index.
                x_start = x[0]

            gfx._last_line = QLineF(
                x_start, ymn,
                x[-1], ymx,
            )
            # print(
            #     f'updating DS curve {self.name}@{time_step}s\n'
            #     f'drawing uppx={uppx} mxmn line: {ymn}, {ymx}'
            # )

        else:
            x, y = gfx.draw_last_datum(
                gfx.path,
                src_array,
                reset_cache,  # never reset path
                array_key,
                self.index_field,
            )
            # print(f'updating NOT DS curve {self.name}')

        gfx.update()

    def default_view(
        self,
        min_bars_from_y: int = int(616 * 4/11),
        y_offset: int = 0,  # in datums

        do_ds: bool = True,
        do_min_bars: bool = False,

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
        else:
            read_slc = slice(0, datum_stop - datum_start + 1)

        index_iv = array[index_field][read_slc]
        uppx: float = self.graphics.x_uppx() or 1

        # l->r distance in scene units, no larger then data span
        data_diff = last_datum - first_datum
        rl_diff = vr - vl
        rescale_to_data: bool = False

        if rl_diff > data_diff:
            rescale_to_data = True
            rl_diff = data_diff

        # orient by offset from the y-axis including
        # space to compensate for the L1 labels.
        if not y_offset:
            _, l1_offset = chartw.pre_l1_xs()

            offset = l1_offset

            if rescale_to_data:
                new_uppx: float = data_diff / self.px_width()
                offset = (offset / uppx) * new_uppx

        else:
            offset = (y_offset * step) + uppx*step

        # NOTE: if we are in the midst of start-up and a bunch of
        # widgets are spawning/rendering  concurrently, it's likely the
        # label size above `l1_offset` won't have yet fully rendered.
        # Here we try to compensate for that ensure at least a static
        # bar gap between the last datum and the y-axis.
        if (
            do_min_bars
            and offset <= (6 * step)
        ):
            offset = 6 * step

        # align right side of view to the rightmost datum + the selected
        # offset from above.
        r_reset = (
            self.graphics.x_last() or last_datum
        ) + offset

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
                log.warning(f'Unknown view state {vl} -> {vr}')
                return
        else:
            # maintain the l->r view distance
            l_reset = r_reset - rl_diff

        if (
            do_min_bars
            and (r_reset - l_reset) < min_bars_from_y
        ):
            l_reset = (
                (r_reset + offset)
                -
                min_bars_from_y * step
            )

        # remove any custom user yrange setttings
        if chartw._static_yrange == 'axis':
            chartw._static_yrange = None

        view.setXRange(
            min=l_reset,
            max=r_reset,
            padding=0,
        )

        if do_ds:
            view.interact_graphics_cycle()

    def incr_info(
        self,
        ds: DisplayState,
        update_uppx: float = 16,
        is_1m: bool = False,

    ) -> tuple:
        '''
        Return a slew of graphics related data-flow metrics to do with
        incrementally updating a data view.

        Output  info includes,
        ----------------------

        uppx: float
            x-domain units-per-pixel.

        liv: bool
            telling if the "last datum" is in vie"last datum" is in
            view.

        do_px_step: bool
            recent data append(s) are enough that the next physical
            pixel-column should be used for drawing.

        i_diff_t: float
            the difference between the last globally recorded time stamp
            aand the current one.

        append_diff: int
           diff between last recorded "append index" (the index at whic
           `do_px_step` was last returned `True`) and the current index.

        do_rt_update: bool
            `True` only when the uppx is less then some threshold
            defined by `update_uppx`.

        should_tread: bool
            determines the first step, globally across all callers, that
            the a set of data views should be "treaded", shifted in the
            x-domain such that the last datum in view is always in the
            same spot in non-view/scene (aka GUI coord) terms.


        '''
        # get most recent right datum index in-view
        l, start, datum_start, datum_stop, stop, r = self.datums_range()
        lasts = self.shm.array[-1]
        i_step = lasts['index']  # last index-specific step.
        i_step_t = lasts['time']  # last time step.

        # fqsn = self.flume.symbol.fqsn

        # check if "last (is) in view" -> is a real-time update necessary?
        if self.index_field == 'index':
            liv = (r >= i_step)
        else:
            liv = (r >= i_step_t)

        # compute the first available graphic obj's x-units-per-pixel
        # TODO: make this not loop through all vizs each time!
        uppx = self.plot.vb.x_uppx()

        # NOTE: this used to be implemented in a dedicated
        # "increment task": ``check_for_new_bars()`` but it doesn't
        # make sense to do a whole task switch when we can just do
        # this simple index-diff and all the fsp sub-curve graphics
        # are diffed on each draw cycle anyway; so updates to the
        # "curve" length is already automatic.
        globalz = ds.globalz
        varz = ds.hist_vars if is_1m else ds.vars

        last_key = 'i_last_slow_t' if is_1m else 'i_last_t'
        glast = globalz[last_key]

        # calc datums diff since last global increment
        i_diff_t: float = i_step_t - glast

        # when the current step is now greater then the last we have
        # read from the display state globals, we presume that the
        # underlying source shm buffer has added a new sample and thus
        # we should increment the global view a step (i.e. tread the
        # view in place to keep the current datum at the same spot on
        # screen).
        should_tread: bool = False
        if i_diff_t > 0:
            globalz[last_key] = i_step_t
            should_tread = True

        # update the "last datum" (aka extending the vizs graphic with
        # new data) only if the number of unit steps is >= the number of
        # such unit steps per pixel (aka uppx). Iow, if the zoom level
        # is such that a datum(s) update to graphics wouldn't span
        # to a new pixel, we don't update yet.
        i_last_append = varz['i_last_append']
        append_diff: int = i_step - i_last_append

        do_px_step = (append_diff * self.index_step()) >= uppx
        do_rt_update = (uppx < update_uppx)

        if (
            do_px_step
        ):
            varz['i_last_append'] = i_step

            # print(
            #     f'DOING APPEND => {fqsn}\n'
            #     f'i_step: {i_step}\n'
            #     f'i_step_t: {i_step_t}\n'
            #     f'glast: {glast}\n'
            #     f'last_append: {i_last_append}\n'
            #     f'r: {r}\n'
            #     '-----------------------------\n'
            #     f'uppx: {uppx}\n'
            #     f'liv: {liv}\n'
            #     f'do_px_step: {do_px_step}\n'
            #     f'i_diff_t: {i_diff_t}\n'
            #     f'do_rt_update: {do_rt_update}\n'
            #     f'append_diff: {append_diff}\n'
            #     f'should_tread: {should_tread}\n'
            # )

        varz['i_last'] = i_step

        # TODO: pack this into a struct?
        return (
            uppx,
            liv,
            do_px_step,
            i_diff_t,
            append_diff,
            do_rt_update,
            should_tread,
        )

    def px_width(self) -> float:
        '''
        Return the width of the view box containing
        this graphic in pixel units.

        '''
        vb = self.plot.vb
        if not vb:
            return 0

        vl, vr = self.view_range()

        return vb.mapViewToDevice(
            QLineF(
                vl, 0,
                vr, 0,
            )
        ).length()

    @lru_cache(maxsize=6116)
    def median_from_range(
        self,
        start: int,
        stop: int,

    ) -> float:
        in_view = self.shm.array[start:stop]
        if self.is_ohlc:
            return np.median(in_view['close'])
        else:
            return np.median(in_view[self.name])

    @lru_cache(maxsize=6116)
    def _dispersion(
        self,
        # xrange: tuple[float, float],
        ymn: float,
        ymx: float,
        yref: float,

    ) -> tuple[float, float]:
        return (
            (ymx - yref) / yref,
            (ymn - yref) / yref,
        )

    def disp_from_range(
        self,
        xrange: tuple[float, float] | None = None,
        yref: float | None = None,
        method: Literal[
            'up',
            'down',
            'full',  # both sides
            'both',  # both up and down as separate scalars

        ] = 'full',

    ) -> float | tuple[float, float] | None:
        '''
        Return a dispersion metric referenced from an optionally
        provided ``yref`` or the left-most datum level by default.

        '''
        vs = self.vs
        yrange = vs.yrange
        if yrange is None:
            return None

        ymn, ymx = yrange
        key = 'open' if self.is_ohlc else self.name
        yref = yref or vs.in_view[0][key]
        # xrange = xrange or vs.xrange

        # call into the lru_cache-d sigma calculator method
        r_up, r_down = self._dispersion(ymn, ymx, yref)
        match method:
            case 'full':
                return r_up - r_down
            case 'up':
                return r_up
            case 'down':
                return r_up
            case 'both':
                return r_up, r_down

    # @lru_cache(maxsize=6116)
    def i_from_t(
        self,
        t: float,
        return_y: bool = False,

    ) -> int | tuple[int, float]:

        istart = slice_from_time(
            self.vs.in_view,
            start_t=t,
            stop_t=t,
            step=self.index_step(),
        ).start

        if not return_y:
            return istart

        vs = self.vs
        arr = vs.in_view
        key = 'open' if self.is_ohlc else self.name
        yref = arr[istart][key]
        return istart, yref

    def scalars_from_index(
        self,
        xref: float | None = None,

    ) -> tuple[
            int,
            float,
            float,
            float,
    ] | None:
        '''
        Calculate and deliver the log-returns scalars specifically
        according to y-data supported on this ``Viz``'s underlying
        x-domain data range from ``xref`` -> ``.vs.xrange[1]``.

        The main use case for this method (currently) is to generate
        scalars which will allow calculating the required y-range for
        some "pinned" curve to be aligned *from* the ``xref`` time
        stamped datum *to* the curve rendered by THIS viz.

        '''
        vs = self.vs
        arr = vs.in_view

        # TODO: make this work by parametrizing over input
        # .vs.xrange input for caching?
        # read_slc_start = self.i_from_t(xref)

        read_slc = slice_from_time(
            arr=self.vs.in_view,
            start_t=xref,
            stop_t=vs.xrange[1],
            step=self.index_step(),
        )
        key = 'open' if self.is_ohlc else self.name

        # NOTE: old code, it's no faster right?
        # read_slc_start = read_slc.start
        # yref = arr[read_slc_start][key]

        read = arr[read_slc][key]
        if not read.size:
            return None

        yref = read[0]
        ymn, ymx = self.vs.yrange
        # print(
        #     f'Viz[{self.name}].scalars_from_index(xref={xref})\n'
        #     f'read_slc: {read_slc}\n'
        #     f'ymnmx: {(ymn, ymx)}\n'
        # )
        return (
            read_slc.start,
            yref,
            (ymx - yref) / yref,
            (ymn - yref) / yref,
        )
