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
Overlay (aka multi-chart) UX machinery.

'''
from __future__ import annotations
from math import (
    isinf,
)
from typing import (
    Any,
    Literal,
    TYPE_CHECKING,
)

import numpy as np
import pyqtgraph as pg

from ..data.types import Struct
from ..data._pathops import slice_from_time
from ..log import get_logger
from .._profile import Profiler

if TYPE_CHECKING:
    from ._chart import ChartPlotWidget
    from ._dataviz import Viz
    from ._interaction import ChartView


log = get_logger(__name__)


class OverlayT(Struct):
    '''
    An overlay co-domain range transformer.

    Used to translate and apply a range from one y-range
    to another based on a returns logarithm:

    R(ymn, ymx, yref) = (ymx - yref)/yref

    which gives the log-scale multiplier, and

    ymx_t = yref * (1 + R)

    which gives the inverse to translate to the same value
    in the target co-domain.

    '''
    start_t: float | None = None
    viz: Viz | None = None

    # % "range" computed from some ref value to the mn/mx
    rng: float | None = None
    in_view: np.ndarray | None = None

    # pinned-minor curve modified mn and max for the major dispersion
    # curve due to one series being shorter and the pin + scaling from
    # that pin point causing the original range to have to increase.
    y_val: float | None = None

    def apply_rng(
        self,
        y_start: float,  # reference value for dispersion metric

    ) -> float:
        return y_start * (1 + self.rng)

    # def loglin_from_range(
    #     self,

    #     y_ref: float,  # reference value for dispersion metric
    #     mn: float,  # min y in target log-lin range
    #     mx: float,  # max y in target log-lin range
    #     offset: float,  # y-offset to start log-scaling from

    # ) -> tuple[float, float]:
    #     r_up = (mx - y_ref) / y_ref
    #     r_down = (mn - y_ref) / y_ref
    #     ymn = offset * (1 + r_down)
    #     ymx = offset * (1 + r_up)

    #     return ymn, ymx


def intersect_from_longer(
    start_t_first: float,
    in_view_first: np.ndarray,

    start_t_second: float,
    in_view_second: np.ndarray,

) -> np.ndarray:

    tdiff = start_t_first - start_t_second

    if tdiff == 0:
        return False

    i: int = 0

    # first time series has an "earlier" first time stamp then the 2nd.
    # aka 1st is "shorter" then the 2nd.
    if tdiff > 0:
        longer = in_view_second
        find_t = start_t_first
        i = 1

    # second time series has an "earlier" first time stamp then the 1st.
    # aka 2nd is "shorter" then the 1st.
    elif tdiff < 0:
        longer = in_view_first
        find_t = start_t_second
        i = 0

    slc = slice_from_time(
        arr=longer,
        start_t=find_t,
        stop_t=find_t,
    )
    return (
        longer[slc.start],
        find_t,
        i,
    )


def _maybe_calc_yrange(
    viz: Viz,
    yrange_kwargs: dict[Viz, dict[str, Any]],
    profiler: Profiler,
    chart_name: str,

) -> tuple[
    slice,
    dict,
] | None:

    if not viz.render:
        return

    # pass in no array which will read and render from the last
    # passed array (normally provided by the display loop.)
    in_view, i_read_range, _ = viz.update_graphics()

    if not in_view:
        return

    profiler(f'{viz.name}@{chart_name} `Viz.update_graphics()`')

    # check if explicit yrange (kwargs) was passed in by the caller
    yrange_kwargs = yrange_kwargs.get(viz) if yrange_kwargs else None
    if yrange_kwargs is not None:
        read_slc = slice(*i_read_range)

    else:
        out = viz.maxmin(i_read_range=i_read_range)
        if out is None:
            log.warning(f'No yrange provided for {viz.name}!?')
            return
        (
            _,  # ixrng,
            read_slc,
            yrange
        ) = out
        profiler(f'{viz.name}@{chart_name} `Viz.maxmin()`')
        yrange_kwargs = {'yrange': yrange}

    return (
        read_slc,
        yrange_kwargs,
    )


def overlay_viewlists(
    active_viz: Viz,
    plots: dict[str, ChartPlotWidget],
    profiler: Profiler,

    # public config ctls
    do_linked_charts: bool = True,
    do_overlay_scaling: bool = True,
    yrange_kwargs: dict[
        str,
        tuple[float, float],
    ] | None = None,

    overlay_technique: Literal[
        'loglin_to_first',
        'loglin_to_sigma',
        'mnmx',
        'solo',
    ] = 'loglin_to_first',


    # internal instrumentation
    debug_print: bool = False,

) -> None:
    '''
    Calculate and apply y-domain (axis y-range) multi-curve overlay adjustments
    a set of ``plots`` based on the requested ``overlay_technique``.

    '''
    chart_name: str
    chart: ChartPlotWidget
    for chart_name, chart in plots.items():

        # Common `PlotItem` maxmin table; presumes that some path
        # graphics (and thus their backing data sets) are in the
        # same co-domain and view box (since the were added
        # a separate graphics objects to a common plot) and thus can
        # be sorted as one set per plot.
        mxmns_by_common_pi: dict[
            pg.PlotItem,
            tuple[float, float],
        ] = {}

        # proportional group auto-scaling per overlay set.
        # -> loop through overlays on each multi-chart widget
        #    and scale all y-ranges based on autoscale config.
        # -> for any "group" overlay we want to dispersion normalize
        #    and scale minor charts onto the major chart: the chart
        #    with the most dispersion in the set.
        major_sigma_viz: Viz = None
        major_mx: float = 0
        major_mn: float = float('inf')
        mx_disp: float = 0

        # collect certain flows have grapics objects **in seperate
        # plots/viewboxes** into groups and do a common calc to
        # determine auto-ranging input for `._set_yrange()`.
        # this is primarly used for our so called "log-linearized
        # multi-plot" overlay technique.
        overlay_table: dict[
            ChartView,
            tuple[
                Viz,
                float,  # y start
                float,  # y min
                float,  # y max
                float,  # y median
                slice,  # in-view array slice
                np.ndarray,  # in-view array
            ],
        ] = {}

        # ONLY auto-yrange the viz mapped to THIS view box
        if not do_overlay_scaling:
            viz = active_viz
            if debug_print:
                print(f'ONLY ranging THIS viz: {viz.name}')

            out = _maybe_calc_yrange(
                viz,
                yrange_kwargs,
                profiler,
                chart_name,
            )
            if out is None:
                continue

            read_slc, yrange_kwargs = out
            viz.plot.vb._set_yrange(**yrange_kwargs)
            profiler(f'{viz.name}@{chart_name} single curve yrange')

            # don't iterate overlays, just move to next chart
            continue

        # create a group overlay log-linearized y-range transform to
        # track and eventually inverse transform all overlay curves
        # to a common target max dispersion range.
        dnt = OverlayT()
        upt = OverlayT()

        if debug_print:
            print(
                f'BEGIN UX GRAPHICS CYCLE: @{chart_name}\n'
                +
                '#'*100
                +
                '\n'
            )

        for name, viz in chart._vizs.items():

            out = _maybe_calc_yrange(
                viz,
                yrange_kwargs,
                profiler,
                chart_name,
            )
            if out is None:
                continue

            read_slc, yrange_kwargs = out
            yrange = yrange_kwargs['yrange']
            pi = viz.plot

            # handle multiple graphics-objs per viewbox cases
            mxmn = mxmns_by_common_pi.get(pi)
            if mxmn:
                yrange = mxmns_by_common_pi[pi] = (
                    min(yrange[0], mxmn[0]),
                    max(yrange[1], mxmn[1]),
                )

            else:
                mxmns_by_common_pi[pi] = yrange

            profiler(f'{viz.name}@{chart_name} common pi sort')

            # non-overlay group case
            if not viz.is_ohlc:
                pi.vb._set_yrange(yrange=yrange)
                profiler(
                    f'{viz.name}@{chart_name} simple std `._set_yrange()`'
                )

            # handle overlay log-linearized group scaling cases
            # TODO: a better predicate here, likely something
            # to do with overlays and their settings..
            # TODO: we probably eventually might want some other
            # charts besides OHLC?
            else:
                ymn, ymx = yrange

                # determine start datum in view
                arr = viz.shm.array
                in_view = arr[read_slc]
                if not in_view.size:
                    log.warning(f'{viz.name} not in view?')
                    continue

                # row_start = arr[read_slc.start - 1]
                row_start = arr[read_slc.start]

                if viz.is_ohlc:
                    y_ref = row_start['open']
                else:
                    y_ref = row_start[viz.name]

                profiler(f'{viz.name}@{chart_name} MINOR curve median')

                overlay_table[viz.plot.vb] = (
                    viz,
                    y_ref,
                    ymn,
                    ymx,
                    read_slc,
                    in_view,
                )

                key = 'open' if viz.is_ohlc else viz.name
                start_t = in_view[0]['time']
                r_down = (ymn - y_ref) / y_ref
                r_up = (ymx - y_ref) / y_ref

                msg = (
                    f'### {viz.name}@{chart_name} ###\n'
                    f'y_ref: {y_ref}\n'
                    f'down disp: {r_down}\n'
                    f'up disp: {r_up}\n'
                )
                profiler(msg)
                if debug_print:
                    print(msg)

                # track the "major" curve as the curve with most
                # dispersion.
                if (
                    dnt.rng is None
                    or (
                        r_down < dnt.rng
                        and r_down < 0
                    )
                ):
                    dnt.viz = viz
                    dnt.rng = r_down
                    dnt.in_view = in_view
                    dnt.start_t = in_view[0]['time']
                    major_mn = ymn

                    msg = f'NEW DOWN: {viz.name}@{chart_name} r:{r_down}\n'
                    profiler(msg)
                    if debug_print:
                        print(msg)
                else:
                    # minor in the down swing range so check that if
                    # we apply the current rng to the minor that it
                    # doesn't go outside the current range for the major
                    # otherwise we recompute the minor's range (when
                    # adjusted for it's intersect point to be the new
                    # major's range.
                    intersect = intersect_from_longer(
                        dnt.start_t,
                        dnt.in_view,
                        start_t,
                        in_view,
                    )
                    profiler(f'{viz.name}@{chart_name} intersect by t')

                    if intersect:
                        longer_in_view, _t, i = intersect

                        scaled_mn = dnt.apply_rng(y_ref)
                        if scaled_mn > ymn:
                            # after major curve scaling we detected
                            # the minor curve is still out of range
                            # so we need to adjust the major's range
                            # to include the new composed range.
                            y_maj_ref = longer_in_view[key]
                            new_major_ymn = (
                                y_maj_ref
                                *
                                (1 + r_down)
                            )

                            # rewrite the major range to the new
                            # minor-pinned-to-major range and mark
                            # the transform as "virtual".
                            msg = (
                                f'EXPAND DOWN bc {viz.name}@{chart_name}\n'
                                f'y_start epoch time @ {_t}:\n'
                                f'y_maj_ref @ {_t}: {y_maj_ref}\n'
                                f'R: {dnt.rng} -> {r_down}\n'
                                f'MN: {major_mn} -> {new_major_ymn}\n'
                            )
                            dnt.rng = r_down
                            major_mn = dnt.y_val = new_major_ymn
                            profiler(msg)
                            if debug_print:
                                print(msg)

                if (
                    upt.rng is None
                    or (
                        r_up > upt.rng
                        and r_up > 0
                    )
                ):
                    upt.rng = r_up
                    upt.viz = viz
                    upt.in_view = in_view
                    upt.start_t = in_view[0]['time']
                    major_mx = ymx
                    msg = f'NEW UP: {viz.name}@{chart_name} r:{r_up}\n'
                    profiler(msg)
                    if debug_print:
                        print(msg)

                else:
                    intersect = intersect_from_longer(
                        upt.start_t,
                        upt.in_view,
                        start_t,
                        in_view,
                    )
                    profiler(f'{viz.name}@{chart_name} intersect by t')

                    if intersect:
                        longer_in_view, _t, i = intersect

                        scaled_mx = upt.apply_rng(y_ref)
                        if scaled_mx < ymx:
                            # after major curve scaling we detected
                            # the minor curve is still out of range
                            # so we need to adjust the major's range
                            # to include the new composed range.
                            y_maj_ref = longer_in_view[key]
                            new_major_ymx = (
                                y_maj_ref
                                *
                                (1 + r_up)
                            )

                            # rewrite the major range to the new
                            # minor-pinned-to-major range and mark
                            # the transform as "virtual".
                            msg = (
                                f'EXPAND UP bc {viz.name}@{chart_name}:\n'
                                f'y_maj_ref @ {_t}: {y_maj_ref}\n'
                                f'R: {upt.rng} -> {r_up}\n'
                                f'MX: {major_mx} -> {new_major_ymx}\n'
                            )
                            upt.rng = r_up
                            major_mx = upt.y_val = new_major_ymx
                            profiler(msg)
                            print(msg)

                # find curve with max dispersion
                disp = abs(ymx - ymn) / y_ref
                if disp > mx_disp:
                    major_sigma_viz = viz
                    mx_disp = disp
                    major_mn = ymn
                    major_mx = ymx

                profiler(f'{viz.name}@{chart_name} MINOR curve scale')

        # NOTE: if no there were no overlay charts
        # detected/collected (could be either no group detected or
        # chart with a single symbol, thus a single viz/overlay)
        # then we ONLY set the lone chart's (viz) yrange and short
        # circuit to the next chart in the linked charts loop. IOW
        # there's no reason to go through the overlay dispersion
        # scaling in the next loop below when only one curve is
        # detected.
        if (
            not mxmns_by_common_pi
            and len(overlay_table) < 2
        ):
            if debug_print:
                print(f'ONLY ranging major: {viz.name}')

            out = _maybe_calc_yrange(
                viz,
                yrange_kwargs,
                profiler,
                chart_name,
            )
            if out is None:
                continue

            read_slc, yrange_kwargs = out
            viz.plot.vb._set_yrange(**yrange_kwargs)
            profiler(f'{viz.name}@{chart_name} single curve yrange')

            # move to next chart in linked set since
            # no overlay transforming is needed.
            continue

        elif (
            mxmns_by_common_pi
            and not major_sigma_viz
        ):
            # move to next chart in linked set since
            # no overlay transforming is needed.
            continue

        profiler(f'<{chart_name}>.interact_graphics_cycle({name})')

        # if a minor curves scaling brings it "outside" the range of
        # the major curve (in major curve co-domain terms) then we
        # need to rescale the major to also include this range. The
        # below placeholder denotes when this occurs.
        # group_mxmn: None | tuple[float, float] = None

        # TODO: probably re-write this loop as a compiled cpython or
        # numba func.

        # conduct "log-linearized multi-plot" scalings for all groups
        for (
            view,
            (
                viz,
                y_start,
                y_min,
                y_max,
                read_slc,
                minor_in_view,
            )
        ) in overlay_table.items():

            key = 'open' if viz.is_ohlc else viz.name

            if (
                isinf(ymx)
                or isinf(ymn)
            ):
                log.warning(
                    f'BAD ymx/ymn: {(ymn, ymx)}'
                )
                continue

            ymn = dnt.apply_rng(y_start)
            ymx = upt.apply_rng(y_start)

            # NOTE XXX: we have to set each curve's range once (and
            # ONLY ONCE) here since we're doing this entire routine
            # inside of a single render cycle (and apparently calling
            # `ViewBox.setYRange()` multiple times within one only takes
            # the first call as serious...) XD
            view._set_yrange(
                yrange=(ymn, ymx),
            )
            profiler(f'{viz.name}@{chart_name} log-SCALE minor')

            if debug_print:
                print(
                    '------------------------------\n'
                    f'LOGLIN SCALE CYCLE: {viz.name}@{chart_name}\n'
                    f'UP MAJOR C: {upt.viz.name} with disp: {upt.rng}\n'
                    f'DOWN MAJOR C: {dnt.viz.name} with disp: {dnt.rng}\n'
                    f'y_start: {y_start}\n'
                    f'y min: {y_min}\n'
                    f'y max: {y_max}\n'
                    f'T scaled ymn: {ymn}\n'
                    f'T scaled ymx: {ymx}\n'
                    '------------------------------\n'
                )

        # profiler(f'{viz.name}@{chart_name} log-SCALE major')
        # major_mx, major_mn = group_mxmn
        # vrs = major_sigma_viz.plot.vb.viewRange()
        # if vrs[1][0] > major_mn:
        #     breakpoint()

        if debug_print:
            print(
                f'END UX GRAPHICS CYCLE: @{chart_name}\n'
                +
                '#'*100
                +
                '\n'
            )
        if not do_linked_charts:
            return

    profiler.finish()
