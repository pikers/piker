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
from operator import itemgetter
from typing import (
    Any,
    Literal,
    TYPE_CHECKING,
)

import numpy as np
import pendulum
import pyqtgraph as pg

from piker.types import Struct
from ..data.tsp import slice_from_time
from ..log import get_logger
from ..toolz import Profiler

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
    viz: Viz | None = None
    start_t: float | None = None

    # % "range" computed from some ref value to the mn/mx
    rng: float | None = None
    in_view: np.ndarray | None = None

    # pinned-minor curve modified mn and max for the major dispersion
    # curve due to one series being shorter and the pin + scaling from
    # that pin point causing the original range to have to increase.
    y_val: float | None = None

    def apply_r(
        self,
        y_ref: float,  # reference value for dispersion metric

    ) -> float:
        return y_ref * (1 + self.rng)


def intersect_from_longer(
    start_t_first: float,
    in_view_first: np.ndarray,

    start_t_second: float,
    in_view_second: np.ndarray,
    step: float,

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
        step=step,
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

    method: Literal[
        'loglin_ref_to_curve',
        'loglin_ref_to_first',
        'mxmn',
        'solo',

    ] = 'loglin_ref_to_curve',

    # internal debug
    debug_print: bool = False,

) -> None:
    '''
    Calculate and apply y-domain (axis y-range) multi-curve overlay
    adjustments a set of ``plots`` based on the requested
    ``method``.

    '''
    chart_name: str
    chart: ChartPlotWidget

    for chart_name, chart in plots.items():

        overlay_viz_items: dict = chart._vizs

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

        # ONLY auto-yrange the viz mapped to THIS view box
        if (
            not do_overlay_scaling
            or len(overlay_viz_items) < 2
        ):
            viz = active_viz
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

            if debug_print:
                print(f'ONLY ranging THIS viz: {viz.name}')

            # don't iterate overlays, just move to next chart
            continue

        if debug_print:
            divstr = '#'*46
            print(
                f'BEGIN UX GRAPHICS CYCLE: @{chart_name}\n'
                +
                divstr
                +
                '\n'
            )

        # create a group overlay log-linearized y-range transform to
        # track and eventually inverse transform all overlay curves
        # to a common target max dispersion range.
        dnt = OverlayT()
        upt = OverlayT()

        # collect certain flows have grapics objects **in seperate
        # plots/viewboxes** into groups and do a common calc to
        # determine auto-ranging input for `._set_yrange()`.
        # this is primarly used for our so called "log-linearized
        # multi-plot" overlay technique.
        # vizs_by_disp: list[tuple[float, Viz]] = []
        overlay_table: dict[
            float,
            tuple[
                ChartView,
                Viz,
                float,  # y start
                float,  # y min
                float,  # y max
                float,  # y median
                slice,  # in-view array slice
                np.ndarray,  # in-view array
                float,  # returns up scalar
                float,  # return down scalar
            ],
        ] = {}

        # multi-curve overlay processing stage
        for name, viz in overlay_viz_items.items():

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
            if (
                not viz.is_ohlc
                or method == 'solo'
            ):
                pi.vb._set_yrange(yrange=yrange)
                profiler(
                    f'{viz.name}@{chart_name} simple std `._set_yrange()`'
                )
                continue

            # handle overlay log-linearized group scaling cases
            # TODO: a better predicate here, likely something
            # to do with overlays and their settings..
            # TODO: we probably eventually might want some other
            # charts besides OHLC?
            else:
                ymn, ymx = yrange

                # determine start datum in view
                in_view = viz.vs.in_view
                if in_view.size < 2:
                    if debug_print:
                        print(f'{viz.name} not in view?')
                    continue

                row_start = in_view[0]
                if viz.is_ohlc:
                    y_ref = row_start['open']
                else:
                    y_ref = row_start[viz.name]

                profiler(f'{viz.name}@{chart_name} MINOR curve median')

                key = 'open' if viz.is_ohlc else viz.name
                start_t = row_start['time']

                # returns scalars
                r_up: float = (ymx - y_ref) / y_ref
                r_down: float = (ymn - y_ref) / y_ref
                disp: float = r_up - r_down

                msg = (
                    f'Viz[{viz.name}][{key}]: @{chart_name}\n'
                    f' .yrange = {viz.vs.yrange}\n'
                    f' .xrange = {viz.vs.xrange}\n\n'
                    f'start_t: {start_t}\n'
                    f'y_ref: {y_ref}\n'
                    f'ymn: {ymn}\n'
                    f'ymx: {ymx}\n'
                    f'r_up: {r_up}\n'
                    f'r_down: {r_down}\n'
                    f'(full) disp: {disp}\n'
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
                    dnt.y_val = ymn

                    profiler(f'NEW DOWN: {viz.name}@{chart_name} r: {r_down}')
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
                        viz.index_step(),
                    )
                    profiler(f'{viz.name}@{chart_name} intersect by t')

                    if intersect:
                        longer_in_view, _t, i = intersect

                        scaled_mn = dnt.apply_r(y_ref)
                        if scaled_mn > ymn:
                            # after major curve scaling we detected
                            # the minor curve is still out of range
                            # so we need to adjust the major's range
                            # to include the new composed range.
                            y_maj_ref = longer_in_view[key]
                            new_major_ymn = y_maj_ref * (1 + r_down)

                            # rewrite the major range to the new
                            # minor-pinned-to-major range and mark
                            # the transform as "virtual".
                            msg = (
                                f'EXPAND DOWN bc {viz.name}@{chart_name}\n'
                                f'y_start epoch time @ {_t}:\n'
                                f'y_maj_ref @ {_t}: {y_maj_ref}\n'
                                f'R: {dnt.rng} -> {r_down}\n'
                                f'MN: {dnt.y_val} -> {new_major_ymn}\n'
                            )
                            dnt.rng = r_down
                            dnt.y_val = new_major_ymn
                            profiler(msg)
                            if debug_print:
                                print(msg)

                # is the current up `OverlayT` not yet defined or
                # the current `r_up` greater then the previous max.
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
                    upt.y_val = ymx
                    profiler(f'NEW UP: {viz.name}@{chart_name} r: {r_up}')

                else:
                    intersect = intersect_from_longer(
                        upt.start_t,
                        upt.in_view,
                        start_t,
                        in_view,
                        viz.index_step(),
                    )
                    profiler(f'{viz.name}@{chart_name} intersect by t')

                    if intersect:
                        longer_in_view, _t, i = intersect

                        # after major curve scaling we detect if
                        # the minor curve is still out of range
                        # so we need to adjust the major's range
                        # to include the new composed range.
                        scaled_mx = upt.apply_r(y_ref)
                        if scaled_mx < ymx:
                            y_maj_ref = longer_in_view[key]
                            new_major_ymx = y_maj_ref * (1 + r_up)

                            # rewrite the major range to the new
                            # minor-pinned-to-major range and mark
                            # the transform as "virtual".
                            msg = (
                                f'EXPAND UP bc {viz.name}@{chart_name}:\n'
                                f'y_maj_ref @ {_t}: {y_maj_ref}\n'
                                f'R: {upt.rng} -> {r_up}\n'
                                f'MX: {upt.y_val} -> {new_major_ymx}\n'
                            )
                            upt.rng = r_up
                            upt.y_val = new_major_ymx
                            profiler(msg)
                            if debug_print:
                                print(msg)

                # register curves by a "full" dispersion metric for
                # later sort order in the overlay (technique
                # ) application loop below.
                pair: tuple[float, Viz] = (disp, viz)

                # time series are so similar they have same
                # dispersion with `float` precision..
                if entry := overlay_table.get(pair):
                    raise RuntimeError('Duplicate entry!? -> {entry}')

                # vizs_by_disp.append(pair)
                overlay_table[pair] = (
                    viz.plot.vb,
                    viz,
                    y_ref,
                    ymn,
                    ymx,
                    read_slc,
                    in_view,
                    r_up,
                    r_down,
                )
                profiler(f'{viz.name}@{chart_name} yrange scan complete')

        # __ END OF scan phase (loop) __

        # NOTE: if no there were no overlay charts
        # detected/collected (could be either no group detected or
        # chart with a single symbol, thus a single viz/overlay)
        # then we ONLY set the mone chart's (viz) yrange and short
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
            and not overlay_table
        ):
            # move to next chart in linked set since
            # no overlay transforming is needed.
            continue

        profiler('`Viz` curve (first) scan phase complete\n')

        r_up_mx: float
        r_dn_mn: float
        mx_pair: tuple = max(overlay_table, key=itemgetter(0))

        if debug_print:
            # print overlay table in descending dispersion order
            msg = 'overlays in dispersion order:\n'
            for i, disp in enumerate(reversed(overlay_table)):
                entry = overlay_table[disp]
                msg += f' [{i}] {disp}: {entry[1].name}\n'

            print(
                'TRANSFORM PHASE' + '-'*100 + '\n\n'
                +
                msg
            )

        if method == 'loglin_ref_to_curve':
            mx_entry = overlay_table.pop(mx_pair)
        else:
            # TODO: for pin to first-in-view we need to NOT pop this from the
            # table, but can we simplify below code even more?
            mx_entry = overlay_table[mx_pair]

        (
            mx_view,  # viewbox
            mx_viz,  # viz
            _,  # y_ref
            mx_ymn,
            mx_ymx,
            _,  # read_slc
            mx_in_view,  # in_view array
            r_up_mx,
            r_dn_mn,
        ) = mx_entry
        mx_time = mx_in_view['time']
        mx_xref = mx_time[0]

        # conduct "log-linearized multi-plot" range transform
        # calculations for curves detected as overlays in the previous
        # loop:
        #  -> iterate all curves Ci in dispersion-measure sorted order
        #     going from smallest swing to largest via the
        #     ``overlay_table: dict``,
        #  -> match on overlay ``method: str`` provided by caller,
        #  -> calc y-ranges from each curve's time series and store in
        #     a final table ``scaled: dict`` for final application in the
        #     scaling loop; the final phase.
        scaled: dict[
            float,
            tuple[Viz, float, float, float, float]
        ] = {}

        for pair in sorted(
            overlay_table,
            key=itemgetter(0),
            reverse=True,
        ):
            (
                view,
                viz,
                y_start,
                y_min,
                y_max,
                read_slc,
                minor_in_view,
                r_up,
                r_dn,
            ) = overlay_table[pair]

            key = 'open' if viz.is_ohlc else viz.name
            xref = minor_in_view[0]['time']
            match method:
                # Pin this curve to the "major dispersion" (or other
                # target) curve:
                #
                # - find the intersect datum and then scaling according
                #   to the returns log-lin tranform 'at that intersect
                #   reference data'.
                # - if the pinning/log-returns-based transform scaling
                #   results in this minor/pinned curve being out of
                #   view, adjust the scalars to match **this** curve's
                #   y-range to stay in view and then backpropagate that
                #   scaling to all curves, including the major-target,
                #   which were previously scaled before.
                case 'loglin_ref_to_curve':

                    # calculate y-range scalars from the earliest
                    # "intersect" datum with the target-major
                    # (dispersion) curve so as to "pin" the curves
                    # in the y-domain at that spot.
                    # NOTE: there are 2 cases for un-matched support
                    # in x-domain (where one series is shorter then the
                    # other):
                    # => major is longer then minor:
                    #  - need to scale the minor *from* the first
                    #    supported datum in both series.
                    #
                    # => major is shorter then minor:
                    #  - need to scale the minor *from* the first
                    #    supported datum in both series (the
                    #    intersect x-value) but using the
                    #    intersecting point from the minor **not**
                    #    its first value in view!
                    yref = y_start

                    if mx_xref > xref:
                        (
                            xref_pin,
                            yref,
                        ) = viz.i_from_t(
                            mx_xref,
                            return_y=True,
                        )
                        xref_pin_dt = pendulum.from_timestamp(xref_pin)
                        xref = mx_xref

                        if debug_print:
                            print(
                                'MAJOR SHORTER!!!\n'
                                f'xref: {xref}\n'
                                f'xref_pin: {xref_pin}\n'
                                f'xref_pin-dt: {xref_pin_dt}\n'
                                f'yref@xref_pin: {yref}\n'
                            )

                    # XXX: we need to handle not-in-view cases?
                    # still not sure why or when tf this happens..
                    mx_scalars = mx_viz.scalars_from_index(xref)
                    if mx_scalars is None:
                        continue
                    (
                        i_start,
                        y_ref_major,
                        r_up_from_major_at_xref,
                        r_down_from_major_at_xref,
                    ) = mx_scalars

                    if debug_print:
                        print(
                            'MAJOR PIN SCALING\n'
                            f'mx_xref: {mx_xref}\n'
                            f'major i_start: {i_start}\n'
                            f'y_ref_major: {y_ref_major}\n'
                            f'r_up_from_major_at_xref '
                            f'{r_up_from_major_at_xref}\n'
                            f'r_down_from_major_at_xref: '
                            f'{r_down_from_major_at_xref}\n'
                            f'-----to minor-----\n'
                            f'xref: {xref}\n'
                            f'y_start: {y_start}\n'
                            f'yref: {yref}\n'
                        )
                    ymn = yref * (1 + r_down_from_major_at_xref)
                    ymx = yref * (1 + r_up_from_major_at_xref)

                    # if this curve's y-range is detected as **not
                    # being in view** after applying the
                    # target-major's transform, adjust the
                    # target-major curve's range to (log-linearly)
                    # include it (the extra missing range) by
                    # adjusting the y-mxmn to this new y-range and
                    # applying the inverse transform of the minor
                    # back on the target-major (and possibly any
                    # other previously-scaled-to-target/major, minor
                    # curves).
                    if ymn >= y_min:
                        ymn = y_min
                        r_dn_minor = (ymn - yref) / yref

                        # rescale major curve's y-max to include new
                        # range increase required by **this minor**.
                        mx_ymn = y_ref_major * (1 + r_dn_minor)
                        mx_viz.vs.yrange = mx_ymn, mx_viz.vs.yrange[1]

                        if debug_print:
                            print(
                                f'RESCALE {mx_viz.name} DUE TO {viz.name} '
                                f'ymn -> {y_min}\n'
                                f'-> MAJ ymn (w r_down: {r_dn_minor}) '
                                f'-> {mx_ymn}\n\n'
                            )
                        # rescale all already scaled curves to new
                        # increased range for this side as
                        # determined by ``y_min`` staying in view;
                        # re-set the `scaled: dict` entry to
                        # ensure that this minor curve will be
                        # entirely in view.
                        # TODO: re updating already-scaled minor curves
                        # - is there a faster way to do this by
                        #   mutating state on some object instead?
                        for _view in scaled:
                            _viz, _yref, _ymn, _ymx, _xref = scaled[_view]
                            (
                                _,
                                _,
                                _,
                                r_down_from_out_of_range,
                            ) = mx_viz.scalars_from_index(_xref)

                            new_ymn = _yref * (1 + r_down_from_out_of_range)

                            scaled[_view] = (
                                _viz, _yref, new_ymn, _ymx, _xref)

                            if debug_print:
                                print(
                                    f'RESCALE {_viz.name} ymn -> {new_ymn}'
                                    f'RESCALE MAJ ymn -> {mx_ymn}'
                                )

                    # same as above but for minor being out-of-range
                    # on the upside.
                    if ymx <= y_max:
                        ymx = y_max
                        r_up_minor = (ymx - yref) / yref
                        mx_ymx = y_ref_major * (1 + r_up_minor)
                        mx_viz.vs.yrange = mx_viz.vs.yrange[0], mx_ymx

                        if debug_print:
                            print(
                                f'RESCALE {mx_viz.name} DUE TO {viz.name} '
                                f'ymx -> {y_max}\n'
                                f'-> MAJ ymx (r_up: {r_up_minor} '
                                f'-> {mx_ymx}\n\n'
                            )

                        for _view in scaled:
                            _viz, _yref, _ymn, _ymx, _xref = scaled[_view]
                            (
                                _,
                                _,
                                r_up_from_out_of_range,
                                _,
                            ) = mx_viz.scalars_from_index(_xref)

                            new_ymx = _yref * (1 + r_up_from_out_of_range)
                            scaled[_view] = (
                                _viz, _yref, _ymn, new_ymx, _xref)

                            if debug_print:
                                print(
                                    f'RESCALE {_viz.name} ymn -> {new_ymx}'
                                )

                    # register all overlays for a final pass where we
                    # apply all pinned-curve y-range transform scalings.
                    scaled[view] = (viz, yref, ymn, ymx, xref)

                    if debug_print:
                        print(
                            f'Viz[{viz.name}]: @ {chart_name}\n'
                            f' .yrange = {viz.vs.yrange}\n'
                            f' .xrange = {viz.vs.xrange}\n\n'
                            f'xref: {xref}\n'
                            f'xref-dt: {pendulum.from_timestamp(xref)}\n'
                            f'y_min: {y_min}\n'
                            f'y_max: {y_max}\n'
                            f'RESCALING\n'
                            f'r dn: {r_down_from_major_at_xref}\n'
                            f'r up: {r_up_from_major_at_xref}\n'
                            f'ymn: {ymn}\n'
                            f'ymx: {ymx}\n'
                        )

                # Pin all curves by their first datum in view to all
                # others such that each curve's earliest datum provides the
                # reference point for returns vs. every other curve in
                # view.
                case 'loglin_ref_to_first':
                    ymn = dnt.apply_r(y_start)
                    ymx = upt.apply_r(y_start)
                    view._set_yrange(yrange=(ymn, ymx))

                # Do not pin curves by log-linearizing their y-ranges,
                # instead allow each curve to fully scale to the
                # time-series in view's min and max y-values.
                case 'mxmn':
                    view._set_yrange(yrange=(y_min, y_max))

                case _:
                    raise RuntimeError(
                        f'overlay ``method`` is invalid `{method}'
                    )

        # __ END OF transform calc phase (loop) __

        # finally, scale the major target/dispersion curve to
        # the (possibly re-scaled/modified) values were set in
        # transform phase loop.
        mx_view._set_yrange(yrange=(mx_ymn, mx_ymx))

        if scaled:
            if debug_print:
                print(
                    'SCALING PHASE' + '-'*100 + '\n\n'
                    '_________MAJOR INFO___________\n'
                    f'SIGMA MAJOR C: {mx_viz.name} -> {mx_pair[0]}\n'
                    f'UP MAJOR C: {upt.viz.name} with disp: {upt.rng}\n'
                    f'DOWN MAJOR C: {dnt.viz.name} with disp: {dnt.rng}\n'
                    f'xref: {mx_xref}\n'
                    f'xref-dt: {pendulum.from_timestamp(mx_xref)}\n'
                    f'dn: {r_dn_mn}\n'
                    f'up: {r_up_mx}\n'
                    f'mx_ymn: {mx_ymn}\n'
                    f'mx_ymx: {mx_ymx}\n'
                    '------------------------------'
                )

            for (
                view,
                (viz, yref, ymn, ymx, xref)
            ) in scaled.items():

                # NOTE XXX: we have to set each curve's range once (and
                # ONLY ONCE) here since we're doing this entire routine
                # inside of a single render cycle (and apparently calling
                # `ViewBox.setYRange()` multiple times within one only takes
                # the first call as serious...) XD
                view._set_yrange(yrange=(ymn, ymx))
                profiler(f'{viz.name}@{chart_name} log-SCALE minor')

                if debug_print:
                    print(
                        '_________MINOR INFO___________\n'
                        f'Viz[{viz.name}]: @ {chart_name}\n'
                        f' .yrange = {viz.vs.yrange}\n'
                        f' .xrange = {viz.vs.xrange}\n\n'
                        f'xref: {xref}\n'
                        f'xref-dt: {pendulum.from_timestamp(xref)}\n'
                        f'y_start: {y_start}\n'
                        f'y min: {y_min}\n'
                        f'y max: {y_max}\n'
                        f'T scaled ymn: {ymn}\n'
                        f'T scaled ymx: {ymx}\n\n'
                        '--------------------------------\n'
                    )

        # __ END OF overlay scale phase (loop) __

        if debug_print:
            print(
                f'END UX GRAPHICS CYCLE: @{chart_name}\n'
                +
                divstr
                +
                '\n'
            )

        profiler(f'<{chart_name}>.interact_graphics_cycle()')

        if not do_linked_charts:
            break

    profiler.finish()
