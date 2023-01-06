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
real-time display tasks for charting graphics update.

this module ties together quote and computational (fsp) streams with
graphics update methods via our custom ``pyqtgraph`` charting api.

'''
from functools import partial
import itertools
from math import floor
import time
from typing import Optional, Any, Callable

import tractor
import trio
import pyqtgraph as pg

# from .. import brokers
from ..data.feed import (
    open_feed,
    Feed,
    Flume,
)
from ..data.types import Struct
from ..data._sharedmem import (
    ShmArray,
)
from ..data._sampling import (
    _tick_groups,
    open_sample_stream,
)
from ._axes import YAxisLabel
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
    GodWidget,
)
from ._dataviz import Viz
from ._l1 import L1Labels
from ._style import hcolor
from ._fsp import (
    update_fsp_chart,
    start_fsp_displays,
    has_vlm,
    open_vlm_displays,
)
from ._forms import (
    FieldsForm,
    mk_order_pane_layout,
)
from . import _pg_overrides as pgo
# from ..data._source import tf_in_1s
from .order_mode import (
    open_order_mode,
    OrderMode,
)
from .._profile import (
    pg_profile_enabled,
    ms_slower_then,
)
from ..log import get_logger
from .._profile import Profiler

log = get_logger(__name__)


# TODO: delegate this to each `Viz.maxmin()` which includes
# caching and further we should implement the following stream based
# approach, likely with ``numba``:
# https://arxiv.org/abs/cs/0610046
# https://github.com/lemire/pythonmaxmin
def chart_maxmin(
    chart: ChartPlotWidget,
    fqsn: str,
    # ohlcv_shm: ShmArray,
    vlm_chart: ChartPlotWidget | None = None,

) -> tuple[

    tuple[int, int, int, int],

    float,
    float,
    float,
]:
    '''
    Compute max and min datums "in view" for range limits.

    '''
    main_viz = chart.get_viz(chart.name)
    last_bars_range = main_viz.bars_range()
    out = chart.maxmin(name=fqsn)

    if out is None:
        return (last_bars_range, 0, 0, 0)

    mn, mx = out

    mx_vlm_in_view = 0

    # TODO: we need to NOT call this to avoid a manual
    # np.max/min trigger and especially on the vlm_chart
    # vizs which aren't shown.. like vlm?
    if vlm_chart:
        out = vlm_chart.maxmin()
        if out:
            _, mx_vlm_in_view = out

    return (
        last_bars_range,
        mx,
        max(mn, 0),  # presuming price can't be negative?
        mx_vlm_in_view,
    )


class DisplayState(Struct):
    '''
    Chart-local real-time graphics state container.

    '''
    godwidget: GodWidget
    quotes: dict[str, Any]

    maxmin: Callable
    flume: Flume

    # high level chart handles and underlying ``Viz``
    chart: ChartPlotWidget
    viz: Viz
    hist_chart: ChartPlotWidget
    hist_viz: Viz

    # axis labels
    l1: L1Labels
    last_price_sticky: YAxisLabel
    hist_last_price_sticky: YAxisLabel

    # misc state tracking
    vars: dict[str, Any] = {
        'tick_margin': 0,
        'i_last': 0,
        'i_last_append': 0,
        'last_mx_vlm': 0,
        'last_mx': 0,
        'last_mn': 0,
    }
    hist_vars: dict[str, Any] = {
        'tick_margin': 0,
        'i_last': 0,
        'i_last_append': 0,
        'last_mx_vlm': 0,
        'last_mx': 0,
        'last_mn': 0,
    }

    globalz: None | dict[str, Any] = None

    vlm_chart: Optional[ChartPlotWidget] = None
    vlm_sticky: Optional[YAxisLabel] = None
    wap_in_history: bool = False


async def increment_history_view(
    ds: DisplayState,
):
    hist_chart = ds.hist_chart
    hist_viz = ds.hist_viz
    assert 'hist' in hist_viz.shm.token['shm_name']

    # TODO: seems this is more reliable at keeping the slow
    # chart incremented in view more correctly?
    # - It might make sense to just inline this logic with the
    #   main display task? => it's a tradeoff of slower task
    #   wakeups/ctx switches verus logic checks (as normal)
    # - we need increment logic that only does the view shift
    #   call when the uppx permits/needs it
    async with open_sample_stream(1.) as istream:
        async for msg in istream:

            # l3 = ds.viz.shm.array[-3:]
            # print(
            #     f'fast step for {ds.flume.symbol.fqsn}:\n'
            #     f'{list(l3["time"])}\n'
            #     f'{l3}\n'
            # )
            # check if slow chart needs an x-domain shift and/or
            # y-range resize.
            (
                uppx,
                liv,
                do_append,
                i_diff_t,
                append_diff,
                do_rt_update,
                should_tread,

            ) = hist_viz.incr_info(
                ds=ds,
                is_1m=True,
            )

            if (
                do_append
                and liv
            ):
                hist_viz.plot.vb._set_yrange()

            # check if tread-in-place x-shift is needed
            if should_tread:
                hist_chart.increment_view(datums=append_diff)


async def graphics_update_loop(

    nurse: trio.Nursery,
    godwidget: GodWidget,
    feed: Feed,
    pis: dict[str, list[pgo.PlotItem, pgo.PlotItem]] = {},
    wap_in_history: bool = False,
    vlm_charts: dict[str, ChartPlotWidget] = {},

) -> None:
    '''
    The 'main' (price) chart real-time update loop.

    Receive from the primary instrument quote stream and update the OHLC
    chart.

    '''
    # TODO: bunch of stuff (some might be done already, can't member):
    # - I'm starting to think all this logic should be
    #   done in one place and "graphics update routines"
    #   should not be doing any length checking and array diffing.
    # - handle odd lot orders
    # - update last open price correctly instead
    #   of copying it from last bar's close
    # - 1-5 sec bar lookback-autocorrection like tws does?
    #   (would require a background history checker task)
    linked: LinkedSplits = godwidget.rt_linked
    display_rate = godwidget.window.current_screen().refreshRate()

    fast_chart = linked.chart
    hist_chart = godwidget.hist_linked.chart
    assert hist_chart

    # per-viz-set global last index tracking for global chart
    # view UX incrementing; these values are singleton
    # per-multichart-set such that automatic x-domain shifts are only
    # done once per time step update.
    globalz = {
        'i_last_t':  0,  # multiview-global fast (1s) step index
        'i_last_slow_t':  0,  # multiview-global slow (1m) step index
    }

    dss: dict[str, DisplayState] = {}

    for fqsn, flume in feed.flumes.items():
        ohlcv = flume.rt_shm
        hist_ohlcv = flume.hist_shm
        symbol = flume.symbol
        fqsn = symbol.fqsn

        # update last price sticky
        fast_viz = fast_chart._vizs[fqsn]
        index_field = fast_viz.index_field
        fast_pi = fast_viz.plot
        last_price_sticky = fast_pi.getAxis('right')._stickies[fqsn]
        last_price_sticky.update_from_data(
            *ohlcv.array[-1][[
                index_field,
                'close',
            ]]
        )
        last_price_sticky.show()

        hist_viz = hist_chart._vizs[fqsn]
        slow_pi = hist_viz.plot
        hist_last_price_sticky = slow_pi.getAxis('right')._stickies[fqsn]
        hist_last_price_sticky.update_from_data(
            *hist_ohlcv.array[-1][[
                index_field,
                'close',
            ]]
        )

        vlm_chart = vlm_charts[fqsn]
        maxmin = partial(
            chart_maxmin,
            fast_chart,
            fqsn,
            vlm_chart,
        )
        last_bars_range: tuple[float, float]
        (
            last_bars_range,
            last_mx,
            last_mn,
            last_mx_vlm,
        ) = maxmin()

        last, volume = ohlcv.array[-1][['close', 'volume']]

        symbol = flume.symbol

        l1 = L1Labels(
            fast_pi,
            # determine precision/decimal lengths
            digits=symbol.tick_size_digits,
            size_digits=symbol.lot_size_digits,
        )

        # TODO:
        # - in theory we should be able to read buffer data faster
        #   then msgs arrive.. needs some tinkering and testing

        # - if trade volume jumps above / below prior L1 price
        #   levels this might be dark volume we need to
        #   present differently -> likely dark vlm

        tick_size = symbol.tick_size
        tick_margin = 3 * tick_size

        fast_chart.show()
        last_quote_s = time.time()

        dss[fqsn] = ds = linked.display_state = DisplayState(**{
            'godwidget': godwidget,
            'quotes': {},
            'maxmin': maxmin,

            'flume': flume,

            'chart': fast_chart,
            'viz': fast_viz,
            'last_price_sticky': last_price_sticky,

            'hist_chart': hist_chart,
            'hist_viz': hist_viz,
            'hist_last_price_sticky': hist_last_price_sticky,

            'l1': l1,

            'vars': {
                'tick_margin': tick_margin,
                'i_last': 0,
                'i_last_append': 0,
                'last_mx_vlm': last_mx_vlm,
                'last_mx': last_mx,
                'last_mn': last_mn,
            },
            'globalz': globalz,
        })

        if vlm_chart:
            vlm_pi = vlm_chart._vizs['volume'].plot
            vlm_sticky = vlm_pi.getAxis('right')._stickies['volume']
            ds.vlm_chart = vlm_chart
            ds.vlm_sticky = vlm_sticky

        fast_chart.default_view()

        # ds.hist_vars.update({
        #     'i_last_append': 0,
        #     'i_last': 0,
        # })

        nurse.start_soon(
            increment_history_view,
            ds,
        )

        if ds.hist_vars['i_last'] < ds.hist_vars['i_last_append']:
            breakpoint()

    # main real-time quotes update loop
    stream: tractor.MsgStream
    async with feed.open_multi_stream() as stream:
        assert stream
        async for quotes in stream:
            quote_period = time.time() - last_quote_s
            quote_rate = round(
                1/quote_period, 1) if quote_period > 0 else float('inf')
            if (
                quote_period <= 1/_quote_throttle_rate

                # in the absolute worst case we shouldn't see more then
                # twice the expected throttle rate right!?
                # and quote_rate >= _quote_throttle_rate * 2
                and quote_rate >= display_rate
            ):
                log.warning(f'High quote rate {symbol.key}: {quote_rate}')

            last_quote_s = time.time()

            for fqsn, quote in quotes.items():
                ds = dss[fqsn]
                ds.quotes = quote

                rt_pi, hist_pi = pis[fqsn]

                # chart isn't active/shown so skip render cycle and
                # pause feed(s)
                if (
                    fast_chart.linked.isHidden()
                    or not rt_pi.isVisible()
                ):
                    print(f'{fqsn} skipping update for HIDDEN CHART')
                    fast_chart.pause_all_feeds()
                    continue

                ic = fast_chart.view._ic
                if ic:
                    fast_chart.pause_all_feeds()
                    print(f'{fqsn} PAUSING DURING INTERACTION')
                    await ic.wait()
                    fast_chart.resume_all_feeds()

                # sync call to update all graphics/UX components.
                graphics_update_cycle(
                    ds,
                    quote,
                )


def graphics_update_cycle(
    ds: DisplayState,
    quote: dict,

    wap_in_history: bool = False,
    trigger_all: bool = False,  # flag used by prepend history updates
    prepend_update_index: Optional[int] = None,

) -> None:

    # TODO: SPEEDing this all up..
    # - optimize this whole graphics stack with ``numba`` hopefully
    #   or at least a little `mypyc` B)
    # - pass more direct refs as input to avoid so many attr accesses?
    # - use a streaming minmax algo and drop the use of the
    #   state-tracking ``chart_maxmin()`` routine from above?

    chart = ds.chart
    hist_chart = ds.hist_chart

    flume = ds.flume
    sym = flume.symbol
    fqsn = sym.fqsn
    main_viz = chart._vizs[fqsn]
    index_field = main_viz.index_field

    profiler = Profiler(
        msg=f'Graphics loop cycle for: `{chart.name}`',
        delayed=True,
        disabled=not pg_profile_enabled(),
        ms_threshold=ms_slower_then,
    )

    # unpack multi-referenced components
    vlm_chart = ds.vlm_chart

    # rt "HFT" chart
    l1 = ds.l1
    ohlcv = flume.rt_shm
    array = ohlcv.array

    varz = ds.vars
    tick_margin = varz['tick_margin']

    (
        uppx,
        liv,
        do_append,
        i_diff_t,
        append_diff,
        do_rt_update,
        should_tread,
    ) = main_viz.incr_info(ds=ds)

    # TODO: we should only run mxmn when we know
    # an update is due via ``do_append`` above.
    (
        brange,
        mx_in_view,
        mn_in_view,
        mx_vlm_in_view,
    ) = ds.maxmin()
    l, lbar, rbar, r = brange
    mx = mx_in_view + tick_margin
    mn = mn_in_view - tick_margin
    profiler('`ds.maxmin()` call')

    # TODO: eventually we want to separate out the dark vlm and show
    # them as an additional graphic.
    clear_types = _tick_groups['clears']

    # update ohlc sampled price bars
    if (
        do_rt_update
        or do_append
        or trigger_all
    ):
        chart.update_graphics_from_flow(
            fqsn,
            # chart.name,
            # do_append=do_append,
        )
        main_viz.draw_last(array_key=fqsn)

        hist_chart.update_graphics_from_flow(
            fqsn,
            # chart.name,
            # do_append=do_append,
        )

    # don't real-time "shift" the curve to the
    # left unless we get one of the following:
    if (
        (
            should_tread
            and do_append
            and liv
        )
        or trigger_all
    ):
        chart.increment_view(datums=append_diff)
        main_viz.plot.vb._set_yrange()

        # NOTE: since vlm and ohlc charts are axis linked now we don't
        # need the double increment request?
        # if vlm_chart:
        #     vlm_chart.increment_view(datums=append_diff)

        profiler('view incremented')

    # iterate frames of ticks-by-type such that we only update graphics
    # using the last update per type where possible.
    ticks_by_type = quote.get('tbt', {})
    for typ, ticks in ticks_by_type.items():

        # NOTE: ticks are `.append()`-ed to the `ticks_by_type: dict` by the
        # `._sampling.uniform_rate_send()` loop
        tick = ticks[-1]  # get most recent value

        price = tick.get('price')
        size = tick.get('size')

        # compute max and min prices (including bid/ask) from
        # tick frames to determine the y-range for chart
        # auto-scaling.
        if (
            liv

            # TODO: make sure IB doesn't send ``-1``!
            and price > 0
        ):
            mx = max(price + tick_margin, mx)
            mn = min(price - tick_margin, mn)

        # clearing price update:
        # generally, we only want to update grahpics from the *last*
        # tick event once - thus showing the most recent state.
        if typ in clear_types:

            # update price sticky(s)
            end_ic = array[-1][[
                index_field,
                'close',
            ]]
            ds.last_price_sticky.update_from_data(*end_ic)
            ds.hist_last_price_sticky.update_from_data(*end_ic)

            if wap_in_history:
                # update vwap overlay line
                chart.update_graphics_from_flow('bar_wap')

        # L1 book label-line updates
        if typ in ('last',):

            label = {
                l1.ask_label.fields['level']: l1.ask_label,
                l1.bid_label.fields['level']: l1.bid_label,
            }.get(price)

            if (
                label is not None
                and liv
            ):
                label.update_fields(
                    {'level': price, 'size': size}
                )

                # TODO: on trades should we be knocking down
                # the relevant L1 queue manually ourselves?
                # label.size -= size

        # NOTE: right now we always update the y-axis labels
        # despite the last datum not being in view. Ideally
        # we have a guard for this when we detect that the range
        # of those values is not in view and then we disable these
        # blocks.
        elif (
            typ in _tick_groups['asks']
        ):
            l1.ask_label.update_fields({'level': price, 'size': size})

        elif (
            typ in _tick_groups['bids']
        ):
            l1.bid_label.update_fields({'level': price, 'size': size})

    # check for y-autorange re-size
    lmx = varz['last_mx']
    lmn = varz['last_mn']
    mx_diff = mx - lmx
    mn_diff = mn - lmn

    if (
        mx_diff
        or mn_diff
    ):
        if (
            abs(mx_diff) > .25 * lmx
            or
            abs(mn_diff) > .25 * lmn
        ):
            log.error(
                f'WTF MN/MX IS WAY OFF:\n'
                f'lmn: {lmn}\n'
                f'mn: {mn}\n'
                f'lmx: {lmx}\n'
                f'mx: {mx}\n'
                f'mx_diff: {mx_diff}\n'
                f'mn_diff: {mn_diff}\n'
            )
        # fast chart resize case
        elif (
            liv
            and not chart._static_yrange == 'axis'
        ):
            main_vb = chart._vizs[fqsn].plot.vb
            if (
                main_vb._ic is None
                or not main_vb._ic.is_set()
            ):
                yr = (mn, mx)
                # print(
                #     f'updating y-range due to mxmn\n'
                #     f'{fqsn}: {yr}'
                # )

                main_vb._set_yrange(
                    # TODO: we should probably scale
                    # the view margin based on the size
                    # of the true range? This way you can
                    # slap in orders outside the current
                    # L1 (only) book range.
                    # range_margin=0.1,
                    yrange=yr
                )

        # check if slow chart needs a resize
        hist_viz = hist_chart._vizs[fqsn]
        (
            _,
            hist_liv,
            _,
            _,
            _,
            _,
            _,
        ) = hist_viz.incr_info(
            ds=ds,
            is_1m=True,
        )
        if hist_liv:
            hist_viz.plot.vb._set_yrange()

    # XXX: update this every draw cycle to make
    varz['last_mx'], varz['last_mn'] = mx, mn

    # run synchronous update on all linked viz
    # TODO: should the "main" (aka source) viz be special?
    for curve_name, viz in chart._vizs.items():
        # update any overlayed fsp flows
        if (
            # curve_name != chart.data_key
            curve_name != fqsn
            and not viz.is_ohlc
        ):
            update_fsp_chart(
                chart,
                viz,
                curve_name,
                array_key=curve_name,
            )

        # even if we're downsampled bigly
        # draw the last datum in the final
        # px column to give the user the mx/mn
        # range of that set.
        if (
            liv
            # and not do_append
            # and not do_rt_update
        ):
            viz.draw_last(
                array_key=curve_name,
                only_last_uppx=True,
            )

    # volume chart logic..
    # TODO: can we unify this with the above loop?
    if vlm_chart:
        # print(f"DOING VLM {fqsn}")
        vlm_vizs = vlm_chart._vizs

        # always update y-label
        ds.vlm_sticky.update_from_data(
            *array[-1][[
                index_field,
                'volume',
            ]]
        )

        if (
            (
                do_rt_update
                or do_append
                and liv
            )
            or trigger_all
        ):
            # TODO: make it so this doesn't have to be called
            # once the $vlm is up?
            vlm_chart.update_graphics_from_flow(
                'volume',
                # UGGGh, see ``maxmin()`` impl in `._fsp` for
                # the overlayed plotitems... we need a better
                # bay to invoke a maxmin per overlay..
                render=False,
                # XXX: ^^^^ THIS IS SUPER IMPORTANT! ^^^^
                # without this, since we disable the
                # 'volume' (units) chart after the $vlm starts
                # up we need to be sure to enable this
                # auto-ranging otherwise there will be no handler
                # connected to update accompanying overlay
                # graphics..
            )
            profiler('`vlm_chart.update_graphics_from_flow()`')

            if (
                mx_vlm_in_view != varz['last_mx_vlm']
            ):
                vlm_yr = (0, mx_vlm_in_view * 1.375)
                vlm_chart.view._set_yrange(yrange=vlm_yr)
                profiler('`vlm_chart.view._set_yrange()`')
                # print(f'mx vlm: {last_mx_vlm} -> {mx_vlm_in_view}')
                varz['last_mx_vlm'] = mx_vlm_in_view

        # update all downstream FSPs
        for curve_name, viz in vlm_vizs.items():

            if (
                curve_name not in {'volume', fqsn}
                and viz.render
                and (
                    liv and do_rt_update
                    or do_append
                )
                # and not viz.is_ohlc
                # and curve_name != fqsn
            ):
                update_fsp_chart(
                    vlm_chart,
                    viz,
                    curve_name,
                    array_key=curve_name,
                    # do_append=uppx < update_uppx,
                    # do_append=do_append,
                )
                # is this even doing anything?
                # (pretty sure it's the real-time
                # resizing from last quote?)
                fvb = viz.plot.vb
                fvb._set_yrange(
                    name=curve_name,
                )

            elif (
                curve_name != 'volume'
                and not do_append
                and liv
                and uppx >= 1
                # even if we're downsampled bigly
                # draw the last datum in the final
                # px column to give the user the mx/mn
                # range of that set.
            ):
                # always update the last datum-element
                # graphic for all vizs
                # print(f'drawing last {viz.name}')
                viz.draw_last(array_key=curve_name)


async def link_views_with_region(
    rt_chart: ChartPlotWidget,
    hist_chart: ChartPlotWidget,
    flume: Flume,

) -> None:

    # these value are be only pulled once during shm init/startup
    izero_hist = flume.izero_hist
    izero_rt = flume.izero_rt

    # Add the LinearRegionItem to the ViewBox, but tell the ViewBox
    # to exclude this item when doing auto-range calculations.
    rt_pi = rt_chart.plotItem
    hist_pi = hist_chart.plotItem

    region = pg.LinearRegionItem(
        movable=False,
        # color scheme that matches sidepane styling
        pen=pg.mkPen(hcolor('gunmetal')),
        brush=pg.mkBrush(hcolor('default_darkest')),
    )

    region.setOpacity(0)
    hist_pi.addItem(region, ignoreBounds=True)
    region.setOpacity(6/16)

    viz = rt_chart.get_viz(flume.symbol.fqsn)
    assert viz
    index_field = viz.index_field

    # XXX: no idea why this doesn't work but it's causing
    # a weird placement of the region on the way-far-left..
    # region.setClipItem(viz.graphics)

    if index_field == 'time':

        # in the (epoch) index case we can map directly
        # from the fast chart's x-domain values since they are
        # on the same index as the slow chart.

        def update_region_from_pi(
            window,
            viewRange: tuple[tuple, tuple],
            is_manual: bool = True,
        ) -> None:
            # put linear region "in front" in layer terms
            region.setZValue(10)

            # set the region on the history chart
            # to the range currently viewed in the
            # HFT/real-time chart.
            rng = mn, mx = viewRange[0]

            # hist_viz = hist_chart.get_viz(flume.symbol.fqsn)
            # hist = hist_viz.shm.array[-3:]
            # print(
            #     f'mn: {mn}\n'
            #     f'mx: {mx}\n'
            #     f'slow last 3 epochs: {list(hist["time"])}\n'
            #     f'slow last 3: {hist}\n'
            # )

            region.setRegion(rng)

    else:
        # poll for datums load and timestep detection
        for _ in range(100):
            try:
                _, _, ratio = flume.get_ds_info()
                break
            except IndexError:
                await trio.sleep(0.01)
                continue
        else:
            raise RuntimeError(
                'Failed to detect sampling periods from shm!?')

        # sampling rate transform math:
        # -----------------------------
        # define the fast chart to slow chart as a linear mapping
        # over the fast index domain `i` to the slow index domain
        # `j` as:
        #
        # j = i - i_offset
        #     ------------  + j_offset
        #         j/i
        #
        # conversely the inverse function is:
        #
        # i = j/i * (j - j_offset) + i_offset
        #
        # Where `j_offset` is our ``izero_hist`` and `i_offset` is our
        # `izero_rt`, the ``ShmArray`` offsets which correspond to the
        # indexes in each array where the "current" time is indexed at init.
        # AKA the index where new data is "appended to" and historical data
        # if "prepended from".
        #
        # more practically (and by default) `i` is normally an index
        # into 1s samples and `j` is an index into 60s samples (aka 1m).
        # in the below handlers ``ratio`` is the `j/i` and ``mn``/``mx``
        # are the low and high index input from the source index domain.

        def update_region_from_pi(
            window,
            viewRange: tuple[tuple, tuple],
            is_manual: bool = True,

        ) -> None:
            # put linear region "in front" in layer terms
            region.setZValue(10)

            # set the region on the history chart
            # to the range currently viewed in the
            # HFT/real-time chart.
            mn, mx = viewRange[0]
            ds_mn = (mn - izero_rt)/ratio
            ds_mx = (mx - izero_rt)/ratio
            lhmn = ds_mn + izero_hist
            lhmx = ds_mx + izero_hist
            # print(
            #     f'rt_view_range: {(mn, mx)}\n'
            #     f'ds_mn, ds_mx: {(ds_mn, ds_mx)}\n'
            #     f'lhmn, lhmx: {(lhmn, lhmx)}\n'
            # )
            region.setRegion((
                lhmn,
                lhmx,
            ))

            # TODO: if we want to have the slow chart adjust range to
            # match the fast chart's selection -> results in the
            # linear region expansion never can go "outside of view".
            # hmn, hmx = hvr = hist_chart.view.state['viewRange'][0]
            # print((hmn, hmx))
            # if (
            #     hvr
            #     and (lhmn < hmn or lhmx > hmx)
            # ):
            #     hist_pi.setXRange(
            #         lhmn,
            #         lhmx,
            #         padding=0,
            #     )
            #     hist_linked.graphics_cycle()

    # connect region to be updated on plotitem interaction.
    rt_pi.sigRangeChanged.connect(update_region_from_pi)

    def update_pi_from_region():
        region.setZValue(10)
        mn, mx = region.getRegion()
        # print(f'region_x: {(mn, mx)}')
        rt_pi.setXRange(
            ((mn - izero_hist) * ratio) + izero_rt,
            ((mx - izero_hist) * ratio) + izero_rt,
            padding=0,
        )

    # TODO BUG XXX: seems to cause a real perf hit and a recursion error
    # (but used to work before generalizing for 1s ohlc offset?)..
    # something to do with the label callback handlers?

    # region.sigRegionChanged.connect(update_pi_from_region)
    # region.sigRegionChangeFinished.connect(update_pi_from_region)


# force 0 to always be in view
def multi_maxmin(
    chart: ChartPlotWidget,
    names: list[str],

) -> tuple[float, float]:
    '''
    Viz "group" maxmin loop; assumes all named vizs
    are in the same co-domain and thus can be sorted
    as one set.

    Iterates all the named vizs and calls the chart
    api to find their range values and return.

    TODO: really we should probably have a more built-in API
    for this?

    '''
    mx = 0
    for name in names:
        ymn, ymx = chart.maxmin(name=name)
        mx = max(mx, ymx)

        return 0, mx


_quote_throttle_rate: int = 60 - 6


async def display_symbol_data(
    godwidget: GodWidget,
    fqsns: list[str],
    loglevel: str,
    order_mode_started: trio.Event,

) -> None:
    '''
    Spawn a real-time updated chart for ``symbol``.

    Spawned ``LinkedSplits`` chart widgets can remain up but hidden so
    that multiple symbols can be viewed and switched between extremely
    fast from a cached watch-list.

    '''
    sbar = godwidget.window.status_bar
    # historical data fetch
    # brokermod = brokers.get_brokermod(provider)

    # ohlc_status_done = sbar.open_status(
    #     'retreiving OHLC history.. ',
    #     clear_on_next=True,
    #     group_key=loading_sym_key,
    # )

    for fqsn in fqsns:
        loading_sym_key = sbar.open_status(
            f'loading {fqsn} ->',
            group_key=True
        )

    # (TODO: make this not so shit XD)
    # close group status once a symbol feed fully loads to view.
    # sbar._status_groups[loading_sym_key][1]()

    # TODO: ctl over update loop's maximum frequency.
    # - load this from a config.toml!
    # - allow dyanmic configuration from chart UI?
    global _quote_throttle_rate
    from ._window import main_window
    display_rate = main_window().current_screen().refreshRate()
    _quote_throttle_rate = floor(display_rate) - 6

    feed: Feed
    async with open_feed(
        fqsns,
        loglevel=loglevel,

        # limit to at least display's FPS
        # avoiding needless Qt-in-guest-mode context switches
        tick_throttle=min(
            round(_quote_throttle_rate/len(fqsns)),
            22,  # aka 6 + 16
        ),

    ) as feed:

        # use expanded contract symbols passed back from feed layer.
        fqsns = list(feed.flumes.keys())

        # step_size_s = 1
        # tf_key = tf_in_1s[step_size_s]
        godwidget.window.setWindowTitle(
            f'{fqsns} '
            # f'tick:{symbol.tick_size} '
            # f'step:{tf_key} '
        )
        # generate order mode side-pane UI
        # A ``FieldsForm`` form to configure order entry
        # and add as next-to-y-axis singleton pane
        pp_pane: FieldsForm = mk_order_pane_layout(godwidget)
        godwidget.pp_pane = pp_pane

        # create top history view chart above the "main rt chart".
        rt_linked = godwidget.rt_linked
        hist_linked = godwidget.hist_linked

        # NOTE: here we insert the slow-history chart set into
        # the fast chart's splitter -> so it's a splitter of charts
        # inside the first widget slot of a splitter of charts XD
        rt_linked.splitter.insertWidget(0, hist_linked)

        rt_chart: None | ChartPlotWidget = None
        hist_chart: None | ChartPlotWidget = None
        vlm_chart: None | ChartPlotWidget = None

        # TODO: I think some palette's based on asset group types
        # would be good, for eg:
        # - underlying and opts contracts
        # - index and underlyings + futures
        # - gradient in "lightness" based on liquidity, or lifetime in derivs?
        palette = itertools.cycle([
            # curve color, last bar curve color
            ['grayest', 'i3'],
            ['default_dark', 'default'],

            ['grayer', 'bracket'],
            ['i3', 'gray'],
        ])

        pis: dict[str, list[pgo.PlotItem, pgo.PlotItem]] = {}

        # load in ohlc data to a common linked but split chart set.
        fitems: list[
            tuple[str, Flume]
        ] = list(feed.flumes.items())

        # use array int-indexing when no aggregate feed overlays are
        # loaded.
        if len(fitems) == 1:
            from ._dataviz import Viz
            Viz._index_field = 'index'

        # for the "first"/selected symbol we create new chart widgets
        # and sub-charts for FSPs
        fqsn, flume = fitems[0]

        # TODO NOTE: THIS CONTROLS WHAT SYMBOL IS USED FOR ORDER MODE
        # SUBMISSIONS, we need to make this switch based on selection.
        rt_linked._symbol = flume.symbol
        hist_linked._symbol = flume.symbol

        ohlcv: ShmArray = flume.rt_shm
        hist_ohlcv: ShmArray = flume.hist_shm

        symbol = flume.symbol
        brokername = symbol.brokers[0]
        fqsn = symbol.fqsn

        hist_chart = hist_linked.plot_ohlc_main(
            symbol,
            hist_ohlcv,
            flume,
            # in the case of history chart we explicitly set `False`
            # to avoid internal pane creation.
            # sidepane=False,
            sidepane=godwidget.search,
        )

        # ensure the last datum graphic is generated
        # for zoom-interaction purposes.
        hist_chart.get_viz(fqsn).draw_last(
            array_key=fqsn,
            # only_last_uppx=True,
        )
        pis.setdefault(fqsn, [None, None])[1] = hist_chart.plotItem

        # don't show when not focussed
        hist_linked.cursor.always_show_xlabel = False

        rt_chart = rt_linked.plot_ohlc_main(
            symbol,
            ohlcv,
            flume,
            # in the case of history chart we explicitly set `False`
            # to avoid internal pane creation.
            sidepane=pp_pane,
        )
        pis.setdefault(fqsn, [None, None])[0] = rt_chart.plotItem

        # for pause/resume on mouse interaction
        rt_chart.feed = feed

        async with trio.open_nursery() as ln:
            # if available load volume related built-in display(s)
            vlm_charts: dict[
                str,
                None | ChartPlotWidget
            ] = {}.fromkeys(feed.flumes)
            if (
                not symbol.broker_info[brokername].get('no_vlm', False)
                and has_vlm(ohlcv)
                and vlm_chart is None
            ):
                vlm_charts[fqsn] = await ln.start(
                    open_vlm_displays,
                    rt_linked,
                    flume,
                )

            # load (user's) FSP set (otherwise known as "indicators")
            # from an input config.
            ln.start_soon(
                start_fsp_displays,
                rt_linked,
                flume,
                loading_sym_key,
                loglevel,
            )

            # XXX: FOR SOME REASON THIS IS CAUSING HANGZ!?!
            # plot historical vwap if available
            wap_in_history = False
            # if (
            #     brokermod._show_wap_in_history
            #     and 'bar_wap' in bars.dtype.fields
            # ):
            #     wap_in_history = True
            #     rt_chart.draw_curve(
            #         name='bar_wap',
            #         shm=ohlcv,
            #         color='default_light',
            #         add_label=False,
            #     )

            godwidget.resize_all()
            await trio.sleep(0)

            for fqsn, flume in fitems[1:]:
                # get a new color from the palette
                bg_chart_color, bg_last_bar_color = next(palette)

                ohlcv: ShmArray = flume.rt_shm
                hist_ohlcv: ShmArray = flume.hist_shm

                symbol = flume.symbol
                fqsn = symbol.fqsn

                hist_pi = hist_chart.overlay_plotitem(
                    name=fqsn,
                    axis_title=fqsn,
                )
                # only show a singleton bottom-bottom axis by default.
                hist_pi.hideAxis('bottom')

                # XXX: TODO: THIS WILL CAUSE A GAP ON OVERLAYS,
                # i think it needs to be "removed" instead when there
                # are none?
                hist_pi.hideAxis('left')

                viz = hist_chart.draw_curve(
                    fqsn,
                    hist_ohlcv,
                    flume,
                    array_key=fqsn,
                    overlay=hist_pi,
                    pi=hist_pi,
                    is_ohlc=True,

                    color=bg_chart_color,
                    last_bar_color=bg_last_bar_color,
                )

                # ensure the last datum graphic is generated
                # for zoom-interaction purposes.
                viz.draw_last(
                    array_key=fqsn,
                    # only_last_uppx=True,
                )

                hist_pi.vb.maxmin = partial(
                    hist_chart.maxmin,
                    name=fqsn,
                )
                # TODO: we need a better API to do this..
                # specially store ref to shm for lookup in display loop
                # since only a placeholder of `None` is entered in
                # ``.draw_curve()``.
                viz = hist_chart._vizs[fqsn]
                assert viz.plot is hist_pi
                pis.setdefault(fqsn, [None, None])[1] = hist_pi

                rt_pi = rt_chart.overlay_plotitem(
                    name=fqsn,
                    axis_title=fqsn,
                )

                rt_pi.hideAxis('left')
                rt_pi.hideAxis('bottom')

                viz = rt_chart.draw_curve(
                    fqsn,
                    ohlcv,
                    flume,
                    array_key=fqsn,
                    overlay=rt_pi,
                    pi=rt_pi,
                    is_ohlc=True,

                    color=bg_chart_color,
                    last_bar_color=bg_last_bar_color,
                )
                rt_pi.vb.maxmin = partial(
                    rt_chart.maxmin,
                    name=fqsn,
                )

                # TODO: we need a better API to do this..
                # specially store ref to shm for lookup in display loop
                # since only a placeholder of `None` is entered in
                # ``.draw_curve()``.
                viz = rt_chart._vizs[fqsn]
                assert viz.plot is rt_pi
                pis.setdefault(fqsn, [None, None])[0] = rt_pi

            rt_chart.setFocus()

            # NOTE: we must immediately tell Qt to show the OHLC chart
            # to avoid a race where the subplots get added/shown to
            # the linked set *before* the main price chart!
            rt_linked.show()
            rt_linked.focus()
            await trio.sleep(0)

            # XXX: if we wanted it at the bottom?
            # rt_linked.splitter.addWidget(hist_linked)

            # greedily do a view range default and pane resizing
            # on startup  before loading the order-mode machinery.
            for fqsn, flume in feed.flumes.items():

                # size view to data prior to order mode init
                rt_chart.default_view()
                rt_linked.graphics_cycle()

                # TODO: look into this because not sure why it was
                # commented out / we ever needed it XD
                # NOTE: we pop the volume chart from the subplots set so
                # that it isn't double rendered in the display loop
                # above since we do a maxmin calc on the volume data to
                # determine if auto-range adjustements should be made.
                # rt_linked.subplots.pop('volume', None)

                hist_chart.default_view()
                hist_linked.graphics_cycle()

                godwidget.resize_all()
                await trio.sleep(0)

            await link_views_with_region(
                rt_chart,
                hist_chart,
                flume,
            )

            # start update loop task
            ln.start_soon(
                graphics_update_loop,
                ln,
                godwidget,
                feed,
                pis,
                wap_in_history,
                vlm_charts,
            )

            # boot order-mode
            order_ctl_symbol: str = fqsns[0]
            mode: OrderMode
            async with (
                open_order_mode(
                    feed,
                    godwidget,
                    fqsns[0],
                    order_mode_started
                ) as mode
            ):

                rt_linked.mode = mode

                viz = rt_chart.get_viz(order_ctl_symbol)
                viz.plot.setFocus()

                # default view adjuments and sidepane alignment
                # as final default UX touch.
                rt_chart.default_view()
                rt_chart.view.enable_auto_yrange()
                await trio.sleep(0)

                hist_chart.default_view()
                hist_chart.view.enable_auto_yrange()
                await trio.sleep(0)

                godwidget.resize_all()

                await trio.sleep_forever()  # let the app run.. bby
