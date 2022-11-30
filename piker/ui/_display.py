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
from ._axes import YAxisLabel
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
    GodWidget,
)
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
from ..data._sampling import _tick_groups
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


_i_last: int = 0
_i_last_append: int = 0


class DisplayState(Struct):
    '''
    Chart-local real-time graphics state container.

    '''
    godwidget: GodWidget
    quotes: dict[str, Any]

    maxmin: Callable
    flume: Flume
    ohlcv: ShmArray
    hist_ohlcv: ShmArray

    # high level chart handles
    chart: ChartPlotWidget

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

    vlm_chart: Optional[ChartPlotWidget] = None
    vlm_sticky: Optional[YAxisLabel] = None
    wap_in_history: bool = False

    def incr_info(
        self,
        chart: Optional[ChartPlotWidget] = None,
        shm: Optional[ShmArray] = None,
        state: Optional[dict] = None,  # pass in a copy if you don't

        update_state: bool = True,
        update_uppx: float = 16,
        is_1m: bool = False,

    ) -> tuple:

        shm = shm or self.ohlcv
        chart = chart or self.chart
        # state = state or self.vars

        if (
            not update_state
            and state
        ):
            state = state.copy()

        # compute the first available graphic's x-units-per-pixel
        uppx = chart.view.x_uppx()

        # NOTE: this used to be implemented in a dedicated
        # "increment task": ``check_for_new_bars()`` but it doesn't
        # make sense to do a whole task switch when we can just do
        # this simple index-diff and all the fsp sub-curve graphics
        # are diffed on each draw cycle anyway; so updates to the
        # "curve" length is already automatic.

        # increment the view position by the sample offset.
        # i_step = shm.index
        i_step = shm.array[-1]['time']
        # i_diff = i_step - state['i_last']
        # state['i_last'] = i_step
        global _i_last, _i_last_append
        i_diff = i_step - _i_last
        # update global state
        if (
            # state is None
            not is_1m
            and i_diff > 0
        ):
            _i_last = i_step

        # append_diff = i_step - state['i_last_append']
        append_diff = i_step - _i_last_append

        # real-time update necessary?
        main_viz = chart.get_viz(chart.name)
        _, _, _, r = main_viz.bars_range()
        liv = r >= shm.index

        # update the "last datum" (aka extending the vizs graphic with
        # new data) only if the number of unit steps is >= the number of
        # such unit steps per pixel (aka uppx). Iow, if the zoom level
        # is such that a datum(s) update to graphics wouldn't span
        # to a new pixel, we don't update yet.
        do_append = (
            append_diff >= uppx
            and i_diff
        )
        if (
            do_append
            and not is_1m
        ):
            _i_last_append = i_step
            # fqsn = self.flume.symbol.fqsn
            # print(
            #     f'DOING APPEND => {fqsn}\n'
            #     f'i_step:{i_step}\n'
            #     f'i_diff:{i_diff}\n'
            #     f'last:{_i_last}\n'
            #     f'last_append:{_i_last_append}\n'
            #     f'append_diff:{append_diff}\n'
            #     f'r: {r}\n'
            #     f'liv: {liv}\n'
            #     f'uppx: {uppx}\n'
            # )

        do_rt_update = uppx < update_uppx

        # TODO: pack this into a struct
        return (
            uppx,
            liv,
            do_append,
            i_diff,
            append_diff,
            do_rt_update,
        )


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

        slow_pi = hist_chart._vizs[fqsn].plot
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
        # TODO: this is just wrong now since we can have multiple L1-label
        # sets, so instead we should have the l1 associated with the
        # plotitem or y-axis likely?
        # fast_chart._l1_labels = l1

        # TODO:
        # - in theory we should be able to read buffer data faster
        #   then msgs arrive.. needs some tinkering and testing

        # - if trade volume jumps above / below prior L1 price
        #   levels this might be dark volume we need to
        #   present differently -> likely dark vlm

        tick_size = symbol.tick_size
        tick_margin = 3 * tick_size

        fast_chart.show()
        last_quote = time.time()
        # global _i_last
        i_last = ohlcv.index

        dss[fqsn] = ds = linked.display_state = DisplayState(**{
            'godwidget': godwidget,
            'quotes': {},
            'maxmin': maxmin,
            'flume': flume,
            'ohlcv': ohlcv,
            'hist_ohlcv': hist_ohlcv,
            'chart': fast_chart,
            'last_price_sticky': last_price_sticky,
            'hist_last_price_sticky': hist_last_price_sticky,
            'l1': l1,

            'vars': {
                'tick_margin': tick_margin,
                'i_last': i_last,
                'i_last_append': i_last,
                'last_mx_vlm': last_mx_vlm,
                'last_mx': last_mx,
                'last_mn': last_mn,
            }
        })

        if vlm_chart:
            vlm_pi = vlm_chart._vizs['volume'].plot
            vlm_sticky = vlm_pi.getAxis('right')._stickies['volume']
            ds.vlm_chart = vlm_chart
            ds.vlm_sticky = vlm_sticky

        fast_chart.default_view()

        # TODO: probably factor this into some kinda `DisplayState`
        # API that can be reused at least in terms of pulling view
        # params (eg ``.bars_range()``).
        async def increment_history_view():
            i_last = hist_ohlcv.index
            state = ds.vars.copy() | {
                'i_last_append': i_last,
                'i_last': i_last,
            }
            _, hist_step_size_s, _ = flume.get_ds_info()

            async with flume.index_stream(
                # int(hist_step_size_s)
                # TODO: seems this is more reliable at keeping the slow
                # chart incremented in view more correctly?
                # - It might make sense to just inline this logic with the
                #   main display task? => it's a tradeoff of slower task
                #   wakeups/ctx switches verus logic checks (as normal)
                # - we need increment logic that only does the view shift
                #   call when the uppx permits/needs it
                int(1),
            ) as istream:
                async for msg in istream:

                    # check if slow chart needs an x-domain shift and/or
                    # y-range resize.
                    (
                        uppx,
                        liv,
                        do_append,
                        i_diff,
                        append_diff,
                        do_rt_update,
                    ) = ds.incr_info(
                        chart=hist_chart,
                        shm=ds.hist_ohlcv,
                        state=state,
                        is_1m=True,
                        # update_state=False,
                    )
                    # print(
                    #     f'liv: {liv}\n'
                    #     f'do_append: {do_append}\n'
                    #     f'append_diff: {append_diff}\n'
                    # )

                    if (
                        do_append
                        and liv
                    ):
                        # hist_chart.increment_view(steps=i_diff)
                        viz = hist_chart._vizs[fqsn]
                        viz.plot.vb._set_yrange(
                            # yrange=hist_chart.maxmin(name=fqsn)
                        )
                        # hist_chart.view._set_yrange(yrange=hist_chart.maxmin())

        nurse.start_soon(increment_history_view)

    # main real-time quotes update loop
    stream: tractor.MsgStream
    async with feed.open_multi_stream() as stream:
        assert stream
        async for quotes in stream:
            quote_period = time.time() - last_quote
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

            last_quote = time.time()

            for sym, quote in quotes.items():
                ds = dss[sym]
                ds.quotes = quote

                rt_pi, hist_pi = pis[sym]

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
    # TODO: eventually optimize this whole graphics stack with ``numba``
    # hopefully XD

    chart = ds.chart
    # TODO: just pass this as a direct ref to avoid so many attr accesses?
    hist_chart = ds.godwidget.hist_linked.chart

    flume = ds.flume
    sym = flume.symbol
    fqsn = sym.fqsn
    main_viz = chart._vizs[fqsn]
    index_field = main_viz.index_field

    profiler = Profiler(
        msg=f'Graphics loop cycle for: `{chart.name}`',
        delayed=True,
        disabled=not pg_profile_enabled(),
        # disabled=True,
        ms_threshold=ms_slower_then,

        # ms_threshold=1/12 * 1e3,
    )

    # unpack multi-referenced components
    vlm_chart = ds.vlm_chart

    # rt "HFT" chart
    l1 = ds.l1
    # ohlcv = ds.ohlcv
    ohlcv = flume.rt_shm
    array = ohlcv.array

    vars = ds.vars
    tick_margin = vars['tick_margin']

    (
        uppx,
        liv,
        do_append,
        i_diff,
        append_diff,
        do_rt_update,
    ) = ds.incr_info()

    # don't real-time "shift" the curve to the
    # left unless we get one of the following:
    if (
        (
            do_append
            and liv
        )
        or trigger_all
    ):
        # print(f'INCREMENTING {fqsn}')
        chart.increment_view(steps=i_diff)
        main_viz.plot.vb._set_yrange(
            # yrange=(mn, mx),
        )

        # NOTE: since vlm and ohlc charts are axis linked now we don't
        # need the double increment request?
        # if vlm_chart:
        #     vlm_chart.increment_view(steps=i_diff)

        profiler('view incremented')

    # frames_by_type: dict[str, dict] = {}
    # lasts = {}

    # build tick-type "frames" of tick sequences since
    # likely the tick arrival rate is higher then our
    # (throttled) quote stream rate.

    # iterate in FIFO order per tick-frame
    # if sym != fqsn:
    #     continue

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

    if (
        prepend_update_index is not None
        and lbar > prepend_update_index
    ):
        # on a history update (usually from the FSP subsys)
        # if the segment of history that is being prepended
        # isn't in view there is no reason to do a graphics
        # update.
        log.info('Skipping prepend graphics cycle: frame not in view')
        return

    # TODO: eventually we want to separate out the utrade (aka
    # dark vlm prices) here and show them as an additional
    # graphic.
    clear_types = _tick_groups['clears']

    # XXX: if we wanted to iterate in "latest" (i.e. most
    # current) tick first order as an optimization where we only
    # update from the last tick from each type class.
    # last_clear_updated: bool = False

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

    # from pprint import pformat
    # frame_counts = {
    #     typ: len(frame) for typ, frame in frames_by_type.items()
    # }
    # print(
    #     f'{pformat(frame_counts)}\n'
    #     f'framed: {pformat(frames_by_type)}\n'
    #     f'lasts: {pformat(lasts)}\n'
    # )
    # for typ, tick in lasts.items():
    # ticks_frame = quote.get('ticks', ())
    ticks_by_type = quote.get('tbt', {})

    # for tick in ticks_frame:
    for typ, ticks in ticks_by_type.items():

        # NOTE: ticks are `.append()`-ed to the `ticks_by_type: dict` by the
        # `._sampling.uniform_rate_send()` loop
        tick = ticks[-1]
        # typ = tick.get('type')
        price = tick.get('price')
        size = tick.get('size')

        # compute max and min prices (including bid/ask) from
        # tick frames to determine the y-range for chart
        # auto-scaling.
        # TODO: we need a streaming minmax algo here, see def above.
        if liv:
            mx = max(price + tick_margin, mx)
            mn = min(price - tick_margin, mn)

        if typ in clear_types:
            # XXX: if we only wanted to update graphics from the
            # "current"/"latest received" clearing price tick
            # once (see alt iteration order above).
            # if last_clear_updated:
            #     continue

            # last_clear_updated = True
            # we only want to update grahpics from the *last*
            # tick event that falls under the "clearing price"
            # set.

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
        # XXX: is this correct for ib?
        # if ticktype in ('trade', 'last'):
        # if ticktype in ('last',):  # 'size'):
        if typ in ('last',):  # 'size'):

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
                # the relevant L1 queue?
                # label.size -= size

        elif (
            typ in _tick_groups['asks']
            # TODO: instead we could check if the price is in the
            # y-view-range?
            and liv
        ):
            l1.ask_label.update_fields({'level': price, 'size': size})

        elif (
            typ in _tick_groups['bids']
            # TODO: instead we could check if the price is in the
            # y-view-range?
            and liv
        ):
            l1.bid_label.update_fields({'level': price, 'size': size})

    # check for y-range re-size
    if (mx > vars['last_mx']) or (mn < vars['last_mn']):

        # fast chart resize case
        if (
            liv
            and not chart._static_yrange == 'axis'
        ):
            # main_vb = chart.view
            main_vb = chart._vizs[fqsn].plot.vb
            if (
                main_vb._ic is None
                or not main_vb._ic.is_set()
            ):
                # print(f'updating range due to mxmn')
                main_vb._set_yrange(
                    # TODO: we should probably scale
                    # the view margin based on the size
                    # of the true range? This way you can
                    # slap in orders outside the current
                    # L1 (only) book range.
                    # range_margin=0.1,
                    # yrange=(mn, mx),
                )

        # check if slow chart needs a resize
        (
            _,
            hist_liv,
            _,
            _,
            _,
            _,
        ) = ds.incr_info(
            chart=hist_chart,
            shm=ds.hist_ohlcv,
            update_state=False,
            is_1m=True,
        )
        if hist_liv:
            viz = hist_chart._vizs[fqsn]
            viz.plot.vb._set_yrange(
                # yrange=hist_chart.maxmin(name=fqsn),
            )

    # XXX: update this every draw cycle to make L1-always-in-view work.
    vars['last_mx'], vars['last_mn'] = mx, mn

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
                mx_vlm_in_view != vars['last_mx_vlm']
            ):
                yrange = (0, mx_vlm_in_view * 1.375)
                vlm_chart.view._set_yrange(
                    yrange=yrange,
                )
                profiler('`vlm_chart.view._set_yrange()`')
                # print(f'mx vlm: {last_mx_vlm} -> {mx_vlm_in_view}')
                vars['last_mx_vlm'] = mx_vlm_in_view

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

    viz = rt_chart._vizs[flume.symbol.fqsn]
    assert viz

    # XXX: no idea why this doesn't work but it's causing
    # a weird placement of the region on the way-far-left..
    # region.setClipItem(viz.graphics)

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
            ['i3', 'gray'],
            ['grayer', 'bracket'],
            ['grayest', 'i3'],
            ['default_dark', 'default'],
        ])

        pis: dict[str, list[pgo.PlotItem, pgo.PlotItem]] = {}

        # load in ohlc data to a common linked but split chart set.
        fitems: list[
            tuple[str, Flume]
        ] = list(feed.flumes.items())

        # for the "first"/selected symbol we create new chart widgets
        # and sub-charts for FSPs
        fqsn, flume = fitems[0]

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

            for fqsn, flume in fitems[1:]:
                # get a new color from the palette
                bg_chart_color, bg_last_bar_color = next(palette)

                rt_linked._symbol = flume.symbol
                hist_linked._symbol = flume.symbol

                ohlcv: ShmArray = flume.rt_shm
                hist_ohlcv: ShmArray = flume.hist_shm

                symbol = flume.symbol
                fqsn = symbol.fqsn

                hist_pi = hist_chart.overlay_plotitem(
                    name=fqsn,
                    axis_title=fqsn,
                )
                hist_pi.hideAxis('left')
                hist_pi.hideAxis('bottom')

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
            rt_linked.focus()

            godwidget.resize_all()

            # add all additional symbols as overlays
            for fqsn, flume in feed.flumes.items():

                # size view to data prior to order mode init
                rt_chart.default_view()
                rt_linked.graphics_cycle()
                await trio.sleep(0)

                hist_chart.default_view(
                    bars_from_y=int(len(hist_ohlcv.array)),  # size to data
                    y_offset=6116*2,  # push it a little away from the y-axis
                )
                hist_linked.graphics_cycle()
                await trio.sleep(0)

                godwidget.resize_all()

                # trigger another view reset if no sub-chart
                hist_chart.default_view()
                rt_chart.default_view()
                # let qt run to render all widgets and make sure the
                # sidepanes line up vertically.
                await trio.sleep(0)

                # dynamic resize steps
                godwidget.resize_all()

                # TODO: look into this because not sure why it was
                # commented out / we ever needed it XD
                # NOTE: we pop the volume chart from the subplots set so
                # that it isn't double rendered in the display loop
                # above since we do a maxmin calc on the volume data to
                # determine if auto-range adjustements should be made.
                # rt_linked.subplots.pop('volume', None)

                # TODO: make this not so shit XD
                # close group status
                # sbar._status_groups[loading_sym_key][1]()

                hist_linked.graphics_cycle()
                rt_chart.default_view()
                await trio.sleep(0)

                bars_in_mem = int(len(hist_ohlcv.array))
                hist_chart.default_view(
                    bars_from_y=bars_in_mem,  # size to data
                    # push it 1/16th away from the y-axis
                    y_offset=round(bars_in_mem / 16),
                )
                godwidget.resize_all()

            await link_views_with_region(
                rt_chart,
                hist_chart,
                flume,
            )

            # start graphics update loop after receiving first live quote
            ln.start_soon(
                graphics_update_loop,
                ln,
                godwidget,
                feed,
                pis,
                wap_in_history,
                vlm_charts,
            )

            mode: OrderMode
            async with (
                open_order_mode(
                    feed,
                    godwidget,
                    fqsns[-1],
                    order_mode_started
                ) as mode
            ):
                rt_linked.mode = mode

                rt_chart.default_view()
                rt_chart.view.enable_auto_yrange()
                hist_chart.default_view()
                hist_chart.view.enable_auto_yrange()

                await trio.sleep_forever()  # let the app run.. bby
