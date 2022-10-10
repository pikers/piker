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
import time
from typing import Optional, Any, Callable

import tractor
import trio
import pyqtgraph as pg

# from .. import brokers
from ..data.feed import (
    open_feed,
    Feed,
)
from ..data.types import Struct
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
from ..data._sharedmem import ShmArray
from ..data._source import tf_in_1s
from ._forms import (
    FieldsForm,
    mk_order_pane_layout,
)
from .order_mode import (
    open_order_mode,
    OrderMode,
)
from .._profile import (
    pg_profile_enabled,
    ms_slower_then,
)
from ..log import get_logger

log = get_logger(__name__)

# TODO: load this from a config.toml!
_quote_throttle_rate: int = 16  # Hz


# a working tick-type-classes template
_tick_groups = {
    'clears': {'trade', 'utrade', 'last'},
    'bids': {'bid', 'bsize'},
    'asks': {'ask', 'asize'},
}


# TODO: delegate this to each `Flow.maxmin()` which includes
# caching and further we should implement the following stream based
# approach, likely with ``numba``:
# https://arxiv.org/abs/cs/0610046
# https://github.com/lemire/pythonmaxmin
def chart_maxmin(
    chart: ChartPlotWidget,
    ohlcv_shm: ShmArray,
    vlm_chart: Optional[ChartPlotWidget] = None,

) -> tuple[

    tuple[int, int, int, int],

    float,
    float,
    float,
]:
    '''
    Compute max and min datums "in view" for range limits.

    '''
    last_bars_range = chart.bars_range()
    out = chart.maxmin()

    if out is None:
        return (last_bars_range, 0, 0, 0)

    mn, mx = out

    mx_vlm_in_view = 0

    # TODO: we need to NOT call this to avoid a manual
    # np.max/min trigger and especially on the vlm_chart
    # flows which aren't shown.. like vlm?
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

    ) -> tuple:

        shm = shm or self.ohlcv
        chart = chart or self.chart
        state = state or self.vars

        if not update_state:
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
        i_step = shm.index
        i_diff = i_step - state['i_last']
        state['i_last'] = i_step

        append_diff = i_step - state['i_last_append']

        # update the "last datum" (aka extending the flow graphic with
        # new data) only if the number of unit steps is >= the number of
        # such unit steps per pixel (aka uppx). Iow, if the zoom level
        # is such that a datum(s) update to graphics wouldn't span
        # to a new pixel, we don't update yet.
        do_append = (append_diff >= uppx)
        if do_append:
            state['i_last_append'] = i_step

        do_rt_update = uppx < update_uppx

        _, _, _, r = chart.bars_range()
        liv = r >= i_step

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
    wap_in_history: bool = False,
    vlm_chart: Optional[ChartPlotWidget] = None,

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

    ohlcv = feed.rt_shm
    hist_ohlcv = feed.hist_shm

    # update last price sticky
    last_price_sticky = fast_chart._ysticks[fast_chart.name]
    last_price_sticky.update_from_data(
        *ohlcv.array[-1][['index', 'close']]
    )

    hist_last_price_sticky = hist_chart._ysticks[hist_chart.name]
    hist_last_price_sticky.update_from_data(
        *hist_ohlcv.array[-1][['index', 'close']]
    )

    maxmin = partial(
        chart_maxmin,
        fast_chart,
        ohlcv,
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

    symbol = fast_chart.linked.symbol

    l1 = L1Labels(
        fast_chart,
        # determine precision/decimal lengths
        digits=symbol.tick_size_digits,
        size_digits=symbol.lot_size_digits,
    )
    fast_chart._l1_labels = l1

    # TODO:
    # - in theory we should be able to read buffer data faster
    #   then msgs arrive.. needs some tinkering and testing

    # - if trade volume jumps above / below prior L1 price
    #   levels this might be dark volume we need to
    #   present differently -> likely dark vlm

    tick_size = fast_chart.linked.symbol.tick_size
    tick_margin = 3 * tick_size

    fast_chart.show()
    last_quote = time.time()
    i_last = ohlcv.index

    ds = linked.display_state = DisplayState(**{
        'godwidget': godwidget,
        'quotes': {},
        'maxmin': maxmin,
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
        vlm_sticky = vlm_chart._ysticks['volume']
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
        _, hist_step_size_s, _ = feed.get_ds_info()

        async with feed.index_stream(
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
                    hist_chart.increment_view(steps=i_diff)
                    hist_chart.view._set_yrange(yrange=hist_chart.maxmin())

    nurse.start_soon(increment_history_view)

    # main real-time quotes update loop
    stream: tractor.MsgStream = feed.stream
    async for quotes in stream:

        ds.quotes = quotes
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

        # chart isn't active/shown so skip render cycle and pause feed(s)
        if fast_chart.linked.isHidden():
            # print('skipping update')
            fast_chart.pause_all_feeds()
            continue

        # ic = fast_chart.view._ic
        # if ic:
        #     fast_chart.pause_all_feeds()
        #     await ic.wait()
        #     fast_chart.resume_all_feeds()

        # sync call to update all graphics/UX components.
        graphics_update_cycle(ds)


def graphics_update_cycle(
    ds: DisplayState,
    wap_in_history: bool = False,
    trigger_all: bool = False,  # flag used by prepend history updates
    prepend_update_index: Optional[int] = None,

) -> None:
    # TODO: eventually optimize this whole graphics stack with ``numba``
    # hopefully XD

    chart = ds.chart
    # TODO: just pass this as a direct ref to avoid so many attr accesses?
    hist_chart = ds.godwidget.hist_linked.chart

    profiler = pg.debug.Profiler(
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
    ohlcv = ds.ohlcv
    array = ohlcv.array

    vars = ds.vars
    tick_margin = vars['tick_margin']

    for sym, quote in ds.quotes.items():
        (
            uppx,
            liv,
            do_append,
            i_diff,
            append_diff,
            do_rt_update,
        ) = ds.incr_info()

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
            log.debug('Skipping prepend graphics cycle: frame not in view')
            return

        # don't real-time "shift" the curve to the
        # left unless we get one of the following:
        if (
            (do_append and liv)
            or trigger_all
        ):
            chart.increment_view(steps=i_diff)
            chart.view._set_yrange(yrange=(mn, mx))

            if vlm_chart:
                vlm_chart.increment_view(steps=i_diff)

            profiler('view incremented')

        ticks_frame = quote.get('ticks', ())

        frames_by_type: dict[str, dict] = {}
        lasts = {}

        # build tick-type "frames" of tick sequences since
        # likely the tick arrival rate is higher then our
        # (throttled) quote stream rate.
        for tick in ticks_frame:
            price = tick.get('price')
            ticktype = tick.get('type')

            if ticktype == 'n/a' or price == -1:
                # okkk..
                continue

            # keys are entered in olded-event-inserted-first order
            # since we iterate ``ticks_frame`` in standard order
            # above. in other words the order of the keys is the order
            # of tick events by type from the provider feed.
            frames_by_type.setdefault(ticktype, []).append(tick)

            # overwrites so the last tick per type is the entry
            lasts[ticktype] = tick

        # from pprint import pformat
        # frame_counts = {
        #     typ: len(frame) for typ, frame in frames_by_type.items()
        # }
        # print(f'{pformat(frame_counts)}')
        # print(f'framed: {pformat(frames_by_type)}')
        # print(f'lasts: {pformat(lasts)}')

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
                chart.name,
                do_append=do_append,
            )
            hist_chart.update_graphics_from_flow(
                chart.name,
                do_append=do_append,
            )

        # NOTE: we always update the "last" datum
        # since the current range should at least be updated
        # to it's max/min on the last pixel.

        # iterate in FIFO order per tick-frame
        for typ, tick in lasts.items():

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
                end = array[-1]
                ds.last_price_sticky.update_from_data(
                    *end[['index', 'close']]
                )
                ds.hist_last_price_sticky.update_from_data(
                    *end[['index', 'close']]
                )

                if wap_in_history:
                    # update vwap overlay line
                    chart.update_graphics_from_flow(
                        'bar_wap',
                    )

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
                main_vb = chart.view
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
                        yrange=(mn, mx),
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
            )
            if hist_liv:
                hist_chart.view._set_yrange(yrange=hist_chart.maxmin())

        # XXX: update this every draw cycle to make L1-always-in-view work.
        vars['last_mx'], vars['last_mn'] = mx, mn

        # run synchronous update on all linked flows
        # TODO: should the "main" (aka source) flow be special?
        for curve_name, flow in chart._flows.items():
            # update any overlayed fsp flows
            if curve_name != chart.data_key:
                update_fsp_chart(
                    chart,
                    flow,
                    curve_name,
                    array_key=curve_name,
                )

            # even if we're downsampled bigly
            # draw the last datum in the final
            # px column to give the user the mx/mn
            # range of that set.
            if (
                not do_append
                # and not do_rt_update
                and liv
            ):
                flow.draw_last(
                    array_key=curve_name,
                    only_last_uppx=True,
                )

        # volume chart logic..
        # TODO: can we unify this with the above loop?
        if vlm_chart:
            # always update y-label
            ds.vlm_sticky.update_from_data(
                *array[-1][['index', 'volume']]
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

            for curve_name, flow in vlm_chart._flows.items():

                if (
                    curve_name != 'volume' and
                    flow.render and (
                        liv and
                        do_rt_update or do_append
                    )
                ):
                    update_fsp_chart(
                        vlm_chart,
                        flow,
                        curve_name,
                        array_key=curve_name,
                        # do_append=uppx < update_uppx,
                        do_append=do_append,
                    )
                    # is this even doing anything?
                    # (pretty sure it's the real-time
                    # resizing from last quote?)
                    fvb = flow.plot.vb
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
                    # graphic for all flows
                    # print(f'drawing last {flow.name}')
                    flow.draw_last(array_key=curve_name)


async def display_symbol_data(
    godwidget: GodWidget,
    provider: str,
    sym: str,
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
    loading_sym_key = sbar.open_status(
        f'loading {sym}.{provider} ->',
        group_key=True
    )

    # historical data fetch
    # brokermod = brokers.get_brokermod(provider)

    # ohlc_status_done = sbar.open_status(
    #     'retreiving OHLC history.. ',
    #     clear_on_next=True,
    #     group_key=loading_sym_key,
    # )
    fqsn = '.'.join((sym, provider))

    async with open_feed(
        [fqsn],
        loglevel=loglevel,

        # limit to at least display's FPS
        # avoiding needless Qt-in-guest-mode context switches
        tick_throttle=_quote_throttle_rate,

    ) as feed:
        ohlcv: ShmArray = feed.rt_shm
        hist_ohlcv: ShmArray = feed.hist_shm

        # this value needs to be pulled once and only once during
        # startup
        end_index = feed.startup_hist_index

        symbol = feed.symbols[sym]
        fqsn = symbol.front_fqsn()

        step_size_s = 1
        tf_key = tf_in_1s[step_size_s]

        # load in symbol's ohlc data
        godwidget.window.setWindowTitle(
            f'{fqsn} '
            f'tick:{symbol.tick_size} '
            f'step:{tf_key} '
        )

        rt_linked = godwidget.rt_linked
        rt_linked._symbol = symbol

        # create top history view chart above the "main rt chart".
        hist_linked = godwidget.hist_linked
        hist_linked._symbol = symbol
        hist_chart = hist_linked.plot_ohlc_main(
            symbol,
            feed.hist_shm,
            # in the case of history chart we explicitly set `False`
            # to avoid internal pane creation.
            # sidepane=False,
            sidepane=godwidget.search,
        )
        # don't show when not focussed
        hist_linked.cursor.always_show_xlabel = False

        # generate order mode side-pane UI
        # A ``FieldsForm`` form to configure order entry
        # and add as next-to-y-axis singleton pane
        pp_pane: FieldsForm = mk_order_pane_layout(godwidget)
        godwidget.pp_pane = pp_pane

        # create main OHLC chart
        chart = rt_linked.plot_ohlc_main(
            symbol,
            ohlcv,
            # in the case of history chart we explicitly set `False`
            # to avoid internal pane creation.
            sidepane=pp_pane,
        )

        chart._feeds[symbol.key] = feed
        chart.setFocus()

        # XXX: FOR SOME REASON THIS IS CAUSING HANGZ!?!
        # plot historical vwap if available
        wap_in_history = False
        # if (
        #     brokermod._show_wap_in_history
        #     and 'bar_wap' in bars.dtype.fields
        # ):
        #     wap_in_history = True
        #     chart.draw_curve(
        #         name='bar_wap',
        #         shm=ohlcv,
        #         color='default_light',
        #         add_label=False,
        #     )

        # Add the LinearRegionItem to the ViewBox, but tell the ViewBox
        # to exclude this item when doing auto-range calculations.
        rt_pi = chart.plotItem
        hist_pi = hist_chart.plotItem
        region = pg.LinearRegionItem(
            # color scheme that matches sidepane styling
            pen=pg.mkPen(hcolor('gunmetal')),
            brush=pg.mkBrush(hcolor('default_darkest')),
        )
        region.setZValue(10)  # put linear region "in front" in layer terms
        hist_pi.addItem(region, ignoreBounds=True)
        flow = chart._flows[hist_chart.name]
        assert flow
        # XXX: no idea why this doesn't work but it's causing
        # a weird placement of the region on the way-far-left..
        # region.setClipItem(flow.graphics)

        # poll for datums load and timestep detection
        for _ in range(100):
            try:
                _, _, ratio = feed.get_ds_info()
                break
            except IndexError:
                await trio.sleep(0.01)
                continue
        else:
            raise RuntimeError(
                'Failed to detect sampling periods from shm!?')

        def update_pi_from_region():
            region.setZValue(10)
            mn, mx = region.getRegion()
            # print(f'region_x: {(mn, mx)}')

            # XXX: seems to cause a real perf hit?
            rt_pi.setXRange(
                (mn - end_index) * ratio,
                (mx - end_index) * ratio,
                padding=0,
            )

        region.sigRegionChanged.connect(update_pi_from_region)

        def update_region_from_pi(
            window,
            viewRange: tuple[tuple, tuple],
            is_manual: bool = True,

        ) -> None:
            # set the region on the history chart
            # to the range currently viewed in the
            # HFT/real-time chart.
            mn, mx = viewRange[0]
            ds_mn = mn/ratio
            ds_mx = mx/ratio
            # print(
            #     f'rt_view_range: {(mn, mx)}\n'
            #     f'ds_mn, ds_mx: {(ds_mn, ds_mx)}\n'
            # )
            lhmn = ds_mn + end_index
            lhmx = ds_mx + end_index
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

        # NOTE: we must immediately tell Qt to show the OHLC chart
        # to avoid a race where the subplots get added/shown to
        # the linked set *before* the main price chart!
        rt_linked.show()
        rt_linked.focus()
        await trio.sleep(0)

        # NOTE: here we insert the slow-history chart set into
        # the fast chart's splitter -> so it's a splitter of charts
        # inside the first widget slot of a splitter of charts XD
        rt_linked.splitter.insertWidget(0, hist_linked)
        # XXX: if we wanted it at the bottom?
        # rt_linked.splitter.addWidget(hist_linked)
        rt_linked.focus()

        godwidget.resize_all()

        vlm_chart: Optional[ChartPlotWidget] = None
        async with trio.open_nursery() as ln:

            # if available load volume related built-in display(s)
            if (
                not symbol.broker_info[provider].get('no_vlm', False)
                and has_vlm(ohlcv)
            ):
                vlm_chart = await ln.start(
                    open_vlm_displays,
                    rt_linked,
                    ohlcv,
                )

            # load (user's) FSP set (otherwise known as "indicators")
            # from an input config.
            ln.start_soon(
                start_fsp_displays,
                rt_linked,
                ohlcv,
                loading_sym_key,
                loglevel,
            )

            # start graphics update loop after receiving first live quote
            ln.start_soon(
                graphics_update_loop,
                ln,
                godwidget,
                feed,
                wap_in_history,
                vlm_chart,
            )

            await trio.sleep(0)

            # size view to data prior to order mode init
            chart.default_view()
            rt_linked.graphics_cycle()
            await trio.sleep(0)

            hist_chart.default_view(
                bars_from_y=int(len(hist_ohlcv.array)),  # size to data
                y_offset=6116*2,  # push it a little away from the y-axis
            )
            hist_linked.graphics_cycle()
            await trio.sleep(0)

            godwidget.resize_all()

            mode: OrderMode
            async with (
                open_order_mode(
                    feed,
                    godwidget,
                    fqsn,
                    order_mode_started
                ) as mode
            ):
                if not vlm_chart:
                    # trigger another view reset if no sub-chart
                    chart.default_view()

                rt_linked.mode = mode

                # let Qt run to render all widgets and make sure the
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
                sbar._status_groups[loading_sym_key][1]()

                hist_linked.graphics_cycle()
                await trio.sleep(0)

                bars_in_mem = int(len(hist_ohlcv.array))
                hist_chart.default_view(
                    bars_from_y=bars_in_mem,  # size to data
                    # push it 1/16th away from the y-axis
                    y_offset=round(bars_in_mem / 16),
                )
                godwidget.resize_all()

                # let the app run.. bby
                await trio.sleep_forever()
