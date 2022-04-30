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
from dataclasses import dataclass
from functools import partial
import time
from typing import Optional, Any, Callable

import numpy as np
import tractor
import trio
import pyqtgraph as pg

from .. import brokers
from ..data.feed import open_feed
from ._axes import YAxisLabel
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
    GodWidget,
)
from ._l1 import L1Labels
from ._fsp import (
    update_fsp_chart,
    start_fsp_displays,
    has_vlm,
    open_vlm_displays,
)
from ..data._sharedmem import ShmArray
from ._forms import (
    FieldsForm,
    mk_order_pane_layout,
)
from .order_mode import open_order_mode
# from .._profile import (
#     pg_profile_enabled,
#     ms_slower_then,
# )
from ..log import get_logger

log = get_logger(__name__)

# TODO: load this from a config.toml!
_quote_throttle_rate: int = 22  # Hz


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


@dataclass
class DisplayState:
    '''
    Chart-local real-time graphics state container.

    '''
    quotes: dict[str, Any]

    maxmin: Callable
    ohlcv: ShmArray

    # high level chart handles
    linked: LinkedSplits
    chart: ChartPlotWidget
    vlm_chart: ChartPlotWidget

    # axis labels
    l1: L1Labels
    last_price_sticky: YAxisLabel
    vlm_sticky: YAxisLabel

    # misc state tracking
    vars: dict[str, Any]

    wap_in_history: bool = False


async def graphics_update_loop(

    linked: LinkedSplits,
    stream: tractor.MsgStream,
    ohlcv: np.ndarray,

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
    display_rate = linked.godwidget.window.current_screen().refreshRate()

    chart = linked.chart

    # update last price sticky
    last_price_sticky = chart._ysticks[chart.name]
    last_price_sticky.update_from_data(
        *ohlcv.array[-1][['index', 'close']]
    )

    if vlm_chart:
        vlm_sticky = vlm_chart._ysticks['volume']

    maxmin = partial(
        chart_maxmin,
        chart,
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

    symbol = chart.linked.symbol

    l1 = L1Labels(
        chart,
        # determine precision/decimal lengths
        digits=symbol.tick_size_digits,
        size_digits=symbol.lot_size_digits,
    )
    chart._l1_labels = l1

    # TODO:
    # - in theory we should be able to read buffer data faster
    #   then msgs arrive.. needs some tinkering and testing

    # - if trade volume jumps above / below prior L1 price
    #   levels this might be dark volume we need to
    #   present differently -> likely dark vlm

    tick_size = chart.linked.symbol.tick_size
    tick_margin = 3 * tick_size

    chart.show()
    # view = chart.view
    last_quote = time.time()
    i_last = ohlcv.index

    # async def iter_drain_quotes():
    #     # NOTE: all code below this loop is expected to be synchronous
    #     # and thus draw instructions are not picked up jntil the next
    #     # wait / iteration.
    #     async for quotes in stream:
    #         while True:
    #             try:
    #                 moar = stream.receive_nowait()
    #             except trio.WouldBlock:
    #                 yield quotes
    #                 break
    #             else:
    #                 for sym, quote in moar.items():
    #                     ticks_frame = quote.get('ticks')
    #                     if ticks_frame:
    #                         quotes[sym].setdefault(
    #                             'ticks', []).extend(ticks_frame)
    #                     print('pulled extra')

    #                 yield quotes

    # async for quotes in iter_drain_quotes():

    ds = linked.display_state = DisplayState(**{
        'quotes': {},
        'linked': linked,
        'maxmin': maxmin,
        'ohlcv': ohlcv,
        'chart': chart,
        'last_price_sticky': last_price_sticky,
        'vlm_chart': vlm_chart,
        'vlm_sticky': vlm_sticky,
        'l1': l1,

        'vars': {
            'tick_margin': tick_margin,
            'i_last': i_last,
            'last_mx_vlm': last_mx_vlm,
            'last_mx': last_mx,
            'last_mn': last_mn,
        }
    })

    chart.default_view()

    # main real-time quotes update loop
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
        if chart.linked.isHidden():
            chart.pause_all_feeds()
            continue

        ic = chart.view._ic
        if ic:
            chart.pause_all_feeds()
            await ic.wait()
            chart.resume_all_feeds()

        # sync call to update all graphics/UX components.
        graphics_update_cycle(ds)


def graphics_update_cycle(
    ds: DisplayState,
    wap_in_history: bool = False,
    trigger_all: bool = False,  # flag used by prepend history updates

) -> None:
    # TODO: eventually optimize this whole graphics stack with ``numba``
    # hopefully XD

    chart = ds.chart

    profiler = pg.debug.Profiler(
        msg=f'Graphics loop cycle for: `{chart.name}`',
        disabled=True,  # not pg_profile_enabled(),
        gt=1/12 * 1e3,
        # gt=ms_slower_then,
    )

    # unpack multi-referenced components
    vlm_chart = ds.vlm_chart
    l1 = ds.l1
    ohlcv = ds.ohlcv
    array = ohlcv.array
    vars = ds.vars
    tick_margin = vars['tick_margin']

    update_uppx = 16

    for sym, quote in ds.quotes.items():

        # compute the first available graphic's x-units-per-pixel
        xpx = vlm_chart.view.x_uppx()

        # NOTE: vlm may be written by the ``brokerd`` backend
        # event though a tick sample is not emitted.
        # TODO: show dark trades differently
        # https://github.com/pikers/piker/issues/116

        # NOTE: this used to be implemented in a dedicated
        # "increment task": ``check_for_new_bars()`` but it doesn't
        # make sense to do a whole task switch when we can just do
        # this simple index-diff and all the fsp sub-curve graphics
        # are diffed on each draw cycle anyway; so updates to the
        # "curve" length is already automatic.

        # increment the view position by the sample offset.
        i_step = ohlcv.index
        i_diff = i_step - vars['i_last']
        vars['i_last'] = i_step

        (
            brange,
            mx_in_view,
            mn_in_view,
            mx_vlm_in_view,
        ) = ds.maxmin()

        l, lbar, rbar, r = brange
        mx = mx_in_view + tick_margin
        mn = mn_in_view - tick_margin
        profiler('maxmin call')
        liv = r > i_step  # the last datum is in view

        # don't real-time "shift" the curve to the
        # left unless we get one of the following:
        if (
            (
                i_diff > 0  # no new sample step
                and xpx < 4  # chart is zoomed out very far
                and r >= i_step  # the last datum isn't in view
                and liv
            )
            or trigger_all
        ):
            # TODO: we should track and compute whether the last
            # pixel in a curve should show new data based on uppx
            # and then iff update curves and shift?
            chart.increment_view(steps=i_diff)

        if vlm_chart:
            # always update y-label
            ds.vlm_sticky.update_from_data(
                *array[-1][['index', 'volume']]
            )

            if (
                (
                    xpx < update_uppx
                    or i_diff > 0
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

                if (
                    mx_vlm_in_view != vars['last_mx_vlm']
                ):
                    yrange = (0, mx_vlm_in_view * 1.375)
                    vlm_chart.view._set_yrange(
                        yrange=yrange,
                    )
                    # print(f'mx vlm: {last_mx_vlm} -> {mx_vlm_in_view}')
                    vars['last_mx_vlm'] = mx_vlm_in_view

                for curve_name, flow in vlm_chart._flows.items():

                    if not flow.render:
                        continue

                    update_fsp_chart(
                        vlm_chart,
                        flow,
                        curve_name,
                        array_key=curve_name,
                        do_append=xpx < update_uppx,
                    )
                    # is this even doing anything?
                    # (pretty sure it's the real-time
                    # resizing from last quote?)
                    fvb = flow.plot.vb
                    fvb._set_yrange(
                        autoscale_linked_plots=False,
                        name=curve_name,
                    )

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
            xpx < update_uppx
            or i_diff > 0
            or trigger_all
        ):
            chart.update_graphics_from_flow(
                chart.name,
                do_append=xpx < update_uppx,
            )

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
        if (
            (mx > vars['last_mx']) or (mn < vars['last_mn'])
            and not chart._static_yrange == 'axis'
            and liv
        ):
            main_vb = chart.view
            if (
                main_vb._ic is None
                or not main_vb._ic.is_set()
            ):
                main_vb._set_yrange(
                    # TODO: we should probably scale
                    # the view margin based on the size
                    # of the true range? This way you can
                    # slap in orders outside the current
                    # L1 (only) book range.
                    # range_margin=0.1,
                    yrange=(mn, mx),
                )

            vars['last_mx'], vars['last_mn'] = mx, mn

        # run synchronous update on all linked flows
        for curve_name, flow in chart._flows.items():
            # TODO: should the "main" (aka source) flow be special?
            if curve_name == chart.data_key:
                continue

            update_fsp_chart(
                chart,
                flow,
                curve_name,
                array_key=curve_name,
            )


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
    brokermod = brokers.get_brokermod(provider)

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
        ohlcv: ShmArray = feed.shm
        bars = ohlcv.array
        symbol = feed.symbols[sym]
        fqsn = symbol.front_fqsn()

        # load in symbol's ohlc data
        godwidget.window.setWindowTitle(
            f'{fqsn} '
            f'tick:{symbol.tick_size} '
            f'step:1s '
        )

        linked = godwidget.linkedsplits
        linked._symbol = symbol

        # generate order mode side-pane UI
        # A ``FieldsForm`` form to configure order entry
        pp_pane: FieldsForm = mk_order_pane_layout(godwidget)

        # add as next-to-y-axis singleton pane
        godwidget.pp_pane = pp_pane

        # create main OHLC chart
        chart = linked.plot_ohlc_main(
            symbol,
            ohlcv,
            sidepane=pp_pane,
        )
        chart.default_view()
        chart._feeds[symbol.key] = feed
        chart.setFocus()

        # plot historical vwap if available
        wap_in_history = False

        if brokermod._show_wap_in_history:

            if 'bar_wap' in bars.dtype.fields:
                wap_in_history = True
                chart.draw_curve(
                    name='bar_wap',
                    data=bars,
                    add_label=False,
                )

        # size view to data once at outset
        chart.cv._set_yrange()

        # NOTE: we must immediately tell Qt to show the OHLC chart
        # to avoid a race where the subplots get added/shown to
        # the linked set *before* the main price chart!
        linked.show()
        linked.focus()
        await trio.sleep(0)

        vlm_chart: Optional[ChartPlotWidget] = None
        async with trio.open_nursery() as ln:

            # if available load volume related built-in display(s)
            if has_vlm(ohlcv):
                vlm_chart = await ln.start(
                    open_vlm_displays,
                    linked,
                    ohlcv,
                )

            # load (user's) FSP set (otherwise known as "indicators")
            # from an input config.
            ln.start_soon(
                start_fsp_displays,
                linked,
                ohlcv,
                loading_sym_key,
                loglevel,
            )

            # start graphics update loop after receiving first live quote
            ln.start_soon(
                graphics_update_loop,
                linked,
                feed.stream,
                ohlcv,
                wap_in_history,
                vlm_chart,
            )

            async with (
                open_order_mode(
                    feed,
                    chart,
                    fqsn,
                    order_mode_started
                )
            ):
                # let Qt run to render all widgets and make sure the
                # sidepanes line up vertically.
                await trio.sleep(0)
                linked.resize_sidepanes()

                # NOTE: we pop the volume chart from the subplots set so
                # that it isn't double rendered in the display loop
                # above since we do a maxmin calc on the volume data to
                # determine if auto-range adjustements should be made.
                # linked.subplots.pop('volume', None)

                # TODO: make this not so shit XD
                # close group status
                sbar._status_groups[loading_sym_key][1]()

                # let the app run.. bby
                # linked.graphics_cycle()
                await trio.sleep_forever()
