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
from typing import Optional

import numpy as np
import tractor
import trio

from .. import brokers
from ..data.feed import open_feed, Feed
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
from ..data._sharedmem import ShmArray, try_read
from ._forms import (
    FieldsForm,
    mk_order_pane_layout,
)
from .order_mode import open_order_mode
from ..log import get_logger

log = get_logger(__name__)

# TODO: load this from a config.toml!
_quote_throttle_rate: int = 58  # Hz


# a working tick-type-classes template
_tick_groups = {
    'clears': {'trade', 'utrade', 'last'},
    'bids': {'bid', 'bsize'},
    'asks': {'ask', 'asize'},
}


def chart_maxmin(
    chart: ChartPlotWidget,
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
    # TODO: implement this
    # https://arxiv.org/abs/cs/0610046
    # https://github.com/lemire/pythonmaxmin

    array = chart._arrays[chart.name]
    ifirst = array[0]['index']

    last_bars_range = chart.bars_range()
    l, lbar, rbar, r = last_bars_range
    in_view = array[lbar - ifirst:rbar - ifirst + 1]

    assert in_view.size

    mx, mn = np.nanmax(in_view['high']), np.nanmin(in_view['low'])

    # TODO: when we start using line charts, probably want to make
    # this an overloaded call on our `DataView
    # sym = chart.name
    # mx, mn = np.nanmax(in_view[sym]), np.nanmin(in_view[sym])

    mx_vlm_in_view = 0
    if vlm_chart:
        mx_vlm_in_view = np.max(in_view['volume'])

    return last_bars_range, mx, max(mn, 0), mx_vlm_in_view


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

    chart = linked.chart

    # update last price sticky
    last_price_sticky = chart._ysticks[chart.name]
    last_price_sticky.update_from_data(
        *ohlcv.array[-1][['index', 'close']]
    )

    if vlm_chart:
        vlm_sticky = vlm_chart._ysticks['volume']
        vlm_view = vlm_chart.view

    maxmin = partial(chart_maxmin, chart, vlm_chart)

    chart.default_view()

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
    view = chart.view
    last_quote = time.time()

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

    async for quotes in stream:

        quote_period = time.time() - last_quote
        quote_rate = round(
            1/quote_period, 1) if quote_period > 0 else float('inf')
        if (
            quote_period <= 1/_quote_throttle_rate

            # in the absolute worst case we shouldn't see more then
            # twice the expected throttle rate right!?
            and quote_rate >= _quote_throttle_rate * 1.5
        ):
            log.warning(f'High quote rate {symbol.key}: {quote_rate}')

        last_quote = time.time()

        # chart isn't active/shown so skip render cycle and pause feed(s)
        if chart.linked.isHidden():
            chart.pause_all_feeds()
            continue

        for sym, quote in quotes.items():

            (
                brange,
                mx_in_view,
                mn_in_view,
                mx_vlm_in_view,
            ) = maxmin()
            l, lbar, rbar, r = brange
            mx = mx_in_view + tick_margin
            mn = mn_in_view - tick_margin

            # NOTE: vlm may be written by the ``brokerd`` backend
            # event though a tick sample is not emitted.
            # TODO: show dark trades differently
            # https://github.com/pikers/piker/issues/116
            array = ohlcv.array

            if vlm_chart:
                vlm_chart.update_curve_from_array('volume', array)
                vlm_sticky.update_from_data(*array[-1][['index', 'volume']])

                if (
                    mx_vlm_in_view != last_mx_vlm or
                    mx_vlm_in_view > last_mx_vlm
                ):
                    # print(f'mx vlm: {last_mx_vlm} -> {mx_vlm_in_view}')
                    vlm_view._set_yrange(
                        yrange=(0, mx_vlm_in_view * 1.375)
                    )
                    last_mx_vlm = mx_vlm_in_view

                for curve_name, flow in vlm_chart._flows.items():
                    update_fsp_chart(
                        vlm_chart,
                        flow.shm,
                        curve_name,
                        array_key=curve_name,
                    )
                    # is this even doing anything?
                    flow.plot.vb._set_yrange()

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
            # for typ, tick in reversed(lasts.items()):

            # iterate in FIFO order per frame
            for typ, tick in lasts.items():

                price = tick.get('price')
                size = tick.get('size')

                # compute max and min prices (including bid/ask) from
                # tick frames to determine the y-range for chart
                # auto-scaling.
                # TODO: we need a streaming minmax algo here, see def above.
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
                    last_price_sticky.update_from_data(
                        *end[['index', 'close']]
                    )

                    # update ohlc sampled price bars
                    chart.update_ohlc_from_array(
                        chart.name,
                        array,
                    )

                    if wap_in_history:
                        # update vwap overlay line
                        chart.update_curve_from_array('bar_wap', ohlcv.array)

                # L1 book label-line updates
                # XXX: is this correct for ib?
                # if ticktype in ('trade', 'last'):
                # if ticktype in ('last',):  # 'size'):
                if typ in ('last',):  # 'size'):

                    label = {
                        l1.ask_label.fields['level']: l1.ask_label,
                        l1.bid_label.fields['level']: l1.bid_label,
                    }.get(price)

                    if label is not None:
                        label.update_fields({'level': price, 'size': size})

                        # TODO: on trades should we be knocking down
                        # the relevant L1 queue?
                        # label.size -= size

                # elif ticktype in ('ask', 'asize'):
                elif typ in _tick_groups['asks']:
                    l1.ask_label.update_fields({'level': price, 'size': size})

                # elif ticktype in ('bid', 'bsize'):
                elif typ in _tick_groups['bids']:
                    l1.bid_label.update_fields({'level': price, 'size': size})

            # check for y-range re-size
            if (
                (mx > last_mx) or (mn < last_mn)
                and not chart._static_yrange == 'axis'
            ):
                # print(f'new y range: {(mn, mx)}')
                view._set_yrange(
                    yrange=(mn, mx),
                    # TODO: we should probably scale
                    # the view margin based on the size
                    # of the true range? This way you can
                    # slap in orders outside the current
                    # L1 (only) book range.
                    # range_margin=0.1,
                )

            last_mx, last_mn = mx, mn

            # run synchronous update on all derived fsp subplots
            for name, subchart in linked.subplots.items():
                update_fsp_chart(
                    subchart,
                    subchart._shm,

                    # XXX: do we really needs seperate names here?
                    name,
                    array_key=name,
                )
                subchart.cv._set_yrange()

                # TODO: all overlays on all subplots..

            # run synchronous update on all derived overlays
            for curve_name, flow in chart._flows.items():
                update_fsp_chart(
                    chart,
                    flow.shm,
                    curve_name,
                    array_key=curve_name,
                )
                # chart.view._set_yrange()


async def check_for_new_bars(
    feed: Feed,
    ohlcv: np.ndarray,
    linkedsplits: LinkedSplits,

) -> None:
    '''
    Task which updates from new bars in the shared ohlcv buffer every
    ``delay_s`` seconds.

    '''
    # TODO: right now we'll spin printing bars if the last time
    # stamp is before a large period of no market activity.
    # Likely the best way to solve this is to make this task
    # aware of the instrument's tradable hours?

    price_chart = linkedsplits.chart
    price_chart.default_view()

    async with feed.index_stream() as stream:
        async for index in stream:
            # update chart historical bars graphics by incrementing
            # a time step and drawing the history and new bar

            # When appending a new bar, in the time between the insert
            # from the writing process and the Qt render call, here,
            # the index of the shm buffer may be incremented and the
            # (render) call here might read the new flat bar appended
            # to the buffer (since -1 index read). In that case H==L and the
            # body will be set as None (not drawn) on what this render call
            # *thinks* is the curent bar (even though it's reading data from
            # the newly inserted flat bar.
            #
            # HACK: We need to therefore write only the history (not the
            # current bar) and then either write the current bar manually
            # or place a cursor for visual cue of the current time step.

            array = ohlcv.array
            # avoid unreadable race case on backfills
            while not try_read(array):
                await trio.sleep(0.01)

            # XXX: this puts a flat bar on the current time step
            # TODO: if we eventually have an x-axis time-step "cursor"
            # we can get rid of this since it is extra overhead.
            price_chart.update_ohlc_from_array(
                price_chart.name,
                array,
                just_history=False,
            )

            # main chart overlays
            # for name in price_chart._flows:
            for curve_name in price_chart._flows:
                price_chart.update_curve_from_array(
                    curve_name,
                    price_chart._arrays[curve_name]
                )

            # each subplot
            for name, chart in linkedsplits.subplots.items():

                # TODO: do we need the same unreadable guard as for the
                # price chart (above) here?
                chart.update_curve_from_array(
                    chart.name,
                    chart._shm.array,
                    array_key=chart.data_key
                )

            # shift the view if in follow mode
            price_chart.increment_view()


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

    async with open_feed(
            provider,
            [sym],
            loglevel=loglevel,

            # limit to at least display's FPS
            # avoiding needless Qt-in-guest-mode context switches
            tick_throttle=_quote_throttle_rate,

    ) as feed:
        ohlcv: ShmArray = feed.shm
        bars = ohlcv.array
        symbol = feed.symbols[sym]

        # load in symbol's ohlc data
        godwidget.window.setWindowTitle(
            f'{symbol.key}@{symbol.brokers} '
            f'tick:{symbol.tick_size}'
        )

        linkedsplits = godwidget.linkedsplits
        linkedsplits._symbol = symbol

        # generate order mode side-pane UI
        # A ``FieldsForm`` form to configure order entry
        pp_pane: FieldsForm = mk_order_pane_layout(godwidget)

        # add as next-to-y-axis singleton pane
        godwidget.pp_pane = pp_pane

        # create main OHLC chart
        chart = linkedsplits.plot_ohlc_main(
            symbol,
            bars,
            sidepane=pp_pane,
        )
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

        # TODO: a data view api that makes this less shit
        chart._shm = ohlcv

        # NOTE: we must immediately tell Qt to show the OHLC chart
        # to avoid a race where the subplots get added/shown to
        # the linked set *before* the main price chart!
        linkedsplits.show()
        linkedsplits.focus()
        await trio.sleep(0)

        vlm_chart: Optional[ChartPlotWidget] = None
        async with trio.open_nursery() as ln:

            # if available load volume related built-in display(s)
            if has_vlm(ohlcv):
                vlm_chart = await ln.start(
                    open_vlm_displays,
                    linkedsplits,
                    ohlcv,
                )

            # load (user's) FSP set (otherwise known as "indicators")
            # from an input config.
            ln.start_soon(
                start_fsp_displays,
                linkedsplits,
                ohlcv,
                loading_sym_key,
                loglevel,
            )

            # start graphics update loop after receiving first live quote
            ln.start_soon(
                graphics_update_loop,
                linkedsplits,
                feed.stream,
                ohlcv,
                wap_in_history,
                vlm_chart,
            )

            # start sample step incrementer
            ln.start_soon(
                check_for_new_bars,
                feed,
                ohlcv,
                linkedsplits
            )

            async with (
                open_order_mode(
                    feed,
                    chart,
                    symbol,
                    provider,
                    order_mode_started
                )
            ):
                # let Qt run to render all widgets and make sure the
                # sidepanes line up vertically.
                await trio.sleep(0)
                linkedsplits.resize_sidepanes()

                # NOTE: we pop the volume chart from the subplots set so
                # that it isn't double rendered in the display loop
                # above since we do a maxmin calc on the volume data to
                # determine if auto-range adjustements should be made.
                linkedsplits.subplots.pop('volume', None)

                # TODO: make this not so shit XD
                # close group status
                sbar._status_groups[loading_sym_key][1]()

                # let the app run.. bby
                await trio.sleep_forever()
