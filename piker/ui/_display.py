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
Real-time display tasks for charting / graphics.

'''
from contextlib import asynccontextmanager
import time
from typing import Any
from types import ModuleType

import numpy as np
from pydantic import BaseModel
import tractor
import trio

from .. import brokers
from ..data.feed import (
    open_feed,
    # Feed,
)
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
    GodWidget,
)
from .. import fsp
from ._l1 import L1Labels
from ..data._sharedmem import ShmArray, maybe_open_shm_array
from ._forms import (
    FieldsForm,
    mk_form,
    mk_order_pane_layout,
    open_form_input_handling,
)
from .order_mode import open_order_mode
from ..log import get_logger

log = get_logger(__name__)

_clear_throttle_rate: int = 58  # Hz
_book_throttle_rate: int = 16  # Hz


async def chart_from_quotes(

    chart: ChartPlotWidget,
    stream: tractor.MsgStream,
    ohlcv: np.ndarray,
    wap_in_history: bool = False,

) -> None:
    '''The 'main' (price) chart real-time update loop.

    Receive from the quote stream and update the OHLC chart.

    '''
    # TODO: bunch of stuff:
    # - I'm starting to think all this logic should be
    #   done in one place and "graphics update routines"
    #   should not be doing any length checking and array diffing.
    # - handle odd lot orders
    # - update last open price correctly instead
    #   of copying it from last bar's close
    # - 5 sec bar lookback-autocorrection like tws does?

    # update last price sticky
    last_price_sticky = chart._ysticks[chart.name]
    last_price_sticky.update_from_data(
        *ohlcv.array[-1][['index', 'close']]
    )

    def maxmin():
        # TODO: implement this
        # https://arxiv.org/abs/cs/0610046
        # https://github.com/lemire/pythonmaxmin

        array = chart._arrays['ohlc']
        ifirst = array[0]['index']

        last_bars_range = chart.bars_range()
        l, lbar, rbar, r = last_bars_range
        in_view = array[lbar - ifirst:rbar - ifirst]

        assert in_view.size

        mx, mn = np.nanmax(in_view['high']), np.nanmin(in_view['low'])

        # TODO: when we start using line charts, probably want to make
        # this an overloaded call on our `DataView
        # sym = chart.name
        # mx, mn = np.nanmax(in_view[sym]), np.nanmin(in_view[sym])

        return last_bars_range, mx, max(mn, 0)

    chart.default_view()

    last_bars_range, last_mx, last_mn = maxmin()

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
    # then msgs arrive.. needs some tinkering and testing

    # - if trade volume jumps above / below prior L1 price
    # levels this might be dark volume we need to
    # present differently?

    tick_size = chart.linked.symbol.tick_size
    tick_margin = 2 * tick_size

    last_ask = last_bid = last_clear = time.time()
    chart.show()

    async for quotes in stream:

        # chart isn't actively shown so just skip render cycle
        if chart.linked.isHidden():
            await chart.pause_all_feeds()
            continue

        for sym, quote in quotes.items():

            now = time.time()

            for tick in quote.get('ticks', ()):

                # print(f"CHART: {quote['symbol']}: {tick}")
                ticktype = tick.get('type')
                price = tick.get('price')
                size = tick.get('size')

                if ticktype == 'n/a' or price == -1:
                    # okkk..
                    continue

                # clearing price event
                if ticktype in ('trade', 'utrade', 'last'):

                    # throttle clearing price updates to ~ max 60 FPS
                    period = now - last_clear
                    if period <= 1/_clear_throttle_rate:
                        # faster then display refresh rate
                        continue

                    # print(f'passthrough {tick}\n{1/(now-last_clear)}')
                    # set time of last graphics update
                    last_clear = now

                    array = ohlcv.array

                    # update price sticky(s)
                    end = array[-1]
                    last_price_sticky.update_from_data(
                        *end[['index', 'close']]
                    )

                    # plot bars
                    # update price bar
                    chart.update_ohlc_from_array(
                        chart.name,
                        array,
                    )

                    if wap_in_history:
                        # update vwap overlay line
                        chart.update_curve_from_array('bar_wap', ohlcv.array)

                # l1 book events
                # throttle the book graphics updates at a lower rate
                # since they aren't as critical for a manual user
                # viewing the chart

                elif ticktype in ('ask', 'asize'):
                    if (now - last_ask) <= 1/_book_throttle_rate:
                        # print(f'skipping\n{tick}')
                        continue

                    # print(f'passthrough {tick}\n{1/(now-last_ask)}')
                    last_ask = now

                elif ticktype in ('bid', 'bsize'):
                    if (now - last_bid) <= 1/_book_throttle_rate:
                        continue

                    # print(f'passthrough {tick}\n{1/(now-last_bid)}')
                    last_bid = now

                # compute max and min trade values to display in view
                # TODO: we need a streaming minmax algorithm here, see
                # def above.
                brange, mx_in_view, mn_in_view = maxmin()
                l, lbar, rbar, r = brange

                mx = mx_in_view + tick_margin
                mn = mn_in_view - tick_margin

                # XXX: prettty sure this is correct?
                # if ticktype in ('trade', 'last'):
                if ticktype in ('last',):  # 'size'):

                    label = {
                        l1.ask_label.fields['level']: l1.ask_label,
                        l1.bid_label.fields['level']: l1.bid_label,
                    }.get(price)

                    if label is not None:
                        label.update_fields({'level': price, 'size': size})

                        # on trades should we be knocking down
                        # the relevant L1 queue?
                        # label.size -= size

                elif ticktype in ('ask', 'asize'):
                    l1.ask_label.update_fields({'level': price, 'size': size})

                elif ticktype in ('bid', 'bsize'):
                    l1.bid_label.update_fields({'level': price, 'size': size})

                # update min price in view to keep bid on screen
                mn = min(price - tick_margin, mn)
                # update max price in view to keep ask on screen
                mx = max(price + tick_margin, mx)

                if (mx > last_mx) or (
                    mn < last_mn
                ):
                    # print(f'new y range: {(mn, mx)}')

                    chart._set_yrange(
                        yrange=(mn, mx),
                        # TODO: we should probably scale
                        # the view margin based on the size
                        # of the true range? This way you can
                        # slap in orders outside the current
                        # L1 (only) book range.
                        # range_margin=0.1,
                    )

                last_mx, last_mn = mx, mn


async def fan_out_spawn_fsp_daemons(

    linkedsplits: LinkedSplits,
    fsps: dict[str, str],
    sym: str,
    src_shm: list,
    brokermod: ModuleType,
    group_status_key: str,
    loglevel: str,

) -> None:
    '''Create financial signal processing sub-actors (under flat tree)
    for each entry in config and attach to local graphics update tasks.

    Pass target entrypoint and historical data.

    '''
    linkedsplits.focus()

    uid = tractor.current_actor().uid

    # spawns sub-processes which execute cpu bound FSP code
    async with (
        tractor.open_nursery() as n,
        trio.open_nursery() as ln,
    ):

        # Currently we spawn an actor per fsp chain but
        # likely we'll want to pool them eventually to
        # scale horizonatlly once cores are used up.
        for display_name, conf in fsps.items():

            fsp_func_name = conf['fsp_func_name']

            # TODO: load function here and introspect
            # return stream type(s)

            # TODO: should `index` be a required internal field?
            fsp_dtype = np.dtype([('index', int), (fsp_func_name, float)])

            key = f'{sym}.fsp.{display_name}.{".".join(uid)}'

            # this is all sync currently
            shm, opened = maybe_open_shm_array(
                key,
                # TODO: create entry for each time frame
                dtype=fsp_dtype,
                readonly=True,
            )

            # XXX: fsp may have been opened by a duplicate chart.
            # Error for now until we figure out how to wrap fsps as
            # "feeds".  assert opened, f"A chart for {key} likely
            # already exists?"

            conf['shm'] = shm

            portal = await n.start_actor(
                enable_modules=['piker.fsp._engine'],
                name='fsp.' + display_name,
            )

            # init async
            ln.start_soon(
                run_fsp,
                portal,
                linkedsplits,
                brokermod,
                sym,
                src_shm,
                fsp_func_name,
                display_name,
                conf,
                group_status_key,
                loglevel,
            )

    # blocks here until all fsp actors complete


class FspConfig(BaseModel):
    class Config:
        validate_assignment = True

    name: str
    period: int


@asynccontextmanager
async def open_sidepane(

    linked: LinkedSplits,
    display_name: str,

) -> FspConfig:

    sidepane: FieldsForm = mk_form(
        parent=linked.godwidget,
        fields_schema={
            'name': {
                'label': '**fsp**:',
                'type': 'select',
                'default_value': [
                    f'{display_name}'
                ],
            },

            # TODO: generate this from input map
            'period': {
                'label': '**period**:',
                'type': 'edit',
                'default_value': 14,
            },
        },
    )
    sidepane.model = FspConfig(
        name=display_name,
        period=14,
    )

    # just a logger for now until we get fsp configs up and running.
    async def settings_change(key: str, value: str) -> bool:
        print(f'{key}: {value}')
        return True

    # TODO:
    async with (
        open_form_input_handling(
            sidepane,
            focus_next=linked.godwidget,
            on_value_change=settings_change,
        )
    ):
        yield sidepane


async def run_fsp(

    portal: tractor._portal.Portal,
    linkedsplits: LinkedSplits,
    brokermod: ModuleType,
    sym: str,
    src_shm: ShmArray,
    fsp_func_name: str,
    display_name: str,
    conf: dict[str, Any],
    group_status_key: str,
    loglevel: str,

) -> None:
    '''FSP stream chart update loop.

    This is called once for each entry in the fsp
    config map.

    '''
    done = linkedsplits.window().status_bar.open_status(
        f'loading fsp, {display_name}..',
        group_key=group_status_key,
    )

    async with (
        portal.open_context(

            # chaining entrypoint
            fsp.cascade,

            # name as title of sub-chart
            brokername=brokermod.name,
            src_shm_token=src_shm.token,
            dst_shm_token=conf['shm'].token,
            symbol=sym,
            func_name=fsp_func_name,
            loglevel=loglevel,

        ) as (ctx, last_index),
        ctx.open_stream() as stream,
        open_sidepane(
            linkedsplits,
            display_name,
        ) as sidepane,
    ):

        shm = conf['shm']

        if conf.get('overlay'):
            chart = linkedsplits.chart
            chart.draw_curve(
                name='vwap',
                data=shm.array,
                overlay=True,
            )
            last_val_sticky = None

        else:

            chart = linkedsplits.add_plot(
                name=display_name,
                array=shm.array,

                array_key=conf['fsp_func_name'],
                sidepane=sidepane,

                # curve by default
                ohlc=False,

                # settings passed down to ``ChartPlotWidget``
                **conf.get('chart_kwargs', {})
                # static_yrange=(0, 100),
            )

            # XXX: ONLY for sub-chart fsps, overlays have their
            # data looked up from the chart's internal array set.
            # TODO: we must get a data view api going STAT!!
            chart._shm = shm

            # should **not** be the same sub-chart widget
            assert chart.name != linkedsplits.chart.name

            # sticky only on sub-charts atm
            last_val_sticky = chart._ysticks[chart.name]

            # read from last calculated value
            array = shm.array

            # XXX: fsp func names must be unique meaning we don't have
            # duplicates of the underlying data even if multiple
            # sub-charts reference it under different 'named charts'.
            value = array[fsp_func_name][-1]

            last_val_sticky.update_from_data(-1, value)

        chart.linked.focus()

        # works also for overlays in which case data is looked up from
        # internal chart array set....
        chart.update_curve_from_array(
            display_name,
            shm.array,
            array_key=fsp_func_name
        )

        chart.linked.resize_sidepanes()

        # TODO: figure out if we can roll our own `FillToThreshold` to
        # get brush filled polygons for OS/OB conditions.
        # ``pg.FillBetweenItems`` seems to be one technique using
        # generic fills between curve types while ``PlotCurveItem`` has
        # logic inside ``.paint()`` for ``self.opts['fillLevel']`` which
        # might be the best solution?
        # graphics = chart.update_from_array(chart.name, array[fsp_func_name])
        # graphics.curve.setBrush(50, 50, 200, 100)
        # graphics.curve.setFillLevel(50)

        if fsp_func_name == 'rsi':
            from ._lines import level_line
            # add moveable over-[sold/bought] lines
            # and labels only for the 70/30 lines
            level_line(chart, 20)
            level_line(chart, 30, orient_v='top')
            level_line(chart, 70, orient_v='bottom')
            level_line(chart, 80, orient_v='top')

        chart._set_yrange()

        last = time.time()

        done()

        # i = 0
        # update chart graphics
        async for value in stream:

            # chart isn't actively shown so just skip render cycle
            if chart.linked.isHidden():
                # print(f'{i} unseen fsp cyclce')
                # i += 1
                continue

            now = time.time()
            period = now - last

            # if period <= 1/30:
            if period <= 1/_clear_throttle_rate:
                # faster then display refresh rate
                # print(f'fsp too fast: {1/period}')
                continue

            # TODO: provide a read sync mechanism to avoid this polling.
            # the underlying issue is that a backfill and subsequent shm
            # array first/last index update could result in an empty array
            # read here since the stream is never torn down on the
            # re-compute steps.
            read_tries = 2
            while read_tries > 0:
                try:
                    # read last
                    array = shm.array
                    value = array[-1][fsp_func_name]
                    break

                except IndexError:
                    read_tries -= 1
                    continue

            if last_val_sticky:
                last_val_sticky.update_from_data(-1, value)

            # update graphics
            chart.update_curve_from_array(
                display_name,
                array,
                array_key=fsp_func_name,
            )

            # set time of last graphics update
            last = time.time()


async def check_for_new_bars(feed, ohlcv, linkedsplits):
    """Task which updates from new bars in the shared ohlcv buffer every
    ``delay_s`` seconds.

    """
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

            # XXX: this puts a flat bar on the current time step
            # TODO: if we eventually have an x-axis time-step "cursor"
            # we can get rid of this since it is extra overhead.
            price_chart.update_ohlc_from_array(
                price_chart.name,
                ohlcv.array,
                just_history=False,
            )

            for name in price_chart._overlays:

                price_chart.update_curve_from_array(
                    name,
                    price_chart._arrays[name]
                )

            for name, chart in linkedsplits.subplots.items():
                chart.update_curve_from_array(
                    chart.name,
                    chart._shm.array,
                    array_key=chart.data_key
                )

            # shift the view if in follow mode
            price_chart.increment_view()


def has_vlm(ohlcv: ShmArray) -> bool:
    # make sure that the instrument supports volume history
    # (sometimes this is not the case for some commodities and
    # derivatives)
    volm = ohlcv.array['volume']
    return not bool(np.all(np.isin(volm, -1)) or np.all(np.isnan(volm)))


@asynccontextmanager
async def maybe_open_vlm_display(

    linked: LinkedSplits,
    ohlcv: ShmArray,

) -> ChartPlotWidget:

    # make sure that the instrument supports volume history
    # (sometimes this is not the case for some commodities and
    # derivatives)
    # volm = ohlcv.array['volume']
    # if (
    #     np.all(np.isin(volm, -1)) or
    #     np.all(np.isnan(volm))
    # ):
    if not has_vlm(ohlcv):
        log.warning(f"{linked.symbol.key} does not seem to have volume info")
    else:
        async with open_sidepane(linked, 'volume') as sidepane:
            # built-in $vlm
            shm = ohlcv
            chart = linked.add_plot(
                name='vlm',
                array=shm.array,

                array_key='volume',
                sidepane=sidepane,

                # curve by default
                ohlc=False,

                # vertical bars
                # stepMode=True,
                # static_yrange=(0, 100),
            )

            # XXX: ONLY for sub-chart fsps, overlays have their
            # data looked up from the chart's internal array set.
            # TODO: we must get a data view api going STAT!!
            chart._shm = shm

            # should **not** be the same sub-chart widget
            assert chart.name != linked.chart.name

            # sticky only on sub-charts atm
            last_val_sticky = chart._ysticks[chart.name]

            # read from last calculated value
            value = shm.array['volume'][-1]

            last_val_sticky.update_from_data(-1, value)

            # size view to data once at outset
            chart._set_yrange()

            yield chart


async def display_symbol_data(

    godwidget: GodWidget,
    provider: str,
    sym: str,
    loglevel: str,

    order_mode_started: trio.Event,

) -> None:
    '''Spawn a real-time updated chart for ``symbol``.

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

    async with(
        open_feed(
            provider,
            [sym],
            loglevel=loglevel,

            # 60 FPS to limit context switches
            tick_throttle=_clear_throttle_rate,

        ) as feed,
    ):

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
        chart._set_yrange()

        # TODO: a data view api that makes this less shit
        chart._shm = ohlcv

        # TODO: eventually we'll support some kind of n-compose syntax
        fsp_conf = {
            'rsi': {
                'fsp_func_name': 'rsi',
                'period': 14,
                'chart_kwargs': {
                    'static_yrange': (0, 100),
                },
            },
            # # test for duplicate fsps on same chart
            # 'rsi2': {
            #     'fsp_func_name': 'rsi',
            #     'period': 14,
            #     'chart_kwargs': {
            #         'static_yrange': (0, 100),
            #     },
            # },

        }

        if has_vlm(ohlcv):
            # add VWAP to fsp config for downstream loading
            fsp_conf.update({
                'vwap': {
                    'fsp_func_name': 'vwap',
                    'overlay': True,
                    'anchor': 'session',
                },
            })

        async with (

            trio.open_nursery() as ln,
        ):
            # load initial fsp chain (otherwise known as "indicators")
            ln.start_soon(
                fan_out_spawn_fsp_daemons,
                linkedsplits,
                fsp_conf,
                sym,
                ohlcv,
                brokermod,
                loading_sym_key,
                loglevel,
            )

            # start graphics update loop(s)after receiving first live quote
            ln.start_soon(
                chart_from_quotes,
                chart,
                feed.stream,
                ohlcv,
                wap_in_history,
            )

            ln.start_soon(
                check_for_new_bars,
                feed,
                ohlcv,
                linkedsplits
            )

            async with (
                # XXX: this slipped in during a commits refacotr,
                # it's actually landing proper in #231
                # maybe_open_vlm_display(linkedsplits, ohlcv),

                open_order_mode(
                    feed,
                    chart,
                    symbol,
                    provider,
                    order_mode_started
                )
            ):
                await trio.sleep_forever()
