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
FSP UI and graphics components.

Financial signal processing cluster and real-time graphics management.

'''
from contextlib import asynccontextmanager as acm
from functools import partial
import inspect
from itertools import cycle
from typing import Optional, AsyncGenerator, Any

import numpy as np
from pydantic import create_model
import tractor
import pyqtgraph as pg
import trio
from trio_typing import TaskStatus

from ._axes import PriceAxis
from .._cacheables import maybe_open_context
from ..calc import humanize
from ..data._sharedmem import (
    ShmArray,
    _Token,
    try_read,
)
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
)
from ._forms import (
    FieldsForm,
    mk_form,
    open_form_input_handling,
)
from ..fsp._api import maybe_mk_fsp_shm, Fsp
from ..fsp import cascade
from ..fsp._volume import (
    tina_vwap,
    dolla_vlm,
    flow_rates,
)
from ..log import get_logger

log = get_logger(__name__)


def has_vlm(ohlcv: ShmArray) -> bool:
    # make sure that the instrument supports volume history
    # (sometimes this is not the case for some commodities and
    # derivatives)
    vlm = ohlcv.array['volume']
    return not bool(np.all(np.isin(vlm, -1)) or np.all(np.isnan(vlm)))


def update_fsp_chart(
    chart: ChartPlotWidget,
    shm: ShmArray,
    graphics_name: str,
    array_key: Optional[str],

) -> None:

    array = shm.array
    last_row = try_read(array)

    # guard against unreadable case
    if not last_row:
        log.warning(f'Read-race on shm array: {graphics_name}@{shm.token}')
        return

    # update graphics
    # NOTE: this does a length check internally which allows it
    # staying above the last row check below..
    chart.update_curve_from_array(
        graphics_name,
        array,
        array_key=array_key or graphics_name,
    )

    # XXX: re: ``array_key``: fsp func names must be unique meaning we
    # can't have duplicates of the underlying data even if multiple
    # sub-charts reference it under different 'named charts'.

    # read from last calculated value and update any label
    last_val_sticky = chart._ysticks.get(graphics_name)
    if last_val_sticky:
        # array = shm.array[array_key]
        # if len(array):
        #     value = array[-1]
        last = last_row[array_key]
        last_val_sticky.update_from_data(-1, last)


@acm
async def open_fsp_sidepane(
    linked: LinkedSplits,
    conf: dict[str, dict[str, str]],

) -> FieldsForm:

    schema = {}

    assert len(conf) == 1  # for now

    # add (single) selection widget
    for name, config in conf.items():
        schema[name] = {
                'label': '**fsp**:',
                'type': 'select',
                'default_value': [name],
            }

        # add parameters for selection "options"
        params = config.get('params', {})
        for name, config in params.items():

            default = config['default_value']
            kwargs = config.get('widget_kwargs', {})

            # add to ORM schema
            schema.update({
                name: {
                    'label': f'**{name}**:',
                    'type': 'edit',
                    'default_value': default,
                    'kwargs': kwargs,
                },
            })

    sidepane: FieldsForm = mk_form(
        parent=linked.godwidget,
        fields_schema=schema,
    )

    # https://pydantic-docs.helpmanual.io/usage/models/#dynamic-model-creation
    FspConfig = create_model(
        'FspConfig',
        name=name,
        **params,
    )
    sidepane.model = FspConfig()

    # just a logger for now until we get fsp configs up and running.
    async def settings_change(
        key: str,
        value: str

    ) -> bool:
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


@acm
async def open_fsp_actor_cluster(
    names: list[str] = ['fsp_0', 'fsp_1'],

) -> AsyncGenerator[int, dict[str, tractor.Portal]]:

    from tractor._clustering import open_actor_cluster

    # profiler = pg.debug.Profiler(
    #     delayed=False,
    #     disabled=False
    # )
    async with open_actor_cluster(
        count=2,
        names=names,
        modules=['piker.fsp._engine'],

    ) as cluster_map:
        # profiler('started fsp cluster')
        yield cluster_map


async def run_fsp_ui(

    linkedsplits: LinkedSplits,
    shm: ShmArray,
    started: trio.Event,
    target: Fsp,
    conf: dict[str, dict],
    loglevel: str,
    # profiler: pg.debug.Profiler,
    # _quote_throttle_rate: int = 58,

) -> None:
    '''
    Taskf for UI spawning around a ``LinkedSplits`` chart for fsp
    related graphics/UX management.

    This is normally spawned/called once for each entry in the fsp
    config.

    '''
    name = target.name
    # profiler(f'started UI task for fsp: {name}')

    async with (
        # side UI for parameters/controls
        open_fsp_sidepane(
            linkedsplits,
            {name: conf},
        ) as sidepane,
    ):
        await started.wait()
        # profiler(f'fsp:{name} attached to fsp ctx-stream')

        overlay_with = conf.get('overlay', False)
        if overlay_with:
            if overlay_with == 'ohlc':
                chart = linkedsplits.chart
            else:
                chart = linkedsplits.subplots[overlay_with]

            chart.draw_curve(
                name=name,
                data=shm.array,
                overlay=True,
                color='default_light',
                array_key=name,
                separate_axes=conf.get('separate_axes', False),
                **conf.get('chart_kwargs', {})
            )
            # specially store ref to shm for lookup in display loop
            chart._flows[name].shm = shm

        else:
            # create a new sub-chart widget for this fsp
            chart = linkedsplits.add_plot(
                name=name,
                array=shm.array,

                array_key=name,
                sidepane=sidepane,

                # curve by default
                ohlc=False,

                # settings passed down to ``ChartPlotWidget``
                **conf.get('chart_kwargs', {})
            )

            # XXX: ONLY for sub-chart fsps, overlays have their
            # data looked up from the chart's internal array set.
            # TODO: we must get a data view api going STAT!!
            chart._shm = shm

            # should **not** be the same sub-chart widget
            assert chart.name != linkedsplits.chart.name

        array_key = name

        # profiler(f'fsp:{name} chart created')

        # first UI update, usually from shm pushed history
        update_fsp_chart(
            chart,
            shm,
            name,
            array_key=array_key,
        )

        chart.linked.focus()

        # TODO: figure out if we can roll our own `FillToThreshold` to
        # get brush filled polygons for OS/OB conditions.
        # ``pg.FillBetweenItems`` seems to be one technique using
        # generic fills between curve types while ``PlotCurveItem`` has
        # logic inside ``.paint()`` for ``self.opts['fillLevel']`` which
        # might be the best solution?

        # graphics = chart.update_from_array(chart.name, array[name])
        # graphics.curve.setBrush(50, 50, 200, 100)
        # graphics.curve.setFillLevel(50)

        # if func_name == 'rsi':
        #     from ._lines import level_line
        #     # add moveable over-[sold/bought] lines
        #     # and labels only for the 70/30 lines
        #     level_line(chart, 20)
        #     level_line(chart, 30, orient_v='top')
        #     level_line(chart, 70, orient_v='bottom')
        #     level_line(chart, 80, orient_v='top')

        chart.view._set_yrange()
        # done()  # status updates

        # profiler(f'fsp:{func_name} starting update loop')
        # profiler.finish()

        # update chart graphics
        # last = time.time()

        # XXX: this currently doesn't loop since
        # the FSP engine does **not** push updates atm
        # since we do graphics update in the main loop
        # in ``._display.
        # async for value in stream:
        #     print(value)

        #     # chart isn't actively shown so just skip render cycle
        #     if chart.linked.isHidden():
        #         continue

        #     else:
        #         now = time.time()
        #         period = now - last

        #         if period <= 1/_quote_throttle_rate:
        #             # faster then display refresh rate
        #             print(f'fsp too fast: {1/period}')
        #             continue

        #         # run synchronous update
        #         update_fsp_chart(
        #             chart,
        #             shm,
        #             display_name,
        #             array_key=func_name,
        #         )

        #         # set time of last graphics update
        #         last = time.time()


class FspAdmin:
    '''
    Client API for orchestrating FSP actors and displaying
    real-time graphics output.

    '''
    def __init__(
        self,
        tn: trio.Nursery,
        cluster: dict[str, tractor.Portal],
        linked: LinkedSplits,
        src_shm: ShmArray,

    ) -> None:
        self.tn = tn
        self.cluster = cluster
        self.linked = linked
        self._rr_next_actor = cycle(cluster.items())
        self._registry: dict[
            tuple,
            tuple[tractor.MsgStream, ShmArray]
        ] = {}
        self._flow_registry: dict[_Token, str] = {}
        self.src_shm = src_shm

    def rr_next_portal(self) -> tractor.Portal:
        name, portal = next(self._rr_next_actor)
        return portal

    async def open_chain(
        self,

        portal: tractor.Portal,
        complete: trio.Event,
        started: trio.Event,
        fqsn: str,
        dst_shm: ShmArray,
        conf: dict,
        target: Fsp,
        loglevel: str,

    ) -> None:
        '''
        Task which opens a remote FSP endpoint in the managed
        cluster and sleeps until signalled to exit.

        '''
        ns_path = str(target.ns_path)
        async with (
            portal.open_context(

                # chaining entrypoint
                cascade,

                # data feed key
                fqsn=fqsn,

                # mems
                src_shm_token=self.src_shm.token,
                dst_shm_token=dst_shm.token,

                # target
                ns_path=ns_path,

                loglevel=loglevel,
                zero_on_step=conf.get('zero_on_step', False),
                shm_registry=[
                    (token.as_msg(), fsp_name, dst_token.as_msg())
                    for (token, fsp_name), dst_token
                    in self._flow_registry.items()
                ],

            ) as (ctx, last_index),
            ctx.open_stream() as stream,
        ):
            # register output data
            self._registry[
                (fqsn, ns_path)
            ] = (
                stream,
                dst_shm,
                complete
            )

            started.set()

            # wait for graceful shutdown signal
            async with stream.subscribe() as stream:
                async for msg in stream:
                    if msg == 'update':
                        # if the chart isn't hidden try to update
                        # the data on screen.
                        if not self.linked.isHidden():
                            log.info(f'Re-syncing graphics for fsp: {ns_path}')
                            self.linked.graphics_cycle(trigger_all=True)
                    else:
                        log.info(f'recved unexpected fsp engine msg: {msg}')

            await complete.wait()

    async def start_engine_task(
        self,

        target: Fsp,
        conf: dict[str, dict[str, Any]],

        worker_name: Optional[str] = None,
        loglevel: str = 'info',

    ) -> (ShmArray, trio.Event):

        fqsn = self.linked.symbol.front_fqsn()

        # allocate an output shm array
        key, dst_shm, opened = maybe_mk_fsp_shm(
            fqsn,
            target=target,
            readonly=True,
        )
        self._flow_registry[
            (self.src_shm._token, target.name)
        ] = dst_shm._token

        # if not opened:
        #     raise RuntimeError(
        #         f'Already started FSP `{fqsn}:{func_name}`'
        #     )

        portal = self.cluster.get(worker_name) or self.rr_next_portal()
        complete = trio.Event()
        started = trio.Event()
        self.tn.start_soon(
            self.open_chain,
            portal,
            complete,
            started,
            fqsn,
            dst_shm,
            conf,
            target,
            loglevel,
        )

        return dst_shm, started

    async def open_fsp_chart(
        self,

        target: Fsp,

        conf: dict,  # yeah probably dumb..
        loglevel: str = 'error',

    ) -> (trio.Event, ChartPlotWidget):

        shm, started = await self.start_engine_task(
            target,
            conf,
            loglevel,
        )

        # init async
        self.tn.start_soon(
            partial(
                run_fsp_ui,

                self.linked,
                shm,
                started,
                target,

                conf=conf,
                loglevel=loglevel,
            )
        )
        return started


@acm
async def open_fsp_admin(
    linked: LinkedSplits,
    src_shm: ShmArray,
    **kwargs,

) -> AsyncGenerator[dict, dict[str, tractor.Portal]]:

    async with (
        maybe_open_context(
            # for now make a cluster per client?
            acm_func=open_fsp_actor_cluster,
            kwargs=kwargs,
        ) as (cache_hit, cluster_map),

        trio.open_nursery() as tn,
    ):
        if cache_hit:
            log.info('re-using existing fsp cluster')

        admin = FspAdmin(
            tn,
            cluster_map,
            linked,
            src_shm,
        )
        try:
            yield admin
        finally:
            # terminate all tasks via signals
            for key, entry in admin._registry.items():
                _, _, event = entry
                event.set()


async def open_vlm_displays(

    linked: LinkedSplits,
    ohlcv: ShmArray,
    dvlm: bool = True,

    task_status: TaskStatus[ChartPlotWidget] = trio.TASK_STATUS_IGNORED,

) -> ChartPlotWidget:
    '''
    Volume subchart displays.

    Since "volume" is often included directly alongside OHLCV price
    data, we don't really need a separate FSP-actor + shm array for it
    since it's likely already directly adjacent to OHLC samples from the
    data provider.

    Further only if volume data is detected (it sometimes isn't provided
    eg. forex, certain commodities markets) will volume dependent FSPs
    be spawned here.

    '''
    sig = inspect.signature(flow_rates.func)
    params = sig.parameters

    async with (
        open_fsp_sidepane(
            linked, {
                'flows': {

                    # TODO: add support for dynamically changing these
                    'params': {
                        u'\u03BC' + '_type': {
                            'default_value': str(params['mean_type'].default),
                        },
                        'period': {
                            'default_value': str(params['period'].default),
                            # make widget un-editable for now.
                            'widget_kwargs': {'readonly': True},
                        },
                    },
                }
            },
        ) as sidepane,
        open_fsp_admin(linked, ohlcv) as admin,
    ):
        # TODO: support updates
        # period_field = sidepane.fields['period']
        # period_field.setText(
        #     str(period_param.default)
        # )

        # built-in vlm which we plot ASAP since it's
        # usually data provided directly with OHLC history.
        shm = ohlcv
        chart = linked.add_plot(
            name='volume',
            array=shm.array,

            array_key='volume',
            sidepane=sidepane,

            # curve by default
            ohlc=False,

            # Draw vertical bars from zero.
            # we do this internally ourselves since
            # the curve item internals are pretty convoluted.
            style='step',
        )

        # force 0 to always be in view
        def maxmin(
            names: list[str],

        ) -> tuple[float, float]:

            mx = 0
            for name in names:

                mxmn = chart.maxmin(name=name)
                if mxmn:
                    ymax = mxmn[1]
                    if ymax > mx:
                        mx = ymax

            return 0, mx

        chart.view.maxmin = partial(maxmin, names=['volume'])

        # TODO: fix the x-axis label issue where if you put
        # the axis on the left it's totally not lined up...
        # show volume units value on LHS (for dinkus)
        # chart.hideAxis('right')
        # chart.showAxis('left')

        # XXX: ONLY for sub-chart fsps, overlays have their
        # data looked up from the chart's internal array set.
        # TODO: we must get a data view api going STAT!!
        chart._shm = shm

        # send back new chart to caller
        task_status.started(chart)

        # should **not** be the same sub-chart widget
        assert chart.name != linked.chart.name

        # sticky only on sub-charts atm
        last_val_sticky = chart._ysticks[chart.name]

        # read from last calculated value
        value = shm.array['volume'][-1]

        last_val_sticky.update_from_data(-1, value)

        vlm_curve = chart.update_curve_from_array(
            'volume',
            shm.array,
        )

        # size view to data once at outset
        chart.view._set_yrange()

        # add axis title
        axis = chart.getAxis('right')
        axis.set_title(' vlm')

        if dvlm:

            tasks_ready = []
            # spawn and overlay $ vlm on the same subchart
            dvlm_shm, started = await admin.start_engine_task(
                dolla_vlm,

                {  # fsp engine conf
                    'func_name': 'dolla_vlm',
                    'zero_on_step': True,
                    'params': {
                        'price_func': {
                            'default_value': 'chl3',
                        },
                    },
                },
                # loglevel,
            )
            tasks_ready.append(started)

            # FIXME: we should error on starting the same fsp right
            # since it might collide with existing shm.. or wait we
            # had this before??
            # dolla_vlm,

            tasks_ready.append(started)
            # profiler(f'created shm for fsp actor: {display_name}')

            # wait for all engine tasks to startup
            async with trio.open_nursery() as n:
                for event in tasks_ready:
                    n.start_soon(event.wait)

            # dolla vlm overlay
            # XXX: the main chart already contains a vlm "units" axis
            # so here we add an overlay wth a y-range in
            # $ liquidity-value units (normally a fiat like USD).
            dvlm_pi = chart.overlay_plotitem(
                'dolla_vlm',
                index=0,  # place axis on inside (nearest to chart)
                axis_title=' $vlm',
                axis_side='right',
                axis_kwargs={
                    'typical_max_str': ' 100.0 M ',
                    'formatter': partial(
                        humanize,
                        digits=2,
                    ),
                },
            )

            # all to be overlayed curve names
            fields = [
               'dolla_vlm',
               'dark_vlm',
            ]
            dvlm_rate_fields = [
                'dvlm_rate',
                'dark_dvlm_rate',
            ]
            trade_rate_fields = [
                'trade_rate',
                'dark_trade_rate',
            ]

            # add custom auto range handler
            dvlm_pi.vb._maxmin = partial(
                maxmin,
                # keep both regular and dark vlm in view
                names=fields + dvlm_rate_fields,
            )

            # TODO: is there a way to "sync" the dual axes such that only
            # one curve is needed?
            # hide the original vlm curve since the $vlm one is now
            # displayed and the curves are effectively the same minus
            # liquidity events (well at least on low OHLC periods - 1s).
            vlm_curve.hide()

            # use slightly less light (then bracket) gray
            # for volume from "main exchange" and a more "bluey"
            # gray for "dark" vlm.
            vlm_color = 'i3'
            dark_vlm_color = 'charcoal'

            # add dvlm (step) curves to common view
            def chart_curves(
                names: list[str],
                pi: pg.PlotItem,
                shm: ShmArray,
                step_mode: bool = False,
                style: str = 'solid',

            ) -> None:
                for name in names:
                    if 'dark' in name:
                        color = dark_vlm_color
                    elif 'rate' in name:
                        color = vlm_color
                    else:
                        color = 'bracket'

                    curve, _ = chart.draw_curve(
                        # name='dolla_vlm',
                        name=name,
                        data=shm.array,
                        array_key=name,
                        overlay=pi,
                        color=color,
                        step_mode=step_mode,
                        style=style,
                    )

                    # TODO: we need a better API to do this..
                    # specially store ref to shm for lookup in display loop
                    # since only a placeholder of `None` is entered in
                    # ``.draw_curve()``.
                    chart._flows[name].shm = shm

            chart_curves(
                fields,
                dvlm_pi,
                dvlm_shm,
                step_mode=True,
            )

            # spawn flow rates fsp **ONLY AFTER** the 'dolla_vlm' fsp is
            # up since this one depends on it.

            fr_shm, started = await admin.start_engine_task(
                flow_rates,
                {  # fsp engine conf
                    'func_name': 'flow_rates',
                    'zero_on_step': False,
                },
                # loglevel,
            )
            await started.wait()

            chart_curves(
                dvlm_rate_fields,
                dvlm_pi,
                fr_shm,
            )

            # Trade rate overlay
            # XXX: requires an additional overlay for
            # a trades-per-period (time) y-range.
            tr_pi = chart.overlay_plotitem(
                'trade_rates',

                # TODO: dynamically update period (and thus this axis?)
                # title from user input.
                axis_title='clears',

                axis_side='left',
                axis_kwargs={
                    'typical_max_str': ' 10.0 M ',
                    'formatter': partial(
                        humanize,
                        digits=2,
                    ),
                    'text_color': vlm_color,
                },

            )
            # add custom auto range handler
            tr_pi.vb.maxmin = partial(
                maxmin,
                # keep both regular and dark vlm in view
                names=trade_rate_fields,
            )

            chart_curves(
                trade_rate_fields,
                tr_pi,
                fr_shm,
                # step_mode=True,

                # dashed line to represent "individual trades" being
                # more "granular" B)
                style='dash',
            )

            for pi in (dvlm_pi, tr_pi):
                for name, axis_info in pi.axes.items():
                    # lol this sux XD
                    axis = axis_info['item']
                    if isinstance(axis, PriceAxis):
                        axis.size_to_values()

        # built-in vlm fsps
        for target, conf in {
            tina_vwap: {
                'overlay': 'ohlc',  # overlays with OHLCV (main) chart
                'anchor': 'session',
            },
        }.items():
            started = await admin.open_fsp_chart(
                target,
                conf,
            )


async def start_fsp_displays(

    linked: LinkedSplits,
    ohlcv: ShmArray,
    group_status_key: str,
    loglevel: str,

) -> None:
    '''
    Create fsp charts from a config input attached to a local actor
    compute cluster.

    Pass target entrypoint and historical data via ``ShmArray``.

    '''
    linked.focus()

    # TODO: eventually we'll support some kind of n-compose syntax
    fsp_conf = {
        # 'rsi': {
        #     'func_name': 'rsi',  # literal python func ref lookup name

        #     # map of parameters to place on the fsp sidepane widget
        #     # which should map to dynamic inputs available to the
        #     # fsp function at runtime.
        #     'params': {
        #         'period': {
        #             'default_value': 14,
        #             'widget_kwargs': {'readonly': True},
        #         },
        #     },

        #     # ``ChartPlotWidget`` options passthrough
        #     'chart_kwargs': {
        #         'static_yrange': (0, 100),
        #     },
        # },
    }
    profiler = pg.debug.Profiler(
        delayed=False,
        disabled=False
    )

    async with (

        # NOTE: this admin internally opens an actor cluster
        open_fsp_admin(linked, ohlcv) as admin,
    ):
        statuses = []
        for target, conf in fsp_conf.items():
            started = await admin.open_fsp_chart(
                target,
                conf,
            )
            done = linked.window().status_bar.open_status(
                f'loading fsp, {target}..',
                group_key=group_status_key,
            )
            statuses.append((started, done))

        for fsp_loaded, status_cb in statuses:
            await fsp_loaded.wait()
            profiler(f'attached to fsp portal: {target}')
            status_cb()

    # blocks on nursery until all fsp actors complete
