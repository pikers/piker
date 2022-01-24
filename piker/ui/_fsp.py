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
from itertools import cycle
from typing import Optional, AsyncGenerator, Any

import numpy as np
from pydantic import create_model
import tractor
# from tractor.trionics import gather_contexts
import pyqtgraph as pg
import trio
from trio_typing import TaskStatus

from .._cacheables import maybe_open_context
from ..data._sharedmem import (
    ShmArray,
    maybe_open_shm_array,
    try_read,
)
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
)
from .. import fsp
from ._forms import (
    FieldsForm,
    mk_form,
    open_form_input_handling,
)
from ..log import get_logger

log = get_logger(__name__)


def maybe_mk_fsp_shm(
    sym: str,
    field_name: str,
    display_name: Optional[str] = None,
    readonly: bool = True,

) -> (ShmArray, bool):
    '''
    Allocate a single row shm array for an symbol-fsp pair if none
    exists, otherwise load the shm already existing for that token.

    '''
    uid = tractor.current_actor().uid
    if not display_name:
        display_name = field_name

    # TODO: load function here and introspect
    # return stream type(s)

    # TODO: should `index` be a required internal field?
    fsp_dtype = np.dtype([('index', int), (field_name, float)])

    key = f'{sym}.fsp.{display_name}.{".".join(uid)}'

    shm, opened = maybe_open_shm_array(
        key,
        # TODO: create entry for each time frame
        dtype=fsp_dtype,
        readonly=True,
    )
    return shm, opened


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
    for display_name, config in conf.items():
        schema[display_name] = {
                'label': '**fsp**:',
                'type': 'select',
                'default_value': [display_name],
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
        name=display_name,
        **params,
    )
    sidepane.model = FspConfig()

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
    func_name: str,
    display_name: str,
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
    # profiler(f'started UI task for fsp: {func_name}')

    async with (
        # side UI for parameters/controls
        open_fsp_sidepane(
            linkedsplits,
            {display_name: conf},
        ) as sidepane,
    ):
        await started.wait()
        # profiler(f'fsp:{func_name} attached to fsp ctx-stream')

        overlay_with = conf.get('overlay', False)
        if overlay_with:
            if overlay_with == 'ohlc':
                chart = linkedsplits.chart
            else:
                chart = linkedsplits.subplots[overlay_with]

            chart.draw_curve(
                name=display_name,
                data=shm.array,
                overlay=True,
                color='default_light',
                array_key=func_name,
                separate_axes=conf.get('separate_axes', False),
                **conf.get('chart_kwargs', {})
            )
            # specially store ref to shm for lookup in display loop
            chart._overlays[display_name] = shm

        else:
            # create a new sub-chart widget for this fsp
            chart = linkedsplits.add_plot(
                name=display_name,
                array=shm.array,

                array_key=func_name,
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

        array_key = func_name

        # profiler(f'fsp:{func_name} chart created')

        # first UI update, usually from shm pushed history
        update_fsp_chart(
            chart,
            shm,
            display_name,
            array_key=array_key,
        )

        chart.linked.focus()

        # TODO: figure out if we can roll our own `FillToThreshold` to
        # get brush filled polygons for OS/OB conditions.
        # ``pg.FillBetweenItems`` seems to be one technique using
        # generic fills between curve types while ``PlotCurveItem`` has
        # logic inside ``.paint()`` for ``self.opts['fillLevel']`` which
        # might be the best solution?

        # graphics = chart.update_from_array(chart.name, array[func_name])
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
        self.src_shm = src_shm

    def rr_next_portal(self) -> tractor.Portal:
        name, portal = next(self._rr_next_actor)
        return portal

    async def open_chain(
        self,

        portal: tractor.Portal,
        complete: trio.Event,
        started: trio.Event,
        dst_shm: ShmArray,
        conf: dict,
        func_name: str,
        loglevel: str,

    ) -> None:
        '''
        Task which opens a remote FSP endpoint in the managed
        cluster and sleeps until signalled to exit.

        '''
        brokername, sym = self.linked.symbol.front_feed()
        async with (
            portal.open_context(

                # chaining entrypoint
                fsp.cascade,

                # data feed key
                brokername=brokername,
                symbol=sym,

                # mems
                src_shm_token=self.src_shm.token,
                dst_shm_token=dst_shm.token,

                # target
                func_name=func_name,

                loglevel=loglevel,
                zero_on_step=conf.get('zero_on_step', False),

            ) as (ctx, last_index),
            ctx.open_stream() as stream,
        ):
            # register output data
            self._registry[(brokername, sym, func_name)] = (
                stream, dst_shm, complete)

            started.set()

            # wait for graceful shutdown signal
            await complete.wait()

    async def start_engine_task(
        self,

        display_name: str,
        conf: dict[str, dict[str, Any]],

        worker_name: Optional[str] = None,
        loglevel: str = 'error',

    ) -> (ShmArray, trio.Event):

        # unpack FSP details from config dict
        func_name = conf['func_name']

        # allocate an output shm array
        dst_shm, opened = maybe_mk_fsp_shm(
            self.linked.symbol.front_feed(),
            field_name=func_name,
            display_name=display_name,
            readonly=True,
        )
        if not opened:
            raise RuntimeError(f'Already started FSP {func_name}')

        portal = self.cluster.get(worker_name) or self.rr_next_portal()
        complete = trio.Event()
        started = trio.Event()
        self.tn.start_soon(
            self.open_chain,

            portal,
            complete,
            started,
            dst_shm,
            conf,
            func_name,
            loglevel,
        )

        return dst_shm, started

    async def open_fsp_chart(
        self,
        display_name: str,
        conf: dict,  # yeah probably dumb..
        loglevel: str = 'error',

    ) -> (trio.Event, ChartPlotWidget):

        func_name = conf['func_name']

        shm, started = await self.start_engine_task(
            display_name,
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
                func_name,
                display_name,

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
    async with (
        open_fsp_sidepane(
            linked, {
                'vlm': {
                    'params': {
                        'price_func': {
                            'default_value': 'chl3',
                            # tell target ``Edit`` widget to not allow
                            # edits for now.
                            'widget_kwargs': {'readonly': True},
                        },
                    },
                }
            },
        ) as sidepane,
        open_fsp_admin(linked, ohlcv) as admin,
    ):
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
        def maxmin(name) -> tuple[float, float]:
            mxmn = chart.maxmin(name=name)
            if mxmn:
                return 0, mxmn[1]

            return 0, 0

        chart.view._maxmin = partial(maxmin, name='volume')

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

        chart.update_curve_from_array(
            'volume',
            shm.array,
        )

        # size view to data once at outset
        chart.view._set_yrange()

        if not dvlm:
            return

        # spawn and overlay $ vlm on the same subchart
        shm, started = await admin.start_engine_task(
            'dolla_vlm',
            # linked.symbol.front_feed(),  # data-feed symbol key
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
        # profiler(f'created shm for fsp actor: {display_name}')

        await started.wait()

        pi = chart.overlay_plotitem(
            'dolla_vlm',
        )
        # add custom auto range handler
        pi.vb._maxmin = partial(maxmin, name='dolla_vlm')

        curve, _ = chart.draw_curve(

            name='dolla_vlm',
            data=shm.array,

            array_key='dolla_vlm',
            overlay=pi,
            color='charcoal',
            step_mode=True,
            # **conf.get('chart_kwargs', {})
        )
        # TODO: is there a way to "sync" the dual axes such that only
        # one curve is needed?
        # curve.hide()

        # TODO: we need a better API to do this..
        # specially store ref to shm for lookup in display loop
        # since only a placeholder of `None` is entered in
        # ``.draw_curve()``.
        chart._overlays['dolla_vlm'] = shm

        # XXX: old dict-style config before it was moved into the helper task
        #     'dolla_vlm': {
        #         'func_name': 'dolla_vlm',
        #         'zero_on_step': True,
        #         'overlay': 'volume',
        #         'separate_axes': True,
        #         'params': {
        #             'price_func': {
        #                 'default_value': 'chl3',
        #                 # tell target ``Edit`` widget to not allow
        #                 # edits for now.
        #                 'widget_kwargs': {'readonly': True},
        #             },
        #         },
        #         'chart_kwargs': {'step_mode': True}
        #     },

        # }

        # built-in vlm fsps
        for display_name, conf in {
            'vwap': {
                'func_name': 'vwap',
                'overlay': 'ohlc',  # overlays with OHLCV (main) chart
                'anchor': 'session',
            },
        }.items():
            started = await admin.open_fsp_chart(
                display_name,
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

    # async with gather_contexts((
    async with (

        # NOTE: this admin internally opens an actor cluster
        open_fsp_admin(linked, ohlcv) as admin,
    ):
        statuses = []
        for display_name, conf in fsp_conf.items():
            started = await admin.open_fsp_chart(
                display_name,
                conf,
            )
            done = linked.window().status_bar.open_status(
                f'loading fsp, {display_name}..',
                group_key=group_status_key,
            )
            statuses.append((started, done))

        for fsp_loaded, status_cb in statuses:
            await fsp_loaded.wait()
            profiler(f'attached to fsp portal: {display_name}')
            status_cb()

    # blocks on nursery until all fsp actors complete
