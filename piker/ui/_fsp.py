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
from types import ModuleType
from typing import Optional, AsyncGenerator, Any

import numpy as np
from pydantic import create_model
import tractor
from tractor.trionics import gather_contexts
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
    volm = ohlcv.array['volume']
    return not bool(np.all(np.isin(volm, -1)) or np.all(np.isnan(volm)))


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

    profiler = pg.debug.Profiler(
        delayed=False,
        disabled=False
    )
    async with open_actor_cluster(
        count=2,
        names=names,
        modules=['piker.fsp._engine'],

    ) as cluster_map:
        profiler('started fsp cluster')
        yield cluster_map


class FspAdmin:
    '''
    Client API for orchestrating FSP actors and displaying
    real-time graphics output.

    '''
    def __init__(
        self,
        tn: trio.Nursery,
        cluster: dict[str, tractor.Portal],

    ) -> None:
        self.tn = tn
        self.cluster = cluster
        self._rr_next_actor = cycle(cluster.items())
        self._registry: dict[
            tuple,
            tuple[tractor.MsgStream, ShmArray]
        ] = {}

    def rr_next_portal(self) -> tractor.Portal:
        name, portal = next(self._rr_next_actor)
        return portal

    async def open_remote_fsp(
        self,

        portal: tractor.Portal,
        complete: trio.Event,
        started: trio.Event,

        brokername: str,
        sym: str,

        src_shm: ShmArray,
        dst_shm: ShmArray,

        conf: dict,
        func_name: str,
        loglevel: str,

    ) -> None:
        '''
        Task which opens a remote FSP endpoint in the managed
        cluster and sleeps until signalled to exit.

        '''
        async with (
            portal.open_context(

                # chaining entrypoint
                fsp.cascade,

                # data feed key
                brokername=brokername,
                symbol=sym,

                # mems
                src_shm_token=src_shm.token,
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

    async def start_fsp(
        self,

        display_name: str,
        feed_key: tuple[str, str],
        src_shm: ShmArray,
        conf: dict[str, dict[str, Any]],

        worker_name: Optional[str] = None,
        loglevel: str = 'error',

    ) -> (ShmArray, trio.Event):

        # unpack FSP details from config dict
        func_name = conf['func_name']

        # allocate an output shm array
        dst_shm, opened = maybe_mk_fsp_shm(
            feed_key,
            field_name=func_name,
            display_name=display_name,
            readonly=True,
        )
        if not opened:
            raise RuntimeError("Already started FSP {func_name}")

        portal = self.cluster.get(worker_name) or self.rr_next_portal()
        complete = trio.Event()
        started = trio.Event()

        brokername, sym = feed_key
        self.tn.start_soon(
            self.open_remote_fsp,
            portal,
            complete,
            started,

            brokername,
            sym,

            src_shm,
            dst_shm,

            conf,
            func_name,
            loglevel,
        )

        return dst_shm, started


@acm
async def open_fsp_admin(
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

        admin = FspAdmin(tn, cluster_map)
        try:
            yield admin
        finally:
            # terminate all tasks via signals
            for key, entry in admin._registry.items():
                _, _, event = entry
                event.set()


@acm
async def maybe_open_vlm_display(
    linked: LinkedSplits,
    ohlcv: ShmArray,

) -> ChartPlotWidget:
    '''
    Volume subchart helper.

    Since "volume" is often included directly alongside OHLCV price
    data, we don't really need a separate FSP-actor + shm array for it
    since it's likely already directly adjacent to OHLC samples from the
    data provider.

    '''
    if not has_vlm(ohlcv):
        log.warning(f"{linked.symbol.key} does not seem to have volume info")
        yield
        return

    async with open_fsp_sidepane(
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
    ) as sidepane:

        # built-in $vlm
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

        # show volume units value on LHS (for dinkus)
        chart.hideAxis('right')
        chart.showAxis('left')

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

        chart.update_curve_from_array(
            'volume',
            shm.array,
        )

        # size view to data once at outset
        chart.view._set_yrange()

        yield chart


async def run_fsp_ui(

    shm: ShmArray,
    started: trio.Event,
    linkedsplits: LinkedSplits,
    func_name: str,
    display_name: str,
    conf: dict[str, dict],
    group_status_key: str,
    loglevel: str,
    profiler: pg.debug.Profiler,

    _quote_throttle_rate: int = 58,

) -> None:
    '''
    FSP stream chart update loop.

    This is called once for each entry in the fsp
    config map.

    '''
    profiler(f'started UI task for fsp: {func_name}')

    async with (
        # side UI for parameters/controls
        open_fsp_sidepane(
            linkedsplits,
            {display_name: conf},
        ) as sidepane,
    ):
        await started.wait()
        profiler(f'fsp:{func_name} attached to fsp ctx-stream')

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
            chart = linkedsplits.add_plot(
                name=display_name,
                array=shm.array,

                array_key=func_name,
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

        array_key = func_name

        profiler(f'fsp:{func_name} chart created')

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

        if func_name == 'rsi':
            from ._lines import level_line
            # add moveable over-[sold/bought] lines
            # and labels only for the 70/30 lines
            level_line(chart, 20)
            level_line(chart, 30, orient_v='top')
            level_line(chart, 70, orient_v='bottom')
            level_line(chart, 80, orient_v='top')

        chart.view._set_yrange()
        # done()  # status updates

        profiler(f'fsp:{func_name} starting update loop')
        profiler.finish()

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


async def start_fsp_displays(

    linkedsplits: LinkedSplits,
    brokermod: ModuleType,
    sym: str,
    ohlcv: ShmArray,
    group_status_key: str,
    loglevel: str,

    task_status: TaskStatus[
        tuple[FspAdmin, 'ChartPlotWidet']
    ] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Create sub-actors (under flat tree)
    for each entry in config and attach to local graphics update tasks.

    Pass target entrypoint and historical data.

    '''
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

    if has_vlm(ohlcv):
        fsp_conf.update({
            'vwap': {
                'func_name': 'vwap',
                'overlay': 'ohlc',
                'anchor': 'session',
            },

            'dolla_vlm': {
                'func_name': 'dolla_vlm',
                'zero_on_step': True,
                'overlay': 'volume',
                'separate_axes': True,
                'params': {
                    'price_func': {
                        'default_value': 'chl3',
                        # tell target ``Edit`` widget to not allow
                        # edits for now.
                        'widget_kwargs': {'readonly': True},
                    },
                },
                'chart_kwargs': {'step_mode': True}
            },

        })

    linkedsplits.focus()

    profiler = pg.debug.Profiler(
        delayed=False,
        disabled=False
    )

    async with gather_contexts((

        # NOTE: this admin internally opens an actor pool.
        open_fsp_admin(),

        trio.open_nursery(),

        maybe_open_vlm_display(
            linkedsplits,
            ohlcv,
        ),

    )) as (admin, n, vlm_chart):

        task_status.started((admin, vlm_chart))

        for display_name, conf in fsp_conf.items():
            func_name = conf['func_name']

            done = linkedsplits.window().status_bar.open_status(
                f'loading fsp, {display_name}..',
                group_key=group_status_key,
            )

            shm, started = await admin.start_fsp(
                display_name,
                (brokermod.name, sym),
                ohlcv,
                fsp_conf[display_name],
                loglevel,
            )

            profiler(f'created shm for fsp actor: {display_name}')

            # XXX: fsp may have been opened by a duplicate chart.
            # Error for now until we figure out how to wrap fsps as
            # "feeds".  assert opened, f"A chart for {key} likely
            # already exists?"

            profiler(f'attached to fsp portal: {display_name}')

            # init async
            n.start_soon(
                partial(
                    run_fsp_ui,

                    shm,
                    started,
                    linkedsplits,
                    func_name,
                    display_name,

                    conf=conf,
                    group_status_key=group_status_key,
                    loglevel=loglevel,
                    profiler=profiler,
                )
            )
            await started.wait()
            done()

    # blocks on nursery until all fsp actors complete
