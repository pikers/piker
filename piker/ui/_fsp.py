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
from typing import Optional

import numpy as np
from pydantic import create_model
import tractor

from ..data._sharedmem import (
    ShmArray,
    maybe_open_shm_array,
    try_read,
)
from ._chart import (
    ChartPlotWidget,
    LinkedSplits,
    # GodWidget,
)
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


@acm
async def maybe_open_vlm_display(
    linked: LinkedSplits,
    ohlcv: ShmArray,

) -> ChartPlotWidget:

    if not has_vlm(ohlcv):
        log.warning(f"{linked.symbol.key} does not seem to have volume info")
        yield
        return
    else:

        shm, opened = maybe_mk_fsp_shm(
            linked.symbol.key,
            'vlm',
            readonly=True,
        )

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
            chart._set_yrange()

            yield chart


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
    chart._set_yrange()

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
