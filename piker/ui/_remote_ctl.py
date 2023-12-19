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
Remote control tasks for sending annotations (and maybe more cmds)
to a chart from some other actor.

'''
from __future__ import annotations
from contextlib import asynccontextmanager as acm

import tractor
# import trio
from PyQt5.QtWidgets import (
    QGraphicsItem,
)

from piker.log import get_logger
from ._display import DisplayState
from ._editors import SelectRect
from ._chart import ChartPlotWidget


log = get_logger(__name__)

# NOTE: this is set by the `._display.graphics_update_loop()` once
# all chart widgets / Viz per flume have been initialized allowing
# for remote annotation (control) of any chart-actor's mkt feed by
# fqme lookup Bo
_dss: dict[str, DisplayState] | None = None

# stash each and every client connection so that they can all
# be cancelled on shutdown/error.
_ctxs: set[tractor.Context] = set()

# global map of all uniquely created annotation-graphics
# so that they can be mutated (eventually) by a client.
_annots: dict[int, QGraphicsItem] = {}


@tractor.context
async def remote_annotate(
    ctx: tractor.Context,
) -> None:

    global _dss, _ctxs
    assert _dss

    _ctxs.add(ctx)

    # send back full fqme symbology to caller
    await ctx.started(list(_dss))

    async with ctx.open_stream() as annot_req_stream:
        async for msg in annot_req_stream:
            match msg:
                case {
                    'fqme': fqme,
                    'cmd': 'SelectRect',
                    'color': color,
                    'timeframe': timeframe,
                    # 'meth': str(meth),
                    'meth': 'set_view_pos',
                    'kwargs': {
                        'start_pos': tuple(start_pos),
                        'end_pos': tuple(end_pos),
                    },
                }:
                    ds: DisplayState = _dss[fqme]
                    chart: ChartPlotWidget = {
                        60: ds.hist_chart,
                        1: ds.chart,
                    }[timeframe]

                    # annot type lookup from cmd
                    rect = SelectRect(
                        chart.cv,

                        # TODO: pull from conf.toml?
                        color=color or 'dad_blue',
                    )
                    rect.set_view_pos(
                        start_pos=start_pos,
                        end_pos=end_pos,
                    )
                    await annot_req_stream.send(id(rect))

                case _:
                    log.error(
                        'Unknown remote annotation cmd:\n'
                        f'{msg}'
                    )


@acm
async def open_annots_client(
    uid: tuple[str, str],

) -> 'AnnotClient':
    # TODO: load connetion to a specific chart actor
    # -[ ] pull from either service scan or config
    # -[ ] return some kinda client/proxy thinger?
    #    -[ ] maybe we should finally just provide this as
    #       a `tractor.hilevel.CallableProxy` or wtv?
    # -[ ] use this from the storage.cli stuff to mark up gaps!
    ...
