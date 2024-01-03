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
from contextlib import (
    asynccontextmanager as acm,
    AsyncExitStack,
)
from functools import partial
from pprint import pformat
from typing import (
    # Any,
    AsyncContextManager,
)

import tractor
from tractor import trionics
from tractor import (
    Portal,
    Context,
    MsgStream,
)
from PyQt5.QtWidgets import (
    QGraphicsItem,
)

from piker.log import get_logger
from piker.types import Struct
from piker.service import find_service
from piker.brokers import SymbolNotFound
from ._display import DisplayState
from ._interaction import ChartView
from ._editors import SelectRect
from ._chart import ChartPlotWidget
from ._dataviz import Viz


log = get_logger(__name__)

# NOTE: this is UPDATED by the `._display.graphics_update_loop()`
# once all chart widgets / Viz per flume have been initialized
# allowing for remote annotation (control) of any chart-actor's mkt
# feed by fqme lookup Bo
_dss: dict[str, DisplayState] = {}

# stash each and every client connection so that they can all
# be cancelled on shutdown/error.
# TODO: make `tractor.Context` hashable via is `.cid: str`?
# _ctxs: set[Context] = set()
# TODO: use type statements from 3.12+
IpcCtxTable = dict[
    str,  # each `Context.cid`
    tuple[
        Context,  # handle for ctx-cancellation
        set[int]  # set of annotation (instance) ids
    ]
]

_ctxs: IpcCtxTable = {}

# XXX: global map of all uniquely created annotation-graphics so
# that they can be mutated (eventually) by a client.
# NOTE: this map is only populated on the `chart` actor side (aka
# the "annotations server" which actually renders to a Qt canvas).
# type AnnotsTable = dict[int, QGraphicsItem]
AnnotsTable = dict[int, QGraphicsItem]

_annots: AnnotsTable  = {}


async def serve_rc_annots(
    ipc_key: str,
    annot_req_stream: MsgStream,
    dss: dict[str, DisplayState],
    ctxs: IpcCtxTable,
    annots: AnnotsTable,

) -> None:
    async for msg in annot_req_stream:
        match msg:
            case {
                'cmd': 'SelectRect',
                'fqme': fqme,
                'timeframe': timeframe,
                'meth': str(meth),
                'kwargs': dict(kwargs),
            }:

                ds: DisplayState = _dss[fqme]
                chart: ChartPlotWidget = {
                    60: ds.hist_chart,
                    1: ds.chart,
                }[timeframe]
                cv: ChartView = chart.cv

                # annot type lookup from cmd
                rect = SelectRect(
                    viewbox=cv,

                    # TODO: make this more dynamic?
                    # -[ ] pull from conf.toml?
                    # -[ ] add `.set_color()` method to type?
                    # -[ ] make a green/red based on direction
                    #    instead of default static color?
                    color=kwargs.pop('color', None),
                )
                # XXX NOTE: this is REQUIRED to set the rect
                # resize callback!
                rect.chart: ChartPlotWidget = chart

                # delegate generically to the requested method
                getattr(rect, meth)(**kwargs)
                rect.show()
                aid: int = id(rect)
                annots[aid] = rect
                aids: set[int] = ctxs[ipc_key][1]
                aids.add(aid)
                await annot_req_stream.send(aid)

            case {
                'cmd': 'remove',
                'aid': int(aid),
            }:
                # NOTE: this is normally entered on
                # a client's annotation de-alloc normally
                # prior to detach or modify.
                annot: QGraphicsItem = annots[aid]
                annot.delete()

                # respond to client indicating annot
                # was indeed deleted.
                await annot_req_stream.send(aid)

            case {
                'cmd': 'redraw',
                'fqme': fqme,
                'timeframe': timeframe,

                # TODO: maybe more fields?
                # 'render': int(aid),
                # 'viz_name': str(viz_name),
            }:
            # NOTE: old match from the 60s display loop task
            # | {
            #     'backfilling': (str(viz_name), timeframe),
            # }:
                ds: DisplayState = _dss[fqme]
                viz: Viz = {
                    60: ds.hist_viz,
                    1: ds.viz,
                }[timeframe]
                log.warning(
                    f'Forcing VIZ REDRAW:\n'
                    f'fqme: {fqme}\n'
                    f'timeframe: {timeframe}\n'
                )
                viz.reset_graphics()

            case _:
                log.error(
                    'Unknown remote annotation cmd:\n'
                    f'{pformat(msg)}'
                )


@tractor.context
async def remote_annotate(
    ctx: Context,
) -> None:

    global _dss, _ctxs
    assert _dss

    _ctxs[ctx.cid] = (ctx, set())

    # send back full fqme symbology to caller
    await ctx.started(list(_dss))

    # open annot request handler stream
    async with ctx.open_stream() as annot_req_stream:
        try:
            await serve_rc_annots(
                ipc_key=ctx.cid,
                annot_req_stream=annot_req_stream,
                dss=_dss,
                ctxs=_ctxs,
                annots=_annots,
            )
        finally:
            # ensure all annots for this connection are deleted
            # on any final teardown
            (_ctx, aids) = _ctxs[ctx.cid]
            assert _ctx is ctx
            for aid in aids:
                annot: QGraphicsItem = _annots[aid]
                annot.delete()


class AnnotCtl(Struct):
    '''
    A control for remote "data annotations".

    You know those "squares they always show in machine vision
    UIs.." this API allows you to remotely control stuff like that
    in some other graphics actor.

    '''
    ctx2fqmes: dict[str, str]
    fqme2ipc: dict[str, MsgStream]
    _annot_stack: AsyncExitStack

    # runtime-populated mapping of all annotation
    # ids to their equivalent IPC msg-streams.
    _ipcs: dict[int, MsgStream] = {}

    def _get_ipc(
        self,
        fqme: str,
    ) -> MsgStream:
        ipc: MsgStream = self.fqme2ipc.get(fqme)
        if ipc is None:
            raise SymbolNotFound(
                'No chart (actor) seems to have mkt feed loaded?\n'
                f'{fqme}'
            )
        return ipc

    async def add_rect(
        self,
        fqme: str,
        timeframe: float,
        start_pos: tuple[float, float],
        end_pos: tuple[float, float],

        # TODO: a `Literal['view', 'scene']` for this?
        domain: str = 'view',  # or 'scene'
        color: str = 'dad_blue',

        from_acm: bool = False,

    ) -> int:
        '''
        Add a `SelectRect` annotation to the target view, return
        the instances `id(obj)` from the remote UI actor.

        '''
        ipc: MsgStream = self._get_ipc(fqme)
        await ipc.send({
            'fqme': fqme,
            'cmd': 'SelectRect',
            'timeframe': timeframe,
            # 'meth': str(meth),
            'meth': 'set_view_pos' if domain == 'view' else 'set_scene_pos',
            'kwargs': {
                'start_pos': tuple(start_pos),
                'end_pos': tuple(end_pos),
                'color': color,
                'update_label': False,
            },
        })
        aid: int = await ipc.receive()
        self._ipcs[aid] = ipc
        if not from_acm:
            self._annot_stack.push_async_callback(
                partial(
                    self.remove,
                    aid,
                )
            )
        return aid

    async def remove(
        self,
        aid: int,

    ) -> bool:
        '''
        Remove an existing annotation by instance id.

        '''
        ipc: MsgStream = self._ipcs[aid]
        await ipc.send({
            'cmd': 'remove',
            'aid': aid,
        })
        removed: bool = await ipc.receive()
        return removed

    @acm
    async def open_rect(
        self,
        **kwargs,
    ) -> int:
        try:
            aid: int = await self.add_rect(
                from_acm=True,
                **kwargs,
            )
            yield aid
        finally:
            await self.remove(aid)

    async def redraw(
        self,
        fqme: str,
        timeframe: float,
    ) -> None:
        await self._get_ipc(fqme).send({
            'cmd': 'redraw',
            'fqme': fqme,
            # 'render': int(aid),
            # 'viz_name': str(viz_name),
            'timeframe': timeframe,
        })

    # TODO: do we even need this?
    # async def modify(
    #     self,
    #     aid: int,  # annotation id
    #     meth: str,  # far end graphics object method to invoke
    #     params: dict[str, Any],  # far end `meth(**kwargs)`
    # ) -> bool:
    #     '''
    #     Modify an existing (remote) annotation's graphics
    #     paramters, thus changing it's appearance / state in real
    #     time.

    #     '''
    #     raise NotImplementedError


@acm
async def open_annot_ctl(
    uid: tuple[str, str] | None = None,

) -> AnnotCtl:
    # TODO: load connetion to a specific chart actor
    # -[ ] pull from either service scan or config
    # -[ ] return some kinda client/proxy thinger?
    #    -[ ] maybe we should finally just provide this as
    #       a `tractor.hilevel.CallableProxy` or wtv?
    # -[ ] use this from the storage.cli stuff to mark up gaps!

    maybe_portals: list[Portal] | None
    fqmes: list[str]
    async with find_service(
        service_name='chart',
        first_only=False,
    ) as maybe_portals:

        ctx_mngrs: list[AsyncContextManager] = []

        # TODO: print the current discoverable actor UID set
        # here as well?
        if not maybe_portals:
            raise RuntimeError('No chart UI actors found in service domain?')

        for portal in maybe_portals:
            ctx_mngrs.append(
                portal.open_context(remote_annotate)
            )

        ctx2fqmes: dict[str, set[str]] = {}
        fqme2ipc: dict[str, MsgStream] = {}
        stream_ctxs: list[AsyncContextManager] = []

        async with (
            trionics.gather_contexts(ctx_mngrs) as ctxs,
        ):
            for (ctx, fqmes) in ctxs:
                stream_ctxs.append(ctx.open_stream())

                # fill lookup table of mkt addrs to IPC ctxs
                for fqme in fqmes:
                    if other := fqme2ipc.get(fqme):
                        raise ValueError(
                            f'More then one chart displays {fqme}!?\n'
                            'Other UI actor info:\n'
                            f'channel: {other._ctx.chan}]\n'
                            f'actor uid: {other._ctx.chan.uid}]\n'
                            f'ctx id: {other._ctx.cid}]\n'
                        )

                    ctx2fqmes.setdefault(
                        ctx.cid,
                        set(),
                    ).add(fqme)

            async with trionics.gather_contexts(stream_ctxs) as streams:
                for stream in streams:
                    fqmes: set[str] = ctx2fqmes[stream._ctx.cid]
                    for fqme in fqmes:
                        fqme2ipc[fqme] = stream

                # NOTE: on graceful teardown we always attempt to
                # remove all annots that were created by the
                # entering client.
                # TODO: should we maybe instead/also do this on the
                # server-actor side so that when a client
                # disconnects we always delete all annotations by
                # default instaead of expecting the client to?
                async with AsyncExitStack() as annots_stack:
                    client = AnnotCtl(
                        ctx2fqmes=ctx2fqmes,
                        fqme2ipc=fqme2ipc,
                        _annot_stack=annots_stack,
                    )
                    yield client