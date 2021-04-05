# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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

"""
Chart trading, the only way to scalp.

"""
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pprint import pformat
import time
from typing import Optional, Dict, Callable, Any
import uuid

import pyqtgraph as pg
import trio
from pydantic import BaseModel

from ._graphics._lines import LevelLine, position_line
from ._interaction import LineEditor, ArrowEditor, _order_lines
from ..clearing._client import open_ems, OrderBook
from ..data._source import Symbol
from ..log import get_logger


log = get_logger(__name__)


class Position(BaseModel):
    symbol: Symbol
    size: float
    avg_price: float
    fills: Dict[str, Any] = {}


@dataclass
class OrderMode:
    """Major mode for placing orders on a chart view.

    This is the default mode that pairs with "follow mode"
    (when wathing the rt price update at the current time step)
    and allows entering orders using the ``a, d, f`` keys and
    cancelling moused-over orders with the ``c`` key.

    """
    chart: 'ChartPlotWidget'  #  type: ignore # noqa
    book: OrderBook
    lines: LineEditor
    arrows: ArrowEditor
    _colors = {
        'alert': 'alert_yellow',
        'buy': 'buy_green',
        'sell': 'sell_red',
    }
    _action: str = 'alert'
    _exec_mode: str = 'dark'
    _size: float = 100.0
    _position: Dict[str, Any] = field(default_factory=dict)
    _position_line: dict = None

    key_map: Dict[str, Callable] = field(default_factory=dict)

    def on_position_update(
        self,
        msg: dict,
    ) -> None:
        print(f'Position update {msg}')

        sym = self.chart._lc._symbol
        if msg['symbol'].lower() not in sym.key:
            return

        size = msg['size']

        self._position.update(msg)
        if self._position_line:
            self._position_line.delete()

        if size != 0.0:
            line = self._position_line = position_line(
                self.chart,
                level=msg['avg_price'],
                size=size,
            )
            line.show()

    def uuid(self) -> str:
        return str(uuid.uuid4())

    def set_exec(
        self,
        action: str,
        size: Optional[int] = None,
    ) -> None:
        """Set execution mode.

        """
        self._action = action
        self.lines.stage_line(

            color=self._colors[action],
            # hl_on_hover=True if self._exec_mode == 'live' else False,
            dotted=True if self._exec_mode == 'dark' else False,
            size=size or self._size,
            action=action,
        )

    def on_submit(self, uuid: str) -> dict:
        """On order submitted event, commit the order line
        and registered order uuid, store ack time stamp.

        TODO: annotate order line with submission type ('live' vs.
        'dark').

        """
        line = self.lines.commit_line(uuid)
        req_msg = self.book._sent_orders.get(uuid)
        if req_msg:
            req_msg['ack_time_ns'] = time.time_ns()

        return line

    def on_fill(
        self,
        uuid: str,
        price: float,
        arrow_index: float,
        pointing: Optional[str] = None
    ) -> None:

        line = self.lines._order_lines.get(uuid)
        if line:
            self.arrows.add(
                uuid,
                arrow_index,
                price,
                pointing=pointing,
                color=line.color
            )

    async def on_exec(
        self,
        uuid: str,
        msg: Dict[str, Any],
    ) -> None:

        # only once all fills have cleared and the execution
        # is complet do we remove our "order line"
        line = self.lines.remove_line(uuid=uuid)
        log.debug(f'deleting {line} with oid: {uuid}')

        # DESKTOP NOTIFICATIONS
        #
        # TODO: this in another task?
        # not sure if this will ever be a bottleneck,
        # we probably could do graphics stuff first tho?

        # XXX: linux only for now
        result = await trio.run_process(
            [
                'notify-send',
                '-u', 'normal',
                '-t', '10000',
                'piker',
                f'alert: {msg}',
            ],
        )
        log.runtime(result)

    def on_cancel(self, uuid: str) -> None:

        msg = self.book._sent_orders.pop(uuid, None)

        if msg is not None:
            self.lines.remove_line(uuid=uuid)
            self.chart._cursor.show_xhair()

        else:
            log.warning(
                f'Received cancel for unsubmitted order {pformat(msg)}'
            )

    def submit_exec(
        self,
        size: Optional[float] = None,
    ) -> LevelLine:
        """Send execution order to EMS.

        """
        # register the "staged" line under the cursor
        # to be displayed when above order ack arrives
        # (means the line graphic doesn't show on screen until the
        # order is live in the emsd).
        uid = str(uuid.uuid4())

        size = size or self._size

        chart = self.chart._cursor.active_plot
        y = chart._cursor._datum_xy[1]

        symbol = self.chart._lc._symbol

        action = self._action

        # send order cmd to ems
        self.book.send(
            uuid=uid,
            symbol=symbol.key,
            brokers=symbol.brokers,
            price=y,
            size=size,
            action=action,
            exec_mode=self._exec_mode,
        )

        # make line graphic if order push was
        # sucessful
        line = self.lines.create_order_line(
            uid,
            level=y,
            chart=chart,
            size=size,
            action=action,
        )
        line.oid = uid

        # hook up mouse drag handlers
        line._on_drag_start = self.order_line_modify_start
        line._on_drag_end = self.order_line_modify_complete

        return line

    def cancel_order_under_cursor(self) -> None:
        for line in self.lines.lines_under_cursor():
            self.book.cancel(uuid=line.oid)

    # order-line modify handlers
    def order_line_modify_start(
        self,
        line: LevelLine,
    ) -> None:
        print(f'Line modify: {line}')
        # cancel original order until new position is found

    def order_line_modify_complete(
        self,
        line: LevelLine,
    ) -> None:
        self.book.update(
            uuid=line.oid,

            # TODO: should we round this to a nearest tick here?
            price=line.value(),
        )

    # def on_key_press(
    #     self,
    #     key:
    #     mods:
    #     text: str,
    # ) -> None:
    #     pass


@asynccontextmanager
async def open_order_mode(
    symbol: Symbol,
    chart: pg.PlotWidget,
    book: OrderBook,
):
    view = chart._vb
    lines = LineEditor(view=view, chart=chart, _order_lines=_order_lines)
    arrows = ArrowEditor(chart, {})

    log.info("Opening order mode")

    mode = OrderMode(chart, book, lines, arrows)
    view.mode = mode

    asset_type = symbol.type_key

    if asset_type == 'stock':
        mode._size = 100.0

    elif asset_type in ('future', 'option', 'futures_option'):
        mode._size = 1.0

    else:  # to be safe
        mode._size = 1.0

    try:
        yield mode

    finally:
        # XXX special teardown handling like for ex.
        # - cancelling orders if needed?
        # - closing positions if desired?
        # - switching special condition orders to safer/more reliable variants
        log.info("Closing order mode")


async def start_order_mode(
    chart: 'ChartPlotWidget',  # noqa
    symbol: Symbol,
    brokername: str,
) -> None:
    # spawn EMS actor-service
    async with open_ems(
        brokername,
        symbol,
    ) as (book, trades_stream), open_order_mode(
        symbol,
        chart,
        book,
    ) as order_mode:

        def get_index(time: float):

            # XXX: not sure why the time is so off here
            # looks like we're gonna have to do some fixing..

            ohlc = chart._shm.array
            indexes = ohlc['time'] >= time

            if any(indexes):
                return ohlc['index'][indexes[-1]]
            else:
                return ohlc['index'][-1]

        # Begin order-response streaming

        # this is where we receive **back** messages
        # about executions **from** the EMS actor
        async for msg in trades_stream:

            fmsg = pformat(msg)
            log.info(f'Received order msg:\n{fmsg}')

            resp = msg['resp']

            if resp in (
                'position',
            ):
                # show line label once order is live
                order_mode.on_position_update(msg)
                continue

            # delete the line from view
            oid = msg['oid']

            # response to 'action' request (buy/sell)
            if resp in (
                'dark_submitted',
                'broker_submitted'
            ):

                # show line label once order is live
                order_mode.on_submit(oid)

            # resp to 'cancel' request or error condition
            # for action request
            elif resp in (
                'broker_cancelled',
                'broker_inactive',
                'dark_cancelled'
            ):
                # delete level line from view
                order_mode.on_cancel(oid)

            elif resp in (
                'dark_executed'
            ):
                log.info(f'Dark order triggered for {fmsg}')

                # for alerts add a triangle and remove the
                # level line
                if msg['cmd']['action'] == 'alert':

                    # should only be one "fill" for an alert
                    order_mode.on_fill(
                        oid,
                        price=msg['trigger_price'],
                        arrow_index=get_index(time.time())
                    )
                    await order_mode.on_exec(oid, msg)

            # response to completed 'action' request for buy/sell
            elif resp in (
                'broker_executed',
            ):
                await order_mode.on_exec(oid, msg)

            # each clearing tick is responded individually
            elif resp in ('broker_filled',):
                action = msg['action']
                # TODO: some kinda progress system
                order_mode.on_fill(
                    oid,
                    price=msg['price'],
                    arrow_index=get_index(msg['broker_time']),
                    pointing='up' if action == 'buy' else 'down',
                )
