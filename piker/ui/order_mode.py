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
from pydantic import BaseModel
import trio

from ._lines import LevelLine, position_line
from ._editors import LineEditor, ArrowEditor
from ._window import MultiStatus, main_window
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
    '''Major mode for placing orders on a chart view.

    This is the default mode that pairs with "follow mode"
    (when wathing the rt price update at the current time step)
    and allows entering orders using mouse and keyboard.
    This object is chart oriented, so there is an instance per
    chart / view currently.

    Current manual:
        a -> alert
        s/ctrl -> submission type modifier {on: live, off: dark}
        f (fill) -> buy limit order
        d (dump) -> sell limit order
        c (cancel) -> cancel order under cursor
        cc -> cancel all submitted orders on chart
        mouse click and drag -> modify current order under cursor

    '''
    chart: 'ChartPlotWidget'  #  type: ignore # noqa
    book: OrderBook
    lines: LineEditor
    arrows: ArrowEditor
    status_bar: MultiStatus
    name: str = 'order'

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

    _pending_submissions: dict[str, (LevelLine, Callable)] = field(
        default_factory=dict)

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
        # not initialized yet
        if not self.chart.linked.cursor:
            return

        self._action = action
        self.lines.stage_line(

            color=self._colors[action],
            # hl_on_hover=True if self._exec_mode == 'live' else False,
            dotted=True if (
                self._exec_mode == 'dark' and action != 'alert'
            ) else False,
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

        pending = self._pending_submissions.get(uuid)
        if pending:
            order_line, func = pending
            assert order_line is line
            func()

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
            self.chart.linked.cursor.show_xhair()

            pending = self._pending_submissions.pop(uuid, None)
            if pending:
                order_line, func = pending
                func()
        else:
            log.warning(
                f'Received cancel for unsubmitted order {pformat(msg)}'
            )

    def submit_exec(
        self,
        size: Optional[float] = None,

    ) -> LevelLine:
        """Send execution order to EMS return a level line to
        represent the order on a chart.

        """
        # register the "staged" line under the cursor
        # to be displayed when above order ack arrives
        # (means the line graphic doesn't show on screen until the
        # order is live in the emsd).
        uid = str(uuid.uuid4())

        size = size or self._size

        cursor = self.chart.linked.cursor
        chart = cursor.active_plot
        y = cursor._datum_xy[1]

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

        # TODO: update the line once an ack event comes back
        # from the EMS!

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

        # enter submission which will be popped once a response
        # from the EMS is received to move the order to a different# status
        self._pending_submissions[uid] = (
            line,
            self.status_bar.open_status(
                f'submitting {self._exec_mode}-{action}',
                final_msg=f'submitted {self._exec_mode}-{action}',
                clear_on_next=True,
            )
        )

        # hook up mouse drag handlers
        line._on_drag_start = self.order_line_modify_start
        line._on_drag_end = self.order_line_modify_complete

        return line

    def cancel_orders_under_cursor(self) -> list[str]:
        return self.cancel_orders_from_lines(
            self.lines.lines_under_cursor()
        )

    def cancel_all_orders(self) -> list[str]:
        '''Cancel all orders for the current chart.

        '''
        return self.cancel_orders_from_lines(
            self.lines.all_lines()
        )

    def cancel_orders_from_lines(
        self,
        lines: list[LevelLine],

    ) -> list[str]:

        ids: list = []
        if lines:
            key = self.status_bar.open_status(
                f'cancelling {len(lines)} orders',
                final_msg=f'cancelled {len(lines)} orders',
                group_key=True
            )

            # cancel all active orders and triggers
            for line in lines:
                oid = getattr(line, 'oid', None)

                if oid:
                    self._pending_submissions[oid] = (
                        line,
                        self.status_bar.open_status(
                            f'cancelling order {oid[:6]}',
                            group_key=key,
                        ),
                    )

                    ids.append(oid)
                    self.book.cancel(uuid=oid)

        return ids

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


@asynccontextmanager
async def open_order_mode(
    symbol: Symbol,
    chart: pg.PlotWidget,
    book: OrderBook,
):
    status_bar: MultiStatus = main_window().status_bar
    view = chart._vb
    lines = LineEditor(chart=chart)
    arrows = ArrowEditor(chart, {})

    log.info("Opening order mode")

    mode = OrderMode(chart, book, lines, arrows, status_bar)
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

    started: trio.Event,

) -> None:
    '''Activate chart-trader order mode loop:
      - connect to emsd
      - load existing positions
      - begin order handling loop

    '''
    done = chart.window().status_bar.open_status('starting order mode..')

    # spawn EMS actor-service
    async with (
        open_ems(brokername, symbol) as (book, trades_stream, positions),
        open_order_mode(symbol, chart, book) as order_mode,

        # # start async input handling for chart's view
        # # await godwidget._task_stack.enter_async_context(
        # chart._vb.open_async_input_handler(),
    ):

        # update any exising positions
        for sym, msg in positions.items():
            order_mode.on_position_update(msg)

        def get_index(time: float):

            # XXX: not sure why the time is so off here
            # looks like we're gonna have to do some fixing..

            ohlc = chart._shm.array
            indexes = ohlc['time'] >= time

            if any(indexes):
                return ohlc['index'][indexes][-1]
            else:
                return ohlc['index'][-1]

        # Begin order-response streaming
        done()

        # start async input handling for chart's view
        async with chart._vb.open_async_input_handler():

            # signal to top level symbol loading task we're ready
            # to handle input since the ems connection is ready
            started.set()

            # this is where we receive **back** messages
            # about executions **from** the EMS actor
            async for msg in trades_stream:

                fmsg = pformat(msg)
                log.info(f'Received order msg:\n{fmsg}')

                name = msg['name']
                if name in (
                    'position',
                ):
                    # show line label once order is live
                    order_mode.on_position_update(msg)
                    continue

                resp = msg['resp']
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
                    'dark_triggered'
                ):
                    log.info(f'Dark order triggered for {fmsg}')

                elif resp in (
                    'alert_triggered'
                ):
                    # should only be one "fill" for an alert
                    # add a triangle and remove the level line
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

                    known_order = book._sent_orders.get(oid)
                    if not known_order:
                        log.warning(f'order {oid} is unknown')
                        continue

                    action = known_order.action
                    details = msg['brokerd_msg']

                    # TODO: some kinda progress system
                    order_mode.on_fill(
                        oid,
                        price=details['price'],
                        pointing='up' if action == 'buy' else 'down',

                        # TODO: put the actual exchange timestamp
                        arrow_index=get_index(details['broker_time']),
                    )
