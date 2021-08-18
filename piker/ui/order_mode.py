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
from dataclasses import dataclass, field
from pprint import pformat
import time
from typing import Optional, Dict, Callable, Any
import uuid

from pydantic import BaseModel
import tractor
import trio

from ..clearing._client import open_ems, OrderBook
from ..data._source import Symbol
from ..log import get_logger
from ._editors import LineEditor, ArrowEditor
from ._lines import order_line, LevelLine
from ._position import PositionTracker
from ._window import MultiStatus
from ..clearing._messages import Order
# from ._forms import FieldsForm


log = get_logger(__name__)


class OrderDialog(BaseModel):
    '''Trade dialogue meta-data describing the lifetime
    of an order submission to ``emsd`` from a chart.

    '''
    # TODO: use ``pydantic.UUID4`` field
    uuid: str
    order: Order
    symbol: Symbol
    line: LevelLine
    last_status_close: Callable = lambda: None
    msgs: dict[str, dict] = {}
    fills: Dict[str, Any] = {}

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = False


@dataclass
class OrderMode:
    '''Major UX mode for placing orders on a chart view providing so
    called, "chart trading".

    This is the other "main" mode that pairs with "view mode" (when
    wathing the rt price update at the current time step) and allows
    entering orders using mouse and keyboard.  This object is chart
    oriented, so there is an instance per chart / view currently.

    Current manual:
        a -> alert
        s/ctrl -> submission type modifier {on: live, off: dark}
        f (fill) -> 'buy' limit order
        d (dump) -> 'sell' limit order
        c (cancel) -> cancel order under cursor
        cc -> cancel all submitted orders on chart
        mouse click and drag -> modify current order under cursor

    '''
    chart: 'ChartPlotWidget'  #  type: ignore # noqa
    book: OrderBook
    lines: LineEditor
    arrows: ArrowEditor
    multistatus: MultiStatus
    pp: PositionTracker
    allocator: 'Allocator'  # noqa
    active: bool = False

    name: str = 'order'
    dialogs: dict[str, OrderDialog] = field(default_factory=dict)

    _colors = {
        'alert': 'alert_yellow',
        'buy': 'buy_green',
        'sell': 'sell_red',
    }
    _staged_order: Optional[Order] = None

    def line_from_order(
        self,

        order: Order,
        symbol: Symbol,

        **line_kwargs,

    ) -> LevelLine:

        line = order_line(

            self.chart,
            # TODO: convert these values into human-readable form
            # (i.e. with k, m, M, B) type embedded suffixes
            level=order.price,
            # level_digits=symbol.digits(),
            action=order.action,

            size=order.size,
            # TODO: we need truncation checks in the EMS for this?
            # size_digits=min(symbol.lot_digits(), 3),
            color=self._colors[order.action],

            dotted=True if (
                order.exec_mode == 'dark' and
                order.action != 'alert'
            ) else False,

            **line_kwargs,

        )

        # define a callback applied for each level change to the line
        # which will recompute the order size based on allocator
        # settings:
        def update_from_size_calc(y: float) -> None:

            order_info = self.allocator.get_order_info(
                symbol=symbol,
                price=y,
                action=order.action,
            )
            line.update_labels(order_info)
            order.price = y
            order.size = order_info['size']

            # return is used to update labels implicitly
            return order_info

        update_from_size_calc(order.price)

        # set level update cb
        # line._on_level_change = partial(update_from_size_calc, order=order)
        line._on_level_change = update_from_size_calc

        return line

    def stage_order(
        self,

        action: str,
        trigger_type: str,

    ) -> None:
        '''Stage an order for submission.

        '''
        # not initialized yet
        chart = self.chart
        cursor = chart.linked.cursor
        if not (chart and cursor and cursor.active_plot):
            return

        chart = cursor.active_plot
        price = cursor._datum_xy[1]
        symbol = self.chart.linked.symbol

        order = self._staged_order = Order(
            action=action,
            price=price,
            size=0,
            symbol=symbol,
            brokers=symbol.brokers,
            oid='',  # filled in on submit
            exec_mode=trigger_type,  # dark or live
        )

        line = self.line_from_order(
            order,
            symbol,

            show_markers=True,
            # just for the stage line to avoid
            # flickering while moving the cursor
            # around where it might trigger highlight
            # then non-highlight depending on sensitivity
            always_show_labels=True,
            # don't highlight the "staging" line
            highlight_on_hover=False,
            # prevent flickering of marker while moving/tracking cursor
            only_show_markers_on_hover=False,
        )

        line = self.lines.stage_line(line)

        # hide crosshair y-line and label
        cursor.hide_xhair()

        # add line to cursor trackers
        cursor._trackers.add(line)

        return line

    def submit_order(
        self,

    ) -> OrderDialog:
        '''Send execution order to EMS return a level line to
        represent the order on a chart.

        '''
        staged = self._staged_order
        symbol = staged.symbol
        oid = str(uuid.uuid4())

        # format order data for ems
        order = staged.copy(
            update={
                'symbol': symbol.key,
                'oid': oid,
            }
        )

        line = self.line_from_order(
            order,
            symbol,

            show_markers=True,
            only_show_markers_on_hover=True,
        )

        # register the "submitted" line under the cursor
        # to be displayed when above order ack arrives
        # (means the marker graphic doesn't show on screen until the
        # order is live in the emsd).

        # TODO: update the line once an ack event comes back
        # from the EMS!
        # maybe place a grey line in "submission" mode
        # which will be updated to it's appropriate action
        # color once the submission ack arrives.
        self.lines.submit_line(
            line=line,
            uuid=oid,
        )

        dialog = OrderDialog(
            uuid=oid,
            order=order,
            symbol=symbol,
            line=line,
            last_status_close=self.multistatus.open_status(
                f'submitting {self._trigger_type}-{order.action}',
                final_msg=f'submitted {self._trigger_type}-{order.action}',
                clear_on_next=True,
            )
        )

        # TODO: create a new ``OrderLine`` with this optional var defined
        line.dialog = dialog

        # enter submission which will be popped once a response
        # from the EMS is received to move the order to a different# status
        self.dialogs[oid] = dialog

        # hook up mouse drag handlers
        line._on_drag_start = self.order_line_modify_start
        line._on_drag_end = self.order_line_modify_complete

        # send order cmd to ems
        self.book.send(order)

        return dialog

    # order-line modify handlers

    def order_line_modify_start(
        self,
        line: LevelLine,

    ) -> None:

        print(f'Line modify: {line}')
        # cancel original order until new position is found?
        # TODO: make a config option for this behaviour..

    def order_line_modify_complete(
        self,
        line: LevelLine,

    ) -> None:

        level = line.value()
        # updateb by level change callback set in ``.line_from_order()``
        size = line.dialog.order.size

        self.book.update(
            uuid=line.dialog.uuid,
            price=level,
            size=size,
        )

    # ems response loop handlers

    def on_submit(
        self,
        uuid: str

    ) -> OrderDialog:
        '''
        Order submitted status event handler.

        Commit the order line and registered order uuid, store ack time stamp.

        '''
        line = self.lines.commit_line(uuid)

        # a submission is the start of a new order dialog
        dialog = self.dialogs[uuid]
        dialog.line = line
        dialog.last_status_close()

        return dialog

    def on_fill(
        self,

        uuid: str,
        price: float,
        arrow_index: float,

        pointing: Optional[str] = None,

    ) -> None:
        '''
        Fill msg handler.

        Triggered on reception of a `filled` message from the
        EMS.

        Update relevant UIs:

        - add arrow annotation on bar
        - update fill bar size

        '''
        dialog = self.dialogs[uuid]
        line = dialog.line
        if line:
            self.arrows.add(
                uuid,
                arrow_index,
                price,
                pointing=pointing,
                color=line.color
            )
        else:
            log.warn("No line for order {uuid}!?")

    async def on_exec(
        self,

        uuid: str,
        msg: Dict[str, Any],

    ) -> None:

        # DESKTOP NOTIFICATIONS
        #
        # TODO: this in another task?
        # not sure if this will ever be a bottleneck,
        # we probably could do graphics stuff first tho?

        # TODO: make this not trash.
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

    def on_cancel(
        self,
        uuid: str

    ) -> None:

        msg = self.book._sent_orders.pop(uuid, None)

        if msg is not None:
            self.lines.remove_line(uuid=uuid)
            self.chart.linked.cursor.show_xhair()

            dialog = self.dialogs.pop(uuid, None)
            if dialog:
                dialog.last_status_close()
        else:
            log.warning(
                f'Received cancel for unsubmitted order {pformat(msg)}'
            )

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
            key = self.multistatus.open_status(
                f'cancelling {len(lines)} orders',
                final_msg=f'cancelled {len(lines)} orders',
                group_key=True
            )

            # cancel all active orders and triggers
            for line in lines:
                dialog = getattr(line, 'dialog', None)

                if dialog:
                    oid = dialog.uuid

                    cancel_status_close = self.multistatus.open_status(
                        f'cancelling order {oid[:6]}',
                        group_key=key,
                    )
                    dialog.last_status_close = cancel_status_close

                    ids.append(oid)
                    self.book.cancel(uuid=oid)

        return ids


async def run_order_mode(

    chart: 'ChartPlotWidget',  # noqa
    symbol: Symbol,
    brokername: str,
    started: trio.Event,

) -> None:
    '''Activate chart-trader order mode loop:

      - connect to emsd
      - load existing positions
      - begin EMS response handling loop which updates local
        state, mostly graphics / UI.

    '''
    multistatus = chart.window().status_bar
    done = multistatus.open_status('starting order mode..')

    book: OrderBook
    trades_stream: tractor.MsgStream
    positions: dict

    # spawn EMS actor-service
    async with (

        open_ems(brokername, symbol) as (
            book,
            trades_stream,
            positions
        ),

    ):
        view = chart.view
        lines = LineEditor(chart=chart)
        arrows = ArrowEditor(chart, {})

        log.info("Opening order mode")

        alloc = chart.linked.godwidget.pp_pane.model
        pp_tracker = PositionTracker(chart, alloc=alloc)
        pp_tracker.hide()
        alloc._position = pp_tracker

        mode = OrderMode(
            chart,
            book,
            lines,
            arrows,
            multistatus,
            pp_tracker,
            allocator=alloc,
        )

        # TODO: create a mode "manager" of sorts?
        # -> probably just call it "UxModes" err sumthin?
        # so that view handlers can access it
        view.order_mode = mode

        asset_type = symbol.type_key

        # update any exising position
        for sym, msg in positions.items():

            our_sym = mode.chart.linked._symbol.key
            if sym.lower() in our_sym:
                pp_tracker.update(msg, position=pp_tracker.startup_pp)
                pp_tracker.update(msg)


        # default entry sizing
        if asset_type in ('stock', 'crypto', 'forex'):
            alloc.size_unit = '$ size'
            pp_tracker.pane.fields['size_unit'].setCurrentText(alloc.size_unit)

        elif asset_type in ('future', 'option', 'futures_option'):

            alloc.size_unit = '# units'
            pp_tracker.pane.fields['size_unit'].setCurrentText(alloc.size_unit)

            slots = alloc.slots = pp_tracker.startup_pp.size
            pp_tracker.pane.fields['slots'].setText(str(alloc.slots))

            # make entry step 1.0
            alloc.size = slots
            pp_tracker.pane.fields['size'].setText(str(alloc.size))

        # make fill bar and positioning snapshot
        pp_tracker.init_status_ui()

        # TODO: this should go onto some sort of
        # data-view strimg thinger..right?
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
        async with (
            chart.view.open_async_input_handler(),

            # TODO: config form handler nursery

        ):

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

                    sym = mode.chart.linked.symbol
                    if msg['symbol'].lower() in sym.key:
                        pp_tracker.update(msg)

                    # short circuit to next msg to avoid
                    # uncessary msg content lookups
                    continue

                resp = msg['resp']
                oid = msg['oid']

                dialog = mode.dialogs.get(oid)
                if dialog is None:
                    log.warning(f'received msg for untracked dialog:\n{fmsg}')

                    # TODO: enable pure tracking / mirroring of dialogs
                    # is desired.
                    continue

                # record message to dialog tracking
                dialog.msgs[oid] = msg

                # response to 'action' request (buy/sell)
                if resp in (
                    'dark_submitted',
                    'broker_submitted'
                ):

                    # show line label once order is live
                    mode.on_submit(oid)

                # resp to 'cancel' request or error condition
                # for action request
                elif resp in (
                    'broker_cancelled',
                    'broker_inactive',
                    'dark_cancelled'
                ):
                    # delete level line from view
                    mode.on_cancel(oid)

                elif resp in (
                    'dark_triggered'
                ):
                    log.info(f'Dark order triggered for {fmsg}')

                elif resp in (
                    'alert_triggered'
                ):
                    # should only be one "fill" for an alert
                    # add a triangle and remove the level line
                    mode.on_fill(
                        oid,
                        price=msg['trigger_price'],
                        arrow_index=get_index(time.time()),
                    )
                    mode.lines.remove_line(uuid=oid)
                    await mode.on_exec(oid, msg)

                # response to completed 'action' request for buy/sell
                elif resp in (
                    'broker_executed',
                ):
                    # right now this is just triggering a system alert
                    await mode.on_exec(oid, msg)

                    if msg['brokerd_msg']['remaining'] == 0:
                        mode.lines.remove_line(uuid=oid)

                # each clearing tick is responded individually
                elif resp in ('broker_filled',):

                    known_order = book._sent_orders.get(oid)
                    if not known_order:
                        log.warning(f'order {oid} is unknown')
                        continue

                    action = known_order.action
                    details = msg['brokerd_msg']

                    # TODO: some kinda progress system
                    mode.on_fill(
                        oid,
                        price=details['price'],
                        pointing='up' if action == 'buy' else 'down',

                        # TODO: put the actual exchange timestamp
                        arrow_index=get_index(details['broker_time']),
                    )

                    pp_tracker.live_pp.fills.append(msg)
