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
from functools import partial
from math import copysign
from pprint import pformat
import time
from typing import Optional, Dict, Callable, Any
import uuid

from bidict import bidict
from pydantic import BaseModel
import tractor
import trio

from .. import brokers
from ..calc import percent_change
from ..clearing._client import open_ems, OrderBook
from ..data._source import Symbol
from ..data._normalize import iterticks
from ..data.feed import Feed
from ..log import get_logger
from ._editors import LineEditor, ArrowEditor
from ._lines import order_line, LevelLine
from ._position import PositionTracker, SettingsPane, Allocator, _size_units
from ._window import MultiStatus
from ..clearing._messages import Order
from ._forms import open_form_input_handling


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
    pane: SettingsPane

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

        level = order.price
        line = order_line(

            self.chart,
            # TODO: convert these values into human-readable form
            # (i.e. with k, m, M, B) type embedded suffixes
            level=level,
            action=order.action,

            size=order.size,
            color=self._colors[order.action],

            dotted=True if (
                order.exec_mode == 'dark' and
                order.action != 'alert'
            ) else False,

            **line_kwargs,
        )

        # set level update callback to order pane method and update once
        # immediately
        if order.action != 'alert':
            line._on_level_change = partial(
                self.pane.on_level_change_update_next_order_info,
                line=line,
                order=order,
            )

        else:
            # for alerts we don't need to compute per price sizing via
            # the order mode allocator but we still need to update the
            # "staged" order message we'll send to the ems
            def update_order_price(y: float) -> None:
                order.price = y

            line._on_level_change = update_order_price

        line.set_level(level)

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


@asynccontextmanager
async def open_order_mode(

    feed: Feed,
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
        trio.open_nursery() as n,

    ):
        log.info(f'Opening order mode for {brokername}.{symbol.key}')

        view = chart.view

        # annotations editors
        lines = LineEditor(chart=chart)
        arrows = ArrowEditor(chart, {})

        # allocator
        alloc = Allocator(
            symbol=symbol,
            account=None,
            _accounts=bidict(brokers.config.load_accounts()),
            size_unit=_size_units['currency'],
            units_limit=400,
            currency_limit=5e3,
            slots=4,
        )

        form = chart.sidepane
        form.model = alloc

        pp_tracker = PositionTracker(chart, alloc)
        pp_tracker.hide()

        # order pane widgets and allocation model
        order_pane = SettingsPane(
            tracker=pp_tracker,
            form=form,
            alloc=alloc,
            fill_bar=form.fill_bar,
            pnl_label=form.left_label,
            step_label=form.bottom_label,
            limit_label=form.top_label,
        )

        # XXX: would love to not have to do this separate from edit
        # fields (which are done in an async loop - see below)
        # connect selection signals (from drop down widgets)
        # to order sync pane handler
        for key in ('account', 'size_unit',):
            w = form.fields[key]

            w.currentTextChanged.connect(
                partial(
                    order_pane.on_selection_change,
                    key=key,
                )
            )

        # top level abstraction which wraps all this crazyness into
        # a namespace..
        mode = OrderMode(
            chart,
            book,
            lines,
            arrows,
            multistatus,
            pp_tracker,
            allocator=alloc,
            pane=order_pane,
        )

        # TODO: create a mode "manager" of sorts?
        # -> probably just call it "UxModes" err sumthin?
        # so that view handlers can access it
        view.order_mode = mode

        # update any exising position
        for sym, msg in positions.items():

            our_sym = mode.chart.linked._symbol.key
            if sym.lower() in our_sym:
                pp_tracker.update(msg, position=pp_tracker.startup_pp)
                pp_tracker.update(msg)

        live_pp = mode.pp.live_pp
        size = live_pp.size
        if size:
            global _zero_pp
            _zero_pp = False

            # compute and display pnl status immediately
            mode.pane.pnl_label.format(
                pnl=round(
                    copysign(1, size) * percent_change(
                        live_pp.avg_price,
                        # last historical close price
                        feed.shm.array[-1][['close']][0],
                    ),
                    ndigits=2,
                )
            )

            # spawn updater task
            n.start_soon(
                display_pnl,
                feed,
                mode,
            )

        else:
            # set 0% pnl
            mode.pane.pnl_label.format(pnl=0)

        # make fill bar and positioning snapshot
        order_pane.init_status_ui()

        # Begin order-response streaming
        done()

        # start async input handling for chart's view
        async with (

            # ``ChartView`` input async handler startup
            chart.view.open_async_input_handler(),

            # pp pane kb inputs
            open_form_input_handling(
                form,
                focus_next=chart.linked.godwidget,
                on_value_change=order_pane.on_ui_settings_change,
            ),

        ):

            # signal to top level symbol loading task we're ready
            # to handle input since the ems connection is ready
            started.set()

            n.start_soon(
                process_trades_and_update_ui,
                n,
                feed,
                mode,
                trades_stream,
                book,
            )
            yield mode


_zero_pp: bool = True


async def display_pnl(
    feed: Feed,
    order_mode: OrderMode,
) -> None:
    '''Real-time display the current pp's PnL in the appropriate label.

    Error if this task is spawned where there is a net-zero pp.

    '''
    global _zero_pp
    assert not _zero_pp

    pp = order_mode.pp
    live = pp.live_pp

    if live.size < 0:
        types = ('ask', 'last', 'last', 'utrade')

    elif live.size > 0:
        types = ('bid', 'last', 'last', 'utrade')

    else:
        raise RuntimeError('No pp?!?!')

    # real-time update pnl on the status pane
    async with feed.stream.subscribe() as bstream:
        last_tick = time.time()
        async for quotes in bstream:

            now = time.time()
            period = now - last_tick

            for sym, quote in quotes.items():

                for tick in iterticks(quote, types):
                    # print(f'{1/period} Hz')

                    size = live.size

                    if size == 0:
                        # terminate this update task since we're
                        # no longer in a pp
                        _zero_pp = True
                        order_mode.pane.pnl_label.format(pnl=0)
                        return

                    else:
                        # compute and display pnl status
                        order_mode.pane.pnl_label.format(
                            pnl=round(
                                copysign(1, size) * percent_change(
                                    live.avg_price,
                                    tick['price'],
                                ),
                                ndigits=2,
                            )
                        )

                    last_tick = time.time()


async def process_trades_and_update_ui(

    n: trio.Nursery,
    feed: Feed,
    mode: OrderMode,
    trades_stream: tractor.MsgStream,
    book: OrderBook,

) -> None:

    get_index = mode.chart.get_index
    tracker = mode.pp
    global _zero_pp

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
                tracker.update(msg)

                # update order pane widgets
                mode.pane.update_status_ui()

            if mode.pp.live_pp.size and _zero_pp:
                _zero_pp = False
                n.start_soon(
                    display_pnl,
                    feed,
                    mode,
                )
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

            tracker.live_pp.fills.append(msg)
