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
from pprint import pformat
import time
from typing import Optional, Dict, Callable, Any
import uuid

from pydantic import BaseModel
import tractor
import trio

from .. import config
from ..clearing._client import open_ems, OrderBook
from ..clearing._allocate import (
    mk_allocator,
    Position,
)
from ..data._source import Symbol
from ..data.feed import Feed
from ..log import get_logger
from ._editors import LineEditor, ArrowEditor
from ._lines import order_line, LevelLine
from ._position import (
    PositionTracker,
    SettingsPane,
)
from ._label import FormatLabel
from ._window import MultiStatus
from ..clearing._messages import Order, BrokerdPosition
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


def on_level_change_update_next_order_info(

    level: float,

    # these are all ``partial``-ed in at callback assignment time.
    line: LevelLine,
    order: Order,
    tracker: PositionTracker,

) -> None:
    '''A callback applied for each level change to the line
    which will recompute the order size based on allocator
    settings. this is assigned inside
    ``OrderMode.line_from_order()``

    '''
    # NOTE: the ``Order.account`` is set at order stage time
    # inside ``OrderMode.line_from_order()``.
    order_info = tracker.alloc.next_order_info(
        startup_pp=tracker.startup_pp,
        live_pp=tracker.live_pp,
        price=level,
        action=order.action,
    )
    line.update_labels(order_info)

    # update bound-in staged order
    order.price = level
    order.size = order_info['size']


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
    nursery: trio.Nursery
    quote_feed: Feed
    book: OrderBook
    lines: LineEditor
    arrows: ArrowEditor
    multistatus: MultiStatus
    pane: SettingsPane
    trackers: dict[str, PositionTracker]

    # switched state, the current position
    current_pp: Optional[PositionTracker] = None
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
                on_level_change_update_next_order_info,
                line=line,
                order=order,
                tracker=self.current_pp,
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
            account=self.current_pp.alloc.account,
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

    # The keys in this dict **must** be in set our set of "normalized"
    # symbol names (i.e. the same names you'd get back in search
    # results) in order for position msgs to correctly trigger the
    # display of a position indicator on screen.
    position_msgs: dict[str, list[BrokerdPosition]]

    # spawn EMS actor-service
    async with (

        open_ems(brokername, symbol) as (
            book,
            trades_stream,
            position_msgs,
            brokerd_accounts,
        ),
        trio.open_nursery() as tn,

    ):
        log.info(f'Opening order mode for {brokername}.{symbol.key}')

        view = chart.view

        # annotations editors
        lines = LineEditor(chart=chart)
        arrows = ArrowEditor(chart, {})

        # symbol id
        symbol = chart.linked.symbol
        symkey = symbol.key

        # map of per-provider account keys to position tracker instances
        trackers: dict[str, PositionTracker] = {}

        # load account names from ``brokers.toml``
        accounts_def = config.load_accounts(
            providers=symbol.brokers
        )

        # XXX: ``brokerd`` delivers a set of account names that it allows
        # use of but the user also can define the accounts they'd like
        # to use, in order, in their `brokers.toml` file.
        accounts = {}
        for name in brokerd_accounts:
            # ensure name is in ``brokers.toml``
            accounts[name] = accounts_def[name]

        # first account listed is the one we select at startup
        # (aka order based selection).
        pp_account = next(
            # choose first account based on line order from `brokers.toml`.
            iter(accounts.keys())
        ) if accounts else 'paper'

        # NOTE: requires the backend exactly specifies
        # the expected symbol key in its positions msg.
        pp_msgs = position_msgs.get(symkey, ())
        pps_by_account = {msg['account']: msg for msg in pp_msgs}

        # update pp trackers with data relayed from ``brokerd``.
        for account_name in accounts:

            # net-zero pp
            startup_pp = Position(
                symbol=symbol,
                size=0,
                avg_price=0,
            )
            msg = pps_by_account.get(account_name)
            if msg:
                log.info(f'Loading pp for {symkey}:\n{pformat(msg)}')
                startup_pp.update_from_msg(msg)

            # allocator
            alloc = mk_allocator(
                symbol=symbol,
                account=account_name,

                # if this startup size is greater the allocator limit,
                # the limit is increased internally in this factory.
                startup_pp=startup_pp,
            )

            pp_tracker = PositionTracker(
                chart,
                alloc,
                startup_pp
            )
            pp_tracker.hide()
            trackers[account_name] = pp_tracker

            assert pp_tracker.startup_pp.size == pp_tracker.live_pp.size

            # TODO: do we even really need the "startup pp" or can we
            # just take the max and pass that into the some state / the
            # alloc?
            pp_tracker.update_from_pp()

            # on existing position, show pp tracking graphics
            if pp_tracker.startup_pp.size != 0:
                pp_tracker.show()
                pp_tracker.hide_info()

        # setup order mode sidepane widgets
        form = chart.sidepane
        vbox = form.vbox

        from textwrap import dedent

        from PyQt5.QtCore import Qt

        from ._style import _font, _font_small
        from ..calc import humanize

        feed_label = FormatLabel(
            fmt_str=dedent("""
            actor: **{actor_name}**\n
            |_ @**{host}:{port}**\n
            |_ throttle_hz: **{throttle_rate}**\n
            |_ streams: **{symbols}**\n
            |_ shm: **{shm}**\n
            """),
            font=_font.font,
            font_size=_font_small.px_size,
            font_color='default_lightest',
        )

        form.feed_label = feed_label

        # add feed info label to top
        vbox.insertWidget(
            0,
            feed_label,
            alignment=Qt.AlignBottom,
        )
        # vbox.setAlignment(feed_label, Qt.AlignBottom)
        # vbox.setAlignment(Qt.AlignBottom)
        blank_h = chart.height() - (
            form.height() +
            form.fill_bar.height()
            # feed_label.height()
        )
        vbox.setSpacing(
            int((1 + 5/8)*_font.px_size)
        )

        # fill in brokerd feed info
        host, port = feed.portal.channel.raddr
        if host == '127.0.0.1':
            host = 'localhost'
        mpshm = feed.shm._shm
        shmstr = f'{humanize(mpshm.size)}'
        form.feed_label.format(
            actor_name=feed.portal.channel.uid[0],
            host=host,
            port=port,
            symbols=len(feed.symbols),
            shm=shmstr,
            throttle_rate=feed.throttle_rate,
        )

        order_pane = SettingsPane(
            form=form,
            # XXX: ugh, so hideous...
            fill_bar=form.fill_bar,
            pnl_label=form.left_label,
            step_label=form.bottom_label,
            limit_label=form.top_label,
        )
        order_pane.set_accounts(list(trackers.keys()))

        # update pp icons
        for name, tracker in trackers.items():
            order_pane.update_account_icons({name: tracker.live_pp})

        # top level abstraction which wraps all this crazyness into
        # a namespace..
        mode = OrderMode(
            chart,
            tn,
            feed,
            book,
            lines,
            arrows,
            multistatus,
            pane=order_pane,
            trackers=trackers,

        )
        # XXX: MUST be set
        order_pane.order_mode = mode

        # select a pp to track
        tracker = trackers[pp_account]
        mode.current_pp = tracker
        tracker.show()
        tracker.hide_info()

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

        # make fill bar and positioning snapshot
        order_pane.on_ui_settings_change('limit', tracker.alloc.limit())
        order_pane.update_status_ui(pp=tracker)

        # TODO: create a mode "manager" of sorts?
        # -> probably just call it "UxModes" err sumthin?
        # so that view handlers can access it
        view.order_mode = mode

        order_pane.on_ui_settings_change('account', pp_account)
        mode.pane.display_pnl(mode.current_pp)

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

            tn.start_soon(
                process_trades_and_update_ui,
                tn,
                feed,
                mode,
                trades_stream,
                book,
            )
            yield mode


async def process_trades_and_update_ui(

    n: trio.Nursery,
    feed: Feed,
    mode: OrderMode,
    trades_stream: tractor.MsgStream,
    book: OrderBook,

) -> None:

    get_index = mode.chart.get_index
    global _pnl_tasks

    # this is where we receive **back** messages
    # about executions **from** the EMS actor
    async for msg in trades_stream:

        fmsg = pformat(msg)
        log.info(f'Received order msg:\n{fmsg}')

        name = msg['name']
        if name in (
            'position',
        ):
            sym = mode.chart.linked.symbol
            if msg['symbol'].lower() in sym.key:

                tracker = mode.trackers[msg['account']]
                tracker.live_pp.update_from_msg(msg)
                # update order pane widgets
                tracker.update_from_pp()
                mode.pane.update_status_ui(tracker)

                if tracker.live_pp.size:
                    # display pnl
                    mode.pane.display_pnl(tracker)

            # short circuit to next msg to avoid
            # unnecessary msg content lookups
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
            'broker_errored',
            'dark_cancelled'
        ):
            # delete level line from view
            mode.on_cancel(oid)
            broker_msg = msg['brokerd_msg']
            log.warning(f'Order {oid} failed with:\n{pformat(broker_msg)}')

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

            # TODO: how should we look this up?
            # tracker = mode.trackers[msg['account']]
            # tracker.live_pp.fills.append(msg)
