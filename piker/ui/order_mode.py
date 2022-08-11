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
import platform
import time
from typing import Optional, Dict, Callable, Any
import uuid

import tractor
import trio
from PyQt5.QtCore import Qt

from .. import config
from ..pp import Position
from ..clearing._client import open_ems, OrderBook
from ..clearing._allocate import (
    mk_allocator,
)
from ._style import _font
from ..data._source import Symbol
from ..data.feed import Feed
from ..data.types import Struct
from ..log import get_logger
from ._editors import LineEditor, ArrowEditor
from ._lines import order_line, LevelLine
from ._position import (
    PositionTracker,
    SettingsPane,
)
from ._forms import FieldsForm
from ._window import MultiStatus
from ..clearing._messages import (
    Order,
    Status,
    # BrokerdOrder,
    # BrokerdStatus,
    BrokerdPosition,
)
from ._forms import open_form_input_handling


log = get_logger(__name__)


class Dialog(Struct):
    '''
    Trade dialogue meta-data describing the lifetime
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


def on_level_change_update_next_order_info(

    level: float,

    # these are all ``partial``-ed in at callback assignment time.
    line: LevelLine,
    order: Order,
    tracker: PositionTracker,

) -> None:
    '''
    A callback applied for each level change to the line
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
    '''
    Major UX mode for placing orders on a chart view providing so
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
    dialogs: dict[str, Dialog] = field(default_factory=dict)

    _colors = {
        'alert': 'alert_yellow',
        'buy': 'buy_green',
        'sell': 'sell_red',
    }
    _staged_order: Optional[Order] = None

    def line_from_order(
        self,
        order: Order,
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
                order.exec_mode == 'dark'
                and order.action != 'alert'
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
        send_msg: bool = True,
        order: Optional[Order] = None,

    ) -> Dialog:
        '''
        Send execution order to EMS return a level line to
        represent the order on a chart.

        '''
        if not order:
            staged = self._staged_order
            oid = str(uuid.uuid4())
            # symbol: Symbol = staged.symbol

            # format order data for ems
            order = staged.copy()
            order.oid = oid

        order.symbol = order.symbol.front_fqsn()

        line = self.line_from_order(
            order,

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
            uuid=order.oid,
        )

        dialog = Dialog(
            uuid=order.oid,
            order=order,
            symbol=order.symbol,
            line=line,
            last_status_close=self.multistatus.open_status(
                f'submitting {order.exec_mode}-{order.action}',
                final_msg=f'submitted {order.exec_mode}-{order.action}',
                clear_on_next=True,
            )
        )

        # TODO: create a new ``OrderLine`` with this optional var defined
        line.dialog = dialog

        # enter submission which will be popped once a response
        # from the EMS is received to move the order to a different# status
        self.dialogs[order.oid] = dialog

        # hook up mouse drag handlers
        line._on_drag_start = self.order_line_modify_start
        line._on_drag_end = self.order_line_modify_complete

        # send order cmd to ems
        if send_msg:
            self.book.send(order)
        else:
            # just register for control over this order
            # TODO: some kind of mini-perms system here based on
            # an out-of-band tagging/auth sub-sys for multiplayer
            # order control?
            self.book._sent_orders[order.oid] = order

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

    ) -> Dialog:
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
        msg: Status,

    ) -> None:

        # DESKTOP NOTIFICATIONS
        #
        # TODO: this in another task?
        # not sure if this will ever be a bottleneck,
        # we probably could do graphics stuff first tho?

        # TODO: make this not trash.
        # XXX: linux only for now
        if platform.system() == "Windows":
            return

        result = await trio.run_process(
            [
                'notify-send',
                '-u', 'normal',
                '-t', '1616',
                'piker',

                # TODO: add in standard fill/exec info that maybe we
                # pack in a broker independent way?
                f'{msg.resp}: {msg.req.price}',
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
                        f'cancelling order {oid}',
                        group_key=key,
                    )
                    dialog.last_status_close = cancel_status_close

                    ids.append(oid)
                    self.book.cancel(uuid=oid)

        return ids

    def load_unknown_dialog_from_msg(
        self,
        msg: Status,

    ) -> Dialog:

        # NOTE: the `.order` attr **must** be set with the
        # equivalent order msg in order to be loaded.
        order = Order(**msg.req)
        oid = str(msg.oid)
        symbol = order.symbol

        # TODO: MEGA UGGG ZONEEEE!
        src = msg.src
        if (
            src
            and src != 'dark'
            and src not in symbol
        ):
            fqsn = symbol + '.' + src
            brokername = src
        else:
            fqsn = symbol
            *head, brokername = fqsn.rsplit('.')

        # fill out complex fields
        order.oid = str(order.oid)
        order.brokers = [brokername]
        order.symbol = Symbol.from_fqsn(
            fqsn=fqsn,
            info={},
        )
        dialog = self.submit_order(
            send_msg=False,
            order=order,
        )
        assert self.dialogs[oid] == dialog
        return dialog


@asynccontextmanager
async def open_order_mode(

    feed: Feed,
    chart: 'ChartPlotWidget',  # noqa
    fqsn: str,
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
        open_ems(fqsn) as (
            book,
            trades_stream,
            position_msgs,
            brokerd_accounts,
            ems_dialog_msgs,
        ),
        trio.open_nursery() as tn,

    ):
        log.info(f'Opening order mode for {fqsn}')
        view = chart.view

        # annotations editors
        lines = LineEditor(chart=chart)
        arrows = ArrowEditor(chart, {})

        # symbol id
        symbol = chart.linked.symbol
        symkey = symbol.front_fqsn()

        # map of per-provider account keys to position tracker instances
        trackers: dict[str, PositionTracker] = {}

        # load account names from ``brokers.toml``
        accounts_def = config.load_accounts(
            providers=symbol.brokers
        )

        # XXX: ``brokerd`` delivers a set of account names that it
        # allows use of but the user also can define the accounts they'd
        # like to use, in order, in their `brokers.toml` file.
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

        # Pack position messages by account, should only be one-to-one.
        # NOTE: requires the backend exactly specifies
        # the expected symbol key in its positions msg.
        pps_by_account = {}
        for (broker, acctid), msgs in position_msgs.items():
            for msg in msgs:

                sym = msg['symbol']
                if (
                    (sym == symkey) or (
                        # mega-UGH, i think we need to fix the FQSN
                        # stuff sooner then later..
                        sym == symkey.removesuffix(f'.{broker}'))
                ):
                    pps_by_account[acctid] = msg

        # update pp trackers with data relayed from ``brokerd``.
        for account_name in accounts:

            # net-zero pp
            startup_pp = Position(
                symbol=symbol,
                size=0,
                ppu=0,

                # XXX: BLEH, do we care about this on the client side?
                bsuid=symbol,
            )
            msg = pps_by_account.get(account_name)
            if msg:
                log.info(f'Loading pp for {symkey}:\n{pformat(msg)}')
                startup_pp.update_from_msg(msg)

            # allocator config
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
        form: FieldsForm = chart.sidepane
        form.vbox.setSpacing(
            int((1 + 5 / 8) * _font.px_size)
        )

        from ._feedstatus import mk_feed_label

        feed_label = mk_feed_label(
            form,
            feed,
            chart,
        )

        # XXX: we set this because?
        form.feed_label = feed_label
        order_pane = SettingsPane(
            form=form,
            # XXX: ugh, so hideous...
            fill_bar=form.fill_bar,
            pnl_label=form.left_label,
            step_label=form.bottom_label,
            limit_label=form.top_label,
        )
        order_pane.set_accounts(list(trackers.keys()))

        form.vbox.addWidget(
            feed_label,
            alignment=Qt.AlignBottom,
        )

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
        tracker: PositionTracker = trackers[pp_account]
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

            for oid, msg in ems_dialog_msgs.items():

                # HACK ALERT: ensure a resp field is filled out since
                # techincally the call below expects a ``Status``. TODO:
                # parse into proper ``Status`` equivalents ems-side?
                # msg.setdefault('resp', msg['broker_details']['resp'])
                # msg.setdefault('oid', msg['broker_details']['oid'])
                msg['brokerd_msg'] = msg

                await process_trade_msg(
                    mode,
                    book,
                    msg,
                )

            tn.start_soon(
                process_trades_and_update_ui,
                trades_stream,
                mode,
                book,
            )

            yield mode


async def process_trades_and_update_ui(

    trades_stream: tractor.MsgStream,
    mode: OrderMode,
    book: OrderBook,

) -> None:

    # this is where we receive **back** messages
    # about executions **from** the EMS actor
    async for msg in trades_stream:
        await process_trade_msg(
            mode,
            book,
            msg,
        )


async def process_trade_msg(
    mode: OrderMode,
    book: OrderBook,
    msg: dict,

) -> tuple[Dialog, Status]:

    get_index = mode.chart.get_index
    fmsg = pformat(msg)
    log.debug(f'Received order msg:\n{fmsg}')
    name = msg['name']

    if name in (
        'position',
    ):
        sym = mode.chart.linked.symbol
        pp_msg_symbol = msg['symbol'].lower()
        fqsn = sym.front_fqsn()
        broker, key = sym.front_feed()
        if (
            pp_msg_symbol == fqsn
            or pp_msg_symbol == fqsn.removesuffix(f'.{broker}')
        ):
            log.info(f'{fqsn} matched pp msg: {fmsg}')
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
        return

    msg = Status(**msg)
    resp = msg.resp
    oid = msg.oid
    dialog: Dialog = mode.dialogs.get(oid)

    match msg:
        case Status(resp='dark_open' | 'open'):

            if dialog is not None:
                # show line label once order is live
                mode.on_submit(oid)

            else:
                log.warning(
                    f'received msg for untracked dialog:\n{fmsg}'
                )
                assert msg.resp in ('open', 'dark_open'), f'Unknown msg: {msg}'

                sym = mode.chart.linked.symbol
                fqsn = sym.front_fqsn()
                order = Order(**msg.req)
                if (
                    ((order.symbol + f'.{msg.src}') == fqsn)

                    # a existing dark order for the same symbol
                    or (
                        order.symbol == fqsn
                        and (msg.src == 'dark') or (msg.src in fqsn)
                    )
                ):
                    dialog = mode.load_unknown_dialog_from_msg(msg)
                    mode.on_submit(oid)
                    # return dialog, msg

        case Status(resp='error'):
            # delete level line from view
            mode.on_cancel(oid)
            broker_msg = msg.brokerd_msg
            log.error(
                f'Order {oid}->{resp} with:\n{pformat(broker_msg)}'
            )

        case Status(resp='canceled'):
            # delete level line from view
            mode.on_cancel(oid)
            req = Order(**msg.req)
            log.cancel(f'Canceled {req.action}:{oid}')

        case Status(
            resp='triggered',
            # req=Order(exec_mode='dark')  # TODO:
            req={'exec_mode': 'dark'},
        ):
            # TODO: UX for a "pending" clear/live order
            log.info(f'Dark order triggered for {fmsg}')

        case Status(
            resp='triggered',
            # req=Order(exec_mode='live', action='alert') as req, # TODO
            req={'exec_mode': 'live', 'action': 'alert'} as req,
        ):
            # should only be one "fill" for an alert
            # add a triangle and remove the level line
            req = Order(**req)
            mode.on_fill(
                oid,
                price=req.price,
                arrow_index=get_index(time.time()),
            )
            mode.lines.remove_line(uuid=oid)
            msg.req = req
            await mode.on_exec(oid, msg)

        # response to completed 'dialog' for order request
        case Status(
            resp='closed',
            # req=Order() as req,  # TODO
            req=req,
        ):
            msg.req = Order(**req)
            await mode.on_exec(oid, msg)
            mode.lines.remove_line(uuid=oid)

        # each clearing tick is responded individually
        case Status(resp='fill'):

            # handle out-of-piker fills reporting?
            known_order = book._sent_orders.get(oid)
            if not known_order:
                log.warning(f'order {oid} is unknown')
                return

            action = known_order.action
            details = msg.brokerd_msg

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

    # record message to dialog tracking
    if dialog:
        dialog.msgs[oid] = msg

    return dialog, msg
