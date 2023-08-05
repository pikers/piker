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

"""
Chart trading, the only way to scalp.

"""
from __future__ import annotations
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import partial
from pprint import pformat
import time
from typing import (
    Callable,
    Any,
    TYPE_CHECKING,
)
import uuid

from bidict import bidict
import tractor
import trio
from PyQt5.QtCore import Qt

from piker import config
from piker.accounting import (
    Allocator,
    Position,
    mk_allocator,
    MktPair,
    Symbol,
)
from piker.clearing import (
    open_ems,
    OrderClient,
)
from piker.clearing._messages import (
    Order,
    Status,
    BrokerdPosition,
)
from piker.data import (
    Feed,
    Flume,
)
from piker.types import Struct
from piker.log import get_logger
from ._editors import LineEditor, ArrowEditor
from ._lines import order_line, LevelLine
from ._position import (
    PositionTracker,
    SettingsPane,
)
from ._forms import FieldsForm
from ._window import MultiStatus
from ._style import _font
from ._forms import open_form_input_handling
from ._notify import notify_from_ems_status_msg


if TYPE_CHECKING:
    from ._chart import (
        ChartPlotWidget,
        GodWidget,
    )

log = get_logger(__name__)


class Dialog(Struct):
    '''
    Trade dialogue meta-data describing the lifetime
    of an order submission to ``emsd`` from a chart.

    '''
    # TODO: use ``pydantic.UUID4`` field
    uuid: str
    order: Order
    symbol: str
    lines: list[LevelLine]
    last_status_close: Callable | None = None
    msgs: dict[str, dict] = {}
    fills: dict[str, Any] = {}


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
    godw: GodWidget
    feed: Feed
    chart: ChartPlotWidget  #  type: ignore # noqa
    hist_chart: ChartPlotWidget  #  type: ignore # noqa
    nursery: trio.Nursery  # used by ``ui._position`` code?
    client: OrderClient
    lines: LineEditor
    arrows: ArrowEditor
    multistatus: MultiStatus
    pane: SettingsPane
    trackers: dict[str, PositionTracker]

    # switched state, the current position
    current_pp: PositionTracker | None = None
    active: bool = False
    name: str = 'order'
    dialogs: dict[str, Dialog] = field(default_factory=dict)

    _colors = {
        'alert': 'alert_yellow',
        'buy': 'buy_green',
        'sell': 'sell_red',
    }
    _staged_order: Order | None = None

    def on_level_change_update_next_order_info(
        self,
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
        ``OrderMode.new_line_from_order()``

        '''
        # NOTE: the ``Order.account`` is set at order stage time inside
        # ``OrderMode.new_line_from_order()`` or is inside ``Order`` msg
        # field for loaded orders.
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

        # when an order is changed we flip the settings side-pane to
        # reflect the corresponding account and pos info.
        self.pane.on_ui_settings_change('account', order.account)

    def new_line_from_order(
        self,
        order: Order,
        chart: ChartPlotWidget | None = None,
        **line_kwargs,

    ) -> LevelLine:

        level = order.price

        line = order_line(
            chart or self.chart,
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
                self.on_level_change_update_next_order_info,
                line=line,
                order=order,
                # use the corresponding position tracker for the
                # order's account.
                tracker=self.trackers[order.account],
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

    def lines_from_order(
        self,
        order: Order,
        **line_kwargs,

    ) -> list[LevelLine]:

        lines: list[LevelLine] = []
        for chart, kwargs in [
            (self.chart, {}),
            (self.hist_chart, {'only_show_markers_on_hover': True}),
        ]:
            kwargs.update(line_kwargs)
            line = self.new_line_from_order(
                order=order,
                chart=chart,
                **kwargs,
            )
            lines.append(line)

        return lines

    def stage_order(
        self,

        action: str,
        trigger_type: str,

    ) -> list[LevelLine]:
        '''
        Stage an order for submission by showing level lines and
        configuring the order request message dynamically based on
        allocator settings.

        '''
        # not initialized yet
        cursor = self.godw.get_cursor()
        if not cursor:
            return

        chart = cursor.linked.chart
        if (
            not chart
            and cursor
            and cursor.active_plot
        ):
            return

        chart = cursor.active_plot
        price = cursor._datum_xy[1]
        if not price:
            # zero prices are not supported by any means
            # since that's illogical / a no-op.
            return

        mkt: MktPair = self.chart.linked.mkt

        # NOTE : we could also use instead,
        # mkt.quantize(price, quantity_type='price')
        # but it returns a Decimal and it's probably gonna
        # be slower?
        # TODO: should we be enforcing this precision
        # at a different layer in the stack? right now
        # any precision error will literally be relayed
        # all the way back from the backend.

        price = round(
            price,
            ndigits=mkt.price_tick_digits,
        )

        order = self._staged_order = Order(
            action=action,
            price=price,
            account=self.current_pp.alloc.account,
            size=0,
            symbol=mkt.fqme,
            brokers=[mkt.broker],
            oid='',  # filled in on submit
            exec_mode=trigger_type,  # dark or live
        )

        # TODO: staged line mirroring? - need to keep track of multiple
        # staged lines in editor - need to call
        # `LineEditor.unstage_line()` on all staged lines..
        # lines = self.lines_from_order(

        line = self.new_line_from_order(
            order,
            chart=chart,
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
        self.lines.stage_line(line)

        # add line to cursor trackers
        cursor._trackers.add(line)

        # TODO: see above about mirroring.
        # for line in lines:
        #     if line._chart is chart:
        #         self.lines.stage_line(line)
        #         cursor._trackers.add(line)
        #         break

        # hide crosshair y-line and label
        cursor.hide_xhair()

        return line

    def submit_order(
        self,
        send_msg: bool = True,
        order: Order | None = None,

    ) -> Dialog:
        '''
        Send execution order to EMS return a level line to
        represent the order on a chart.

        '''
        if not order:
            staged: Order = self._staged_order

            # apply order fields for ems
            oid = str(uuid.uuid4())

            # NOTE: we have to str-ify `MktPair` first since we can't
            # cast to it without being mega explicit with
            # `msgspec.Struct`, which we're not yet..
            order: Order = staged.copy({
                'symbol': str(staged.symbol),
                'oid': oid,
            })

        lines = self.lines_from_order(
            order,
            show_markers=True,
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
        self.lines.submit_lines(
            lines=lines,
            uuid=order.oid,
        )

        dialog = Dialog(
            uuid=order.oid,
            order=order,
            symbol=order.symbol,  # XXX: always a str?
            lines=lines,
            last_status_close=self.multistatus.open_status(
                f'submitting {order.exec_mode}-{order.action}',
                final_msg=f'submitted {order.exec_mode}-{order.action}',
                clear_on_next=True,
            )
        )
        # enter submission which will be popped once a response
        # from the EMS is received to move the order to a different# status
        self.dialogs[order.oid] = dialog

        for line in lines:

            # TODO: create a new ``OrderLine`` with this optional var defined
            line.dialog = dialog

            # hook up mouse drag handlers
            line._on_drag_start = self.order_line_modify_start
            line._on_drag_end = self.order_line_modify_complete

        # send order cmd to ems
        if send_msg:
            self.client.send_nowait(order)
        else:
            # just register for control over this order
            # TODO: some kind of mini-perms system here based on
            # an out-of-band tagging/auth sub-sys for multiplayer
            # order control?
            self.client._sent_orders[order.oid] = order

        return dialog

    # order-line modify handlers

    def order_line_modify_start(
        self,
        line: LevelLine,

    ) -> None:

        log.info(f'Order modify: {line}')
        # cancel original order until new position is found?
        # TODO: make a config option for this behaviour..

    def order_line_modify_complete(
        self,
        line: LevelLine,

    ) -> None:
        '''
        Retreive the level line's end state, compute the size
        and price for the new price-level, send an update msg to
        the EMS, adjust mirrored level line on secondary chart.

        '''
        mktinfo: MktPair = self.chart.linked.mkt
        level = round(
            line.value(),
            ndigits=mktinfo.price_tick_digits,
        )
        # updated by level change callback set in ``.new_line_from_order()``
        dialog = line.dialog
        size = dialog.order.size

        # NOTE: sends modified order msg to EMS
        self.client.update_nowait(
            uuid=line.dialog.uuid,
            price=level,
            size=size,
        )

        # adjust corresponding slow/fast chart line
        # to match level
        for ln in dialog.lines:
            if ln is not line:
                ln.set_level(line.value())

    # EMS response msg handlers
    def on_submit(
        self,
        uuid: str,
        order: Order | None = None,

    ) -> Dialog:
        '''
        Order submitted status event handler.

        Commit the order line and registered order uuid, store ack time stamp.

        '''
        lines = self.lines.commit_line(uuid)

        # a submission is the start of a new order dialog
        dialog = self.dialogs[uuid]
        dialog.lines = lines
        cls: Callable | None = dialog.last_status_close
        if cls:
            cls()

        for line in lines:

            # if an order msg is provided update the line
            # **from** that msg.
            if order:
                line.set_level(order.price)
                self.on_level_change_update_next_order_info(
                    level=order.price,
                    line=line,
                    order=order,
                    # use the corresponding position tracker for the
                    # order's account.
                    tracker=self.trackers[order.account],
                )

            # hide any lines not currently moused-over
            if not line.get_cursor():
                line.hide_labels()

        return dialog

    def on_fill(
        self,

        uuid: str,
        price: float,
        time_s: float,

        pointing: str | None = None,

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
        lines = dialog.lines
        chart = self.chart

        # XXX: seems to fail on certain types of races?
        # assert len(lines) == 2
        if lines:
            flume: Flume = self.feed.flumes[chart.linked.mkt.fqme]
            _, _, ratio = flume.get_ds_info()

            for chart, shm in [
                (self.chart, flume.rt_shm),
                (self.hist_chart, flume.hist_shm),
            ]:
                viz = chart.get_viz(chart.name)
                index_field = viz.index_field
                arr = shm.array

                # TODO: borked for int index based..
                index = flume.get_index(time_s, arr)

                # get absolute index for arrow placement
                arrow_index = arr[index_field][index]

                self.arrows.add(
                    chart.plotItem,
                    uuid,
                    arrow_index,
                    price,
                    pointing=pointing,
                    color=lines[0].color
                )
        else:
            log.warn("No line(s) for order {uuid}!?")

    def on_cancel(
        self,
        uuid: str

    ) -> None:

        msg: Order = self.client._sent_orders.pop(uuid, None)

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
        return self.cancel_orders(
            self.oids_from_lines(
                self.lines.lines_under_cursor()
            )
        )

    def oids_from_lines(
        self,
        lines: list[LevelLine],

    ) -> list[Dialog]:

        oids: set[str] = set()
        for line in lines:
            dialog: Dialog = getattr(line, 'dialog', None)
            oid: str = dialog.uuid
            if (
                dialog
                and oid not in oids
            ):
                oids.add(oid)

        return oids

    def cancel_orders(
        self,
        oids: list[str],

    ) -> None:
        '''
        Cancel all orders from a list of order ids: `oids`.

        '''
        key = self.multistatus.open_status(
            f'cancelling {len(oids)} orders',
            final_msg=f'cancelled orders:\n{oids}',
            group_key=True
        )
        for oid in oids:
            if dialog := self.dialogs.get(oid):
                self.client.cancel_nowait(uuid=oid)
                cancel_status_close = self.multistatus.open_status(
                    f'cancelling order {oid}',
                    group_key=key,
                )
                dialog.last_status_close = cancel_status_close

    def cancel_all_orders(self) -> None:
        '''
        Cancel all unique orders / executions by extracting unique
        order ids from all order lines and then submitting cancel
        requests for each dialog.

        '''
        return self.cancel_orders(
            self.oids_from_lines(
                self.lines.all_lines()
            )
        )

    def load_unknown_dialog_from_msg(
        self,
        msg: Status,

    ) -> Dialog:
        # NOTE: the `.order` attr **must** be set with the
        # equivalent order msg in order to be loaded.
        order = msg.req
        oid = str(msg.oid)
        symbol = order.symbol

        # TODO: MEGA UGGG ZONEEEE!
        src = msg.src
        if (
            src
            and src not in ('dark', 'paperboi')
            and src not in symbol
        ):
            fqme = symbol + '.' + src
            brokername = src
        else:
            fqme = symbol
            *head, brokername = fqme.rsplit('.')

        # fill out complex fields
        order.oid = str(order.oid)
        order.brokers = [brokername]

        # TODO: change this over to `MktPair`, but it's
        # gonna be tough since we don't have any such data
        # really in our clearing msg schema..
        order.symbol = Symbol.from_fqme(
            fqsn=fqme,
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
    godw: GodWidget,
    fqme: str,
    started: trio.Event,
    loglevel: str = 'info'

) -> None:
    '''
    Activate chart-trader order mode loop:

      - connect to emsd
      - load existing positions
      - begin EMS response handling loop which updates local
        state, mostly graphics / UI.

    '''
    chart = godw.rt_linked.chart
    hist_chart = godw.hist_linked.chart

    multistatus = chart.window().status_bar
    done = multistatus.open_status('starting order mode..')

    client: OrderClient
    trades_stream: tractor.MsgStream

    # The keys in this dict **must** be in set our set of "normalized"
    # symbol names (i.e. the same names you'd get back in search
    # results) in order for position msgs to correctly trigger the
    # display of a position indicator on screen.
    position_msgs: dict[str, dict[str, BrokerdPosition]]

    # spawn EMS actor-service
    async with (
        open_ems(
            fqme,
            loglevel=loglevel,
        ) as (
            client,
            trades_stream,
            position_msgs,
            brokerd_accounts,
            ems_dialog_msgs,
        ),
        trio.open_nursery() as tn,

    ):
        log.info(f'Opening order mode for {fqme}')

        # annotations editors
        lines = LineEditor(godw=godw)
        arrows = ArrowEditor(godw=godw)

        # market endpoint info
        mkt: MktPair = chart.linked.mkt

        # map of per-provider account keys to position tracker instances
        trackers: dict[str, PositionTracker] = {}

        # load account names from ``brokers.toml``
        accounts_def: bidict[str, str | None] = config.load_accounts(
            providers=[mkt.broker],
        )

        # XXX: ``brokerd`` delivers a set of account names that it
        # allows use of but the user also can define the accounts they'd
        # like to use, in order, in their `brokers.toml` file.
        accounts: dict[str, str] = {}
        for name in brokerd_accounts:
            # ensure name is in ``brokers.toml``
            accounts[name] = accounts_def[name]

        # always add a paper entry so that paper cleared
        # order dialogs can be tracked in the order mode UIs.
        accounts['paper'] = 'paper'

        # first account listed is the one we select at startup
        # (aka order based selection).
        pp_account = next(
            # choose first account based on line order from `brokers.toml`.
            iter(accounts.keys())
        ) if accounts else 'paper'

        # update pp trackers with data relayed from ``brokerd``.
        for account_name in accounts:

            # net-zero pp
            startup_pp = Position(
                mkt=mkt,

                # XXX: BLEH, do we care about this on the client side?
                bs_mktid=mkt.key,
            )

            # allocator config
            alloc: Allocator = mk_allocator(
                mkt=mkt,
                account=account_name,

                # if this startup size is greater the allocator limit,
                # the limit is increased internally in this factory.
                startup_pp=startup_pp,
            )

            pp_tracker = PositionTracker(
                [chart, hist_chart],
                alloc,
                startup_pp
            )
            pp_tracker.nav.hide()
            trackers[account_name] = pp_tracker

            assert pp_tracker.startup_pp.cumsize == pp_tracker.live_pp.cumsize

            # TODO: do we even really need the "startup pp" or can we
            # just take the max and pass that into the some state / the
            # alloc?
            pp_tracker.update_from_pp()

            # on existing position, show pp tracking graphics
            if pp_tracker.startup_pp.cumsize != 0:
                pp_tracker.nav.show()
                pp_tracker.nav.hide_info()

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
            godw,
            feed,
            chart,
            hist_chart,
            tn,
            client,
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
        tracker.nav.show()
        tracker.nav.hide_info()

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
        order_pane.update_status_ui(tracker)

        # TODO: create a mode "manager" of sorts?
        # -> probably just call it "UxModes" err sumthin?
        # so that view handlers can access it
        chart.view.order_mode = mode
        hist_chart.view.order_mode = mode

        order_pane.on_ui_settings_change('account', pp_account)
        mode.pane.display_pnl(mode.current_pp)

        # Begin order-response streaming
        done()

        # Pack position messages by account, should only be one-to-one.
        # NOTE: requires the backend exactly specifies
        # the expected symbol key in its positions msg.
        for (
            (broker, acctid),
            pps_by_fqme
        ) in position_msgs.items():
            for msg in pps_by_fqme.values():
                await process_trade_msg(
                    mode,
                    client,
                    msg,
                )

        # start async input handling for chart's view
        async with (

            # ``ChartView`` input async handler startup
            chart.view.open_async_input_handler(),
            hist_chart.view.open_async_input_handler(),

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
                    client,
                    msg,
                )

            tn.start_soon(
                process_trades_and_update_ui,
                trades_stream,
                mode,
                client,
            )

            yield mode


async def process_trades_and_update_ui(

    trades_stream: tractor.MsgStream,
    mode: OrderMode,
    client: OrderClient,

) -> None:

    # this is where we receive **back** messages
    # about executions **from** the EMS actor
    async for msg in trades_stream:
        await process_trade_msg(
            mode,
            client,
            msg,
        )


async def process_trade_msg(
    mode: OrderMode,
    client: OrderClient,
    msg: dict,

    # emit linux DE notification?
    # XXX: currently my experience with `dunst` is that this
    # is horrible slow and clunky and invasive and noisy so i'm
    # disabling it for now until we find a better UX solution..
    do_notify: bool = False,

) -> tuple[Dialog, Status]:

    fmsg = pformat(msg)
    log.debug(f'Received order msg:\n{fmsg}')
    name = msg['name']

    if name in (
        'position',
    ):
        sym: MktPair = mode.chart.linked.mkt
        pp_msg_symbol = msg['symbol'].lower()
        fqme = sym.fqme
        broker = sym.broker
        if (
            pp_msg_symbol == fqme
            or pp_msg_symbol == fqme.removesuffix(f'.{broker}')
        ):
            log.info(
                f'Loading position for `{fqme}`:\n'
                f'{fmsg}'
            )
            tracker = mode.trackers[msg['account']]
            tracker.live_pp.update_from_msg(msg)
            tracker.update_from_pp(
                set_as_startup=True,
            )
            # status/pane UI
            mode.pane.update_status_ui(tracker)

            if tracker.live_pp.cumsize:
                # display pnl
                mode.pane.display_pnl(tracker)

        # short circuit to next msg to avoid
        # unnecessary msg content lookups
        return

    msg = Status(**msg)
    resp = msg.resp
    oid = msg.oid
    dialog: Dialog = mode.dialogs.get(oid)

    if dialog:
        fqme = dialog.symbol

    match msg:
        case Status(
            resp='dark_open' | 'open',
        ) if msg.req['action'] != 'cancel':

            order = Order(**msg.req)

            if (
                dialog is not None
                and order.action != 'cancel'
            ):
                # show line label once order is live
                mode.on_submit(oid, order=order)

            elif order.action != 'cancel':
                log.warning(
                    f'received msg for untracked dialog:\n{fmsg}'
                )
                assert msg.resp in ('open', 'dark_open'), f'Unknown msg: {msg}'

                sym: MktPair = mode.chart.linked.mkt
                fqme = sym.fqme
                if (
                    ((order.symbol + f'.{msg.src}') == fqme)

                    # a existing dark order for the same symbol
                    or (
                        order.symbol == fqme
                        and (
                            msg.src in ('dark', 'paperboi')
                            or (msg.src in fqme)

                        )
                    )
                ):
                    msg.req = order
                    dialog = mode.load_unknown_dialog_from_msg(msg)
                    mode.on_submit(oid)

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
            action = msg.req["action"]
            log.cancel(f'Canceled {action}:{oid}')

        case Status(
            resp='triggered',
            # req=Order(exec_mode='dark')  # TODO: msgspec
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
            tm = time.time()
            mode.on_fill(
                oid,
                price=req.price,
                time_s=tm,
            )
            mode.lines.remove_line(uuid=oid)
            msg.req = req
            if do_notify:
                await notify_from_ems_status_msg(msg)

        # response to completed 'dialog' for order request
        case Status(
            resp='closed',
            # req=Order() as req,  # TODO
            req=req,
        ):
            msg.req = Order(**req)
            if do_notify:
                await notify_from_ems_status_msg(msg)
            mode.lines.remove_line(uuid=oid)

        # each clearing tick is responded individually
        case Status(resp='fill'):

            # handle out-of-piker fills reporting?
            order: Order | None
            if not (order := client._sent_orders.get(oid)):

                # set it from last known request msg
                log.warning(f'order {oid} is unknown')
                order = msg.req

            # XXX TODO: have seen order be a dict here!?
            # that should never happen tho?
            action: str = order.action
            details: dict = msg.brokerd_msg

            # TODO: state tracking:
            # - put the actual exchange timestamp?
            # - some kinda progress system?

            # NOTE: currently the ``kraken`` openOrders sub
            # doesn't deliver their engine timestamp as part of
            # it's schema, so this value is **not** from them
            # (see our backend code). We should probably either
            # include all provider-engine timestamps in the
            # summary 'closed' status msg and/or figure out
            # a way to indicate what is a `brokerd` stamp versus
            # a true backend one? This will require finagling
            # with how each backend tracks/summarizes time
            # stamps for the downstream API.
            tm = details['broker_time']
            mode.on_fill(
                oid,
                price=details['price'],
                time_s=tm,
                pointing='up' if action == 'buy' else 'down',
            )

            # TODO: append these fill events to the position's clear
            # table?

            # tracker = mode.trackers[msg['account']]
            # tracker.live_pp.fills.append(msg)

    # record message to dialog tracking
    if dialog:
        dialog.msgs[oid] = msg

    return dialog, msg
