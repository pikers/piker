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
Position info and display

"""
from __future__ import annotations
from copy import copy
from dataclasses import dataclass
from functools import partial
from math import floor, copysign
from typing import (
    Callable,
    Optional,
    TYPE_CHECKING,
)


# from PyQt5.QtWidgets import QStyle
# from PyQt5.QtGui import (
#     QIcon, QPixmap, QColor
# )
from pyqtgraph import functions as fn

from ._annotate import LevelMarker
from ._anchors import (
    pp_tight_and_right,  # wanna keep it straight in the long run
    gpath_pin,
)
from ..calc import humanize, pnl, puterize
from ..clearing._allocate import Allocator, Position
from ..data._normalize import iterticks
from ..data.feed import Feed
from ..data.types import Struct
from ._label import Label
from ._lines import LevelLine, order_line
from ._style import _font
from ._forms import FieldsForm, FillStatusBar, QLabel
from ..log import get_logger

if TYPE_CHECKING:
    from ._chart import (
        ChartPlotWidget,
    )

log = get_logger(__name__)
_pnl_tasks: dict[str, bool] = {}


async def update_pnl_from_feed(

    feed: Feed,
    order_mode: OrderMode,  # noqa
    tracker: PositionTracker,

) -> None:
    '''Real-time display the current pp's PnL in the appropriate label.

    ``ValueError`` if this task is spawned where there is a net-zero pp.

    '''
    global _pnl_tasks

    pp = order_mode.current_pp
    live = pp.live_pp
    key = live.symbol.key

    log.info(f'Starting pnl display for {pp.alloc.account}')

    if live.size < 0:
        types = ('ask', 'last', 'last', 'dark_trade')

    elif live.size > 0:
        types = ('bid', 'last', 'last', 'dark_trade')

    else:
        log.info(f'No position (yet) for {tracker.alloc.account}@{key}')
        return

    # real-time update pnl on the status pane
    try:
        async with feed.stream.subscribe() as bstream:
            # last_tick = time.time()
            async for quotes in bstream:

                # now = time.time()
                # period = now - last_tick

                for sym, quote in quotes.items():

                    for tick in iterticks(quote, types):
                        # print(f'{1/period} Hz')

                        size = order_mode.current_pp.live_pp.size
                        if size == 0:
                            # terminate this update task since we're
                            # no longer in a pp
                            order_mode.pane.pnl_label.format(pnl=0)
                            return

                        else:
                            # compute and display pnl status
                            order_mode.pane.pnl_label.format(
                                pnl=copysign(1, size) * pnl(
                                    # live.ppu,
                                    order_mode.current_pp.live_pp.ppu,
                                    tick['price'],
                                ),
                            )

                        # last_tick = time.time()
    finally:
        assert _pnl_tasks[key]
        assert _pnl_tasks.pop(key)


@dataclass
class SettingsPane:
    '''
    Composite set of widgets plus an allocator model for configuring
    order entry sizes and position limits per tradable instrument.

    '''
    # input fields
    form: FieldsForm

    # output fill status and labels
    fill_bar: FillStatusBar

    step_label: QLabel
    pnl_label: QLabel
    limit_label: QLabel

    # encompasing high level namespace
    order_mode: Optional['OrderMode'] = None  # typing: ignore # noqa

    def set_accounts(
        self,
        names: list[str],
        sizes: Optional[list[float]] = None,
    ) -> None:

        combo = self.form.fields['account']
        return combo.set_items(names)

    def on_selection_change(
        self,
        text: str,
        key: str,

    ) -> None:
        '''
        Called on any order pane drop down selection change.

        '''
        log.info(f'selection input {key}:{text}')
        self.on_ui_settings_change(key, text)

    def on_ui_settings_change(
        self,

        key: str,
        value: str,

    ) -> None:
        '''
        Try to apply some input setting (by the user), revert to
        previous setting if it fails display new value if applied.

        '''
        self.apply_setting(key, value)
        self.update_status_ui(pp=self.order_mode.current_pp)

    def apply_setting(
        self,

        key: str,
        value: str,

    ) -> bool:
        '''
        Called on any order pane edit field value change.

        '''
        mode = self.order_mode
        tracker = mode.current_pp
        alloc = tracker.alloc

        # an account switch request
        if key == 'account':

            # hide details on the old selection
            old_tracker = mode.current_pp
            old_tracker.nav.hide_info()

            # re-assign the order mode tracker
            account_name = value
            tracker = mode.trackers.get(account_name)

            # if selection can't be found (likely never discovered with
            # a ``brokerd`) then error and switch back to the last
            # selection.
            if tracker is None:
                sym = old_tracker.charts[0].linked.symbol.key
                log.error(
                    f'Account `{account_name}` can not be set for {sym}'
                )
                self.form.fields['account'].setCurrentText(
                    old_tracker.alloc.account)
                return

            self.order_mode.current_pp = tracker
            assert tracker.alloc.account == account_name
            self.form.fields['account'].setCurrentText(account_name)
            tracker.nav.show()
            tracker.nav.hide_info()

            self.display_pnl(tracker)

            # load the new account's allocator
            alloc = tracker.alloc

        # WRITE any settings to current pp's allocator
        if key == 'size_unit':
            # implicit re-write of value if input
            # is the "text name" of the units.
            # yah yah, i know this is badd..
            alloc.size_unit = value

        elif key != 'account':  # numeric fields entry
            try:
                value = puterize(value)
            except ValueError as err:
                log.error(err.args[0])
                return False

            if key == 'limit':
                if value <= 0:
                    log.error('limit must be > 0')
                    return False

                pp = mode.current_pp.live_pp

                if alloc.size_unit == 'currency':
                    dsize = pp.dsize
                    if dsize > value:
                        log.error(
                            f'limit must > then current pp: {dsize}'
                        )
                        return False

                    alloc.currency_limit = value

                else:
                    size = pp.size
                    if size > value:
                        log.error(
                            f'limit must > then current pp: {size}'
                        )
                        raise ValueError

                    alloc.units_limit = value

            elif key == 'slots':
                if value <= 0:
                    # raise ValueError('slots must be > 0')
                    log.error('limit must be > 0')
                    return False

                alloc.slots = int(value)

            else:
                log.error(f'Unknown setting {key}')
                raise ValueError

            # don't log account "change" case since it'll be submitted
            # on every mouse interaction.
            log.info(f'settings change: {key}: {value}')

        # TODO: maybe return a diff of settings so if we can an error we
        # can have general input handling code to report it through the
        # UI in some way?
        return True

    def update_status_ui(
        self,
        pp: PositionTracker,

    ) -> None:

        alloc = pp.alloc
        slots = alloc.slots
        used = alloc.slots_used(pp.live_pp)

        # READ out settings and update the status UI / settings widgets
        suffix = {'currency': ' $', 'units': ' u'}[alloc.size_unit]
        limit = alloc.limit()

        step_size, currency_per_slot = alloc.step_sizes()

        if alloc.size_unit == 'currency':
            step_size = currency_per_slot

        self.step_label.format(
            step_size=str(humanize(step_size)) + suffix
        )
        self.limit_label.format(
            limit=str(humanize(limit)) + suffix
        )

        # update size unit in UI
        self.form.fields['size_unit'].setCurrentText(
            alloc._size_units[alloc.size_unit]
        )
        self.form.fields['slots'].setText(str(alloc.slots))
        self.form.fields['limit'].setText(str(limit))

        # update of level marker size label based on any new settings
        pp.update_from_pp()

        # calculate proportion of position size limit
        # that exists and display in fill bar
        # TODO: what should we do for fractional slot pps?
        self.fill_bar.set_slots(
            slots,

            # TODO: how to show "partial" slots?
            # min(round(prop * slots), slots)
            min(used, slots)
        )
        self.update_account_icons({alloc.account: pp.live_pp})

    def update_account_icons(
        self,
        pps: dict[str, Position],

    ) -> None:

        form = self.form
        accounts = form.fields['account']

        for account_name, pp in pps.items():
            icon_name = None

            if pp.size > 0:
                icon_name = 'long_pp'
            elif pp.size < 0:
                icon_name = 'short_pp'

            accounts.set_icon(account_name, icon_name)

    def display_pnl(
        self,
        tracker: PositionTracker,

    ) -> None:
        '''
        Display the PnL for the current symbol and personal positioning
        (pp).

        If a position is open start a background task which will
        real-time update the pnl label in the settings pane.

        '''
        mode = self.order_mode
        sym = mode.chart.linked.symbol
        size = tracker.live_pp.size
        feed = mode.quote_feed
        pnl_value = 0

        if size:
            # last historical close price
            last = feed.rt_shm.array[-1][['close']][0]
            pnl_value = copysign(1, size) * pnl(
                tracker.live_pp.ppu,
                last,
            )

            # maybe start update task
            global _pnl_tasks
            if sym.key not in _pnl_tasks:
                _pnl_tasks[sym.key] = True
                self.order_mode.nursery.start_soon(
                    update_pnl_from_feed,
                    feed,
                    mode,
                    tracker,
                )

        # immediately display in status label
        self.pnl_label.format(pnl=pnl_value)


def position_line(

    chart: ChartPlotWidget,  # noqa
    size: float,
    level: float,
    color: str,

    orient_v: str = 'bottom',
    marker: Optional[LevelMarker] = None,

) -> LevelLine:
    '''
    Convenience routine to create a line graphic representing a "pp"
    aka the acro for a,
    "{piker, private, personal, puny, <place your p-word here>} position".

    If ``marker`` is provided it will be configured appropriately for
    the "direction" of the position.

    '''
    line = order_line(
        chart,
        level,

        # TODO: could we maybe add a ``action=None`` which
        # would be a mechanism to check a marker was passed in?

        color=color,
        highlight_on_hover=False,
        movable=False,
        hide_xhair_on_hover=False,
        only_show_markers_on_hover=False,
        always_show_labels=False,

        # explicitly disable ``order_line()`` factory's creation
        # of a level marker since we do it in this tracer thing.
        show_markers=False,
    )

    if marker:
        # configure marker to position data

        if size > 0:  # long
            style = '|<'  # point "up to" the line
        elif size < 0:  # short
            style = '>|'  # point "down to" the line

        marker.style = style

        # set marker color to same as line
        marker.setPen(line.currentPen)
        marker.setBrush(fn.mkBrush(line.currentPen.color()))
        marker.level = level
        marker.update()
        marker.show()

        # show position marker on view "edge" when out of view
        vb = line.getViewBox()
        vb.sigRangeChanged.connect(marker.position_in_view)

    line.set_level(level)

    return line


_derivs = (
    'future',
    'continuous_future',
    'option',
    'futures_option',
)


# TODO: move into annoate module?
def mk_level_marker(
    chart: ChartPlotWidget,
    size: float,
    level: float,
    on_paint: Callable,

) -> LevelMarker:
    '''
    Allocate and return nan arrow graphics element.

    '''
    # scale marker size with dpi-aware font size
    font_size = _font.font.pixelSize()
    arrow_size = floor(1.375 * font_size)

    if size > 0:
        style = '|<'

    elif size < 0:
        style = '>|'

    arrow = LevelMarker(
        chart=chart,
        style=style,
        get_level=level,
        size=arrow_size,
        on_paint=on_paint,
    )
    arrow.show()
    return arrow


class Nav(Struct):
    '''
    Composite for holding a set of charts and respective (by order)
    graphics-elements which display position information acting as sort
    of "navigation" system for a position.

    '''
    charts: dict[int, ChartPlotWidget]
    pp_labels: dict[str, Label] = {}
    size_labels: dict[str, Label] = {}
    lines: dict[str, Optional[LevelLine]] = {}
    level_markers: dict[str, Optional[LevelMarker]] = {}
    color: str = 'default_lightest'

    def update_ui(
        self,
        account: str,
        price: float,
        size: float,
        slots_used: float,
        size_digits: Optional[int] = None,

    ) -> None:
        '''
        Update personal position level line.

        '''
        for key, chart in self.charts.items():
            size_digits = size_digits or chart.linked.symbol.lot_size_digits
            line = self.lines.get(key)
            level_marker = self.level_markers[key]
            pp_label = self.pp_labels[key]

            if size:
                # create and show a pp line if none yet exists
                if line is None:
                    arrow = self.level_markers[key]
                    line = position_line(
                        chart=chart,
                        level=price,
                        size=size,
                        color=self.color,
                        marker=arrow,
                    )
                    self.lines[key] = line

                # modify existing indicator line
                else:
                    line.set_level(price)
                    level_marker.level = price
                    level_marker.update()

                # update LHS sizing label
                line.update_labels({
                    'size': size,
                    'size_digits': size_digits,
                    'fiat_size': round(price * size, ndigits=2),

                    # TODO: per account lines on a single (or very
                    # related) symbol
                    'account': account,
                })
                line.show()

            # remove line from view for a net-zero pos
            elif line:
                line.delete()
                self.lines[key] = None

            # label updates
            size_label = self.size_labels[key]
            size_label.fields['slots_used'] = slots_used
            size_label.render()

            level_marker.level = price

            # these updates are critical to avoid lag on view/scene changes
            # TODO: couldn't we integrate this into
            # a ``.inter_ui_elements_and_update()``?
            level_marker.update()  # trigger paint
            pp_label.update()
            size_label.update()

    def level(self) -> float:
        '''
        Return the "level" value from the underlying ``LevelLine`` which tracks
        the "average position" price defined the represented position instance.

        '''
        if self.lines:
            for key, line in self.lines.items():
                if line:
                    return line.value()
        return 0

    def iter_ui_elements(self) -> tuple[
        Label,
        Label,
        LevelLine,
        LevelMarker,
    ]:
        for key, chart in self.charts.items():
            yield (
                self.pp_labels[key],
                self.size_labels[key],
                self.lines.get(key),
                self.level_markers[key],
            )

    def show(self) -> None:
        '''
        Show all UI elements on all charts.

        '''
        for (
            pp_label,
            size_label,
            line,
            level_marker,
        ) in self.iter_ui_elements():

            # labels
            level_marker.show()
            pp_label.show()
            size_label.show()

            if line:
                line.show()
                line.show_labels()

    def hide(self) -> None:
        for (
            pp_label,
            size_label,
            line,
            level_marker,
        ) in self.iter_ui_elements():
            pp_label.hide()
            level_marker.hide()
            size_label.hide()
            if line:
                line.hide()

    def update_graphics(
        self,
        marker: LevelMarker,
    ) -> None:
        '''
        Update all labels callback.

        Meant to be called from the marker ``.paint()``
        for immediate, lag free label draws.

        '''
        for (
            pp_label,
            size_label,
            line,
            level_marker,
        ) in self.iter_ui_elements():
            pp_label.update()
            size_label.update()

            # XXX: MEGALOLZ - this will cause the ui to hannngggg!!!?!?!?!
            # level_marker.update()

    def hide_info(self) -> None:
        '''
        Hide details (just size label?) of position nav elements.

        '''
        for (
            pp_label,
            size_label,
            line,
            level_marker,
        ) in self.iter_ui_elements():
            size_label.hide()
            if line:
                line.hide_labels()


class PositionTracker:
    '''
    Track and display real-time positions for a single symbol
    over multiple accounts on a single chart.

    Graphically composed of a level line and marker as well as labels
    for indcating current position information. Updates are made to the
    corresponding "settings pane" for the chart's "order mode" UX.

    '''
    alloc: Allocator
    startup_pp: Position
    live_pp: Position
    nav: Nav  # holds all UI elements across all charts

    def __init__(
        self,
        charts: list[ChartPlotWidget],
        alloc: Allocator,
        startup_pp: Position,

    ) -> None:

        nav = self.nav = Nav(charts={id(chart): chart for chart in charts})
        self.alloc = alloc
        self.startup_pp = startup_pp
        self.live_pp = copy(startup_pp)

        # TODO: maybe add this as a method ``Nav.add_chart()``
        # init all UI elements
        for key, chart in nav.charts.items():
            view = chart.getViewBox()

            # literally the 'pp' (pee pee) "position price" label that's
            # always in view
            pp_label = Label(
                view=view,
                fmt_str='pp',
                color=nav.color,
                update_on_range_change=False,
            )
            # if nav._level_marker:
            #     nav._level_marker.delete()

            arrow = mk_level_marker(
                chart=chart,
                size=1,
                level=nav.level,
                on_paint=nav.update_graphics,
            )

            view.scene().addItem(arrow)
            nav.level_markers[key] = arrow

            pp_label.scene_anchor = partial(
                gpath_pin,
                gpath=arrow,
                label=pp_label,
            )
            pp_label.render()
            nav.pp_labels[key] = pp_label

            size_label = Label(
                view=view,
                color=self.nav.color,

                # this is "static" label
                # update_on_range_change=False,
                fmt_str='\n'.join((
                    ':{slots_used:.1f}x',
                )),

                fields={
                    'slots_used': 0,
                },
            )
            size_label.render()
            size_label.scene_anchor = partial(
                pp_tight_and_right,
                label=pp_label,
            )
            nav.size_labels[key] = size_label

        nav.show()

    @property
    def pane(self) -> FieldsForm:
        '''
        Return handle to pp side pane form.

        '''
        return self.chart.linked.godwidget.pp_pane

    def update_from_pp(
        self,
        position: Optional[Position] = None,
        set_as_startup: bool = False,

    ) -> None:
        '''
        Update graphics and data from average price and size passed in
        our EMS ``BrokerdPosition`` msg.

        '''
        # live pp updates
        pp = position or self.live_pp
        if set_as_startup:
            startup_pp = pp
        else:
            startup_pp = self.startup_pp
        alloc = self.alloc

        # update allocator settings
        asset_type = pp.symbol.type_key

        # specific configs by asset class / type
        if asset_type in _derivs:
            # since it's harder to know how currency "applies" in this case
            # given leverage properties
            alloc.size_unit = '# units'

            # set units limit to slots size thus making make the next
            # entry step 1.0
            alloc.units_limit = alloc.slots

        else:
            alloc.size_unit = 'currency'

        # if the current position is already greater then the limit
        # settings, increase the limit to the current position
        if alloc.size_unit == 'currency':
            startup_size = self.startup_pp.size * startup_pp.ppu

            if startup_size > alloc.currency_limit:
                alloc.currency_limit = round(startup_size, ndigits=2)

        else:
            startup_size = abs(startup_pp.size)

            if startup_size > alloc.units_limit:
                alloc.units_limit = startup_size

                if asset_type in _derivs:
                    alloc.slots = alloc.units_limit

        self.nav.update_ui(
            self.alloc.account,
            pp.ppu,
            pp.size,
            round(alloc.slots_used(pp), ndigits=1),  # slots used
        )

        if pp.size == 0:
            self.nav.hide()

        else:
            if self.live_pp.size:
                self.nav.show()

            # don't show side and status widgets unless
            # order mode is "engaged" (which done via input controls)
            self.nav.hide_info()
