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
from dataclasses import dataclass
from enum import Enum
from functools import partial
from math import floor
from typing import Optional


from bidict import bidict
from pyqtgraph import functions as fn
from pydantic import BaseModel, validator

from ._annotate import LevelMarker
from ._anchors import (
    pp_tight_and_right,  # wanna keep it straight in the long run
    gpath_pin,
)
from ..calc import humanize
from ..clearing._messages import BrokerdPosition, Status
from ..data._source import Symbol
from ._label import Label
from ._lines import LevelLine, level_line
from ._style import _font
from ._forms import FieldsForm, FillStatusBar, QLabel
from ..log import get_logger
from ..clearing._messages import Order

log = get_logger(__name__)


class Position(BaseModel):
    '''Basic pp (personal position) model with attached fills history.

    This type should be IPC wire ready?

    '''
    symbol: Symbol

    # last size and avg entry price
    size: float
    avg_price: float  # TODO: contextual pricing

    # ordered record of known constituent trade messages
    fills: list[Status] = []


_size_units = bidict({
    'currency': '$ size',
    'units': '# units',
    # TODO: but we'll need a `<brokermod>.get_accounts()` or something
    # 'percent_of_port': '% of port',
})
SizeUnit = Enum(
    'SizeUnit',
    _size_units,
)


class Allocator(BaseModel):

    class Config:
        validate_assignment = True
        copy_on_model_validation = False
        arbitrary_types_allowed = True

        # required to get the account validator lookup working?
        extra = 'allow'
        # underscore_attrs_are_private = False

    symbol: Symbol

    account: Optional[str] = 'paper'
    _accounts: bidict[str, Optional[str]]

    @validator('account', pre=True)
    def set_account(cls, v, values):
        if v:
            return values['_accounts'][v]

    size_unit: SizeUnit = 'currency'
    _size_units: dict[str, Optional[str]] = _size_units

    @validator('size_unit')
    def lookup_key(cls, v):
        # apply the corresponding enum key for the text "description" value
        return v.name

    # TODO: if we ever want ot support non-uniform entry-slot-proportion
    # "sizes"
    # disti_weight: str = 'uniform'

    units_limit: float
    currency_limit: float
    slots: int

    def step_sizes(
        self,
    ) -> (float, float):
        '''Return the units size for each unit type as a tuple.

        '''
        slots = self.slots
        return (
            self.units_limit / slots,
            self.currency_limit / slots,
        )

    def limit(self) -> float:
        if self.size_unit == 'currency':
            return self.currency_limit
        else:
            return self.units_limit

    def next_order_info(
        self,

        startup_pp: Position,
        live_pp: Position,
        price: float,
        action: str,

    ) -> dict:
        '''Generate order request info for the "next" submittable order
        depending on position / order entry config.

        '''
        sym = self.symbol
        ld = sym.lot_size_digits

        live_size = abs(live_pp.size)
        startup_size = abs(startup_pp.size)

        u_per_slot, currency_per_slot = self.step_sizes()

        if self.size_unit == 'units':
            enter_step = u_per_slot
            l_sub_pp = self.units_limit - live_size

        elif self.size_unit == 'currency':
            live_cost_basis = live_size * live_pp.avg_price
            enter_step = currency_per_slot / price
            l_sub_pp = (self.currency_limit - live_cost_basis) / price

        # an entry (adding-to or starting a pp)
        if (
            action == 'buy' and startup_size > 0 or
            action == 'sell' and startup_size < 0 or
            live_size == 0
        ):

            order_size = min(enter_step, l_sub_pp)

        # an exit (removing-from or going to net-zero pp)
        else:
            # always exit "at least" a unit-limit slot's worth of units
            # since higher-level per-slot-calcs can result in
            # non-uniform exit sizes per price, port alloc, etc.
            exit_step = max(
                startup_size / self.slots,
                enter_step,
            )

            # exit at a slot size's worth of units or the remaining
            # units left for the position to be net-zero, whichever
            # is smaller
            order_size = min(
                exit_step,
                live_size,
            )

        return {
            'size': round(order_size, ndigits=ld),
            'size_digits': ld
        }


@dataclass
class OrderModePane:
    '''Composite set of widgets plus an allocator model for configuring
    order entry sizes and position limits per tradable instrument.

    '''
    # config for and underlying validation model
    tracker: PositionTracker
    alloc: Allocator

    # input fields
    form: FieldsForm

    # output fill status and labels
    fill_bar: FillStatusBar

    step_label: QLabel
    pnl_label: QLabel
    limit_label: QLabel

    def transform_to(self, size_unit: str) -> None:
        if self.alloc.size_unit == size_unit:
            return

    def on_selection_change(
        self,

        text: str,
        key: str,

    ) -> None:
        '''Called on any order pane drop down selection change.

        '''
        print(f'selection input: {text}')
        setattr(self.alloc, key, text)
        self.on_ui_settings_change(key, text)

    def on_ui_settings_change(
        self,

        key: str,
        value: str,

    ) -> bool:
        '''Called on any order pane edit field value change.

        '''
        print(f'settings change: {key}: {value}')
        alloc = self.alloc
        size_unit = alloc.size_unit

        # write any passed settings to allocator
        if key == 'limit':
            if size_unit == 'currency':
                alloc.currency_limit = float(value)
            else:
                alloc.units_limit = float(value)

        elif key == 'slots':
            alloc.slots = int(value)

        elif key == 'size_unit':
            # TODO: if there's a limit size unit change re-compute
            # the current settings in the new units
            pass

        elif key == 'account':
            print(f'TODO: change account -> {value}')

        else:
            raise ValueError(f'Unknown setting {key}')

        # read out settings and update UI

        suffix = {'currency': ' $', 'units': ' u'}[size_unit]
        limit = alloc.limit()

        # TODO: a reverse look up from the position to the equivalent
        # account(s), if none then look to user config for default?
        self.update_status_ui()

        step_size, currency_per_slot = alloc.step_sizes()

        if size_unit == 'currency':
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

        # TODO: maybe return a diff of settings so if we can an error we
        # can have general input handling code to report it through the
        # UI in some way?
        return True

    def init_status_ui(
        self,
    ):
        alloc = self.alloc
        asset_type = alloc.symbol.type_key
        # form = self.form

        # TODO: pull from piker.toml
        # default config
        slots = 4
        currency_limit = 5e3

        startup_pp = self.tracker.startup_pp

        alloc.slots = slots
        alloc.currency_limit = currency_limit

        # default entry sizing
        if asset_type in ('stock', 'crypto', 'forex'):

            alloc.size_unit = '$ size'

        elif asset_type in ('future', 'option', 'futures_option'):

            # since it's harder to know how currency "applies" in this case
            # given leverage properties
            alloc.size_unit = '# units'

            # set units limit to slots size thus making make the next
            # entry step 1.0
            alloc.units_limit = slots

        # if the current position is already greater then the limit
        # settings, increase the limit to the current position
        if alloc.size_unit == 'currency':
            startup_size = startup_pp.size * startup_pp.avg_price

            if startup_size > alloc.currency_limit:
                alloc.currency_limit = round(startup_size, ndigits=2)

            limit_text = alloc.currency_limit

        else:
            startup_size = startup_pp.size

            if startup_size > alloc.units_limit:
                alloc.units_limit = startup_size

            limit_text = alloc.units_limit

        self.on_ui_settings_change('limit', limit_text)

        self.update_status_ui(size=startup_size)

    def update_status_ui(
        self,
        size: float = None,

    ) -> None:

        alloc = self.alloc
        live_pp = self.tracker.live_pp
        live_pp_size = abs(live_pp.size)
        slots = alloc.slots

        if alloc.size_unit == 'currency':
            live_currency_size = size or (
                live_pp_size * live_pp.avg_price
            )
            prop = live_currency_size / alloc.currency_limit

        else:
            prop = (size or live_pp_size) / alloc.units_limit

        # calculate proportion of position size limit
        # that exists and display in fill bar
        # TODO: what should we do for fractional slot pps?
        self.fill_bar.set_slots(
            slots,
            min(round(prop * slots), slots)
        )

    def on_level_change_update_next_order_info(
        self,

        level: float,
        line: LevelLine,
        order: Order,

    ) -> None:
        '''A callback applied for each level change to the line
        which will recompute the order size based on allocator
        settings. this is assigned inside
        ``OrderMode.line_from_order()``

        '''
        order_info = self.alloc.next_order_info(
            startup_pp=self.tracker.startup_pp,
            live_pp=self.tracker.live_pp,
            price=level,
            action=order.action,
        )
        line.update_labels(order_info)

        # update bound-in staged order
        order.price = level
        order.size = order_info['size']


def position_line(

    chart: 'ChartPlotWidget',  # noqa
    size: float,
    level: float,
    color: str,

    orient_v: str = 'bottom',
    marker: Optional[LevelMarker] = None,

) -> LevelLine:
    '''Convenience routine to create a line graphic representing a "pp"
    aka the acro for a,
    "{piker, private, personal, puny, <place your p-word here>} position".

    If ``marker`` is provided it will be configured appropriately for
    the "direction" of the position.

    '''
    line = level_line(
        chart,
        level,
        color=color,
        add_label=False,
        highlight_on_hover=False,
        movable=False,
        hide_xhair_on_hover=False,
        use_marker_margin=True,
        only_show_markers_on_hover=False,
        always_show_labels=True,
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


class PositionTracker:
    '''Track and display a real-time position for a single symbol
    on a chart.

    '''
    # inputs
    chart: 'ChartPlotWidget'  # noqa

    # allocated
    startup_pp: Position
    live_pp: Position
    pp_label: Label
    size_label: Label
    line: Optional[LevelLine] = None

    _color: str = 'default_lightest'

    def __init__(
        self,
        chart: 'ChartPlotWidget',  # noqa

    ) -> None:

        self.chart = chart
        self.live_pp = Position(
            symbol=chart.linked.symbol,
            size=0,
            avg_price=0,
        )
        self.startup_pp = self.live_pp.copy()

        view = chart.getViewBox()

        # literally the 'pp' (pee pee) label that's always in view
        self.pp_label = pp_label = Label(
            view=view,
            fmt_str='pp',
            color=self._color,
            update_on_range_change=False,
        )

        # create placeholder 'up' level arrow
        self._level_marker = None
        self._level_marker = self.level_marker(size=1)

        pp_label.scene_anchor = partial(
            gpath_pin,
            gpath=self._level_marker,
            label=pp_label,
        )
        pp_label.render()

        nsize = self.chart.linked.symbol.lot_size_digits

        self.size_label = size_label = Label(
            view=view,
            color=self._color,

            # this is "static" label
            # update_on_range_change=False,
            fmt_str='\n'.join((
                ':{entry_size:.{size_digits}f}x',
            )),

            fields={
                'entry_size': 0,
                'size_digits': nsize
            },
        )
        size_label.render()

        size_label.scene_anchor = partial(
            pp_tight_and_right,
            label=self.pp_label,
        )

        # size_label.scene_anchor = lambda: (
        #     self.pp_label.txt.pos() + QPointF(self.pp_label.w, 0)
        # )
        # size_label.scene_anchor = lambda: (
        #     self.pp_label.scene_br().bottomRight() - QPointF(
        #     self.size_label.w, self.size_label.h/3)
        # )

        # TODO: if we want to show more position-y info?
        #     fmt_str='\n'.join((
        #         # '{entry_size}x ',
        #         '{percent_pnl} % PnL',
        #         # '{percent_of_port}% of port',
        #         '${base_unit_value}',
        #     )),

        #     fields={
        #         # 'entry_size': 0,
        #         'percent_pnl': 0,
        #         'percent_of_port': 2,
        #         'base_unit_value': '1k',
        #     },
        # )

    @property
    def pane(self) -> FieldsForm:
        '''Return handle to pp side pane form.

        '''
        return self.chart.linked.godwidget.pp_pane

    def update_graphics(
        self,
        marker: LevelMarker

    ) -> None:
        '''Update all labels.

        Meant to be called from the maker ``.paint()``
        for immediate, lag free label draws.

        '''
        self.pp_label.update()
        self.size_label.update()

    def update(
        self,
        msg: BrokerdPosition,
        position: Optional[Position] = None,

    ) -> None:
        '''Update graphics and data from average price and size.

        '''
        # XXX: better place to do this?
        symbol = self.chart.linked.symbol
        avg_price, size = (
            round(msg['avg_price'], ndigits=symbol.tick_size_digits),
            round(msg['size'], ndigits=symbol.lot_size_digits),
        )

        # live pp updates
        pp = position or self.live_pp
        pp.avg_price = avg_price
        pp.size = size

        self.update_line(avg_price, size)

        # label updates
        self.size_label.fields['entry_size'] = size
        self.size_label.render()

        if size == 0:
            self.hide()

        else:
            self._level_marker.level = avg_price

            # these updates are critical to avoid lag on view/scene changes
            self._level_marker.update()  # trigger paint
            self.pp_label.update()
            self.size_label.update()

            self.show()

            # don't show side and status widgets unless
            # order mode is "engaged" (which done via input controls)
            self.hide_info()

    def level(self) -> float:
        if self.line:
            return self.line.value()
        else:
            return 0

    def show(self) -> None:
        if self.live_pp.size:
            self.line.show()
            self._level_marker.show()
            self.pp_label.show()
            self.size_label.show()

    def hide(self) -> None:
        self.pp_label.hide()
        self._level_marker.hide()
        self.size_label.hide()
        if self.line:
            self.line.hide()

    def hide_info(self) -> None:
        '''Hide details (right now just size label?) of position.

        '''
        # TODO: add remove status bar widgets here
        self.size_label.hide()

    # TODO: move into annoate module
    def level_marker(
        self,
        size: float,

    ) -> LevelMarker:

        if self._level_marker:
            self._level_marker.delete()

        # arrow marker
        # scale marker size with dpi-aware font size
        font_size = _font.font.pixelSize()

        # scale marker size with dpi-aware font size
        arrow_size = floor(1.375 * font_size)

        if size > 0:
            style = '|<'

        elif size < 0:
            style = '>|'

        arrow = LevelMarker(
            chart=self.chart,
            style=style,
            get_level=self.level,
            size=arrow_size,
            on_paint=self.update_graphics,
        )

        self.chart.getViewBox().scene().addItem(arrow)
        arrow.show()

        return arrow

    # TODO: per account lines on a single (or very related) symbol
    def update_line(
        self,
        price: float,
        size: float,

    ) -> None:
        '''Update personal position level line.

        '''
        # do line update
        line = self.line

        if line is None and size:

            # create and show a pp line
            line = self.line = position_line(
                chart=self.chart,
                level=price,
                size=size,
                color=self._color,
                marker=self._level_marker,
            )
            line.show()

        elif line:

            if size != 0.0:
                line.set_level(price)
                self._level_marker.level = price
                self._level_marker.update()
                # line.update_labels({'size': size})
                line.show()

            else:
                # remove pp line from view
                line.delete()
                self.line = None
