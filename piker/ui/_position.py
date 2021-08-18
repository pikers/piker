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
from enum import Enum
from functools import partial
from math import floor
# from pprint import pprint
import sys
from typing import Optional


from PyQt5.QtWidgets import QWidget
from bidict import bidict
from pyqtgraph import functions as fn
from pydantic import BaseModel, validator

from ._annotate import LevelMarker
from ._anchors import (
    pp_tight_and_right,  # wanna keep it straight in the long run
    gpath_pin,
)
from ..clearing._messages import BrokerdPosition, Status
from ..data._source import Symbol
from ._label import Label
from ._lines import LevelLine, level_line
from ._style import _font
from ._forms import FieldsForm
from ..log import get_logger


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


def mk_alloc(

    accounts: dict[str, Optional[str]] = {
        'paper': None,
    },

) -> Allocator:  # noqa

    from ..brokers import config
    conf, path = config.load()
    section = conf.get('accounts')
    if section is None:
        log.warning('No accounts config found?')

    for brokername, account_labels in section.items():
        for name, value in account_labels.items():
            accounts[f'{brokername}.{name}'] = value

    # lol we have to do this module patching bc ``pydantic``
    # needs types to exist at module level:
    # https://pydantic-docs.helpmanual.io/usage/postponed_annotations/
    mod = sys.modules[__name__]

    accounts = bidict(accounts)
    Account = mod.Account = Enum('Account', accounts)

    size_units = bidict({
        'currency': '$ size',
        'units': '# units',
        # 'percent_of_port': '% of port',  # TODO:
    })
    SizeUnit = mod.SizeUnit = Enum(
        'SizeUnit',
        size_units,
    )

    class Allocator(BaseModel):

        class Config:
            validate_assignment = True
            copy_on_model_validation = False
            extra = 'allow'

        account: Account = None
        _accounts: dict[str, Optional[str]] = accounts

        @validator('account', pre=True)
        def set_account(cls, v):
            if v:
                return cls._accounts[v]

        size_unit: SizeUnit = 'currency'
        _size_units: dict[str, Optional[str]] = size_units

        @validator('size_unit')
        def lookup_key(cls, v):
            # apply the corresponding enum key for the text "description" value
            return v.name

        disti_weight: str = 'uniform'

        size: float
        slots: int

        _position: Position = None
        _widget: QWidget = None

        def slotted_units(
            self,
            symbol: Symbol,
            size: float,
            price: float,
        ) -> float:
            return size / self.slots

        def size_from_currency_limit(
            self,
            symbol: Symbol,
            size: float,
            price: float,
        ) -> float:
            return size / self.slots / price

        _sizers = {
            'currency': size_from_currency_limit,
            'units': slotted_units,
            # 'percent_of_port': lambda: 0,
        }

        def get_order_info(
            self,

            # TODO: apply the symbol when the chart it is selected
            symbol: Symbol,
            price: float,
            action: str,

        ) -> dict:
            '''Generate order request info for the "next" submittable order
            depending on position / order entry config.

            '''
            tracker = self._position
            pp_size = tracker.live_pp.size
            ld = symbol.lot_size_digits

            if (
                action == 'buy' and pp_size > 0 or
                action == 'sell' and pp_size < 0 or
                pp_size == 0
            ):  # an entry

                # try to read existing position and compute
                # next entry/exit size from distribution weight policy
                # (and possibly TODO: commissions info).
                entry_size = self._sizers[self.size_unit](
                    self, symbol, self.size, price
                )

                if ld == 0:
                    # in discrete units case (eg. stocks, futures, opts)
                    # we always round down
                    units = floor(entry_size)
                else:
                    # we can return a float lot size rounded to nearest tick
                    units = round(entry_size, ndigits=ld)

                return {
                    'size': units,
                    'size_digits': ld
                }

            elif action != 'alert':  # an exit

                pp_size = tracker.startup_pp.size
                if ld == 0:
                    # exit at the slot size worth of units or the remaining
                    # units left for the position to be net-zero, whichever
                    # is smaller
                    evenly, r = divmod(pp_size,  self.slots)
                    exit_size = min(evenly, pp_size)

                    # "front" weight the exit order sizes
                    # TODO: make this configurable?
                    if r:
                        exit_size += 1

                else:  # we can return a float lot size rounded to nearest tick
                    exit_size = min(
                        round(pp_size / self.slots, ndigits=ld),
                        pp_size
                    )

                return {
                    'size': exit_size,
                    'size_digits': ld
                }

            else:  # likely an alert
                return {'size': 0}

    return Allocator(
        account=None,
        size_unit=size_units['currency'],
        size=5e3,
        slots=4,
    )


class PositionTracker:
    '''Track and display a real-time position for a single symbol
    on a chart.

    '''
    # inputs
    chart: 'ChartPlotWidget'  # noqa
    # alloc: 'Allocator'

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
        alloc: 'Allocator',  # noqa

    ) -> None:

        self.chart = chart
        self.alloc = alloc
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
        avg_price, size = msg['avg_price'], msg['size']

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

    def position_line(
        self,

        size: float,
        level: float,

        orient_v: str = 'bottom',

    ) -> LevelLine:
        '''Convenience routine to add a line graphic representing an order
        execution submitted to the EMS via the chart's "order mode".

        '''
        self.line = line = level_line(
            self.chart,
            level,
            color=self._color,
            add_label=False,
            highlight_on_hover=False,
            movable=False,
            hide_xhair_on_hover=False,
            use_marker_margin=True,
            only_show_markers_on_hover=False,
            always_show_labels=True,
        )

        if size > 0:
            style = '|<'
        elif size < 0:
            style = '>|'

        marker = self._level_marker
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
            line = self.line = self.position_line(
                level=price,
                size=size,
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

    def init_status_ui(
        self,
    ):
        pp = self.startup_pp

        # calculate proportion of position size limit
        # that exists and display in fill bar
        size = pp.size

        if self.alloc.size_unit == 'currency':
            size = size * pp.avg_price

        self.update_status_ui(size)

    def update_status_ui(
        self,
        size: float,

    ) -> None:
        alloc = self.alloc
        prop = size / alloc.size
        slots = alloc.slots

        pane = self.pane
        pane.fill_bar.set_slots(slots, min(floor(prop * slots), slots))
