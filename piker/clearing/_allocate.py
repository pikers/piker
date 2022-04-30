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

'''
Position allocation logic and protocols.

'''
from enum import Enum
from typing import Optional

from bidict import bidict
from pydantic import BaseModel, validator

from ..data._source import Symbol
from ._messages import BrokerdPosition, Status


class Position(BaseModel):
    '''
    Basic pp (personal position) model with attached fills history.

    This type should be IPC wire ready?

    '''
    symbol: Symbol

    # last size and avg entry price
    size: float
    avg_price: float  # TODO: contextual pricing

    # ordered record of known constituent trade messages
    fills: list[Status] = []

    def update_from_msg(
        self,
        msg: BrokerdPosition,

    ) -> None:

        # XXX: better place to do this?
        symbol = self.symbol

        lot_size_digits = symbol.lot_size_digits
        avg_price, size = (
            round(msg['avg_price'], ndigits=symbol.tick_size_digits),
            round(msg['size'], ndigits=lot_size_digits),
        )

        self.avg_price = avg_price
        self.size = size

    @property
    def dsize(self) -> float:
        '''
        The "dollar" size of the pp, normally in trading (fiat) unit
        terms.

        '''
        return self.avg_price * self.size


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
        underscore_attrs_are_private = False

    symbol: Symbol
    account: Optional[str] = 'paper'
    # TODO: for enums this clearly doesn't fucking work, you can't set
    # a default at startup by passing in a `dict` but yet you can set
    # that value through assignment..for wtv cucked reason.. honestly, pure
    # unintuitive garbage.
    size_unit: str = 'currency'
    _size_units: dict[str, Optional[str]] = _size_units

    @validator('size_unit', pre=True)
    def maybe_lookup_key(cls, v):
        # apply the corresponding enum key for the text "description" value
        if v not in _size_units:
            return _size_units.inverse[v]

        assert v in _size_units
        return v

    # TODO: if we ever want ot support non-uniform entry-slot-proportion
    # "sizes"
    # disti_weight: str = 'uniform'

    units_limit: float
    currency_limit: float
    slots: int

    def step_sizes(
        self,
    ) -> (float, float):
        '''
        Return the units size for each unit type as a tuple.

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

        # we only need a startup size for exit calcs, we can the
        # determine how large slots should be if the initial pp size was
        # larger then the current live one, and the live one is smaller
        # then the initial config settings.
        startup_pp: Position,
        live_pp: Position,
        price: float,
        action: str,

    ) -> dict:
        '''
        Generate order request info for the "next" submittable order
        depending on position / order entry config.

        '''
        sym = self.symbol
        ld = sym.lot_size_digits

        size_unit = self.size_unit
        live_size = live_pp.size
        abs_live_size = abs(live_size)
        abs_startup_size = abs(startup_pp.size)

        u_per_slot, currency_per_slot = self.step_sizes()

        if size_unit == 'units':
            slot_size = u_per_slot
            l_sub_pp = self.units_limit - abs_live_size

        elif size_unit == 'currency':
            live_cost_basis = abs_live_size * live_pp.avg_price
            slot_size = currency_per_slot / price
            l_sub_pp = (self.currency_limit - live_cost_basis) / price

        else:
            raise ValueError(
                f"Not valid size unit '{size_unit}'"
            )

        # an entry (adding-to or starting a pp)
        if (
            action == 'buy' and live_size > 0 or
            action == 'sell' and live_size < 0 or
            live_size == 0
        ):

            order_size = min(slot_size, l_sub_pp)

        # an exit (removing-from or going to net-zero pp)
        else:
            # when exiting a pp we always try to slot the position
            # in the instrument's units, since doing so in a derived
            # size measure (eg. currency value, percent of port) would
            # result in a mis-mapping of slots sizes in unit terms
            # (i.e. it would take *more* slots to exit at a profit and
            # *less* slots to exit at a loss).
            pp_size = max(abs_startup_size, abs_live_size)
            slotted_pp = pp_size / self.slots

            if size_unit == 'currency':
                # compute the "projected" limit's worth of units at the
                # current pp (weighted) price:
                slot_size = currency_per_slot / live_pp.avg_price

            else:
                slot_size = u_per_slot

            # TODO: ensure that the limit can never be set **lower**
            # then the current pp size? It should be configured
            # correctly at startup right?

            # if our position is greater then our limit setting
            # we'll want to use slot sizes which are larger then what
            # the limit would normally determine.
            order_size = max(slotted_pp, slot_size)

            if (
                abs_live_size < slot_size or

                # NOTE: front/back "loading" heurstic:
                # if the remaining pp is in between 0-1.5x a slot's
                # worth, dump the whole position in this last exit
                # therefore conducting so called "back loading" but
                # **without** going past a net-zero pp. if the pp is
                # > 1.5x a slot size, then front load: exit a slot's and
                # expect net-zero to be acquired on the final exit.
                slot_size < pp_size < round((1.5*slot_size), ndigits=ld) or

                # underlying requires discrete (int) units (eg. stocks)
                # and thus our slot size (based on our limit) would
                # exit a fractional unit's worth so, presuming we aren't
                # supporting a fractional-units-style broker, we need
                # exit the final unit.
                ld == 0 and abs_live_size == 1
            ):
                order_size = abs_live_size

        slots_used = 1.0  # the default uniform policy
        if order_size < slot_size:
            # compute a fractional slots size to display
            slots_used = self.slots_used(
                Position(symbol=sym, size=order_size, avg_price=price)
            )

        return {
            'size': abs(round(order_size, ndigits=ld)),
            'size_digits': ld,

            # TODO: incorporate multipliers for relevant derivatives
            'fiat_size': round(order_size * price, ndigits=2),
            'slots_used': slots_used,

            # update line LHS label with account name
            'account': self.account,
        }

    def slots_used(
        self,
        pp: Position,

    ) -> float:
        '''
        Calc and return the number of slots used by this ``Position``.

        '''
        abs_pp_size = abs(pp.size)

        if self.size_unit == 'currency':
            # live_currency_size = size or (abs_pp_size * pp.avg_price)
            live_currency_size = abs_pp_size * pp.avg_price
            prop = live_currency_size / self.currency_limit

        else:
            # return (size or abs_pp_size) / alloc.units_limit
            prop = abs_pp_size / self.units_limit

        # TODO: REALLY need a way to show partial slots..
        # for now we round at the midway point between slots
        return round(prop * self.slots)


_derivs = (
    'future',
    'continuous_future',
    'option',
    'futures_option',
)


def mk_allocator(

    symbol: Symbol,
    startup_pp: Position,

    # default allocation settings
    defaults: dict[str, float] = {
        'account': None,  # select paper by default
        'size_unit': 'currency',
        'units_limit': 400,
        'currency_limit': 5e3,
        'slots': 4,
    },
    **kwargs,

) -> Allocator:

    if kwargs:
        defaults.update(kwargs)

    # load and retreive user settings for default allocations
    # ``config.toml``
    user_def = {
        'currency_limit': 6e3,
        'slots': 6,
    }

    defaults.update(user_def)

    alloc = Allocator(
        symbol=symbol,
        **defaults,
    )

    asset_type = symbol.type_key

    # specific configs by asset class / type

    if asset_type in _derivs:
        # since it's harder to know how currency "applies" in this case
        # given leverage properties
        alloc.size_unit = '# units'

        # set units limit to slots size thus making make the next
        # entry step 1.0
        alloc.units_limit = alloc.slots

    # if the current position is already greater then the limit
    # settings, increase the limit to the current position
    if alloc.size_unit == 'currency':
        startup_size = startup_pp.size * startup_pp.avg_price

        if startup_size > alloc.currency_limit:
            alloc.currency_limit = round(startup_size, ndigits=2)

    else:
        startup_size = abs(startup_pp.size)

        if startup_size > alloc.units_limit:
            alloc.units_limit = startup_size

            if asset_type in _derivs:
                alloc.slots = alloc.units_limit

    return alloc
