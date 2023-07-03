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

from ._pos import Position
from . import MktPair
from ..data.types import Struct


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


class Allocator(Struct):

    mkt: MktPair

    # TODO: if we ever want ot support non-uniform entry-slot-proportion
    # "sizes"
    # disti_weight: str = 'uniform'

    units_limit: float
    currency_limit: float
    slots: int
    account: Optional[str] = 'paper'

    _size_units: bidict[str, Optional[str]] = _size_units

    # TODO: for enums this clearly doesn't fucking work, you can't set
    # a default at startup by passing in a `dict` but yet you can set
    # that value through assignment..for wtv cucked reason.. honestly, pure
    # unintuitive garbage.
    _size_unit: str = 'currency'

    @property
    def size_unit(self) -> str:
        return self._size_unit

    @size_unit.setter
    def size_unit(self, v: str) -> Optional[str]:
        if v not in _size_units:
            v = _size_units.inverse[v]

        assert v in _size_units
        self._size_unit = v
        return v

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

    def limit_info(self) -> tuple[str, float]:
        return self.size_unit, self.limit()

    def next_order_info(
        self,

        # we only need a startup size for exit calcs, we can then
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
        mkt: MktPair = self.mkt
        ld: int = mkt.size_tick_digits

        size_unit = self.size_unit
        live_size = live_pp.cumsize
        abs_live_size = abs(live_size)
        abs_startup_size = abs(startup_pp.cumsize)

        u_per_slot, currency_per_slot = self.step_sizes()

        if size_unit == 'units':
            slot_size: float = u_per_slot
            l_sub_pp: float = self.units_limit - abs_live_size

        elif size_unit == 'currency':
            live_cost_basis: float = abs_live_size * live_pp.ppu
            slot_size: float = currency_per_slot / price
            l_sub_pp: float = (self.currency_limit - live_cost_basis) / price

        else:
            raise ValueError(
                f"Not valid size unit '{size_unit}'"
            )

        # an entry (adding-to or starting a pp)
        if (
            live_size == 0
            or (
                action == 'buy'
                and live_size > 0
            )
            or (
                action == 'sell'
                and live_size < 0
            )
        ):
            order_size = min(
                slot_size,
                max(l_sub_pp, 0),
            )

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
                slot_size = currency_per_slot / live_pp.ppu

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
                abs_live_size < slot_size

                # NOTE: front/back "loading" heurstic:
                # if the remaining pp is in between 0-1.5x a slot's
                # worth, dump the whole position in this last exit
                # therefore conducting so called "back loading" but
                # **without** going past a net-zero pp. if the pp is
                # > 1.5x a slot size, then front load: exit a slot's and
                # expect net-zero to be acquired on the final exit.
                or slot_size < pp_size < round((1.5*slot_size), ndigits=ld)
                or (

                    # underlying requires discrete (int) units (eg. stocks)
                    # and thus our slot size (based on our limit) would
                    # exit a fractional unit's worth so, presuming we aren't
                    # supporting a fractional-units-style broker, we need
                    # exit the final unit.
                    ld == 0
                    and abs_live_size == 1
                )
            ):
                order_size = abs_live_size

        slots_used = 1.0  # the default uniform policy
        if order_size < slot_size:
            # compute a fractional slots size to display
            slots_used = self.slots_used(
                Position(
                    mkt=mkt,
                    bs_mktid=mkt.bs_mktid,
                )
            )

        # TODO: render an actual ``Executable`` type here?
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
        abs_pp_size = abs(pp.cumsize)

        if self.size_unit == 'currency':
            # live_currency_size = size or (abs_pp_size * pp.ppu)
            live_currency_size = abs_pp_size * pp.ppu
            prop = live_currency_size / self.currency_limit

        else:
            # return (size or abs_pp_size) / alloc.units_limit
            prop = abs_pp_size / self.units_limit

        # TODO: REALLY need a way to show partial slots..
        # for now we round at the midway point between slots
        return round(prop * self.slots)


def mk_allocator(

    mkt: MktPair,
    startup_pp: Position,

    # default allocation settings
    defaults: dict[str, float] = {
        'account': None,  # select paper by default
        # 'size_unit': 'currency',
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

    return Allocator(
        mkt=mkt,
        **defaults,
    )
