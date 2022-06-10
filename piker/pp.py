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
'''
Personal/Private position parsing, calculmating, summarizing in a way
that doesn't try to cuk most humans who prefer to not lose their moneys..
(looking at you `ib` and shitzy friends)

'''
from typing import (
    Any,
    Optional,
    Union,
)

from msgspec import Struct

from . import config
from .clearing._messages import BrokerdPosition, Status
from .data._source import Symbol
from .brokers import get_brokermod


class TradeRecord(Struct):
    fqsn: str  # normally fqsn
    tid: Union[str, int]
    size: float
    price: float
    cost: float  # commisions or other additional costs

    # optional key normally derived from the broker
    # backend which ensures the instrument-symbol this record
    # is for is truly unique.
    symkey: Optional[Union[str, int]] = None


class Position(Struct):
    '''
    Basic pp (personal position) model with attached fills history.

    This type should be IPC wire ready?

    '''
    symbol: Symbol

    # last size and avg entry price
    size: float
    avg_price: float  # TODO: contextual pricing

    # ordered record of known constituent trade messages
    fills: list[Union[str, int, Status]] = []

    def to_dict(self):
        return {
            f: getattr(self, f)
            for f in self.__struct_fields__
        }

    def update_from_msg(
        self,
        msg: BrokerdPosition,

    ) -> None:

        # XXX: better place to do this?
        symbol = self.symbol

        lot_size_digits = symbol.lot_size_digits
        avg_price, size = (
            round(
                msg['avg_price'],
                ndigits=symbol.tick_size_digits
            ),
            round(
                msg['size'],
                ndigits=lot_size_digits
            ),
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

    def lifo_update(
        self,
        size: float,
        price: float,

    ) -> (float, float):
        '''
        Incremental update using a LIFO-style weighted mean.

        '''
        # "avg position price" calcs
        # TODO: eventually it'd be nice to have a small set of routines
        # to do this stuff from a sequence of cleared orders to enable
        # so called "contextual positions".
        new_size = self.size + size

        # old size minus the new size gives us size diff with
        # +ve -> increase in pp size
        # -ve -> decrease in pp size
        size_diff = abs(new_size) - abs(self.size)

        if new_size == 0:
            self.avg_price = 0

        elif size_diff > 0:
            # XXX: LOFI incremental update:
            # only update the "average price" when
            # the size increases not when it decreases (i.e. the
            # position is being made smaller)
            self.avg_price = (
                abs(size) * price  # weight of current exec
                +
                self.avg_price * abs(self.size)  # weight of previous pp
            ) / abs(new_size)

        self.size = new_size

        return new_size, self.avg_price


def update_pps(
    brokername: str,
    ledger: dict[str, Union[str, float]],
    pps: Optional[dict[str, TradeRecord]] = None

) -> dict[str, TradeRecord]:
    '''
    Compile a set of positions from a trades ledger.

    '''
    brokermod = get_brokermod(brokername)

    pps: dict[str, Position] = pps or {}
    records = brokermod.norm_trade_records(ledger)
    for r in records:
        key = r.symkey or r.fqsn
        pp = pps.setdefault(
            key,
            Position(
                Symbol.from_fqsn(r.fqsn, info={}),
                size=0.0,
                avg_price=0.0,
            )
        )

        # lifo style average price calc
        pp.lifo_update(r.size, r.price)
        pp.fills.append(r.tid)

    return pps


async def load_pps_from_ledger(

    brokername: str,
    acctname: str,

) -> dict[str, Any]:

    with config.open_trade_ledger(
        brokername,
        acctname,
    ) as ledger:
        pass  # readonly

    pps = update_pps(brokername, ledger)

    active_pps = {}
    for k, pp in pps.items():

        if pp.size == 0:
            continue

        active_pps[pp.symbol.front_fqsn()] = pp.to_dict()
    # pprint({pp.symbol.front_fqsn(): pp.to_dict() for k, pp in pps.items()})

    from pprint import pprint
    pprint(active_pps)
    # pprint({pp.symbol.front_fqsn(): pp.to_dict() for k, pp in pps.items()})


def update_pps_conf(
    trade_records: list[TradeRecord],
):
    conf, path = config.load('pp')



if __name__ == '__main__':
    import trio
    trio.run(
        load_pps_from_ledger, 'ib', 'algopaper',
    )
