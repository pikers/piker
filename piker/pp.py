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
    fills: list[Status] = []

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


def parse_pps(

    brokername: str,
    acctname: str,

    ledger: Optional[dict[str, Union[str, float]]] = None,

) -> dict[str, Any]:

    pps: dict[str, Position] = {}

    if not ledger:
        with config.open_trade_ledger(
            brokername,
            acctname,
        ) as ledger:
            pass  # readonly

    by_date = ledger[brokername]

    for date, by_id in by_date.items():
        for tid, record in by_id.items():

            # ib specific record parsing
            # date, time = record['dateTime']
            # conid = record['condid']
            # cost = record['cost']
            # comms = record['ibCommission']
            symbol = record['symbol']
            price = record['tradePrice']
            # action = record['buySell']

            # NOTE: can be -ve on sells
            size = float(record['quantity'])

            pp = pps.setdefault(
                symbol,
                Position(
                    Symbol(key=symbol),
                    size=0.0,
                    avg_price=0.0,
                )
            )

            # LOFI style average price calc
            pp.lifo_update(size, price)

    from pprint import pprint
    pprint(pps)


if __name__ == '__main__':
    parse_pps('ib', 'algopaper')
