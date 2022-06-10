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
    records: dict[str, TradeRecord],

    pps: Optional[dict[str, Position]] = None

) -> dict[str, Position]:
    '''
    Compile a set of positions from a trades ledger.

    '''

    pps: dict[str, Position] = pps or {}

    # lifo update all pps from records
    for r in records:
        key = r.symkey or r.fqsn
        pp = pps.setdefault(
            key,
            Position(
                Symbol.from_fqsn(
                    r.fqsn,
                    info={},
                ),
                size=0.0,
                avg_price=0.0,
            )
        )

        # lifo style average price calc
        pp.lifo_update(r.size, r.price)
        pp.fills.append(r.tid)

    assert len(set(pp.fills)) == len(pp.fills)
    return pps


def _split_active(
    pps: dict[str, Position],

) -> tuple[dict, dict]:

    active = {}
    closed = {}

    for k, pp in pps.items():
        fqsn = pp.symbol.front_fqsn()
        asdict = pp.to_dict()
        if pp.size == 0:
            closed[fqsn] = asdict
        else:
            active[fqsn] = asdict

    return active, closed


def load_pps_from_ledger(

    brokername: str,
    acctname: str,

) -> dict[str, Any]:

    with config.open_trade_ledger(
        brokername,
        acctname,
    ) as ledger:
        pass  # readonly

    brokermod = get_brokermod(brokername)
    records = brokermod.norm_trade_records(ledger)
    pps = update_pps(
        brokername,
        records,
    )
    return _split_active(pps)


def update_pps_conf(
    brokername: str,
    acctid: str,
    trade_records: Optional[list[TradeRecord]] = None,
):
    conf, path = config.load('pps')
    brokersection = conf.setdefault(brokername, {})
    entries = brokersection.setdefault(acctid, {})

    if not entries:

        # no pps entry yet for this broker/account
        active, closed = load_pps_from_ledger(
            brokername,
            acctid,
        )

    elif trade_records:

        # table for map-back to object form
        pps = {}

        # load ``pps.toml`` config entries back into object form.
        for fqsn, entry in entries.items():
            pps[fqsn] = Position(
                Symbol.from_fqsn(fqsn, info={}),
                size=entry['size'],
                avg_price=entry['avg_price'],
            )

        pps = update_pps(
            brokername,
            trade_records,
            pps=pps,
        )
        active, closed = _split_active(pps)

    for fqsn in closed:
        print(f'removing closed pp: {fqsn}')
        entries.pop(fqsn, None)

    for fqsn, pp_dict in active.items():
        print(f'Updating active pp: {fqsn}')

        # normalize to a simpler flat dict format
        _ = pp_dict.pop('symbol')
        entries[fqsn.rstrip(f'.{brokername}')] = pp_dict

    config.write(
        conf,
        'pps',
        encoder=config.toml.Encoder(preserve=True),
    )

    from pprint import pprint
    pprint(conf)


if __name__ == '__main__':
    update_pps_conf('ib', 'algopaper')
