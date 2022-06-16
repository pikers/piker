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
Personal/Private position parsing, calculating, summarizing in a way
that doesn't try to cuk most humans who prefer to not lose their moneys..
(looking at you `ib` and dirt-bird friends)

'''
from contextlib import contextmanager as cm
import os
from os import path
from typing import (
    Any,
    Optional,
    Union,
)

from msgspec import Struct
import toml

from . import config
from .brokers import get_brokermod
from .clearing._messages import BrokerdPosition, Status
from .data._source import Symbol
from .log import get_logger

log = get_logger(__name__)


@cm
def open_trade_ledger(
    broker: str,
    account: str,

) -> str:
    '''
    Indempotently create and read in a trade log file from the
    ``<configuration_dir>/ledgers/`` directory.

    Files are named per broker account of the form
    ``<brokername>_<accountname>.toml``. The ``accountname`` here is the
    name as defined in the user's ``brokers.toml`` config.

    '''
    ldir = path.join(config._config_dir, 'ledgers')
    if not path.isdir(ldir):
        os.makedirs(ldir)

    fname = f'trades_{broker}_{account}.toml'
    tradesfile = path.join(ldir, fname)

    if not path.isfile(tradesfile):
        log.info(
            f'Creating new local trades ledger: {tradesfile}'
        )
        with open(tradesfile, 'w') as cf:
            pass  # touch
    with open(tradesfile, 'r') as cf:
        ledger = toml.load(tradesfile)
        cpy = ledger.copy()
    try:
        yield cpy
    finally:
        if cpy != ledger:
            # TODO: show diff output?
            # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
            print(f'Updating ledger for {tradesfile}:\n')
            ledger.update(cpy)

            # we write on close the mutated ledger data
            with open(tradesfile, 'w') as cf:
                return toml.dump(ledger, cf)


class Transaction(Struct):
    fqsn: str  # normally fqsn
    tid: Union[str, int]  # unique transaction id
    size: float
    price: float
    cost: float  # commisions or other additional costs

    # dt: datetime

    # optional key normally derived from the broker
    # backend which ensures the instrument-symbol this record
    # is for is truly unique.
    bsuid: Optional[Union[str, int]] = None


class Position(Struct):
    '''
    Basic pp (personal position) model with attached fills history.

    This type should be IPC wire ready?

    '''
    symbol: Symbol

    # last size and avg entry price
    size: float
    avg_price: float  # TODO: contextual pricing
    bsuid: str

    # ordered record of known constituent trade messages
    fills: dict[
        Union[str, int, Status],  # trade id
        float,  # cost
    ] = {}

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
        cost: float = 0,

        # TODO: idea: "real LIFO" dynamic positioning.
        # - when a trade takes place where the pnl for
        # the (set of) trade(s) is below the breakeven price
        # it may be that the trader took a +ve pnl on a short(er)
        # term trade in the same account.
        # - in this case we could recalc the be price to
        # be reverted back to it's prior value before the nearest term
        # trade was opened.?
        dynamic_breakeven_price: bool = False,

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
                cost  # transaction cost
                +
                self.avg_price * abs(self.size)  # weight of previous pp
            ) / abs(new_size)

        self.size = new_size

        return new_size, self.avg_price


def update_pps(
    records: dict[str, Transaction],
    pps: Optional[dict[str, Position]] = None

) -> dict[str, Position]:
    '''
    Compile a set of positions from a trades ledger.

    '''
    pps: dict[str, Position] = pps or {}

    # lifo update all pps from records
    for r in records:

        pp = pps.setdefault(
            r.fqsn or r.bsuid,

            # if no existing pp, allocate fresh one.
            Position(
                Symbol.from_fqsn(
                    r.fqsn,
                    info={},
                ),
                size=0.0,
                avg_price=0.0,
                bsuid=r.bsuid,
            )
        )
        # don't do updates for ledger records we already have
        # included in the current pps state.
        if r.tid in pp.fills:
            # NOTE: likely you'll see repeats of the same
            # ``Transaction`` passed in here if/when you are restarting
            # a ``brokerd.ib`` where the API will re-report trades from
            # the current session, so we need to make sure we don't
            # "double count" these in pp calculations.
            continue

        # lifo style "breakeven" price calc
        pp.lifo_update(
            r.size,
            r.price,

            # include transaction cost in breakeven price
            # and presume the worst case of the same cost
            # to exit this transaction (even though in reality
            # it will be dynamic based on exit stratetgy).
            cost=2*r.cost,
        )

        # track clearing costs
        pp.fills[r.tid] = r.cost

    assert len(set(pp.fills)) == len(pp.fills)
    return pps


def dump_active(
    pps: dict[str, Position],

) -> tuple[
    dict[str, Any],
    dict[str, Any],
]:
    '''
    Split pps into those that are "active" (non-zero size) and "closed"
    (zero size) and return in 2 dicts.

    Returning the "closed" set is important for updating the pps state
    in any ``pps.toml`` such that we remove entries which are no longer
    part of any "VaR" set (well presumably, except of course your liquidity
    asset could be full of "risk" XD ).

    '''
    active = {}
    closed = {}

    for k, pp in pps.items():
        asdict = pp.to_dict()
        if pp.size == 0:
            closed[k] = asdict
        else:
            active[k] = asdict

    return active, closed


def load_pps_from_ledger(

    brokername: str,
    acctname: str,

) -> tuple[dict, dict]:
    '''
    Open a ledger file by broker name and account and read in and
    process any trade records into our normalized ``Transaction``
    form and then pass these into the position processing routine
    and deliver the two dict-sets of the active and closed pps.

    '''
    with open_trade_ledger(
        brokername,
        acctname,
    ) as ledger:
        pass  # readonly

    if not ledger:
        # null case, no ledger file with content
        return {}, {}

    brokermod = get_brokermod(brokername)
    records = brokermod.norm_trade_records(ledger)
    pps = update_pps(records)
    return dump_active(pps)


def get_pps(
    brokername: str,
    acctids: Optional[set[str]] = set(),
    key_by: Optional[str] = None,

) -> dict[str, dict[str, Position]]:
    '''
    Read out broker-specific position entries from
    incremental update file: ``pps.toml``.

    '''
    conf, path = config.load('pps')
    all_active = {}

    # try to load any ledgers if no section found
    bconf, path = config.load('brokers')
    accounts = bconf[brokername]['accounts']
    for account in accounts:

        # TODO: instead of this filter we could
        # always send all known pps but just not audit
        # them since an active client might not be up?
        if (
            acctids and
            f'{brokername}.{account}' not in acctids
        ):
            continue

        active = update_pps_conf(brokername, account, key_by=key_by)
        all_active.setdefault(account, {}).update(active)

    return all_active


def update_pps_conf(
    brokername: str,
    acctid: str,
    trade_records: Optional[list[Transaction]] = None,
    key_by: Optional[str] = None,

) -> dict[str, Position]:

    conf, path = config.load('pps')
    brokersection = conf.setdefault(brokername, {})
    pps = brokersection.setdefault(acctid, {})

    if not pps:
        # no pps entry yet for this broker/account so parse
        # any available ledgers to build a pps state.
        pps, closed = load_pps_from_ledger(
            brokername,
            acctid,
        )
        if not pps:
            log.warning(
                f'No trade history could be loaded for {brokername}:{acctid}'
            )

    # unmarshal/load ``pps.toml`` config entries into object form.
    pp_objs = {}
    for fqsn, entry in pps.items():
        pp_objs[fqsn] = Position(
            Symbol.from_fqsn(fqsn, info={}),
            size=entry['size'],
            avg_price=entry['avg_price'],
            bsuid=entry['bsuid'],

            # XXX: super critical, we need to be sure to include
            # all pps.toml fills to avoid reusing fills that were
            # already included in the current incremental update
            # state, since today's records may have already been
            # processed!
            fills=entry['fills'],
        )

    # update all pp objects from any (new) trade records which
    # were passed in (aka incremental update case).
    if trade_records:
        pp_objs = update_pps(
            trade_records,
            pps=pp_objs,
        )

    active, closed = dump_active(pp_objs)

    # dict-serialize all active pps
    pp_entries = {}
    for fqsn, pp_dict in active.items():
        print(f'Updating active pp: {fqsn}')

        # normalize to a simpler flat dict format
        _ = pp_dict.pop('symbol')

        # XXX: ugh, it's cuz we push the section under
        # the broker name.. maybe we need to rethink this?
        brokerless_key = fqsn.rstrip(f'.{brokername}')
        pp_entries[brokerless_key] = pp_dict

    for fqsn in closed:
        pp_objs.pop(fqsn, None)

    conf[brokername][acctid] = pp_entries
    config.write(
        conf,
        'pps',

        # TODO: make nested tables and/or inline tables work?
        # encoder=config.toml.Encoder(preserve=True),
    )

    if key_by:
        pps_objs = {getattr(pp, key_by): pp for pp in pps_objs}

    # deliver object form of all pps in table to caller
    return pp_objs


if __name__ == '__main__':
    import sys

    args = sys.argv
    assert len(args) > 1, 'Specifiy account(s) from `brokers.toml`'
    args = args[1:]
    for acctid in args:
        broker, name = acctid.split('.')
        update_pps_conf(broker, name)
