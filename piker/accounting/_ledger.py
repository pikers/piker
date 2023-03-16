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
from __future__ import annotations
from contextlib import contextmanager as cm
import os
from os import path
import time
from typing import (
    Any,
    Iterator,
    Union,
    Generator
)

from pendulum import (
    datetime,
)
import tomli
import toml

from .. import config
from ..data.types import Struct
from ..log import get_logger
from ._mktinfo import (
    Symbol,  # legacy
    MktPair,
    Asset,
)

log = get_logger(__name__)


@cm
def open_trade_ledger(
    broker: str,
    account: str,

) -> Generator[dict, None, None]:
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
    with open(tradesfile, 'rb') as cf:
        start = time.time()
        ledger = tomli.load(cf)
        log.info(f'Ledger load took {time.time() - start}s')
        cpy = ledger.copy()

    try:
        yield cpy
    finally:
        if cpy != ledger:

            # TODO: show diff output?
            # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
            log.info(f'Updating ledger for {tradesfile}:\n')
            ledger.update(cpy)

            # we write on close the mutated ledger data
            with open(tradesfile, 'w') as cf:
                toml.dump(ledger, cf)


class Transaction(Struct, frozen=True):

    # TODO: unify this with the `MktPair`,
    # once we have that as a required field,
    # we don't really need the fqsn any more..
    fqsn: str

    # TODO: drop the Symbol type

    # the underlying "transaction system", normally one of a ``MktPair``
    # (a description of a tradable double auction) or a ledger-recorded
    # ("ledger" in any sense as long as you can record transfers) of any
    # sort) ``Asset``.
    sym: MktPair | Asset | Symbol

    @property
    def sys(self) -> Symbol:
        return self.sym

    tid: Union[str, int]  # unique transaction id
    size: float
    price: float
    cost: float  # commisions or other additional costs
    dt: datetime
    expiry: datetime | None = None

    # remap for back-compat
    @property
    def fqme(self) -> str:
        return self.fqsn

    # (optional) key-id defined by the broker-service backend which
    # ensures the instrument-symbol market key for this record is unique
    # in the "their backend/system" sense; i.e. this uid for the market
    # as defined (internally) in some namespace defined by the broker
    # service.
    bsuid: str | int | None = None

    @property
    def bs_mktid(self) -> str | int | None:
        print(f'STOP USING .bsuid` for {self.fqme}')
        return self.bs_mktid

    # XXX NOTE: this will come from the `MktPair`
    # instead of defined here right?
    # optional fqsn for the source "asset"/money symbol?
    # from: Optional[str] = None


def iter_by_dt(
    clears: dict[str, Any],

) -> Iterator[tuple[str, dict]]:
    '''
    Iterate entries of a ``clears: dict`` table sorted by entry recorded
    datetime presumably set at the ``'dt'`` field in each entry.

    '''
    for tid, data in sorted(
        list(clears.items()),
        key=lambda item: item[1]['dt'],
    ):
        yield tid, data
