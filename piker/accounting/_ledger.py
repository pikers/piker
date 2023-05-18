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
Trade and transaction ledger processing.

'''
from __future__ import annotations
from collections import UserDict
from contextlib import contextmanager as cm
from pathlib import Path
import time
from typing import (
    Any,
    Iterator,
    Union,
    Generator
)

from pendulum import (
    datetime,
    parse,
)
import tomlkit
import tomli

from .. import config
from ..data.types import Struct
from ..log import get_logger
from ._mktinfo import (
    Symbol,  # legacy
    MktPair,
    Asset,
)

log = get_logger(__name__)


class Transaction(Struct, frozen=True):

    # TODO: unify this with the `MktPair`,
    # once we have that as a required field,
    # we don't really need the fqsn any more..
    fqsn: str

    tid: Union[str, int]  # unique transaction id
    size: float
    price: float
    cost: float  # commisions or other additional costs
    dt: datetime

    # TODO: we can drop this right since we
    # can instead expect the backend to provide this
    # via the `MktPair`?
    expiry: datetime | None = None

    # remap for back-compat
    @property
    def fqme(self) -> str:
        return self.fqsn

    # TODO: drop the Symbol type, construct using
    # t.sys (the transaction system)

    # the underlying "transaction system", normally one of a ``MktPair``
    # (a description of a tradable double auction) or a ledger-recorded
    # ("ledger" in any sense as long as you can record transfers) of any
    # sort) ``Asset``.
    sym: MktPair | Asset | Symbol | None = None

    @property
    def sys(self) -> Symbol:
        return self.sym

    # (optional) key-id defined by the broker-service backend which
    # ensures the instrument-symbol market key for this record is unique
    # in the "their backend/system" sense; i.e. this uid for the market
    # as defined (internally) in some namespace defined by the broker
    # service.
    bs_mktid: str | int | None = None

    def to_dict(self) -> dict:
        dct = super().to_dict()

        # TODO: switch to sys!
        dct.pop('sym')

        # ensure we use a pendulum formatted
        # ISO style str here!@
        dct['dt'] = str(self.dt)
        return dct


class TransactionLedger(UserDict):
    '''
    Very simple ``dict`` wrapper + ``pathlib.Path`` handle to
    a TOML formatted transaction file for enabling file writes
    dynamically whilst still looking exactly like a ``dict`` from the
    outside.

    '''
    def __init__(
        self,
        ledger_dict: dict,
        file_path: Path,

    ) -> None:
        self.file_path = file_path
        super().__init__(ledger_dict)

    def write_config(self) -> None:
        '''
        Render the self.data ledger dict to it's TOML file form.

        '''
        towrite: dict[str, Any] = self.data.copy()

        for tid, txdict in self.data.items():

            # drop key for non-expiring assets
            if (
                'expiry' in txdict
                and txdict['expiry'] is None
            ):
                txdict.pop('expiry')

            # re-write old acro-key
            fqme = txdict.get('fqsn')
            if fqme:
                txdict['fqme'] = fqme

        print(f'WRITING LEDGER {self.file_path}')
        with self.file_path.open(mode='w') as fp:
            tomlkit.dump(towrite, fp)
        print(f'FINISHED WRITING LEDGER {self.file_path}')

    def update_from_t(
        self,
        t: Transaction,
    ) -> None:
        self.data[t.tid] = t.to_dict()

    def iter_trans(
        self,
        mkt_by_fqme: dict[str, MktPair],
        broker: str = 'paper',

    ) -> Generator[
        tuple[str, Transaction],
        None,
        None,
    ]:
        '''
        Deliver trades records in ``(key: str, t: Transaction)``
        form via generator.

        '''
        if broker != 'paper':
            raise NotImplementedError('Per broker support not dun yet!')

            # TODO: lookup some standard normalizer
            # func in the backend?
            # from ..brokers import get_brokermod
            # mod = get_brokermod(broker)
            # trans_dict = mod.norm_trade_records(self.data)

            # NOTE: instead i propose the normalizer is
            # a one shot routine (that can be lru cached)
            # and instead call it for each entry incrementally:
            # normer = mod.norm_trade_record(txdict)

        for tid, txdict in self.data.items():
            # special field handling for datetimes
            # to ensure pendulum is used!
            fqme = txdict.get('fqme', txdict['fqsn'])
            dt = parse(txdict['dt'])
            expiry = txdict.get('expiry')

            mkt = mkt_by_fqme.get(fqme)
            if not mkt:
                # we can't build a trans if we don't have
                # the ``.sys: MktPair`` info, so skip.
                continue

            yield (
                tid,
                Transaction(
                    fqsn=fqme,
                    tid=txdict['tid'],
                    dt=dt,
                    price=txdict['price'],
                    size=txdict['size'],
                    cost=txdict.get('cost', 0),
                    bs_mktid=txdict['bs_mktid'],

                    # TODO: change to .sys!
                    sym=mkt,
                    expiry=parse(expiry) if expiry else None,
                )
            )

    def to_trans(
        self,
        **kwargs,

    ) -> dict[str, Transaction]:
        '''
        Return entire output from ``.iter_trans()`` in a ``dict``.

        '''
        return dict(self.iter_trans(**kwargs))


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
    ledger_dict, fpath = config.load_ledger(broker, account)
    cpy = ledger_dict.copy()
    ledger = TransactionLedger(
        ledger_dict=cpy,
        file_path=fpath,
    )
    try:
        yield ledger
    finally:
        if ledger.data != ledger_dict:

            # TODO: show diff output?
            # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
            log.info(f'Updating ledger for {fpath}:\n')
            ledger.write_config()


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
