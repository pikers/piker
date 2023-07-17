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
from typing import (
    Any,
    Callable,
    Iterator,
    Union,
    Generator
)

from pendulum import (
    datetime,
    DateTime,
    from_timestamp,
    parse,
)
import tomli_w  # for fast ledger writing

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
    # we don't really need the fqme any more..
    fqme: str

    tid: Union[str, int]  # unique transaction id
    size: float
    price: float
    cost: float  # commisions or other additional costs
    dt: datetime

    # TODO: we can drop this right since we
    # can instead expect the backend to provide this
    # via the `MktPair`?
    expiry: datetime | None = None

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
        tx_sort: Callable,

    ) -> None:
        self.file_path = file_path
        self.tx_sort = tx_sort
        super().__init__(ledger_dict)

    def update_from_t(
        self,
        t: Transaction,
    ) -> None:
        '''
        Given an input `Transaction`, cast to `dict` and update
        from it's transaction id.

        '''
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

        # TODO: use tx_sort here yah?
        for tid, txdict in self.data.items():
            # special field handling for datetimes
            # to ensure pendulum is used!
            fqme = txdict.get('fqme') or txdict['fqsn']
            dt = parse(txdict['dt'])
            expiry = txdict.get('expiry')

            mkt = mkt_by_fqme.get(fqme)
            if not mkt:
                # we can't build a trans if we don't have
                # the ``.sys: MktPair`` info, so skip.
                continue

            tx = Transaction(
                fqme=fqme,
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
            yield tid, tx

    def to_trans(
        self,
        **kwargs,

    ) -> dict[str, Transaction]:
        '''
        Return entire output from ``.iter_trans()`` in a ``dict``.

        '''
        return dict(self.iter_trans(**kwargs))

    def write_config(
        self,

    ) -> None:
        '''
        Render the self.data ledger dict to it's TOML file form.

        '''
        cpy = self.data.copy()
        towrite: dict[str, Any] = {}
        for tid, trans in cpy.items():

            # drop key for non-expiring assets
            txdict = towrite[tid] = self.data[tid]
            if (
                'expiry' in txdict
                and txdict['expiry'] is None
            ):
                txdict.pop('expiry')

            # re-write old acro-key
            fqme = txdict.get('fqsn')
            if fqme:
                txdict['fqme'] = fqme

        with self.file_path.open(mode='wb') as fp:
            tomli_w.dump(towrite, fp)


def iter_by_dt(
    records: dict[str, Any],

    # NOTE: parsers are looked up in the insert order
    # so if you know that the record stats show some field
    # is more common then others, stick it at the top B)
    parsers: dict[tuple[str], Callable] = {
        'dt': None,  # parity case
        'datetime': parse,  # datetime-str
        'time': from_timestamp,  # float epoch
    },
    key: Callable | None = None,

) -> Iterator[tuple[str, dict]]:
    '''
    Iterate entries of a ``records: dict`` table sorted by entry recorded
    datetime presumably set at the ``'dt'`` field in each entry.

    '''
    def dyn_parse_to_dt(
        pair: tuple[str, dict],
    ) -> DateTime:
        _, txdict = pair
        k, v, parser = next(
            (k, txdict[k], parsers[k]) for k in parsers if k in txdict
        )

        return parser(v) if parser else v

    for tid, data in sorted(
        records.items(),
        key=key or dyn_parse_to_dt,
    ):
        yield tid, data


def load_ledger(
    brokername: str,
    acctid: str,

) -> tuple[dict, Path]:
    '''
    Load a ledger (TOML) file from user's config directory:
    $CONFIG_DIR/accounting/ledgers/trades_<brokername>_<acctid>.toml

    Return its `dict`-content and file path.

    '''
    import time
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    ldir: Path = config._config_dir / 'accounting' / 'ledgers'
    if not ldir.is_dir():
        ldir.mkdir()

    fname = f'trades_{brokername}_{acctid}.toml'
    fpath: Path = ldir / fname

    if not fpath.is_file():
        log.info(
            f'Creating new local trades ledger: {fpath}'
        )
        fpath.touch()

    with fpath.open(mode='rb') as cf:
        start = time.time()
        ledger_dict = tomllib.load(cf)
        log.debug(f'Ledger load took {time.time() - start}s')

    return ledger_dict, fpath


@cm
def open_trade_ledger(
    broker: str,
    account: str,

    # default is to sort by detected datetime-ish field
    tx_sort: Callable = iter_by_dt,

) -> Generator[TransactionLedger, None, None]:
    '''
    Indempotently create and read in a trade log file from the
    ``<configuration_dir>/ledgers/`` directory.

    Files are named per broker account of the form
    ``<brokername>_<accountname>.toml``. The ``accountname`` here is the
    name as defined in the user's ``brokers.toml`` config.

    '''
    ledger_dict, fpath = load_ledger(broker, account)
    cpy = ledger_dict.copy()
    ledger = TransactionLedger(
        ledger_dict=cpy,
        file_path=fpath,
        tx_sort=tx_sort,
    )
    try:
        yield ledger
    finally:
        if ledger.data != ledger_dict:

            # TODO: show diff output?
            # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
            log.info(f'Updating ledger for {fpath}:\n')
            ledger.write_config()
