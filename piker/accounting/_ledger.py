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
from pprint import pformat
from types import ModuleType
from typing import (
    Any,
    Callable,
    Generator,
    Literal,
    TYPE_CHECKING,
)

from pendulum import (
    DateTime,
)
import tomli_w  # for fast ledger writing

from .. import config
from ..data.types import Struct
from ..log import get_logger
from .calc import (
    iter_by_dt,
)

if TYPE_CHECKING:
    from ..data._symcache import (
        SymbologyCache,
    )

log = get_logger(__name__)


TxnType = Literal[
    'clear',
    'transfer',

    # TODO: see https://github.com/pikers/piker/issues/510
    # 'split',
    # 'rename',
    # 'resize',
    # 'removal',
]


class Transaction(Struct, frozen=True):

    # NOTE: this is a unified acronym also used in our `MktPair`
    # and can stand for any of a
    # "fully qualified <blank> endpoint":
    # - "market" in the case of financial trades
    #   (btcusdt.spot.binance).
    # - "merkel (tree)" aka a blockchain system "wallet tranfers"
    #   (btc.blockchain)
    # - "money" for tradtitional (digital databases)
    #   *bank accounts* (usd.swift, eur.sepa)
    fqme: str

    tid: str | int  # unique transaction id
    size: float
    price: float
    cost: float  # commisions or other additional costs
    dt: DateTime

    # the "event type" in terms of "market events" see above and
    # https://github.com/pikers/piker/issues/510
    etype: TxnType = 'clear'

    # TODO: we can drop this right since we
    # can instead expect the backend to provide this
    # via the `MktPair`?
    expiry: DateTime | None = None

    # (optional) key-id defined by the broker-service backend which
    # ensures the instrument-symbol market key for this record is unique
    # in the "their backend/system" sense; i.e. this uid for the market
    # as defined (internally) in some namespace defined by the broker
    # service.
    bs_mktid: str | int | None = None

    def to_dict(
        self,
        **kwargs,
    ) -> dict:
        dct: dict[str, Any] = super().to_dict(**kwargs)

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
    # NOTE: see `open_trade_ledger()` for defaults, this should
    # never be constructed manually!
    def __init__(
        self,
        ledger_dict: dict,
        file_path: Path,
        account: str,
        mod: ModuleType,  # broker mod
        tx_sort: Callable,
        symcache: SymbologyCache,

    ) -> None:
        self.account: str = account
        self.file_path: Path = file_path
        self.mod: ModuleType = mod
        self.tx_sort: Callable = tx_sort

        self._symcache: SymbologyCache = symcache

        # any added txns we keep in that form for meta-data
        # gathering purposes
        self._txns: dict[str, Transaction] = {}

        super().__init__(ledger_dict)

    def __repr__(self) -> str:
        return (
            f'TransactionLedger: {len(self)}\n'
            f'{pformat(list(self.data))}'
        )

    @property
    def symcache(self) -> SymbologyCache:
        '''
        Read-only ref to backend's ``SymbologyCache``.

        '''
        return self._symcache

    def update_from_t(
        self,
        t: Transaction,
    ) -> None:
        '''
        Given an input `Transaction`, cast to `dict` and update
        from it's transaction id.

        '''
        self.data[t.tid] = t.to_dict()
        self._txns[t.tid] = t

    def iter_txns(
        self,
        symcache: SymbologyCache | None = None,

    ) -> Generator[
        Transaction,
        None,
        None,
    ]:
        '''
        Deliver trades records in ``(key: str, t: Transaction)``
        form via generator.

        '''
        symcache = symcache or self._symcache

        if self.account == 'paper':
            from piker.clearing import _paper_engine
            norm_trade = _paper_engine.norm_trade
        else:
            norm_trade = self.mod.norm_trade

        # datetime-sort and pack into txs
        for tid, txdict in self.tx_sort(self.data.items()):
            txn: Transaction = norm_trade(
                tid,
                txdict,
                pairs=symcache.pairs,
            )
            yield txn

    def to_txns(
        self,
        symcache: SymbologyCache | None = None,

    ) -> dict[str, Transaction]:
        '''
        Return entire output from ``.iter_txns()`` in a ``dict``.

        '''
        txns: dict[str, Transaction] = {}
        for t in self.iter_txns(symcache=symcache):

            if not t:
                log.warning(f'{self.mod.name}:{self.account} TXN is -> {t}')
                continue

            txns[t.tid] = t

        return txns

    def write_config(self) -> None:
        '''
        Render the self.data ledger dict to its TOML file form.

        ALWAYS order datetime sorted!

        '''
        is_paper: bool = self.account == 'paper'

        towrite: dict[str, Any] = {}
        for tid, txdict in self.tx_sort(self.data.copy()):
            # write blank-str expiry for non-expiring assets
            if (
                'expiry' in txdict
                and txdict['expiry'] is None
            ):
                txdict['expiry'] = ''

            # (maybe) re-write old acro-key
            if is_paper:
                fqme: str = txdict.pop('fqsn', None) or txdict['fqme']
                bs_mktid: str | None = txdict.get('bs_mktid')

                if (
                    fqme not in self._symcache.mktmaps
                    or (
                        # also try to see if this is maybe a paper
                        # engine ledger in which case the bs_mktid
                        # should be the fqme as well!
                        bs_mktid
                        and fqme != bs_mktid
                    )
                ):
                    # always take any (paper) bs_mktid if defined and
                    # in the backend's cache key set.
                    if bs_mktid in self._symcache.mktmaps:
                        fqme: str = bs_mktid
                    else:
                        best_fqme: str = list(self._symcache.search(fqme))[0]
                        log.warning(
                            f'Could not find FQME: {fqme} in qualified set?\n'
                            f'Qualifying and expanding {fqme} -> {best_fqme}'
                        )
                        fqme = best_fqme

                if (
                    bs_mktid
                    and bs_mktid != fqme
                ):
                    # in paper account case always make sure both the
                    # fqme and bs_mktid are fully qualified..
                    txdict['bs_mktid'] = fqme

                # in paper ledgers always write the latest
                # symbology key field: an FQME.
                txdict['fqme'] = fqme

            towrite[tid] = txdict

        with self.file_path.open(mode='wb') as fp:
            tomli_w.dump(towrite, fp)


def load_ledger(
    brokername: str,
    acctid: str,

    # for testing or manual load from file
    dirpath: Path | None = None,

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

    ldir: Path = (
        dirpath
        or
        config._config_dir / 'accounting' / 'ledgers'
    )
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

    allow_from_sync_code: bool = False,
    symcache: SymbologyCache | None = None,

    # default is to sort by detected datetime-ish field
    tx_sort: Callable = iter_by_dt,
    rewrite: bool = False,

    # for testing or manual load from file
    _fp: Path | None = None,

) -> Generator[TransactionLedger, None, None]:
    '''
    Indempotently create and read in a trade log file from the
    ``<configuration_dir>/ledgers/`` directory.

    Files are named per broker account of the form
    ``<brokername>_<accountname>.toml``. The ``accountname`` here is the
    name as defined in the user's ``brokers.toml`` config.

    '''
    from ..brokers import get_brokermod
    mod: ModuleType = get_brokermod(broker)

    ledger_dict, fpath = load_ledger(
        broker,
        account,
        dirpath=_fp,
    )
    cpy = ledger_dict.copy()

    # XXX NOTE: if not provided presume we are being called from
    # sync code and need to maybe run `trio` to generate..
    if symcache is None:

        # XXX: be mega pendantic and ensure the caller knows what
        # they're doing..
        if not allow_from_sync_code:
            raise RuntimeError(
                'You MUST set `allow_from_sync_code=True` when '
                'calling `open_trade_ledger()` from sync code! '
                'If you are calling from async code you MUST '
                'instead pass a `symcache: SymbologyCache`!'
            )

        from ..data._symcache import (
            get_symcache,
        )
        symcache: SymbologyCache = get_symcache(broker)

    assert symcache

    ledger = TransactionLedger(
        ledger_dict=cpy,
        file_path=fpath,
        account=account,
        mod=mod,
        symcache=symcache,
        tx_sort=getattr(mod, 'tx_sort', tx_sort),
    )
    try:
        yield ledger
    finally:
        if (
            ledger.data != ledger_dict
            or rewrite
        ):
            # TODO: show diff output?
            # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
            log.info(f'Updating ledger for {fpath}:\n')
            ledger.write_config()
