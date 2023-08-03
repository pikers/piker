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
from __future__ import annotations
from contextlib import contextmanager as cm
from decimal import Decimal
from pprint import pformat
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Iterator,
    Generator
)

import pendulum
from pendulum import (
    datetime,
    now,
)
import polars as pl
import tomlkit

from ._ledger import (
    Transaction,
    TransactionLedger,
)
from ._mktinfo import (
    MktPair,
    Asset,
    unpack_fqme,
)
from .calc import (
    ppu,
    # iter_by_dt,
)
from .. import config
from ..clearing._messages import (
    BrokerdPosition,
)
from ..data.types import Struct
from ..data._symcache import SymbologyCache
from ..log import get_logger

log = get_logger(__name__)


class Position(Struct):
    '''
    An asset "position" model with attached clearing transaction history.

    A financial "position" in `piker` terms is a summary of accounting
    metrics computed from a transaction ledger; generally it describes
    some accumulative "size" and "average price" from the summarized
    underlying transaction set.

    In piker we focus on the `.ppu` (price per unit) and the `.bep`
    (break even price) including all transaction entries and exits since
    the last "net-zero" size of the destination asset's holding.

    This interface serves as an object API for computing and
    tracking positions as well as supports serialization for
    storage in the local file system (in TOML) and to interchange
    as a msg over IPC.

    '''
    mkt: MktPair

    # can be +ve or -ve for long/short
    # size: float

    # "price-per-unit price" above or below which pnl moves above and
    # below zero for the entirety of the current "trade state". The ppu
    # is only modified on "increases of" the absolute size of a position
    # in one of a long/short "direction" (i.e. abs(.size_i) > 0 after
    # the next transaction given .size was > 0 before that tx, and vice
    # versa for -ve sized positions).
    # ppu: float

    # TODO: break-even-price support!
    # bep: float

    # unique "backend system market id"
    bs_mktid: str

    split_ratio: int | None = None

    # TODO: use a `pl.DataFrame` intead?
    _events: dict[str, Transaction | dict] = {}

    @property
    def expiry(self) -> datetime | None:
        '''
        Security expiry if it has a limited lifetime.

        For non-derivative markets this is normally `None`.

        '''
        exp: str | None = self.mkt.expiry
        if exp is None:
            return None

        match exp.lower():
            # empty str, 'perp' (contract) or simply a null
            # signifies instrument with NO expiry.
            case 'perp' | '' | None:
                return None

            case str():
                return pendulum.parse(exp)

            case _:
                raise ValueError(
                    f'Unhandled `MktPair.expiry`: `{exp}`'
                )

    # TODO: idea: "real LIFO" dynamic positioning.
    # - when a trade takes place where the pnl for
    # the (set of) trade(s) is below the breakeven price
    # it may be that the trader took a +ve pnl on a short(er)
    # term trade in the same account.
    # - in this case we could recalc the be price to
    # be reverted back to it's prior value before the nearest term
    # trade was opened.?
    # def bep() -> float:
    #     ...
    def clears_df(self) -> pl.DataFrame:
        ...

    def clearsitems(self) -> list[(str, dict)]:
        return ppu(
            self.iter_by_type('clear'),
            as_ledger=True
        )

    def iter_by_type(
        self,
        etype: str,

    ) -> Iterator[dict | Transaction]:
        '''
        Iterate the internally managed ``._events: dict`` table in
        datetime-stamped order.

        '''
        # sort on the expected datetime field
        # for event in iter_by_dt(
        for event in sorted(
            self._events.values(),
            key=lambda entry: entry.dt
        ):
            # if event.etype == etype:
            match event:
                case (
                    {'etype': _etype} |
                    Transaction(etype=str(_etype))
                ):
                    assert _etype == etype
                    yield event


    def minimized_clears(self) -> dict[str, dict]:
        '''
        Minimize the position's clears entries by removing
        all transactions before the last net zero size except for when
        a clear event causes a position "side" change (i.e. long to short
        after a single fill) wherein we store the transaction prior to the
        net-zero pass.

        This avoids unnecessary history irrelevant to the current
        non-net-zero size state when serializing for offline storage.

        '''
        # scan for the last "net zero" position by iterating
        # transactions until the next net-zero cumsize, rinse,
        # repeat.
        cumsize: float = 0
        clears_since_zero: list[dict] = []

        for tid, cleardict in self.clearsitems():
            cumsize = float(
                # self.mkt.quantize(cumsize + cleardict['tx'].size
                self.mkt.quantize(cleardict['cumsize'])
            )
            clears_since_zero.append(cleardict)

            # NOTE: always pop sign change since we just use it to
            # determine which entry to clear "up to".
            sign_change: bool = cleardict.pop('sign_change')
            if cumsize == 0:
                clears_since_zero = clears_since_zero[:-2]
                # clears_since_zero.clear()

            elif sign_change:
                clears_since_zero = clears_since_zero[:-1]

        return clears_since_zero

    def to_pretoml(self) -> tuple[str, dict]:
        '''
        Prep this position's data contents for export as an entry
        in a TOML "account file" (such as
        `account.binance.paper.toml`) including re-structuring of
        the ``._events`` entries as an array of inline-subtables
        for better ``pps.toml`` compactness.

        '''
        mkt: MktPair = self.mkt
        assert isinstance(mkt, MktPair)

        # TODO: we need to figure out how to have one top level
        # listing venue here even when the backend isn't providing
        # it via the trades ledger..
        # drop symbol obj in serialized form
        fqme: str = mkt.fqme
        broker, mktep, venue, suffix = unpack_fqme(fqme)

        # an asset resolved mkt where we have ``Asset`` info about
        # each tradeable asset in the market.
        asset_type: str = 'n/a'
        if mkt.resolved:
            dst: Asset = mkt.dst
            asset_type = dst.atype

        asdict: dict[str, Any] = {
            'bs_mktid': self.bs_mktid,
            # 'expiry': self.expiry or '',
            'asset_type': asset_type,
            'price_tick': mkt.price_tick,
            'size_tick': mkt.size_tick,
        }
        if exp := self.expiry:
            asdict['expiry'] = exp

        clears_since_zero: list[dict] = self.minimized_clears()

        # setup a "multi-line array of inline tables" which we call
        # the "clears table", contained by each position entry in
        # an "account file".
        clears_table: tomlkit.Array = tomlkit.array()
        clears_table.multiline(
            multiline=True,
            indent='',
        )

        for entry in clears_since_zero:
            inline_table = tomlkit.inline_table()

            # insert optional clear fields in column order
            for k in ['ppu', 'cumsize']:
                if val := entry.get(k):
                    inline_table[k] = val

            # insert required fields
            for k in ['price', 'size', 'cost']:
                inline_table[k] = entry[k]

            # NOTE: we don't actually need to serialize datetime to parsable `str`
            # since `tomlkit` supports a native `DateTime` but
            # seems like we're not doing it entirely in clearing
            # tables yet?
            inline_table['dt'] = entry['dt']  # .isoformat('T')

            tid: str = entry['tid']
            inline_table['tid'] = tid
            clears_table.append(inline_table)

        # assert not events
        asdict['clears'] = clears_table

        return fqme, asdict

    def update_from_msg(
        self,
        msg: BrokerdPosition,

    ) -> None:
        '''
        Hard-set the current position from a remotely-received
        (normally via IPC) msg by applying the msg as the one (and
        only) txn in the `._events` table thus forcing the current
        asset allocation blindly.

        '''
        mkt: MktPair = self.mkt
        now_dt: pendulum.DateTime = now()
        now_str: str = str(now_dt)

        # XXX: wipe all prior txn history since we wanted it we wouldn't
        # be using this method to compute our state!
        self._events.clear()

        # NOTE WARNING XXX: we summarize the pos with a single
        # summary transaction (for now) until we either pass THIS
        # type as msg directly from emsd or come up with a better
        # way?
        t = Transaction(
            fqme=mkt.fqme,
            bs_mktid=mkt.bs_mktid,
            size=msg['size'],
            price=msg['avg_price'],
            cost=0,

            # NOTE: special provisions required!
            # - tid needs to be unique or this txn will be ignored!!
            tid=now_str,

            # TODO: also figure out how to avoid this!
            dt=now_dt,
        )
        self.add_clear(t)

    @property
    def dsize(self) -> float:
        '''
        The "dollar" size of the pp, normally in source asset
        (fiat) units.

        '''
        return self.ppu * self.cumsize

    def expired(self) -> bool:
        '''
        Predicate which checks if the contract/instrument is past
        its expiry.

        '''
        return bool(self.expiry) and self.expiry < now()

    def add_clear(
        self,
        t: Transaction,
    ) -> bool:
        '''
        Update clearing table by calculating the rolling ppu and
        (accumulative) size in both the clears entry and local
        attrs state.

        Inserts are always done in datetime sorted order.

        '''
        # added: bool = False
        tid: str = t.tid
        if tid in self._events:
            log.warning(f'{t} is already added?!')
            # return added

        # TODO: apparently this IS possible with a dict but not
        # common and probably not that beneficial unless we're also
        # going to do cum-calcs on each insert?
        # https://stackoverflow.com/questions/38079171/python-insert-new-element-into-sorted-list-of-dictionaries
        # from bisect import insort
        # insort(
        #     self._clears,
        #     clear,
        #     key=lambda entry: entry['dt']
        # )
        self._events[tid] = t
        return True

    # TODO: compute these incrementally instead
    # of re-looping through each time resulting in O(n**2)
    # behaviour..? Can we have some kinda clears len to cached
    # output subsys?
    def calc_ppu(self) -> float:
        return ppu(self.iter_by_type('clear'))

        # # return self.clearsdict()
        # # )
        # return list(self.clearsdict())[-1][1]['ppu']

    @property
    def ppu(self) -> float:
        return round(
            self.calc_ppu(),
            ndigits=self.mkt.price_tick_digits,
        )

    def calc_size(self) -> float:
        '''
        Calculate the unit size of this position in the destination
        asset using the clears/trade event table; zero if expired.

        '''
        # time-expired pps (normally derivatives) are "closed"
        # and have a zero size.
        if self.expired():
            return 0.

        clears: list[(str, dict)] = self.clearsitems()
        if clears:
            return clears[-1][1]['cumsize']
        else:
            return 0.

        # if self.split_ratio is not None:
        #     size = round(size * self.split_ratio)

        # return float(
        #     self.mkt.quantize(size),
        # )

    # TODO: ideally we don't implicitly recompute the
    # full sequence from `.clearsdict()` every read..
    # the writer-updates-local-attr-state was actually kinda nice
    # before, but sometimes led to hard to detect bugs when
    # state was de-synced.
    @property
    def cumsize(self) -> float:

        if (
            self.expiry
            and self.expiry < now()
        ):
            return 0

        return round(
            self.calc_size(),
            ndigits=self.mkt.size_tick_digits,
        )

    # TODO: once we have an `.events` table with diff
    # mkt event types..?
    # def suggest_split(self) -> float:
    #     ...


class Account(Struct):
    '''
    The real-time (double-entry accounting) state of
    a given **asset ownership tracking system**, normally offered
    or measured from some brokerage, CEX or (implied virtual)
    summary crypto$ "wallets" aggregated and tracked over some set
    of DEX-es.

    Both market-mapped and ledger-system-native (aka inter-account
    "transfers") transactions are accounted and they pertain to
    (implied) PnL relatve to any other accountable asset.

    More specifically in piker terms, an account tracks all of:

    - the *balances* of all assets currently available for use either
      in (future) market or (inter-account/wallet) transfer
      transactions.
    - a transaction *ledger* from a given brokerd backend whic
      is a recording of all (know) such transactions from the past.
    - a set of financial *positions* as measured from the current
      ledger state.

    See the semantic origins from double-bookeeping:
    https://en.wikipedia.org/wiki/Double-entry_bookkeeping

    '''
    mod: ModuleType
    acctid: str
    pps: dict[str, Position]

    conf_path: Path
    conf: dict | None = {}

    # TODO: track a table of asset balances as `.balances:
    # dict[Asset, float]`?

    @property
    def brokername(self) -> str:
        return self.mod.name

    def update_from_ledger(
        self,
        ledger: TransactionLedger | dict[str, Transaction],
        cost_scalar: float = 2,
        symcache: SymbologyCache | None = None,

        _mktmap_table: dict[str, MktPair] | None = None,

    ) -> dict[str, Position]:
        '''
        Update the internal `.pps[str, Position]` table from input
        transactions recomputing the price-per-unit (ppu) and
        accumulative size for each entry.

        '''
        if (
            not isinstance(ledger, TransactionLedger)
        ):
            if symcache is None:
                raise RuntimeError(
                    'No ledger provided!\n'
                    'We can not determine the `MktPair`s without a symcache..\n'
                    'Please provide `symcache: SymbologyCache` when '
                    'processing NEW positions!'
                )
            itertxns = sorted(
                ledger.values(),
                key=lambda t: t.dt,
            )
        else:
            itertxns = ledger.iter_txns()
            symcache = ledger.symcache

        pps = self.pps
        updated: dict[str, Position] = {}

        # lifo update all pps from records, ensuring
        # we compute the PPU and size sorted in time!
        for txn in itertxns:
            fqme: str = txn.fqme
            bs_mktid: str = txn.bs_mktid

            # template the mkt-info presuming a legacy market ticks
            # if no info exists in the transactions..
            try:
                mkt: MktPair = symcache.mktmaps[fqme]
            except KeyError:
                if _mktmap_table is None:
                    raise

                # XXX: caller is allowed to provide a fallback
                # mktmap table for the case where a new position is
                # being added and the preloaded symcache didn't
                # have this entry prior (eg. with frickin IB..)
                mkt = _mktmap_table[fqme]

            if not (pos := pps.get(bs_mktid)):

                # if no existing pos, allocate fresh one.
                pos = pps[bs_mktid] = Position(
                    mkt=mkt,
                    bs_mktid=bs_mktid,
                )
            else:
                # NOTE: if for some reason a "less resolved" mkt pair
                # info has been set (based on the `.fqme` being
                # a shorter string), instead use the one from the
                # transaction since it likely has (more) full
                # information from the provider.
                if len(pos.mkt.fqme) < len(fqme):
                    pos.mkt = mkt

            # update clearing acnt!
            # NOTE: likely you'll see repeats of the same
            # ``Transaction`` passed in here if/when you are
            # restarting a ``brokerd.ib`` where the API will
            # re-report trades from the current session, so we need
            # to make sure we don't "double count" these in pp
            # calculations; `Position.add_clear()` stores txs in
            # a `._events: dict[tid, tx]` which should always
            # ensure this is true!
            pos.add_clear(txn)
            updated[txn.bs_mktid] = pos

        # NOTE: deliver only the position entries that were
        # actually updated (modified the state) from the input
        # transaction set.
        return updated

    def dump_active(
        self,
    ) -> tuple[
        dict[str, Position],
        dict[str, Position]
    ]:
        '''
        Iterate all tabulated positions, render active positions to
        a ``dict`` format amenable to serialization (via TOML) and drop
        from state (``.pps``) as well as return in a ``dict`` all
        ``Position``s which have recently closed.

        '''
        # NOTE: newly closed position are also important to report/return
        # since a consumer, like an order mode UI ;), might want to react
        # based on the closure (for example removing the breakeven line
        # and clearing the entry from any lists/monitors).
        closed_pp_objs: dict[str, Position] = {}
        open_pp_objs: dict[str, Position] = {}

        pp_objs = self.pps
        for bs_mktid in list(pp_objs):
            pos = pp_objs[bs_mktid]
            # pos.ensure_state()

            # "net-zero" is a "closed" position
            if pos.cumsize == 0:
                # NOTE: we DO NOT pop the pos here since it can still be
                # used to check for duplicate clears that may come in as
                # new transaction from some backend API and need to be
                # ignored; the closed positions won't be written to the
                # ``pps.toml`` since ``pp_active_entries`` above is what's
                # written.
                closed_pp_objs[bs_mktid] = pos

            else:
                open_pp_objs[bs_mktid] = pos

        return open_pp_objs, closed_pp_objs

    def prep_toml(
        self,
        active: dict[str, Position] | None = None,

    ) -> dict[str, Any]:

        if active is None:
            active, _ = self.dump_active()

        # ONLY dict-serialize all active positions; those that are
        # closed we don't store in the ``pps.toml``.
        to_toml_dict: dict[str, Any] = {}

        pos: Position
        for bs_mktid, pos in active.items():
            # pos.ensure_state()

            # serialize to pre-toml form
            # NOTE: we only store the minimal amount of clears that
            # make up this position since the last net-zero state,
            # see `Position.to_pretoml()` for details
            fqme, asdict = pos.to_pretoml()

            # clears: list[dict] = asdict['clears']
            # assert 'Datetime' not in [0]['dt']
            log.info(f'Updating active pp: {fqme}')

            # XXX: ugh, it's cuz we push the section under
            # the broker name.. maybe we need to rethink this?
            brokerless_key = fqme.removeprefix(f'{self.brokername}.')
            to_toml_dict[brokerless_key] = asdict

        return to_toml_dict

    def write_config(self) -> None:
        '''
        Write the current account state to the user's account TOML file, normally
        something like ``pps.toml``.

        '''
        # TODO: show diff output?
        # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
        # active, closed_pp_objs = acnt.dump_active()

        active, closed = self.dump_active()
        pp_entries = self.prep_toml(active=active)
        if pp_entries:
            log.info(
                f'Updating positions in ``{self.conf_path}``:\n'
                f'n{pformat(pp_entries)}'
            )

            if self.brokername in self.conf:
                log.warning(
                    f'Rewriting {self.conf_path} keys to drop <broker.acct>!'
                )
                # legacy key schema including <brokername.account>, so
                # rewrite all entries to drop those tables since we now
                # put that in the filename!
                accounts = self.conf.pop(self.brokername)
                assert len(accounts) == 1
                entries = accounts.pop(self.acctid)
                self.conf.update(entries)

            self.conf.update(pp_entries)

            # drop any entries that are computed as net-zero
            # we don't care about storing in the pps file.
            if closed:
                bs_mktid: str
                for bs_mktid, pos in closed.items():
                    fqme: str = pos.mkt.fqme
                    if fqme in self.conf:
                        self.conf.pop(fqme)
                    else:
                        # TODO: we reallly need a diff set of
                        # loglevels/colors per subsys.
                        log.warning(
                            f'Recent position for {fqme} was closed!'
                        )

        # if there are no active position entries according
        # to the toml dump output above, then clear the config
        # file of all entries.
        elif self.conf:
            for entry in list(self.conf):
                del self.conf[entry]

        # XXX WTF: if we use a tomlkit.Integer here we get this
        # super weird --1 thing going on for cumsize!?1!
        # NOTE: the fix was to always float() the size value loaded
        # in open_pps() below!
        config.write(
            config=self.conf,
            path=self.conf_path,
            fail_empty=False,
        )


def load_account(
    brokername: str,
    acctid: str,

    dirpath: Path | None = None,

) -> tuple[dict, Path]:
    '''
    Load a accounting (with positions) file from
    $CONFIG_DIR/accounting/account.<brokername>.<acctid>.toml

    Where normally $CONFIG_DIR = ~/.config/piker/
    and we implicitly create a accounting subdir which should
    normally be linked to a git repo managed by the user B)

    '''
    legacy_fn: str = f'pps.{brokername}.{acctid}.toml'
    fn: str = f'account.{brokername}.{acctid}.toml'

    dirpath: Path = dirpath or (config._config_dir / 'accounting')
    if not dirpath.is_dir():
        dirpath.mkdir()

    conf, path = config.load(
        path=dirpath / fn,
        decode=tomlkit.parse,
        touch_if_dne=True,
    )

    if not conf:
        legacypath = dirpath / legacy_fn
        log.warning(
            f'Your account file is using the legacy `pps.` prefix..\n'
            f'Rewriting contents to new name -> {path}\n'
            'Please delete the old file!\n'
            f'|-> {legacypath}\n'
        )
        if legacypath.is_file():
            legacy_config, _ = config.load(
                path=legacypath,

                # TODO: move to tomlkit:
                # - needs to be fixed to support bidict?
                #   https://github.com/sdispater/tomlkit/issues/289
                # - we need to use or fork's fix to do multiline array
                #   indenting.
                decode=tomlkit.parse,
            )
            conf.update(legacy_config)

            # XXX: override the presumably previously non-existant
            # file with legacy's contents.
            config.write(
                conf,
                path=path,
                fail_empty=False,
            )

    return conf, path


# TODO: make this async and offer a `get_account()` that
# can be used from sync code which does the same thing as
# open_trade_ledger()!
@cm
def open_account(
    brokername: str,
    acctid: str,
    write_on_exit: bool = False,

    # for testing or manual load from file
    _fp: Path | None = None,

) -> Generator[Account, None, None]:
    '''
    Read out broker-specific position entries from
    incremental update file: ``pps.toml``.

    '''
    conf: dict
    conf_path: Path
    conf, conf_path = load_account(
        brokername,
        acctid,
        dirpath=_fp,
    )

    if brokername in conf:
        log.warning(
            f'Rewriting {conf_path} keys to drop <broker.acct>!'
        )
        # legacy key schema including <brokername.account>, so
        # rewrite all entries to drop those tables since we now
        # put that in the filename!
        accounts = conf.pop(brokername)
        for acctid in accounts.copy():
            entries = accounts.pop(acctid)
            conf.update(entries)

    # TODO: ideally we can pass in an existing
    # pps state to this right? such that we
    # don't have to do a ledger reload all the
    # time.. a couple ideas I can think of,
    # - mirror this in some client side actor which
    #   does the actual ledger updates (say the paper
    #   engine proc if we decide to always spawn it?),
    # - do diffs against updates from the ledger writer
    #   actor and the in-mem state here?
    from ..brokers import get_brokermod
    mod: ModuleType = get_brokermod(brokername)

    pp_objs: dict[str, Position] = {}
    acnt = Account(
        mod,
        acctid,
        pp_objs,
        conf_path,
        conf=conf,
    )

    # unmarshal/load ``pps.toml`` config entries into object form
    # and update `Account` obj entries.
    for fqme, entry in conf.items():

        # unique broker-backend-system market id
        bs_mktid = str(
            entry.get('bsuid')
            or entry.get('bs_mktid')
        )
        price_tick = Decimal(str(
            entry.get('price_tick_size')
            or entry.get('price_tick')
            or '0.01'
        ))
        size_tick = Decimal(str(
            entry.get('lot_tick_size')
            or entry.get('size_tick')
            or '0.0'
        ))

        # load the pair using the fqme which
        # will make the pair "unresolved" until
        # the backend broker actually loads
        # the market and position info.
        mkt = MktPair.from_fqme(
            fqme,
            price_tick=price_tick,
            size_tick=size_tick,
            bs_mktid=bs_mktid,
        )

        # TODO: RE: general "events" instead of just "clears":
        # - make this an `events` field and support more event types
        #   such as 'split', 'name_change', 'mkt_info', etc..
        # - should be make a ``Struct`` for clear/event entries? convert
        #   "clear events table" from the toml config (list of a dicts)
        #   and load it into object form for use in position processing of
        #   new clear events.

        # convert clears sub-tables (only in this form
        # for toml re-presentation) back into a master table.
        toml_clears_list: list[dict[str, Any]] = entry['clears']
        trans: list[Transaction] = []

        for clears_table in toml_clears_list:
            tid = clears_table['tid']
            dt: tomlkit.items.DateTime | str = clears_table['dt']

            # woa cool, `tomlkit` will actually load datetimes into
            # native form B)
            if isinstance(dt, str):
                dt = pendulum.parse(dt)

            clears_table['dt'] = dt
            trans.append(Transaction(
                fqme=bs_mktid,
                # sym=mkt,
                bs_mktid=bs_mktid,
                tid=tid,
                # XXX: not sure why sometimes these are loaded as
                # `tomlkit.Integer` and are eventually written with
                # an extra `-` in front like `--1`?
                size=float(clears_table['size']),
                price=float(clears_table['price']),
                cost=clears_table['cost'],
                dt=dt,
            ))

        split_ratio = entry.get('split_ratio')

        # if a string-ified expiry field is loaded we try to parse
        # it, THO, they should normally be serialized as native
        # TOML datetimes, since that's supported.
        if (
            (expiry := entry.get('expiry'))
            and isinstance(expiry, str)
        ):
            expiry: pendulum.DateTime = pendulum.parse(expiry)

        pp = pp_objs[bs_mktid] = Position(
            mkt,
            split_ratio=split_ratio,
            bs_mktid=bs_mktid,
        )

        # XXX: super critical, we need to be sure to include
        # all pps.toml clears to avoid reusing clears that were
        # already included in the current incremental update
        # state, since today's records may have already been
        # processed!
        for t in trans:
            pp.add_clear(t)

    try:
        yield acnt
    finally:
        if write_on_exit:
            acnt.write_config()


# TODO: drop the old name and THIS!
@cm
def open_pps(
    *args,
    **kwargs,
) -> Generator[Account, None, None]:
    log.warning(
        '`open_pps()` is now deprecated!\n'
        'Please use `with open_account() as cnt:`'
    )
    with open_account(*args, **kwargs) as acnt:
        yield acnt


def load_account_from_ledger(

    brokername: str,
    acctname: str,

    # post normalization filter on ledger entries to be processed
    filter_by_ids: dict[str, list[str]] | None = None,

    ledger: TransactionLedger | None = None,
    **kwargs,

) -> Account:
    '''
    Open a ledger file by broker name and account and read in and
    process any trade records into our normalized ``Transaction`` form
    and then update the equivalent ``Pptable`` and deliver the two
    bs_mktid-mapped dict-sets of the transactions and pps.

    '''
    acnt: Account
    with open_account(
        brokername,
        acctname,
        **kwargs,
    ) as acnt:
        if ledger is not None:
            acnt.update_from_ledger(ledger)

    return acnt
