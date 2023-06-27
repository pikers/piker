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
from bisect import insort
from contextlib import contextmanager as cm
from decimal import Decimal
from math import copysign
from pprint import pformat
from pathlib import Path
from typing import (
    Any,
    Iterator,
    Generator
)

import pendulum
from pendulum import datetime, now
import tomlkit

from ._ledger import (
    Transaction,
    iter_by_dt,
    open_trade_ledger,
    TransactionLedger,
)
from ._mktinfo import (
    MktPair,
    Asset,
    unpack_fqme,
)
from .. import config
from ..clearing._messages import (
    BrokerdPosition,
)
from ..data.types import Struct
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
    size: float

    # "price-per-unit price" above or below which pnl moves above and
    # below zero for the entirety of the current "trade state". The ppu
    # is only modified on "increases of" the absolute size of a position
    # in one of a long/short "direction" (i.e. abs(.size_i) > 0 after
    # the next transaction given .size was > 0 before that tx, and vice
    # versa for -ve sized positions).
    ppu: float

    # TODO: break-even-price support!
    # bep: float

    # unique "backend system market id"
    bs_mktid: str

    split_ratio: int | None = None

    # ordered record of known constituent trade messages
    _clears: list[
        dict[str, Any],  # transaction history summaries
    ] = []
    first_clear_dt: datetime | None = None

    expiry: datetime | None = None

    def __repr__(self) -> str:
        return pformat(self.to_dict())

    def to_pretoml(self) -> tuple[str, dict]:
        '''
        Prep this position's data contents for export as an entry
        in a TOML "account file" (such as
        `account.binance.paper.toml`) including re-structuring of
        the ``._clears`` entries as an array of inline-subtables
        for better ``pps.toml`` compactness.

        '''
        asdict = self.to_dict()
        clears: list[dict] = asdict.pop('_clears')
        expiry = asdict.pop('expiry')

        if self.split_ratio is None:
            asdict.pop('split_ratio')

        # should be obvious from clears/event table
        asdict.pop('first_clear_dt')

        # TODO: we need to figure out how to have one top level
        # listing venue here even when the backend isn't providing
        # it via the trades ledger..
        # drop symbol obj in serialized form
        mkt: MktPair = asdict.pop('mkt')
        assert isinstance(mkt, MktPair)

        fqme = mkt.fqme
        broker, mktep, venue, suffix = unpack_fqme(fqme)

        # an asset resolved mkt where we have ``Asset`` info about
        # each tradeable asset in the market.
        if mkt.resolved:
            dst: Asset = mkt.dst
            asdict['asset_type'] = dst.atype

        asdict['price_tick'] = mkt.price_tick
        asdict['size_tick'] = mkt.size_tick

        if self.expiry is None:
            asdict.pop('expiry', None)
        elif expiry:
            asdict['expiry'] = str(expiry)

        clears_table: tomlkit.Array = tomlkit.array()
        clears_table.multiline(
            multiline=True,
            indent='',
        )

        # reverse sort so latest clears are at top of section?
        for entry in iter_by_dt(clears):

            inline_table = tomlkit.inline_table()

            # serialize datetime to parsable `str`
            dtstr = inline_table['dt'] = entry['dt'].isoformat('T')
            assert 'Datetime' not in dtstr

            # insert optional clear fields in column order
            for k in ['ppu', 'accum_size']:
                if val := entry.get(k):
                    inline_table[k] = val

            # insert required fields
            for k in ['price', 'size', 'cost']:
                inline_table[k] = entry[k]

            inline_table['tid'] = entry['tid']
            clears_table.append(inline_table)

        asdict['clears'] = clears_table

        return fqme, asdict

    def ensure_state(self) -> None:
        '''
        Audit either the `.size` and `.ppu` local instance vars against
        the clears table calculations and return the calc-ed values if
        they differ and log warnings to console.

        '''
        clears: list[dict] = self._clears
        self.first_clear_dt = min(clears, key=lambda e: e['dt'])['dt']
        last_clear: dict = clears[-1]
        csize: float = self.calc_size()
        accum: float = last_clear['accum_size']

        if not self.expired():
            if (
                csize != accum
                and csize != round(accum * (self.split_ratio or 1))
            ):
                raise ValueError(f'Size mismatch: {csize}')
        else:
            assert csize == 0, 'Contract is expired but non-zero size?'

        if self.size != csize:
            log.warning(
                'Position state mismatch:\n'
                f'{self.size} => {csize}'
            )
            self.size = csize

        cppu: float = self.calc_ppu()
        ppu: float = last_clear['ppu']
        if (
            cppu != ppu
            and self.split_ratio is not None

            # handle any split info entered (for now) manually by user
            and cppu != (ppu / self.split_ratio)
        ):
            raise ValueError(f'PPU mismatch: {cppu}')

        if self.ppu != cppu:
            log.warning(
                'Position state mismatch:\n'
                f'{self.ppu} => {cppu}'
            )
            self.ppu = cppu

    def update_from_msg(
        self,
        msg: BrokerdPosition,

    ) -> None:

        # XXX: better place to do this?
        mkt = self.mkt
        size_tick_digits = mkt.size_tick_digits
        price_tick_digits = mkt.price_tick_digits

        self.ppu = round(
            # TODO: change this to ppu?
            msg['avg_price'],
            ndigits=price_tick_digits,
        )
        self.size = round(
            msg['size'],
            ndigits=size_tick_digits,
        )

    @property
    def dsize(self) -> float:
        '''
        The "dollar" size of the pp, normally in trading (fiat) unit
        terms.

        '''
        return self.ppu * self.size

    # TODO: idea: "real LIFO" dynamic positioning.
    # - when a trade takes place where the pnl for
    # the (set of) trade(s) is below the breakeven price
    # it may be that the trader took a +ve pnl on a short(er)
    # term trade in the same account.
    # - in this case we could recalc the be price to
    # be reverted back to it's prior value before the nearest term
    # trade was opened.?
    # def lifo_price() -> float:
    #     ...

    def iter_clears(self) -> Iterator[tuple[str, dict]]:
        '''
        Iterate the internally managed ``._clears: dict`` table in
        datetime-stamped order.

        '''
        # sort on the already existing datetime that should have
        # been generated for the entry's table
        return iter_by_dt(
            self._clears,
            key=lambda entry: entry['dt']
        )

    def calc_ppu(
        self,

        # include transaction cost in breakeven price
        # and presume the worst case of the same cost
        # to exit this transaction (even though in reality
        # it will be dynamic based on exit stratetgy).
        cost_scalar: float = 2,

    ) -> float:
        '''
        Compute the "price-per-unit" price for the given non-zero sized
        rolling position.

        The recurrence relation which computes this (exponential) mean
        per new clear which **increases** the accumulative postiion size
        is:

        ppu[-1] = (
            ppu[-2] * accum_size[-2]
            +
            ppu[-1] * size
        ) / accum_size[-1]

        where `cost_basis` for the current step is simply the price
        * size of the most recent clearing transaction.

        '''
        asize_h: list[float] = []  # historical accumulative size
        ppu_h: list[float] = []  # historical price-per-unit

        entry: dict[str, Any]
        for entry in self.iter_clears():
            clear_size = entry['size']
            clear_price: str | float = entry['price']
            is_clear: bool = not isinstance(clear_price, str)

            last_accum_size = asize_h[-1] if asize_h else 0
            accum_size = last_accum_size + clear_size
            accum_sign = copysign(1, accum_size)

            sign_change: bool = False

            if accum_size == 0:
                ppu_h.append(0)
                asize_h.append(0)
                continue

            # on transfers we normally write some non-valid
            # price since withdrawal to another account/wallet
            # has nothing to do with inter-asset-market prices.
            # TODO: this should be better handled via a `type: 'tx'`
            # field as per existing issue surrounding all this:
            # https://github.com/pikers/piker/issues/510
            if isinstance(clear_price, str):
                # TODO: we can't necessarily have this commit to
                # the overall pos size since we also need to
                # include other positions contributions to this
                # balance or we might end up with a -ve balance for
                # the position..
                continue

            # test if the pp somehow went "passed" a net zero size state
            # resulting in a change of the "sign" of the size (+ve for
            # long, -ve for short).
            sign_change = (
                copysign(1, last_accum_size) + accum_sign == 0
                and last_accum_size != 0
            )

            # since we passed the net-zero-size state the new size
            # after sum should be the remaining size the new
            # "direction" (aka, long vs. short) for this clear.
            if sign_change:
                clear_size = accum_size
                abs_diff = abs(accum_size)
                asize_h.append(0)
                ppu_h.append(0)

            else:
                # old size minus the new size gives us size diff with
                # +ve -> increase in pp size
                # -ve -> decrease in pp size
                abs_diff = abs(accum_size) - abs(last_accum_size)

            # XXX: LIFO breakeven price update. only an increaze in size
            # of the position contributes the breakeven price,
            # a decrease does not (i.e. the position is being made
            # smaller).
            # abs_clear_size = abs(clear_size)
            abs_new_size = abs(accum_size)

            if (
                abs_diff > 0
                and is_clear
            ):

                cost_basis = (
                    # cost basis for this clear
                    clear_price * abs(clear_size)
                    +
                    # transaction cost
                    accum_sign * cost_scalar * entry['cost']
                )

                if asize_h:
                    size_last = abs(asize_h[-1])
                    cb_last = ppu_h[-1] * size_last
                    ppu = (cost_basis + cb_last) / abs_new_size

                else:
                    ppu = cost_basis / abs_new_size

                ppu_h.append(ppu)
                asize_h.append(accum_size)

            else:
                # TODO: for PPU we should probably handle txs out
                # (aka withdrawals) similarly by simply not having
                # them contrib to the running PPU calc and only
                # when the next entry clear comes in (which will
                # then have a higher weighting on the PPU).

                # on "exit" clears from a given direction,
                # only the size changes not the price-per-unit
                # need to be updated since the ppu remains constant
                # and gets weighted by the new size.
                asize_h.append(accum_size)
                ppu_h.append(ppu_h[-1])

        final_ppu = ppu_h[-1] if ppu_h else 0

        # handle any split info entered (for now) manually by user
        if self.split_ratio is not None:
            final_ppu /= self.split_ratio

        return final_ppu

    def expired(self) -> bool:
        '''
        Predicate which checks if the contract/instrument is past its expiry.

        '''
        return bool(self.expiry) and self.expiry < now()

    def calc_size(self) -> float:
        '''
        Calculate the unit size of this position in the destination
        asset using the clears/trade event table; zero if expired.

        '''
        size: float = 0.

        # time-expired pps (normally derivatives) are "closed"
        # and have a zero size.
        if self.expired():
            return 0.

        for entry in self._clears:
            size += entry['size']
            # XXX: do we need it every step?
            # no right since rounding is an LT?
            # size = self.mkt.quantize(
            #     size + entry['size'],
            #     quantity_type='size',
            # )

        if self.split_ratio is not None:
            size = round(size * self.split_ratio)

        return float(
            self.mkt.quantize(size),
        )

    def minimize_clears(
        self,

    ) -> dict[str, dict]:
        '''
        Minimize the position's clears entries by removing
        all transactions before the last net zero size to avoid
        unnecessary history irrelevant to the current pp state.

        '''
        size: float = 0
        clears_since_zero: list[dict] = []

        # TODO: we might just want to always do this when iterating
        # a ledger? keep a state of the last net-zero and only do the
        # full iterate when no state was stashed?

        # scan for the last "net zero" position by iterating
        # transactions until the next net-zero size, rinse, repeat.
        for clear in self._clears:
            size = float(
                self.mkt.quantize(size + clear['size'])
            )
            clears_since_zero.append(clear)

            if size == 0:
                clears_since_zero.clear()

        self._clears = clears_since_zero
        return self._clears

    def add_clear(
        self,
        t: Transaction,
    ) -> dict:
        '''
        Update clearing table by calculating the rolling ppu and
        (accumulative) size in both the clears entry and local
        attrs state.

        Inserts are always done in datetime sorted order.

        '''
        clear: dict[str, float | str | int] = {
            'tid': t.tid,
            'cost': t.cost,
            'price': t.price,
            'size': t.size,
            'dt': t.dt
        }

        insort(
            self._clears,
            clear,
            key=lambda entry: entry['dt']
        )

        # TODO: compute these incrementally instead
        # of re-looping through each time resulting in O(n**2)
        # behaviour..?

        # NOTE: we compute these **after** adding the entry in order to
        # make the recurrence relation math work inside
        # ``.calc_size()``.
        self.size = clear['accum_size'] = self.calc_size()
        self.ppu = clear['ppu'] = self.calc_ppu()

        return clear

    # TODO: once we have an `.events` table with diff
    # mkt event types..?
    # def suggest_split(self) -> float:
    #     ...


# TODO: maybe a better name is just `Account` and we include
# a table of asset balances as `.balances: dict[Asset, float]`?
class PpTable(Struct):

    brokername: str
    acctid: str
    pps: dict[str, Position]
    conf_path: Path
    conf: dict | None = {}

    def update_from_trans(
        self,
        trans: dict[str, Transaction],
        cost_scalar: float = 2,

    ) -> dict[str, Position]:
        '''
        Update the internal `.pps[str, Position]` table from input
        transactions recomputing the price-per-unit (ppu) and
        accumulative size for each entry.

        '''
        pps = self.pps
        updated: dict[str, Position] = {}

        # lifo update all pps from records, ensuring
        # we compute the PPU and size sorted in time!
        for t in sorted(
            trans.values(),
            key=lambda t: t.dt,
            # reverse=True,
        ):
            fqme = t.fqme
            bs_mktid = t.bs_mktid

            # template the mkt-info presuming a legacy market ticks
            # if no info exists in the transactions..
            mkt: MktPair = t.sys
            pos = pps.get(bs_mktid)
            if not pos:
                # if no existing pos, allocate fresh one.
                pos = pps[bs_mktid] = Position(
                    mkt=mkt,
                    size=0.0,
                    ppu=0.0,
                    bs_mktid=bs_mktid,
                    expiry=t.expiry,
                )
            else:
                # NOTE: if for some reason a "less resolved" mkt pair
                # info has been set (based on the `.fqme` being
                # a shorter string), instead use the one from the
                # transaction since it likely has (more) full
                # information from the provider.
                if len(pos.mkt.fqme) < len(fqme):
                    pos.mkt = mkt

            clears: list[dict] = pos._clears
            if clears:
                first_clear_dt = pos.first_clear_dt

                # don't do updates for ledger records we already have
                # included in the current pps state.
                if (
                    t.tid in clears
                    or (
                        first_clear_dt
                        and t.dt < first_clear_dt
                    )
                ):
                    # NOTE: likely you'll see repeats of the same
                    # ``Transaction`` passed in here if/when you are restarting
                    # a ``brokerd.ib`` where the API will re-report trades from
                    # the current session, so we need to make sure we don't
                    # "double count" these in pp calculations.
                    continue

            # update clearing table
            pos.add_clear(t)
            updated[t.bs_mktid] = pos

        # re-calc ppu and accumulative sizing.
        for bs_mktid, pos in updated.items():
            pos.ensure_state()

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
            pp = pp_objs[bs_mktid]
            pp.ensure_state()

            if (
                # "net-zero" is a "closed" position
                pp.size == 0

                # time-expired pps (normally derivatives) are "closed"
                or (pp.expiry and pp.expiry < now())
            ):
                # for expired cases
                pp.size = 0

                # NOTE: we DO NOT pop the pp here since it can still be
                # used to check for duplicate clears that may come in as
                # new transaction from some backend API and need to be
                # ignored; the closed positions won't be written to the
                # ``pps.toml`` since ``pp_active_entries`` above is what's
                # written.
                closed_pp_objs[bs_mktid] = pp

            else:
                open_pp_objs[bs_mktid] = pp

        return open_pp_objs, closed_pp_objs

    def to_toml(
        self,
        active: dict[str, Position] | None = None,

    ) -> dict[str, Any]:

        if active is None:
            active, _ = self.dump_active()

        # ONLY dict-serialize all active positions; those that are
        # closed we don't store in the ``pps.toml``.
        to_toml_dict = {}

        pos: Position
        for bs_mktid, pos in active.items():
            # NOTE: we only store the minimal amount of clears that make up this
            # position since the last net-zero state.
            pos.minimize_clears()
            pos.ensure_state()

            # serialize to pre-toml form
            fqme, asdict = pos.to_pretoml()

            assert 'Datetime' not in asdict['clears'][0]['dt']
            log.info(f'Updating active pp: {fqme}')

            # XXX: ugh, it's cuz we push the section under
            # the broker name.. maybe we need to rethink this?
            brokerless_key = fqme.removeprefix(f'{self.brokername}.')
            to_toml_dict[brokerless_key] = asdict

        return to_toml_dict

    def write_config(self) -> None:
        '''
        Write the current position table to the user's ``pps.toml``.

        '''
        # TODO: show diff output?
        # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
        # active, closed_pp_objs = table.dump_active()

        active, closed = self.dump_active()
        pp_entries = self.to_toml(active=active)
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

        config.write(
            config=self.conf,
            path=self.conf_path,
            fail_empty=False,
        )


def load_account(
    brokername: str,
    acctid: str,

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

    dirpath: Path = config._config_dir / 'accounting'
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


@cm
def open_pps(
    brokername: str,
    acctid: str,
    write_on_exit: bool = False,

) -> Generator[PpTable, None, None]:
    '''
    Read out broker-specific position entries from
    incremental update file: ``pps.toml``.

    '''
    conf: dict
    conf_path: Path
    conf, conf_path = load_account(brokername, acctid)

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

    pp_objs = {}
    table = PpTable(
        brokername,
        acctid,
        pp_objs,
        conf_path,
        conf=conf,
    )

    # unmarshal/load ``pps.toml`` config entries into object form
    # and update `PpTable` obj entries.
    for fqme, entry in conf.items():

        # atype = entry.get('asset_type', '<unknown>')

        # unique broker market id
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
            bs_mktid=bs_mktid
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

            tid = clears_table.get('tid')
            dtstr = clears_table['dt']
            dt = pendulum.parse(dtstr)
            clears_table['dt'] = dt

            trans.append(Transaction(
                fqme=bs_mktid,
                sym=mkt,
                bs_mktid=bs_mktid,
                tid=tid,
                size=clears_table['size'],
                price=clears_table['price'],
                cost=clears_table['cost'],
                dt=dt,
            ))

        size = entry['size']

        # TODO: remove but, handle old field name for now
        ppu = entry.get(
            'ppu',
            entry.get('be_price', 0),
        )

        split_ratio = entry.get('split_ratio')

        expiry = entry.get('expiry')
        if expiry:
            expiry = pendulum.parse(expiry)

        pp = pp_objs[bs_mktid] = Position(
            mkt,
            size=size,
            ppu=ppu,
            split_ratio=split_ratio,
            expiry=expiry,
            bs_mktid=bs_mktid,
        )

        # XXX: super critical, we need to be sure to include
        # all pps.toml clears to avoid reusing clears that were
        # already included in the current incremental update
        # state, since today's records may have already been
        # processed!
        for t in trans:
            pp.add_clear(t)

        # audit entries loaded from toml
        pp.ensure_state()

    try:
        yield table
    finally:
        if write_on_exit:
            table.write_config()


def load_pps_from_ledger(

    brokername: str,
    acctname: str,

    # post normalization filter on ledger entries to be processed
    filter_by_ids: list[str] | None = None,

) -> tuple[
    dict[str, Transaction],
    PpTable,
]:
    '''
    Open a ledger file by broker name and account and read in and
    process any trade records into our normalized ``Transaction`` form
    and then update the equivalent ``Pptable`` and deliver the two
    bs_mktid-mapped dict-sets of the transactions and pps.

    '''
    ledger: TransactionLedger
    table: PpTable
    with (
        open_trade_ledger(brokername, acctname) as ledger,
        open_pps(brokername, acctname) as table,
    ):
        if not ledger:
            # null case, no ledger file with content
            return {}

        from ..brokers import get_brokermod
        mod = get_brokermod(brokername)
        src_records: dict[str, Transaction] = mod.norm_trade_records(
            ledger
        )

        if not filter_by_ids:
            # records = src_records
            records = ledger

        else:
            records = {}
            bs_mktids = set(map(str, filter_by_ids))

            # for tid, recdict in ledger.items():
            for tid, r in src_records.items():
                if r.bs_mktid in bs_mktids:
                    records[tid] = r.to_dict()

        # updated = table.update_from_trans(records)

    return records, table
