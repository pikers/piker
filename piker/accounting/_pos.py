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
from math import copysign
import re
from typing import (
    Any,
    Iterator,
    Optional,
    Union,
    Generator
)

import pendulum
from pendulum import datetime, now
import toml

from ._ledger import (
    Transaction,
    iter_by_dt,
    open_trade_ledger,
)
from ._mktinfo import (
    Symbol,
    # MktPair,
    unpack_fqsn,
)
from .. import config
from ..brokers import get_brokermod
from ..clearing._messages import BrokerdPosition, Status
from ..data.types import Struct
from ..log import get_logger

log = get_logger(__name__)


class Position(Struct):
    '''
    Basic pp (personal/piker position) model with attached clearing
    transaction history.

    '''
    symbol: Symbol  # | MktPair

    # can be +ve or -ve for long/short
    size: float

    # "breakeven price" above or below which pnl moves above and below
    # zero for the entirety of the current "trade state".
    ppu: float

    # unique backend symbol id
    bsuid: str

    split_ratio: Optional[int] = None

    # ordered record of known constituent trade messages
    clears: dict[
        Union[str, int, Status],  # trade id
        dict[str, Any],  # transaction history summaries
    ] = {}
    first_clear_dt: Optional[datetime] = None

    expiry: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            f: getattr(self, f)
            for f in self.__struct_fields__
        }

    def to_pretoml(self) -> tuple[str, dict]:
        '''
        Prep this position's data contents for export to toml including
        re-structuring of the ``.clears`` table to an array of
        inline-subtables for better ``pps.toml`` compactness.

        '''
        d = self.to_dict()
        clears = d.pop('clears')
        expiry = d.pop('expiry')

        if self.split_ratio is None:
            d.pop('split_ratio')

        # should be obvious from clears/event table
        d.pop('first_clear_dt')

        # TODO: we need to figure out how to have one top level
        # listing venue here even when the backend isn't providing
        # it via the trades ledger..
        # drop symbol obj in serialized form
        s = d.pop('symbol')
        fqsn = s.fqme

        broker, key, suffix = unpack_fqsn(fqsn)
        sym_info = s.broker_info[broker]

        d['asset_type'] = sym_info['asset_type']
        d['price_tick_size'] = (
            sym_info.get('price_tick_size')
            or
            s.tick_size
        )
        d['lot_tick_size'] = (
            sym_info.get('lot_tick_size')
            or
            s.lot_tick_size
        )

        if self.expiry is None:
            d.pop('expiry', None)
        elif expiry:
            d['expiry'] = str(expiry)

        toml_clears_list = []

        # reverse sort so latest clears are at top of section?
        for tid, data in iter_by_dt(clears):
            inline_table = toml.TomlDecoder().get_empty_inline_table()

            # serialize datetime to parsable `str`
            inline_table['dt'] = str(data['dt'])

            # insert optional clear fields in column order
            for k in ['ppu', 'accum_size']:
                val = data.get(k)
                if val:
                    inline_table[k] = val

            # insert required fields
            for k in ['price', 'size', 'cost']:
                inline_table[k] = data[k]

            inline_table['tid'] = tid
            toml_clears_list.append(inline_table)

        d['clears'] = toml_clears_list

        return fqsn, d

    def ensure_state(self) -> None:
        '''
        Audit either the `.size` and `.ppu` local instance vars against
        the clears table calculations and return the calc-ed values if
        they differ and log warnings to console.

        '''
        clears = list(self.clears.values())
        self.first_clear_dt = min(list(entry['dt'] for entry in clears))
        last_clear = clears[-1]

        csize = self.calc_size()
        accum = last_clear['accum_size']
        if not self.expired():
            if (
                csize != accum
                and csize != round(accum * self.split_ratio or 1)
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

        cppu = self.calc_ppu()
        ppu = last_clear['ppu']
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
        symbol = self.symbol

        lot_size_digits = symbol.lot_size_digits
        self.ppu = round(
            msg['avg_price'],
            ndigits=symbol.tick_size_digits,
        )
        self.size = round(
            msg['size'],
            ndigits=lot_size_digits,
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
        Iterate the internally managed ``.clears: dict`` table in
        datetime-stamped order.

        '''
        return iter_by_dt(self.clears)

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

        tid: str
        entry: dict[str, Any]
        for (tid, entry) in self.iter_clears():
            clear_size = entry['size']
            clear_price = entry['price']

            last_accum_size = asize_h[-1] if asize_h else 0
            accum_size = last_accum_size + clear_size
            accum_sign = copysign(1, accum_size)

            sign_change: bool = False

            if accum_size == 0:
                ppu_h.append(0)
                asize_h.append(0)
                continue

            if accum_size == 0:
                ppu_h.append(0)
                asize_h.append(0)
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

            if abs_diff > 0:

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
        size: float = 0

        # time-expired pps (normally derivatives) are "closed"
        # and have a zero size.
        if self.expired():
            return 0

        for tid, entry in self.clears.items():
            size += entry['size']

        if self.split_ratio is not None:
            size = round(size * self.split_ratio)

        return float(
            self.symbol.quantize(size),
        )

    def minimize_clears(
        self,

    ) -> dict[str, dict]:
        '''
        Minimize the position's clears entries by removing
        all transactions before the last net zero size to avoid
        unecessary history irrelevant to the current pp state.

        '''
        size: float = 0
        clears_since_zero: list[tuple(str, dict)] = []

        # TODO: we might just want to always do this when iterating
        # a ledger? keep a state of the last net-zero and only do the
        # full iterate when no state was stashed?

        # scan for the last "net zero" position by iterating
        # transactions until the next net-zero size, rinse, repeat.
        for tid, clear in self.clears.items():
            size += clear['size']
            clears_since_zero.append((tid, clear))

            if size == 0:
                clears_since_zero.clear()

        self.clears = dict(clears_since_zero)
        return self.clears

    def add_clear(
        self,
        t: Transaction,
    ) -> dict:
        '''
        Update clearing table and populate rolling ppu and accumulative
        size in both the clears entry and local attrs state.

        '''
        clear = self.clears[t.tid] = {
            'cost': t.cost,
            'price': t.price,
            'size': t.size,
            'dt': t.dt
        }

        # TODO: compute these incrementally instead
        # of re-looping through each time resulting in O(n**2)
        # behaviour..?

        # NOTE: we compute these **after** adding the entry in order to
        # make the recurrence relation math work inside
        # ``.calc_size()``.
        self.size = clear['accum_size'] = self.calc_size()
        self.ppu = clear['ppu'] = self.calc_ppu()

        return clear

    # def sugest_split(self) -> float:
    #     ...


class PpTable(Struct):

    brokername: str
    acctid: str
    pps: dict[str, Position]
    conf: Optional[dict] = {}

    def update_from_trans(
        self,
        trans: dict[str, Transaction],
        cost_scalar: float = 2,

    ) -> dict[str, Position]:

        pps = self.pps
        updated: dict[str, Position] = {}

        # lifo update all pps from records, ensuring
        # we compute the PPU and size sorted in time!
        for t in sorted(
            trans.values(),
            key=lambda t: t.dt,
            reverse=True,
        ):
            pp = pps.setdefault(
                t.bsuid,

                # if no existing pp, allocate fresh one.
                Position(
                    Symbol.from_fqsn(
                        t.fqsn,
                        info={},
                    ) if not t.sym else t.sym,
                    size=0.0,
                    ppu=0.0,
                    bsuid=t.bsuid,
                    expiry=t.expiry,
                )
            )
            clears = pp.clears
            if clears:
                first_clear_dt = pp.first_clear_dt

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
            pp.add_clear(t)
            updated[t.bsuid] = pp

        # minimize clears tables and update sizing.
        for bsuid, pp in updated.items():
            pp.ensure_state()

        # deliver only the position entries that were actually updated
        # (modified the state) from the input transaction set.
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
        for bsuid in list(pp_objs):
            pp = pp_objs[bsuid]

            # XXX: debug hook for size mismatches
            # qqqbsuid = 320227571
            # if bsuid == qqqbsuid:
            #     breakpoint()

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
                closed_pp_objs[bsuid] = pp

            else:
                open_pp_objs[bsuid] = pp

        return open_pp_objs, closed_pp_objs

    def to_toml(
        self,
    ) -> dict[str, Any]:

        active, closed = self.dump_active()

        # ONLY dict-serialize all active positions; those that are closed
        # we don't store in the ``pps.toml``.
        to_toml_dict = {}

        for bsuid, pos in active.items():

            # keep the minimal amount of clears that make up this
            # position since the last net-zero state.
            pos.minimize_clears()
            pos.ensure_state()

            # serialize to pre-toml form
            fqsn, asdict = pos.to_pretoml()
            log.info(f'Updating active pp: {fqsn}')

            # XXX: ugh, it's cuz we push the section under
            # the broker name.. maybe we need to rethink this?
            brokerless_key = fqsn.removeprefix(f'{self.brokername}.')
            to_toml_dict[brokerless_key] = asdict

        return to_toml_dict

    def write_config(self) -> None:
        '''
        Write the current position table to the user's ``pps.toml``.

        '''
        # TODO: show diff output?
        # https://stackoverflow.com/questions/12956957/print-diff-of-python-dictionaries
        # active, closed_pp_objs = table.dump_active()
        pp_entries = self.to_toml()
        if pp_entries:
            log.info(
                f'Updating ``pps.toml``:\n'
                f'Current positions:\n{pp_entries}'
            )
            self.conf[self.brokername][self.acctid] = pp_entries

        elif (
            self.brokername in self.conf and
            self.acctid in self.conf[self.brokername]
        ):
            del self.conf[self.brokername][self.acctid]
            if len(self.conf[self.brokername]) == 0:
                del self.conf[self.brokername]

        # TODO: why tf haven't they already done this for inline
        # tables smh..
        enc = PpsEncoder(preserve=True)
        # table_bs_type = type(toml.TomlDecoder().get_empty_inline_table())
        enc.dump_funcs[
            toml.decoder.InlineTableDict
        ] = enc.dump_inline_table

        config.write(
            self.conf,
            'pps',
            encoder=enc,
            fail_empty=False
        )


def load_pps_from_ledger(

    brokername: str,
    acctname: str,

    # post normalization filter on ledger entries to be processed
    filter_by: Optional[list[dict]] = None,

) -> tuple[
    dict[str, Transaction],
    dict[str, Position],
]:
    '''
    Open a ledger file by broker name and account and read in and
    process any trade records into our normalized ``Transaction`` form
    and then update the equivalent ``Pptable`` and deliver the two
    bsuid-mapped dict-sets of the transactions and pps.

    '''
    with (
        open_trade_ledger(brokername, acctname) as ledger,
        open_pps(brokername, acctname) as table,
    ):
        if not ledger:
            # null case, no ledger file with content
            return {}

        mod = get_brokermod(brokername)
        src_records: dict[str, Transaction] = mod.norm_trade_records(ledger)

        if filter_by:
            records = {}
            bsuids = set(filter_by)
            for tid, r in src_records.items():
                if r.bsuid in bsuids:
                    records[tid] = r
        else:
            records = src_records

        updated = table.update_from_trans(records)

    return records, updated


# TODO: instead see if we can hack tomli and tomli-w to do the same:
# - https://github.com/hukkin/tomli
# - https://github.com/hukkin/tomli-w
class PpsEncoder(toml.TomlEncoder):
    '''
    Special "styled" encoder that makes a ``pps.toml`` redable and
    compact by putting `.clears` tables inline and everything else
    flat-ish.

    '''
    separator = ','

    def dump_list(self, v):
        '''
        Dump an inline list with a newline after every element and
        with consideration for denoted inline table types.

        '''
        retval = "[\n"
        for u in v:
            if isinstance(u, toml.decoder.InlineTableDict):
                out = self.dump_inline_table(u)
            else:
                out = str(self.dump_value(u))

            retval += " " + out + "," + "\n"
        retval += "]"
        return retval

    def dump_inline_table(self, section):
        """Preserve inline table in its compact syntax instead of expanding
        into subsection.
        https://github.com/toml-lang/toml#user-content-inline-table
        """
        val_list = []
        for k, v in section.items():
            # if isinstance(v, toml.decoder.InlineTableDict):
            if isinstance(v, dict):
                val = self.dump_inline_table(v)
            else:
                val = str(self.dump_value(v))

            val_list.append(k + " = " + val)

        retval = "{ " + ", ".join(val_list) + " }"
        return retval

    def dump_sections(self, o, sup):
        retstr = ""
        if sup != "" and sup[-1] != ".":
            sup += '.'
        retdict = self._dict()
        arraystr = ""
        for section in o:
            qsection = str(section)
            value = o[section]

            if not re.match(r'^[A-Za-z0-9_-]+$', section):
                qsection = toml.encoder._dump_str(section)

            # arrayoftables = False
            if (
                self.preserve
                and isinstance(value, toml.decoder.InlineTableDict)
            ):
                retstr += (
                    qsection
                    +
                    " = "
                    +
                    self.dump_inline_table(o[section])
                    +
                    '\n'  # only on the final terminating left brace
                )

            # XXX: this code i'm pretty sure is just blatantly bad
            # and/or wrong..
            # if isinstance(o[section], list):
            #     for a in o[section]:
            #         if isinstance(a, dict):
            #             arrayoftables = True
            # if arrayoftables:
            #     for a in o[section]:
            #         arraytabstr = "\n"
            #         arraystr += "[[" + sup + qsection + "]]\n"
            #         s, d = self.dump_sections(a, sup + qsection)
            #         if s:
            #             if s[0] == "[":
            #                 arraytabstr += s
            #             else:
            #                 arraystr += s
            #         while d:
            #             newd = self._dict()
            #             for dsec in d:
            #                 s1, d1 = self.dump_sections(d[dsec], sup +
            #                                             qsection + "." +
            #                                             dsec)
            #                 if s1:
            #                     arraytabstr += ("[" + sup + qsection +
            #                                     "." + dsec + "]\n")
            #                     arraytabstr += s1
            #                 for s1 in d1:
            #                     newd[dsec + "." + s1] = d1[s1]
            #             d = newd
            #         arraystr += arraytabstr

            elif isinstance(value, dict):
                retdict[qsection] = o[section]

            elif o[section] is not None:
                retstr += (
                    qsection
                    +
                    " = "
                    +
                    str(self.dump_value(o[section]))
                )

                # if not isinstance(value, dict):
                if not isinstance(value, toml.decoder.InlineTableDict):
                    # inline tables should not contain newlines:
                    # https://toml.io/en/v1.0.0#inline-table
                    retstr += '\n'

            else:
                raise ValueError(value)

        retstr += arraystr
        return (retstr, retdict)


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
    conf, path = config.load('pps')
    brokersection = conf.setdefault(brokername, {})
    pps = brokersection.setdefault(acctid, {})

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
        conf=conf,
    )

    # unmarshal/load ``pps.toml`` config entries into object form
    # and update `PpTable` obj entries.
    for fqsn, entry in pps.items():
        bsuid = entry['bsuid']
        symbol = Symbol.from_fqsn(
            fqsn,

            # NOTE & TODO: right now we fill in the defaults from
            # `.data._source.Symbol` but eventually these should always
            # either be already written to the pos table or provided at
            # write time to ensure always having these values somewhere
            # and thus allowing us to get our pos sizing precision
            # correct!
            info={
                'asset_type': entry.get('asset_type', '<unknown>'),
                'price_tick_size': entry.get('price_tick_size', 0.01),
                'lot_tick_size': entry.get('lot_tick_size', 0.0),
            }
        )

        # convert clears sub-tables (only in this form
        # for toml re-presentation) back into a master table.
        clears_list = entry['clears']

        # index clears entries in "object" form by tid in a top
        # level dict instead of a list (as is presented in our
        # ``pps.toml``).
        clears = pp_objs.setdefault(bsuid, {})

        # TODO: should be make a ``Struct`` for clear/event entries?
        # convert "clear events table" from the toml config (list of
        # a dicts) and load it into object form for use in position
        # processing of new clear events.
        trans: list[Transaction] = []

        for clears_table in clears_list:
            tid = clears_table.pop('tid')
            dtstr = clears_table['dt']
            dt = pendulum.parse(dtstr)
            clears_table['dt'] = dt

            trans.append(Transaction(
                fqsn=bsuid,
                sym=symbol,
                bsuid=bsuid,
                tid=tid,
                size=clears_table['size'],
                price=clears_table['price'],
                cost=clears_table['cost'],
                dt=dt,
            ))
            clears[tid] = clears_table

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

        pp = pp_objs[bsuid] = Position(
            symbol,
            size=size,
            ppu=ppu,
            split_ratio=split_ratio,
            expiry=expiry,
            bsuid=entry['bsuid'],
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
