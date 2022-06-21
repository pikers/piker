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
import re
from typing import (
    Any,
    Optional,
    Union,
)

from msgspec import Struct
import pendulum
from pendulum import datetime, now
# import tomli
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
    dt: datetime
    expiry: Optional[datetime] = None

    # optional key normally derived from the broker
    # backend which ensures the instrument-symbol this record
    # is for is truly unique.
    bsuid: Optional[Union[str, int]] = None


class Position(Struct):
    '''
    Basic pp (personal/piker position) model with attached clearing
    transaction history.

    '''
    symbol: Symbol

    # can be +ve or -ve for long/short
    size: float

    # "breakeven price" above or below which pnl moves above and below
    # zero for the entirety of the current "trade state".
    be_price: float

    # unique backend symbol id
    bsuid: str

    # ordered record of known constituent trade messages
    clears: dict[
        Union[str, int, Status],  # trade id
        dict[str, Any],  # transaction history summaries
    ] = {}

    expiry: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            f: getattr(self, f)
            for f in self.__struct_fields__
        }

    def to_pretoml(self) -> dict:
        '''
        Prep this position's data contents for export to toml including
        re-structuring of the ``.clears`` table to an array of
        inline-subtables for better ``pps.toml`` compactness.

        '''
        d = self.to_dict()
        clears = d.pop('clears')
        expiry = d.pop('expiry')

        if expiry:
            d['expiry'] = str(expiry)

        clears_list = []

        for tid, data in clears.items():
            inline_table = toml.TomlDecoder().get_empty_inline_table()
            inline_table['tid'] = tid

            for k, v in data.items():
                inline_table[k] = v

            clears_list.append(inline_table)

        d['clears'] = clears_list

        return d

    def update_from_msg(
        self,
        msg: BrokerdPosition,

    ) -> None:

        # XXX: better place to do this?
        symbol = self.symbol

        lot_size_digits = symbol.lot_size_digits
        be_price, size = (
            round(
                msg['avg_price'],
                ndigits=symbol.tick_size_digits
            ),
            round(
                msg['size'],
                ndigits=lot_size_digits
            ),
        )

        self.be_price = be_price
        self.size = size

    @property
    def dsize(self) -> float:
        '''
        The "dollar" size of the pp, normally in trading (fiat) unit
        terms.

        '''
        return self.be_price * self.size

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
            self.be_price = 0

        elif size_diff > 0:
            # XXX: LOFI incremental update:
            # only update the "average price" when
            # the size increases not when it decreases (i.e. the
            # position is being made smaller)
            self.be_price = (
                abs(size) * price  # weight of current exec
                +
                cost  # transaction cost
                +
                self.be_price * abs(self.size)  # weight of previous pp
            ) / abs(new_size)

        self.size = new_size

        return new_size, self.be_price


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
            r.bsuid,

            # if no existing pp, allocate fresh one.
            Position(
                Symbol.from_fqsn(
                    r.fqsn,
                    info={},
                ),
                size=0.0,
                be_price=0.0,
                bsuid=r.bsuid,
                expiry=r.expiry,
            )
        )

        # don't do updates for ledger records we already have
        # included in the current pps state.
        if r.tid in pp.clears:
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

        # track clearing data
        pp.clears[r.tid] = {
            'cost': r.cost,
        }

    assert len(set(pp.clears)) == len(pp.clears)
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

        asdict = pp.to_pretoml()

        if pp.expiry is None:
            asdict.pop('expiry', None)

        if (
            pp.size == 0

            # drop time-expired positions (normally derivatives)
            or (pp.expiry and pp.expiry < now())
        ):
            closed[k] = asdict
        else:
            active[k] = asdict

    return active, closed


def load_pps_from_ledger(

    brokername: str,
    acctname: str,

    # post normalization filter on ledger entries to be processed
    filter_by: Optional[list[Transaction]] = None,

) -> dict[str, Position]:
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
        if not ledger:
            # null case, no ledger file with content
            return {}

    brokermod = get_brokermod(brokername)
    records = brokermod.norm_trade_records(ledger)

    if filter_by:
        bsuids = set(r.bsuid for r in filter_by)
        records = filter(lambda r: r.bsuid in bsuids, records)

    return update_pps(records)


def get_pps(
    brokername: str,
    acctids: Optional[set[str]] = set(),
    key_by: Optional[str] = None,

) -> dict[str, dict[str, Position]]:
    '''
    Read out broker-specific position entries from
    incremental update file: ``pps.toml``.

    '''
    conf, path = config.load(
        'pps',
        # load dicts as inlines to preserve compactness
        # _dict=toml.decoder.InlineTableDict,
    )
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


def load_pps_from_toml(
    brokername: str,
    acctid: str,

    # XXX: there is an edge case here where we may want to either audit
    # the retrieved ``pps.toml`` output or reprocess it since there was
    # an error on write on the last attempt to update the state file
    # even though the ledger *was* updated. For this cases we allow the
    # caller to pass in a symbol set they'd like to reload from the
    # underlying ledger to be reprocessed in computing pps state.
    reload_records: Optional[list[Transaction]] = None,

) -> tuple[dict, dict[str, Position]]:
    '''
    Load and marshal to objects all pps from either an existing
    ``pps.toml`` config, or from scratch from a ledger file when
    none yet exists.

    '''
    conf, path = config.load('pps')
    brokersection = conf.setdefault(brokername, {})
    pps = brokersection.setdefault(acctid, {})
    pp_objs = {}

    if not pps:
        # no pps entry yet for this broker/account so parse
        # any available ledgers to build a pps state.
        pp_objs = load_pps_from_ledger(
            brokername,
            acctid,
        )

    # Reload symbol specific ledger entries if requested by the
    # caller **AND** none exist in the current pps state table.
    elif (
        pps and reload_records and
        not any(r.fqsn in pps for r in reload_records)
    ):
        # no pps entry yet for this broker/account so parse
        # any available ledgers to build a pps state.
        pp_objs = load_pps_from_ledger(
            brokername,
            acctid,
            filter_by=reload_records,
        )

    if not pps:
        log.warning(
            f'No trade history could be loaded for {brokername}:{acctid}'
        )

    # unmarshal/load ``pps.toml`` config entries into object form.
    for fqsn, entry in pps.items():

        # convert clears sub-tables (only in this form
        # for toml re-presentation) back into a master table.
        clears_list = entry['clears']

        # index clears entries in "object" form by tid in a top
        # level dict instead of a list (as is presented in our
        # ``pps.toml``).
        clears = {}
        for clears_table in clears_list:
            tid = clears_table.pop('tid')
            clears[tid] = clears_table

        expiry = entry.get('expiry')
        if expiry:
            expiry = pendulum.parse(expiry)

        pp_objs[fqsn] = Position(
            Symbol.from_fqsn(fqsn, info={}),
            size=entry['size'],
            be_price=entry['be_price'],
            expiry=expiry,
            bsuid=entry['bsuid'],

            # XXX: super critical, we need to be sure to include
            # all pps.toml clears to avoid reusing clears that were
            # already included in the current incremental update
            # state, since today's records may have already been
            # processed!
            clears=clears,
        )

    return conf, pp_objs


def update_pps_conf(
    brokername: str,
    acctid: str,

    trade_records: Optional[list[Transaction]] = None,
    key_by: Optional[str] = None,

) -> dict[str, Position]:

    conf, pp_objs = load_pps_from_toml(
        brokername,
        acctid,
        reload_records=trade_records,
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

    for bsuid, pp_dict in active.items():

        # normalize to a simpler flat dict format
        s = pp_dict.pop('symbol')
        # TODO: we need to figure out how to have one top level
        # listing venue here even when the backend isn't providing
        # it via the trades ledger..
        fqsn = s.front_fqsn()

        print(f'Updating active pp: {fqsn}')
        # XXX: ugh, it's cuz we push the section under
        # the broker name.. maybe we need to rethink this?
        brokerless_key = fqsn.rstrip(f'.{brokername}')
        pp_entries[brokerless_key] = pp_dict

    for bsuid in closed:
        pp_objs.pop(bsuid, None)

    conf[brokername][acctid] = pp_entries

    # TODO: why tf haven't they already done this for inline tables smh..
    enc = PpsEncoder(preserve=True)
    # table_bs_type = type(toml.TomlDecoder().get_empty_inline_table())
    enc.dump_funcs[toml.decoder.InlineTableDict] = enc.dump_inline_table

    config.write(
        conf,
        'pps',
        encoder=enc,
    )

    if key_by:
        pp_objs = {getattr(pp, key_by): pp for pp in pp_objs}

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
