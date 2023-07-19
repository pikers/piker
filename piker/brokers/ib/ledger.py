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
Trade transaction accounting and normalization.

'''
from bisect import insort
from decimal import Decimal
from functools import partial
from pprint import pformat
from typing import (
    Any,
    Callable,
)

from bidict import bidict
import pendulum

from piker.data import (
    Struct,
    SymbologyCache,
)
from piker.accounting import (
    Asset,
    dec_digits,
    digits_to_dec,
    Transaction,
    MktPair,
    iter_by_dt,
)
from ._flex_reports import parse_flex_dt
from ._util import log


tx_sort: Callable = partial(
    iter_by_dt,
    parsers={
        'dateTime': parse_flex_dt,
        'datetime': pendulum.parse,
        # for some some fucking 2022 and
        # back options records...fuck me.
        'date': pendulum.parse,
    }
)


def norm_trade(
    tid: str,
    record: dict[str, Any],

    # this is the dict that was returned from
    # `Client.get_mkt_pairs()` and when running offline ledger
    # processing from `.accounting`, this will be the table loaded
    # into `SymbologyCache.pairs`.
    pairs: dict[str, Struct],
    symcache: SymbologyCache | None = None,

) -> Transaction | None:

    conid = record.get('conId') or record['conid']
    comms = record.get('commission')
    if comms is None:
        comms = -1*record['ibCommission']

    price = record.get('price') or record['tradePrice']

    # the api doesn't do the -/+ on the quantity for you but flex
    # records do.. are you fucking serious ib...!?
    size = record.get('quantity') or record['shares'] * {
        'BOT': 1,
        'SLD': -1,
    }[record['side']]

    symbol: str = record['symbol']
    exch: str = record.get('listingExchange') or record['exchange']

    # NOTE: remove null values since `tomlkit` can't serialize
    # them to file.
    if dnc := record.pop('deltaNeutralContract', None):
        record['deltaNeutralContract'] = dnc

    # likely an opts contract record from a flex report..
    # TODO: no idea how to parse ^ the strike part from flex..
    # (00010000 any, or 00007500 tsla, ..)
    # we probably must do the contract lookup for this?
    if (
        '   ' in symbol
        or '--' in exch
    ):
        underlying, _, tail = symbol.partition('   ')
        exch: str = 'opt'
        expiry: str = tail[:6]
        # otype = tail[6]
        # strike = tail[7:]

        print(f'skipping opts contract {symbol}')
        return None

    # timestamping is way different in API records
    dtstr = record.get('datetime')
    date = record.get('date')
    flex_dtstr = record.get('dateTime')

    if dtstr or date:
        dt = pendulum.parse(dtstr or date)

    elif flex_dtstr:
        # probably a flex record with a wonky non-std timestamp..
        dt = parse_flex_dt(record['dateTime'])

    # special handling of symbol extraction from
    # flex records using some ad-hoc schema parsing.
    asset_type: str = record.get(
        'assetCategory'
    ) or record.get('secType', 'STK')

    if (expiry := (
            record.get('lastTradeDateOrContractMonth')
            or record.get('expiry')
        )
    ):
        expiry: str = str(expiry).strip(' ')
        # NOTE: we directly use the (simple and usually short)
        # date-string expiry token when packing the `MktPair`
        # since we want the fqme to contain *that* token.
        # It might make sense later to instead parse and then
        # render different output str format(s) for this same
        # purpose depending on asset-type-market down the road.
        # Eg. for derivs we use the short token only for fqme
        # but use the isoformat('T') for transactions and
        # account file position entries?
        # dt_str: str = pendulum.parse(expiry).isoformat('T')

    # XXX: pretty much all legacy market assets have a fiat
    # currency (denomination) determined by their venue.
    currency: str = record['currency']
    src = Asset(
        name=currency.lower(),
        atype='fiat',
        tx_tick=Decimal('0.01'),
    )

    match asset_type:
        case 'FUT':
            # (flex) ledger entries don't have any simple 3-char key?
            # TODO: XXX: WOA this is kinda hacky.. probably
            # should figure out the correct future pair key more
            # explicitly and consistently?
            symbol: str = symbol[:3]
            dst = Asset(
                name=symbol.lower(),
                atype='future',
                tx_tick=Decimal('1'),
            )

        case 'STK':
            dst = Asset(
                name=symbol.lower(),
                atype='stock',
                tx_tick=Decimal('1'),
            )

        case 'CASH':
            if currency not in symbol:
                # likely a dict-casted `Forex` contract which
                # has .symbol as the dst and .currency as the
                # src.
                name: str = symbol.lower()
            else:
                # likely a flex-report record which puts
                # EUR.USD as the symbol field and just USD in
                # the currency field.
                name: str = symbol.lower().replace(f'.{src.name}', '')

            dst = Asset(
                name=name,
                atype='fiat',
                tx_tick=Decimal('0.01'),
            )

        case 'OPT':
            dst = Asset(
                name=symbol.lower(),
                atype='option',
                tx_tick=Decimal('1'),
            )

    # try to build out piker fqme from record.
    # src: str = record['currency']
    price_tick: Decimal = digits_to_dec(dec_digits(price))

    # NOTE: can't serlialize `tomlkit.String` so cast to native
    atype: str = str(dst.atype)

    mkt = MktPair(
        bs_mktid=str(conid),
        dst=dst,

        price_tick=price_tick,
        # NOTE: for "legacy" assets, volume is normally discreet, not
        # a float, but we keep a digit in case the suitz decide
        # to get crazy and change it; we'll be kinda ready
        # schema-wise..
        size_tick=Decimal('1'),

        src=src,  # XXX: normally always a fiat

        _atype=atype,

        venue=exch,
        expiry=expiry,
        broker='ib',

        _fqme_without_src=(atype != 'fiat'),
    )

    fqme: str = mkt.fqme

    # XXX: if passed in, we fill out the symcache ad-hoc in order
    # to make downstream accounting work..
    if symcache:
        symcache.mktmaps[fqme] = mkt
        symcache.assets[src.name] = src
        symcache.assets[dst.name] = dst

    # NOTE: for flex records the normal fields for defining an fqme
    # sometimes won't be available so we rely on two approaches for
    # the "reverse lookup" of piker style fqme keys:
    # - when dealing with API trade records received from
    #   `IB.trades()` we do a contract lookup at he time of processing
    # - when dealing with flex records, it is assumed the record
    #   is at least a day old and thus the TWS position reporting system
    #   should already have entries if the pps are still open, in
    #   which case, we can pull the fqme from that table (see
    #   `trades_dialogue()` above).
    return Transaction(
        fqme=fqme,
        tid=tid,
        size=size,
        price=price,
        cost=comms,
        dt=dt,
        expiry=expiry,
        bs_mktid=str(conid),
    )



def norm_trade_records(
    ledger: dict[str, Any],
    symcache: SymbologyCache | None = None,

) -> dict[str, Transaction]:
    '''
    Normalize (xml) flex-report or (recent) API trade records into
    our ledger format with parsing for `MktPair` and `Asset`
    extraction to fill in the `Transaction.sys: MktPair` field.

    '''
    # select: list[transactions] = []
    records: list[Transaction] = []

    for tid, record in ledger.items():

        txn = norm_trade(
            tid,
            record,

            # NOTE: currently no symcache support
            pairs={},
            symcache=symcache,
        )

        if txn is None:
            continue

        insort(
            records,
            txn,
            key=lambda t: t.dt
        )

        # if (
        #     atype == 'fiat'
        #     or atype == 'option'
        # ):
        #     select.append(trans)

    # if select:
    #     breakpoint()

    return {r.tid: r for r in records}


def api_trades_to_ledger_entries(
    accounts: bidict[str, str],

    # TODO: maybe we should just be passing through the
    # ``ib_insync.order.Trade`` instance directly here
    # instead of pre-casting to dicts?
    trade_entries: list[dict],

) -> dict[str, dict]:
    '''
    Convert API execution objects entry objects into ``dict`` form,
    pretty much straight up without modification except add
    a `pydatetime` field from the parsed timestamp.

    '''
    trades_by_account: dict[str, dict] = {}
    for t in trade_entries:
        # NOTE: example of schema we pull from the API client.
        # {
        #     'commissionReport': CommissionReport(...
        #     'contract': {...
        #     'execution': Execution(...
        #     'time': 1654801166.0
        # }

        # flatten all sub-dicts and values into one top level entry.
        entry = {}
        for section, val in t.items():
            match section:
                case 'contract' | 'execution' | 'commissionReport':
                    # sub-dict cases
                    entry.update(val)

                case 'time':
                    # ib has wack ns timestamps, or is that us?
                    continue

                case _:
                    entry[section] = val

        tid = str(entry['execId'])
        dt = pendulum.from_timestamp(entry['time'])
        # TODO: why isn't this showing seconds in the str?
        entry['pydatetime'] = dt
        entry['datetime'] = str(dt)
        acctid = accounts[entry['acctNumber']]

        if not tid:
            # this is likely some kind of internal adjustment
            # transaction, likely one of the following:
            # - an expiry event that will show a "book trade" indicating
            #   some adjustment to cash balances: zeroing or itm settle.
            # - a manual cash balance position adjustment likely done by
            #   the user from the accounts window in TWS where they can
            #   manually set the avg price and size:
            #   https://api.ibkr.com/lib/cstools/faq/web1/index.html#/tag/DTWS_ADJ_AVG_COST
            log.warning(f'Skipping ID-less ledger entry:\n{pformat(entry)}')
            continue

        trades_by_account.setdefault(
            acctid, {}
        )[tid] = entry

    # sort entries in output by python based datetime
    for acctid in trades_by_account:
        trades_by_account[acctid] = dict(sorted(
            trades_by_account[acctid].items(),
            key=lambda entry: entry[1].pop('pydatetime'),
        ))

    return trades_by_account
