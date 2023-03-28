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

"""
"FLEX" report processing utils.

"""
from bidict import bidict
import pendulum
from pprint import pformat
from typing import Any

from .api import (
    get_config,
    log,
)
from piker.accounting import (
    open_trade_ledger,
)


def parse_flex_dt(
    record: str,
) -> pendulum.datetime:
    date, ts = record.split(';')
    dt = pendulum.parse(date)
    ts = f'{ts[:2]}:{ts[2:4]}:{ts[4:]}'
    tsdt = pendulum.parse(ts)
    return dt.set(hour=tsdt.hour, minute=tsdt.minute, second=tsdt.second)


def flex_records_to_ledger_entries(
    accounts: bidict,
    trade_entries: list[object],

) -> dict:
    '''
    Convert flex report entry objects into ``dict`` form, pretty much
    straight up without modification except add a `pydatetime` field
    from the parsed timestamp.

    '''
    trades_by_account = {}
    for t in trade_entries:
        entry = t.__dict__

        # XXX: LOL apparently ``toml`` has a bug
        # where a section key error will show up in the write
        # if you leave a table key as an `int`? So i guess
        # cast to strs for all keys..

        # oddly for some so-called "BookTrade" entries
        # this field seems to be blank, no cuckin clue.
        # trade['ibExecID']
        tid = str(entry.get('ibExecID') or entry['tradeID'])
        # date = str(entry['tradeDate'])

        # XXX: is it going to cause problems if a account name
        # get's lost? The user should be able to find it based
        # on the actual exec history right?
        acctid = accounts[str(entry['accountId'])]

        # probably a flex record with a wonky non-std timestamp..
        dt = entry['pydatetime'] = parse_flex_dt(entry['dateTime'])
        entry['datetime'] = str(dt)

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

    for acctid in trades_by_account:
        trades_by_account[acctid] = dict(sorted(
            trades_by_account[acctid].items(),
            key=lambda entry: entry[1]['pydatetime'],
        ))

    return trades_by_account


def load_flex_trades(
    path: str | None = None,

) -> dict[str, Any]:

    from ib_insync import flexreport, util

    conf = get_config()

    if not path:
        # load ``brokers.toml`` and try to get the flex
        # token and query id that must be previously defined
        # by the user.
        token = conf.get('flex_token')
        if not token:
            raise ValueError(
                'You must specify a ``flex_token`` field in your'
                '`brokers.toml` in order load your trade log, see our'
                'intructions for how to set this up here:\n'
                'PUT LINK HERE!'
            )

        qid = conf['flex_trades_query_id']

        # TODO: hack this into our logging
        # system like we do with the API client..
        util.logToConsole()

        # TODO: rewrite the query part of this with async..httpx?
        report = flexreport.FlexReport(
            token=token,
            queryId=qid,
        )

    else:
        # XXX: another project we could potentially look at,
        # https://pypi.org/project/ibflex/
        report = flexreport.FlexReport(path=path)

    trade_entries = report.extract('Trade')
    ln = len(trade_entries)
    log.info(f'Loaded {ln} trades from flex query')

    trades_by_account = flex_records_to_ledger_entries(
        conf['accounts'].inverse,  # reverse map to user account names
        trade_entries,
    )

    ledger_dict: dict | None = None

    for acctid in trades_by_account:
        trades_by_id = trades_by_account[acctid]

        with open_trade_ledger('ib', acctid) as ledger_dict:
            tid_delta = set(trades_by_id) - set(ledger_dict)
            log.info(
                'New trades detected\n'
                f'{pformat(tid_delta)}'
            )
            if tid_delta:
                sorted_delta = dict(sorted(
                    {tid: trades_by_id[tid] for tid in tid_delta}.items(),
                    key=lambda entry: entry[1].pop('pydatetime'),
                ))
                ledger_dict.update(sorted_delta)

    return ledger_dict


if __name__ == '__main__':
    import sys
    import os

    args = sys.argv
    if len(args) > 1:
        args = args[1:]
        for arg in args:
            path = os.path.abspath(arg)
            load_flex_trades(path=path)
    else:
        # expect brokers.toml to have an entry and
        # pull from the web service.
        load_flex_trades()
