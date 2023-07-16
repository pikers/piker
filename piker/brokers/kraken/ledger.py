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
import math
from pprint import pformat
from typing import (
    Any,
)

import pendulum

from piker.accounting import (
    Transaction,
    Position,
    Account,
    get_likely_pair,
    TransactionLedger,
    # MktPair,
)
from piker.data import (
    # SymbologyCache,
    Struct,
)
from .api import (
    log,
    Client,
    Pair,
)
# from .feed import get_mkt_info


def norm_trade(
    tid: str,
    record: dict[str, Any],

    # this is the dict that was returned from
    # `Client.get_mkt_pairs()` and when running offline ledger
    # processing from `.accounting`, this will be the table loaded
    # into `SymbologyCache.pairs`.
    pairs: dict[str, Struct],

) -> Transaction:

    size: float = float(record.get('vol')) * {
        'buy': 1,
        'sell': -1,
    }[record['type']]

    rest_pair_key: str = record['pair']
    pair: Pair = pairs[rest_pair_key]

    fqme: str = pair.bs_fqme.lower() + '.kraken'

    return Transaction(
        fqme=fqme,
        tid=tid,
        size=size,
        price=float(record['price']),
        cost=float(record['fee']),
        dt=pendulum.from_timestamp(float(record['time'])),
        bs_mktid=pair.bs_mktid,
    )


async def norm_trade_records(
    ledger: dict[str, Any],
    client: Client,

) -> dict[str, Transaction]:
    '''
    Loop through an input ``dict`` of trade records
    and convert them to ``Transactions``.

    '''
    records: dict[str, Transaction] = {}
    for tid, record in ledger.items():

        # manual_fqme: str = f'{bs_mktid.lower()}.kraken'
        # mkt: MktPair = (await get_mkt_info(manual_fqme))[0]
        # fqme: str = mkt.fqme
        # assert fqme == manual_fqme

        records[tid] = norm_trade(
            tid,
            record,
            pairs=client._AssetPairs,
        )

    return records


def has_pp(
    acnt: Account,
    src_fiat: str,
    dst: str,
    size: float,

) -> Position | None:

    src2dst: dict[str, str] = {}
    for bs_mktid in acnt.pps:
        likely_pair = get_likely_pair(
            src_fiat,
            dst,
            bs_mktid,
        )
        if likely_pair:
            src2dst[src_fiat] = dst

    for src, dst in src2dst.items():
        pair: str = f'{dst}{src_fiat}'
        pos: Position = acnt.pps.get(pair)
        if (
            pos
            and math.isclose(pos.size, size)
        ):
            return pos

        elif (
            size == 0
            and pos.size
        ):
            log.warning(
                f'`kraken` account says you have  a ZERO '
                f'balance for {bs_mktid}:{pair}\n'
                f'but piker seems to think `{pos.size}`\n'
                'This is likely a discrepancy in piker '
                'accounting if the above number is'
                "large,' though it's likely to due lack"
                "f tracking xfers fees.."
            )
            return pos

    return None  # indicate no entry found


# TODO: factor most of this "account updating from txns" into the
# the `Account` impl so has to provide for hiding the mostly
# cross-provider updates from txn sets
async def verify_balances(
    acnt: Account,
    src_fiat: str,
    balances: dict[str, float],
    client: Client,
    ledger: TransactionLedger,
    ledger_trans: dict[str, Transaction],  # from toml
    api_trans: dict[str, Transaction],  # from API

    simulate_pp_update: bool = False,

) -> None:
    for dst, size in balances.items():

        # we don't care about tracking positions
        # in the user's source fiat currency.
        if (
            dst == src_fiat
            or not any(
                dst in bs_mktid for bs_mktid in acnt.pps
            )
        ):
            log.warning(
                f'Skipping balance `{dst}`:{size} for position calcs!'
            )
            continue

        # we have a balance for which there is no pos entry
        # - we have to likely update from the ledger?
        if not has_pp(acnt, src_fiat, dst, size):
            updated = acnt.update_from_ledger(
                ledger_trans,
                symcache=ledger.symcache,
            )
            log.info(f'Updated pps from ledger:\n{pformat(updated)}')

            # FIRST try reloading from API records
            if (
                not has_pp(acnt, src_fiat, dst, size)
                and not simulate_pp_update
            ):
                acnt.update_from_ledger(
                    api_trans,
                    symcache=ledger.symcache,
                )

                # get transfers to make sense of abs
                # balances.
                # NOTE: we do this after ledger and API
                # loading since we might not have an
                # entry in the
                # ``account.kraken.spot.toml`` for the
                # necessary pair yet and thus this
                # likely pair grabber will likely fail.
                if not has_pp(acnt, src_fiat, dst, size):
                    for bs_mktid in acnt.pps:
                        likely_pair: str | None = get_likely_pair(
                            src_fiat,
                            dst,
                            bs_mktid,
                        )
                        if likely_pair:
                            break
                    else:
                        raise ValueError(
                            'Could not find a position pair in '
                            'ledger for likely widthdrawal '
                            f'candidate: {dst}'
                        )

                    # this was likely pos that had a withdrawal
                    # from the dst asset out of the account.
                    if likely_pair:
                        xfer_trans = await client.get_xfers(
                            dst,

                            # TODO: not all src assets are
                            # 3 chars long...
                            src_asset=likely_pair[3:],
                        )
                        if xfer_trans:
                            updated = acnt.update_from_ledger(
                                xfer_trans,
                                cost_scalar=1,
                                symcache=ledger.symcache,
                            )
                            log.info(
                                f'Updated {dst} from transfers:\n'
                                f'{pformat(updated)}'
                            )

                if has_pp(acnt, src_fiat, dst, size):
                    raise ValueError(
                        'Could not reproduce balance:\n'
                        f'dst: {dst}, {size}\n'
                    )
