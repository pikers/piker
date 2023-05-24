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
"Accounting for degens": count dem numberz that tracks how much you got
for tendiez.

'''
from ..log import get_logger

from ._ledger import (
    iter_by_dt,
    Transaction,
    TransactionLedger,
    open_trade_ledger,
)
from ._pos import (
    load_pps_from_ledger,
    open_pps,
    Position,
    PpTable,
)
from ._mktinfo import (
    Asset,
    dec_digits,
    digits_to_dec,
    MktPair,
    Symbol,
    unpack_fqme,
)
from ._allocate import (
    mk_allocator,
    Allocator,
)

log = get_logger(__name__)

__all__ = [
    'Allocator',
    'Asset',
    'MktPair',
    'Position',
    'PpTable',
    'Symbol',
    'Transaction',
    'TransactionLedger',
    'dec_digits',
    'digits_to_dec',
    'iter_by_dt',
    'load_pps_from_ledger',
    'mk_allocator',
    'open_pps',
    'open_trade_ledger',
    'unpack_fqme',
]


def get_likely_pair(
    src: str,
    dst: str,
    bs_mktid: str,

) -> str | None:
    '''
    Attempt to get the likely trading pair matching a given destination
    asset `dst: str`.

    '''
    try:
        src_name_start = bs_mktid.rindex(src)
    except (
        ValueError,   # substr not found
    ):
        # TODO: handle nested positions..(i.e.
        # positions where the src fiat was used to
        # buy some other dst which was furhter used
        # to buy another dst..)
        # log.warning(
        #     f'No src fiat {src} found in {bs_mktid}?'
        # )
        return

    likely_dst = bs_mktid[:src_name_start]
    if likely_dst == dst:
        return bs_mktid


if __name__ == '__main__':
    import sys
    from pprint import pformat

    args = sys.argv
    assert len(args) > 1, 'Specifiy account(s) from `brokers.toml`'
    args = args[1:]
    for acctid in args:
        broker, name = acctid.split('.')
        trans, updated_pps = load_pps_from_ledger(broker, name)
        print(
            f'Processing transactions into pps for {broker}:{acctid}\n'
            f'{pformat(trans)}\n\n'
            f'{pformat(updated_pps)}'
        )
