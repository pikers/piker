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
Calculation routines for balance and position tracking such that
you know when you're losing money (if possible) XD

'''
from __future__ import annotations
from math import copysign
from typing import (
    Any,
    Callable,
    Iterator,
    TYPE_CHECKING,
)

from pendulum import (
    # datetime,
    DateTime,
    from_timestamp,
    parse,
)

if TYPE_CHECKING:
    from ._ledger import (
        Transaction,
    )


def ppu(
    clears: Iterator[Transaction],

    # include transaction cost in breakeven price
    # and presume the worst case of the same cost
    # to exit this transaction (even though in reality
    # it will be dynamic based on exit stratetgy).
    cost_scalar: float = 2,

    # return the ledger of clears as a (now dt sorted) dict with
    # new position fields inserted alongside each entry.
    as_ledger: bool = False,

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
    ledger: dict[str, dict] = {}

    # entry: dict[str, Any] | Transaction
    t: Transaction
    for t in clears:
        # tid: str = entry['tid']
        # clear_size = entry['size']
        clear_size: float = t.size
        # clear_price: str | float = entry['price']
        clear_price: str | float = t.price
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
        abs_new_size: float | int = abs(accum_size)

        if (
            abs_diff > 0
            and is_clear
        ):

            cost_basis = (
                # cost basis for this clear
                clear_price * abs(clear_size)
                +
                # transaction cost
                # accum_sign * cost_scalar * entry['cost']
                accum_sign * cost_scalar * t.cost
            )

            if asize_h:
                size_last = abs(asize_h[-1])
                cb_last = ppu_h[-1] * size_last
                ppu = (cost_basis + cb_last) / abs_new_size

            else:
                ppu = cost_basis / abs_new_size

            # ppu_h.append(ppu)
            # asize_h.append(accum_size)

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
            ppu: float = ppu_h[-1]  # set to previous value
            # ppu_h.append(ppu_h[-1])
            # asize_h.append(accum_size)

        # extend with new rolling metric for this step
        ppu_h.append(ppu)
        asize_h.append(accum_size)

        # ledger[t.tid] = {
            # 'tx': t,
        ledger[t.tid] = t.to_dict() | {
            'ppu': ppu,
            'cumsize': accum_size,
            'sign_change': sign_change,

            # TODO: cumpnl, bep
        }

    final_ppu = ppu_h[-1] if ppu_h else 0
    # TODO: once we have etypes in all ledger entries..
    # handle any split info entered (for now) manually by user
    # if self.split_ratio is not None:
    #     final_ppu /= self.split_ratio

    if as_ledger:
        return ledger

    else:
        return final_ppu


def iter_by_dt(
    records: (
        dict[str, dict[str, Any]]
        | list[dict]
        | list[Transaction]  # XXX preferred!
    ),

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
    Iterate entries of a transaction table sorted by entry recorded
    datetime presumably set at the ``'dt'`` field in each entry.

    '''
    # isdict: bool = False
    if isinstance(records, dict):
        # isdict: bool = True
        records = list(records.items())

    def dyn_parse_to_dt(
        tx: tuple[str, dict[str, Any]] | Transaction,
    ) -> DateTime:

        # handle `.items()` inputs
        if isinstance(tx, tuple):
            tx = tx[1]

        # dict or tx object?
        isdict: bool = isinstance(tx, dict)

        # get best parser for this record..
        for k in parsers:
            if (
                isdict and k in tx
                 or getattr(tx, k, None)
            ):
                v = tx[k] if isdict else tx.dt
                if v is None:
                    breakpoint()

                parser = parsers[k]

                # only call parser on the value if not None from the
                # `parsers` table above, otherwise pass through the value
                # and sort on it directly
                return parser(v) if (parser is not None) else v

        else:
            breakpoint()

    entry: tuple[str, dict] | Transaction
    for entry in sorted(
        records,
        key=key or dyn_parse_to_dt,
    ):
        yield entry