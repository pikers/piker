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
from collections.abc import ValuesView
from contextlib import contextmanager as cm
from math import copysign
from typing import (
    Any,
    Callable,
    Iterator,
    TYPE_CHECKING,
)

import polars as pl
from pendulum import (
    DateTime,
    from_timestamp,
    parse,
)

if TYPE_CHECKING:
    from ._ledger import (
        Transaction,
        TransactionLedger,
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

) -> float | list[(str, dict)]:
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

    -----
    TODO: get the BEP computed and working similarly!
    -----
    the equivalent "break even price" or bep at each new clear
    event step conversely only changes when an "position exiting
    clear" which **decreases** the cumulative dst asset size:

    bep[-1] = ppu[-1] - (cum_pnl[-1] / cumsize[-1])

    '''
    asize_h: list[float] = []  # historical accumulative size
    ppu_h: list[float] = []  # historical price-per-unit
    # ledger: dict[str, dict] = {}
    ledger: list[dict] = []

    t: Transaction
    for t in clears:
        clear_size: float = t.size
        clear_price: str | float = t.price
        is_clear: bool = not isinstance(clear_price, str)

        last_accum_size = asize_h[-1] if asize_h else 0
        accum_size: float = last_accum_size + clear_size
        accum_sign = copysign(1, accum_size)
        sign_change: bool = False

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
            clear_size: float = accum_size
            abs_diff: float = abs(accum_size)
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
                accum_sign * cost_scalar * t.cost
            )

            if asize_h:
                size_last: float = abs(asize_h[-1])
                cb_last: float = ppu_h[-1] * size_last
                ppu: float = (cost_basis + cb_last) / abs_new_size

            else:
                ppu: float = cost_basis / abs_new_size

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
            ppu: float = ppu_h[-1] if ppu_h else 0  # set to previous value

        # extend with new rolling metric for this step
        ppu_h.append(ppu)
        asize_h.append(accum_size)

        # ledger[t.tid] = {
            # 'txn': t,
        # ledger[t.tid] = t.to_dict() | {
        ledger.append((
            t.tid,
            t.to_dict() | {
                'ppu': ppu,
                'cumsize': accum_size,
                'sign_change': sign_change,

                # TODO: cum_pnl, bep
            }
        ))

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
        | ValuesView[dict]  # eg. `Position._events.values()`
        | list[dict]
        | list[Transaction]  # XXX preferred!
    ),

    # NOTE: parsers are looked up in the insert order
    # so if you know that the record stats show some field
    # is more common then others, stick it at the top B)
    parsers: dict[str, Callable | None] = {
        'dt': parse,  # parity case
        'datetime': parse,  # datetime-str
        'time': from_timestamp,  # float epoch
    },
    key: Callable | None = None,

) -> Iterator[tuple[str, dict]]:
    '''
    Iterate entries of a transaction table sorted by entry recorded
    datetime presumably set at the ``'dt'`` field in each entry.

    '''
    if isinstance(records, dict):
        records: list[tuple[str, dict]] = list(records.items())

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
                assert v is not None, f'No valid value for `{k}`!?'

                # only call parser on the value if not None from
                # the `parsers` table above (when NOT using
                # `.get()`), otherwise pass through the value and
                # sort on it directly
                if (
                    not isinstance(v, DateTime)
                    and (parser := parsers.get(k))
                ):
                    return parser(v)
                else:
                    return v

        else:
            # XXX: should never get here..
            breakpoint()

    entry: tuple[str, dict] | Transaction
    for entry in sorted(
        records,
        key=key or dyn_parse_to_dt,
    ):
        # NOTE the type sig above; either pairs or txns B)
        yield entry


# TODO: probably just move this into the test suite or
# keep it here for use from as such?
# def ensure_state(self) -> None:
#     '''
#     Audit either the `.cumsize` and `.ppu` local instance vars against
#     the clears table calculations and return the calc-ed values if
#     they differ and log warnings to console.

#     '''
#     # clears: list[dict] = self._clears

#     # self.first_clear_dt = min(clears, key=lambda e: e['dt'])['dt']
#     last_clear: dict = clears[-1]
#     csize: float = self.calc_size()
#     accum: float = last_clear['accum_size']

#     if not self.expired():
#         if (
#             csize != accum
#             and csize != round(accum * (self.split_ratio or 1))
#         ):
#             raise ValueError(f'Size mismatch: {csize}')
#     else:
#         assert csize == 0, 'Contract is expired but non-zero size?'

#     if self.cumsize != csize:
#         log.warning(
#             'Position state mismatch:\n'
#             f'{self.cumsize} => {csize}'
#         )
#         self.cumsize = csize

#     cppu: float = self.calc_ppu()
#     ppu: float = last_clear['ppu']
#     if (
#         cppu != ppu
#         and self.split_ratio is not None

#         # handle any split info entered (for now) manually by user
#         and cppu != (ppu / self.split_ratio)
#     ):
#         raise ValueError(f'PPU mismatch: {cppu}')

#     if self.ppu != cppu:
#         log.warning(
#             'Position state mismatch:\n'
#             f'{self.ppu} => {cppu}'
#         )
#         self.ppu = cppu


@cm
def open_ledger_dfs(

    brokername: str,
    acctname: str,

    ledger: TransactionLedger | None = None,

    **kwargs,

) -> tuple[
    dict[str, pl.DataFrame],
    TransactionLedger,
]:
    '''
    Open a ledger of trade records (presumably from some broker
    backend), normalize the records into `Transactions` via the
    backend's declared endpoint, cast to a `polars.DataFrame` which
    can update the ledger on exit.

    '''

    if not ledger:
        import time
        from tractor._debug import open_crash_handler
        from ._ledger import open_trade_ledger

        now = time.time()
        with (
            open_crash_handler(),

            open_trade_ledger(
                brokername,
                acctname,
                rewrite=True,
                allow_from_sync_code=True,

                # proxied through from caller
                **kwargs,

            ) as ledger,
        ):
            if not ledger:
                raise ValueError(f'No ledger for {acctname}@{brokername} exists?')

            print(f'LEDGER LOAD TIME: {time.time() - now}')

            # process raw TOML ledger into txns using the
            # appropriate backend normalizer.
            # cache: AssetsInfo = get_symcache(
            #     brokername,
            #     allow_reload=True,
            # )

        yield ledger_to_dfs(ledger), ledger


def ledger_to_dfs(
    ledger: TransactionLedger,

) -> dict[str, pl.DataFrame]:

    txns: dict[str, Transaction] = ledger.to_txns()

    # ldf = pl.DataFrame(
    #     list(txn.to_dict() for txn in txns.values()),
    ldf = pl.from_dicts(
        list(txn.to_dict() for txn in txns.values()),

        # only for ordering the cols
        schema=[
            ('fqme', str),
            ('tid', str),
            ('bs_mktid', str),
            ('expiry', str),
            ('etype', str),
            ('dt', str),
            ('size', pl.Float64),
            ('price', pl.Float64),
            ('cost', pl.Float64),
        ],
    ).sort(  # chronological order
        'dt'
    ).with_columns([
        pl.col('dt').str.to_datetime(),
        # pl.col('expiry').str.to_datetime(),
        # pl.col('expiry').dt.date(),
    ])

    # filter out to the columns matching values filter passed
    # as input.
    # if filter_by_ids:
    #     for col, vals in filter_by_ids.items():
    #         str_vals = set(map(str, vals))
    #         pred: pl.Expr = pl.col(col).eq(str_vals.pop())
    #         for val in str_vals:
    #             pred |= pl.col(col).eq(val)

    #     fdf = df.filter(pred)

    # break up into a frame per mkt / fqme
    dfs: dict[str, pl.DataFrame] = ldf.partition_by(
        'bs_mktid',
        as_dict=True,
    )

    # TODO: not sure if this is even possible but..
    # - it'd be more ideal to use `ppt = df.groupby('fqme').agg([`
    # - ppu and bep calcs!
    for key in dfs:

        # covert to lazy form (since apparently we might need it
        # eventually ...)
        df = dfs[key]

        ldf = df.lazy()
        # TODO: pass back the current `Position` object loaded from
        # the account as well? Would provide incentive to do all
        # this ledger loading inside a new async open_account().
        # bs_mktid: str = df[0]['bs_mktid']
        # pos: Position = acnt.pps[bs_mktid]

        df = dfs[key] = ldf.with_columns([

            pl.cumsum('size').alias('cumsize'),

            # amount of source asset "sent" (via buy txns in
            # the market) to acquire the dst asset, PER txn.
            # when this value is -ve (i.e. a sell operation) then
            # the amount sent is actually "returned".
            (
                (pl.col('price') * pl.col('size'))
                +
                (pl.col('cost')) # * pl.col('size').sign())
            ).alias('dst_bot'),

        ]).with_columns([

            # rolling balance in src asset units
            (pl.col('dst_bot').cumsum() * -1).alias('src_balance'),

            # "position operation type" in terms of increasing the
            # amount in the dst asset (entering) or decreasing the
            # amount in the dst asset (exiting).
            pl.when(
                pl.col('size').sign() == pl.col('cumsize').sign()

            ).then(
                pl.lit('enter')  # see above, but is just price * size per txn

            ).otherwise(
                pl.when(pl.col('cumsize') == 0)
                .then(pl.lit('exit_to_zero'))
                .otherwise(pl.lit('exit'))
            ).alias('descr'),

            (pl.col('cumsize').sign() == pl.col('size').sign())
            .alias('is_enter'),

        ]).with_columns([

            # pl.lit(0, dtype=pl.Utf8).alias('virt_cost'),
            pl.lit(0, dtype=pl.Float64).alias('applied_cost'),
            pl.lit(0, dtype=pl.Float64).alias('pos_ppu'),
            pl.lit(0, dtype=pl.Float64).alias('per_txn_pnl'),
            pl.lit(0, dtype=pl.Float64).alias('cum_pos_pnl'),
            pl.lit(0, dtype=pl.Float64).alias('pos_bep'),
            pl.lit(0, dtype=pl.Float64).alias('cum_ledger_pnl'),
            pl.lit(None, dtype=pl.Float64).alias('ledger_bep'),

            # TODO: instead of the iterative loop below i guess we
            # could try using embedded lists to track which txns
            # are part of which ppu / bep calcs? Not sure this will
            # look any better nor be any more performant though xD
            # pl.lit([[0]], dtype=pl.List(pl.Float64)).alias('list'),

        # choose fields to emit for accounting puposes
        ]).select([
            pl.exclude([
                'tid',
                # 'dt',
                'expiry',
                'bs_mktid',
                'etype',
                # 'is_enter',
            ]),
        ]).collect()

        # compute recurrence relations for ppu and bep
        last_ppu: float = 0
        last_cumsize: float = 0
        last_ledger_pnl: float = 0
        last_pos_pnl: float = 0
        virt_costs: list[float, float] = [0., 0.]

        # imperatively compute the PPU (price per unit) and BEP
        # (break even price) iteratively over the ledger, oriented
        # around each position state: a state of split balances in
        # > 1 asset.
        for i, row in enumerate(df.iter_rows(named=True)):

            cumsize: float = row['cumsize']
            is_enter: bool = row['is_enter']
            price: float = row['price']
            size: float = row['size']

            # the profit is ALWAYS decreased, aka made a "loss"
            # by the constant fee charged by the txn provider!
            # see below in final PnL calculation and row element
            # set.
            txn_cost: float = row['cost']
            pnl: float = 0

            # ALWAYS reset per-position cum PnL
            if last_cumsize == 0:
                last_pos_pnl: float = 0

            # a "position size INCREASING" or ENTER transaction
            # which "makes larger", in src asset unit terms, the
            # trade's side-size of the destination asset:
            # - "buying" (more) units of the dst asset
            # - "selling" (more short) units of the dst asset
            if is_enter:

                # Naively include transaction cost in breakeven
                # price and presume the worst case of the
                # exact-same-cost-to-exit this transaction's worth
                # of size even though in reality it will be dynamic
                # based on exit strategy, price, liquidity, etc..
                virt_cost: float = txn_cost

                # cpu: float = cost / size
                # cummean of the cost-per-unit used for modelling
                # a projected future exit cost which we immediately
                # include in the costs incorporated to BEP on enters
                last_cum_costs_size, last_cpu = virt_costs
                cum_costs_size: float = last_cum_costs_size + abs(size)
                cumcpu = (
                    (last_cpu * last_cum_costs_size)
                    +
                    txn_cost
                ) / cum_costs_size
                virt_costs = [cum_costs_size, cumcpu]

                txn_cost = txn_cost + virt_cost
                # df[i, 'virt_cost'] = f'{-virt_cost} FROM {cumcpu}@{cum_costs_size}'

                # a cumulative mean of the price-per-unit acquired
                # in the destination asset:
                # https://en.wikipedia.org/wiki/Moving_average#Cumulative_average
                # You could also think of this measure more
                # generally as an exponential mean with `alpha
                # = 1/N` where `N` is the current number of txns
                # included in the "position" defining set:
                # https://en.wikipedia.org/wiki/Exponential_smoothing
                ppu: float = (
                    (
                        (last_ppu * last_cumsize)
                        +
                        (price * size)
                    ) /
                    cumsize
                )

            # a "position size DECREASING" or EXIT transaction
            # which "makes smaller" the trade's side-size of the
            # destination asset:
            # - selling previously bought units of the dst asset
            #   (aka 'closing' a long position).
            # - buying previously borrowed and sold (short) units
            #   of the dst asset (aka 'covering'/'closing' a short
            #   position).
            else:
                # only changes on position size increasing txns
                ppu: float = last_ppu

                # UNWIND IMPLIED COSTS FROM ENTRIES
                # => Reverse the virtual/modelled (2x predicted) txn
                # cost that was included in the least-recently
                # entered txn that is still part of the current CSi
                # set.
                # => we look up the cost-per-unit cumsum and apply
                # if over the current txn size (by multiplication)
                # and then reverse that previusly applied cost on
                # the txn_cost for this record.
                #
                # NOTE: current "model" is just to previously assumed 2x
                # the txn cost for a matching enter-txn's
                # cost-per-unit; we then immediately reverse this
                # prediction and apply the real cost received here.
                last_cum_costs_size, last_cpu = virt_costs
                prev_virt_cost: float = last_cpu * abs(size)
                txn_cost: float = txn_cost - prev_virt_cost  # +ve thus a "reversal"
                cum_costs_size: float = last_cum_costs_size - abs(size)
                virt_costs = [cum_costs_size, last_cpu]

                # df[i, 'virt_cost'] = (
                #     f'{-prev_virt_cost} FROM {last_cpu}@{cum_costs_size}'
                # )

                # the per-txn profit or loss (PnL) given we are
                # (partially) "closing"/"exiting" the position via
                # this txn.
                pnl: float = (last_ppu - price) * size

            # always subtract txn cost from total txn pnl
            txn_pnl: float = pnl - txn_cost

            # cumulative PnLs per txn
            last_ledger_pnl = (
                last_ledger_pnl + txn_pnl
            )
            last_pos_pnl = df[i, 'cum_pos_pnl'] = (
                last_pos_pnl + txn_pnl
            )

            if cumsize == 0:
                last_ppu = ppu = 0

            # compute the BEP: "break even price", a value that
            # determines at what price the remaining cumsize can be
            # liquidated such that the net-PnL on the current
            # position will result in ZERO gain or loss from open
            # to close including all txn costs B)
            if (
                abs(cumsize) > 0  # non-exit-to-zero position txn
            ):
                ledger_bep: float = (
                    (
                        (ppu * cumsize)
                        -
                        (last_ledger_pnl * copysign(1, cumsize))
                    ) / cumsize
                )

                # NOTE: when we "enter more" dst asset units (aka
                # increase position state) AFTER having exited some
                # units (aka decreasing the pos size some) the bep
                # needs to be RECOMPUTED based on new ppu such that
                # liquidation of the cumsize at the bep price
                # results in a zero-pnl for the existing position
                # (since the last one).
                # for position lifetime BEP we never can have
                # a valid value once the position is "closed"
                # / full exitted Bo
                pos_bep: float = (
                    (
                        (ppu * cumsize)
                        -
                        (last_pos_pnl * copysign(1, cumsize))
                    ) / cumsize
                )

            # inject DF row with all values
            df[i, 'pos_ppu'] = ppu
            df[i, 'per_txn_pnl'] = txn_pnl
            df[i, 'applied_cost'] = -txn_cost
            df[i, 'cum_pos_pnl'] = last_pos_pnl
            df[i, 'pos_bep'] = pos_bep
            df[i, 'cum_ledger_pnl'] = last_ledger_pnl
            df[i, 'ledger_bep'] = ledger_bep

            # keep backrefs to suffice reccurence relation
            last_ppu: float = ppu
            last_cumsize: float = cumsize

    return dfs
