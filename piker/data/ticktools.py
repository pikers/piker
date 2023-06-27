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
Tick event stream processing, filter-by-types, format-normalization.

'''
from itertools import chain
from typing import (
    Any,
    AsyncIterator,
)

# tick-type-classes template for all possible "lowest level" events
# that can can be emitted by the "top of book" L1 queues and
# price-matching (with eventual clearing) in a double auction
# market (queuing) system.
_tick_groups: dict[str, set[str]] = {
    'clears': {'trade', 'dark_trade', 'last'},
    'bids': {'bid', 'bsize'},
    'asks': {'ask', 'asize'},
}

# XXX alo define the flattened set of all such "fundamental ticks"
# so that it can be used as filter, eg. in the graphics display
# loop to compute running windowed y-ranges B)
_auction_ticks: set[str] = set.union(*_tick_groups.values())


def frame_ticks(
    quote: dict[str, Any],

    ticks_by_type: dict | None = None,
    ticks_in_order: list[dict[str, Any]] | None = None

) -> dict[
    str,
    list[dict[str, Any]]
]:
    '''
    XXX: build a tick-by-type table of lists
    of tick messages. This allows for less
    iteration on the receiver side by allowing for
    a single "latest tick event" look up by
    indexing the last entry in each sub-list.

    tbt = {
        'types': ['bid', 'asize', 'last', .. '<type_n>'],

        'bid': [tick0, tick1, tick2, .., tickn],
        'asize': [tick0, tick1, tick2, .., tickn],
        'last': [tick0, tick1, tick2, .., tickn],
        ...
        '<type_n>': [tick0, tick1, tick2, .., tickn],
    }

    If `ticks_in_order` is provided, append any retrieved ticks
    since last iteration into this array/buffer/list.

    '''
    # TODO: once we decide to get fancy really we should
    # have a shared mem tick buffer that is just
    # continually filled and the UI just ready from it
    # at it's display rate.

    tbt = ticks_by_type if ticks_by_type is not None else {}
    if not (ticks := quote.get('ticks')):
        return tbt

    # append in reverse FIFO order for in-order iteration on
    # receiver side.
    tick: dict[str, Any]
    for tick in ticks:
        tbt.setdefault(
            tick['type'],
            [],
        ).append(tick)

    # TODO: do we need this any more or can we just
    # expect the receiver to unwind the below
    # `ticks_by_type: dict`?
    # => undwinding would potentially require a
    # `dict[str, set | list]` instead with an
    # included `'types' field which is an (ordered)
    # set of tick type fields in the order which
    # types arrived?
    if ticks_in_order:
        ticks_in_order.extend(ticks)

    return tbt


def iterticks(
    quote: dict,
    types: tuple[str] = (
        'trade',
        'dark_trade',
    ),
    deduplicate_darks: bool = False,
    reverse: bool = False,

    # TODO: should we offer delegating to `frame_ticks()` above
    # with this?
    frame_by_type: bool = False,

) -> AsyncIterator:
    '''
    Iterate through ticks delivered per quote cycle, filter and
    yield any declared in `types`.

    '''
    if deduplicate_darks:
        assert 'dark_trade' in types

    # print(f"{quote}\n\n")
    ticks = quote.get('ticks', ())
    trades = {}
    darks = {}

    if ticks:

        # do a first pass and attempt to remove duplicate dark
        # trades with the same tick signature.
        if deduplicate_darks:
            for tick in ticks:
                ttype = tick.get('type')

                time = tick.get('time', None)
                if time:
                    sig = (
                        time,
                        tick['price'],
                        tick.get('size')
                    )

                    if ttype == 'dark_trade':
                        darks[sig] = tick

                    elif ttype == 'trade':
                        trades[sig] = tick

            # filter duplicates
            for sig, tick in trades.items():
                tick = darks.pop(sig, None)
                if tick:
                    ticks.remove(tick)
                    # print(f'DUPLICATE {tick}')

            # re-insert ticks
            ticks.extend(list(chain(trades.values(), darks.values())))

        # most-recent-first
        if reverse:
            ticks = reversed(ticks)

        for tick in ticks:
            # print(f"{quote['symbol']}: {tick}")
            ttype = tick.get('type')
            if ttype in types:
                yield tick
