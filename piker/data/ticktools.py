# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
Stream format enforcement.

'''
from itertools import chain
from typing import AsyncIterator


def iterticks(
    quote: dict,
    types: tuple[str] = (
        'trade',
        'dark_trade',
    ),
    deduplicate_darks: bool = False,

) -> AsyncIterator:
    '''
    Iterate through ticks delivered per quote cycle.

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

        for tick in ticks:
            # print(f"{quote['symbol']}: {tick}")
            ttype = tick.get('type')
            if ttype in types:
                yield tick
