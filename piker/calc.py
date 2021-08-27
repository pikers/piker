# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
Handy financial calculations.
"""
import math
import itertools


def humanize(
    number: float,
    digits: int = 1
) -> str:
    '''Convert large numbers to something with at most ``digits`` and
    a letter suffix (eg. k: thousand, M: million, B: billion).

    '''
    try:
        float(number)
    except ValueError:
        return 0
    if not number or number <= 0:
        return round(number, ndigits=digits)

    mag2suffix = {3: 'k', 6: 'M', 9: 'B'}
    mag = math.floor(math.log(number, 10))
    if mag < 3:
        return round(number, ndigits=digits)

    maxmag = max(itertools.takewhile(lambda key: mag >= key, mag2suffix))

    return "{value}{suffix}".format(
        value=round(number/10**maxmag, ndigits=digits),
        suffix=mag2suffix[maxmag],
    )


def percent_change(

    init: float,
    new: float,

) -> float:
    '''Calcuate the percentage change of some ``new`` value
    from some initial value, ``init``.

    '''
    if not (init and new):
        return 0

    return (new - init) / init * 100.
