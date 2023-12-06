# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship of piker0)

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
Fin-sig-proc for the peeps!

'''
from typing import AsyncIterator

import numpy as np

from ._api import (
    maybe_mk_fsp_shm,
    Fsp,
)
from ._engine import (
    cascade,
    Cascade,
)
from ._volume import (
    dolla_vlm,
    flow_rates,
    tina_vwap,
)

__all__: list[str] = [
    'cascade',
    'Cascade',
    'maybe_mk_fsp_shm',
    'Fsp',
    'dolla_vlm',
    'flow_rates',
    'tina_vwap',
]


async def latency(
    source: 'TickStream[Dict[str, float]]',  # noqa
    ohlcv: np.ndarray

) -> AsyncIterator[np.ndarray]:
    '''
    Latency measurements, broker to piker.

    '''
    # TODO: do we want to offer yielding this async
    # before the rt data connection comes up?

    # deliver zeros for all prior history
    yield np.zeros(len(ohlcv))

    async for quote in source:
        ts = quote.get('broker_ts')
        if ts:
            # This is codified in the per-broker normalization layer
            # TODO: Add more measure points and diffs for full system
            # stack tracing.
            value = quote['brokerd_ts'] - quote['broker_ts']
            yield value
