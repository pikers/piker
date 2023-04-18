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
Public abstractions for organizing, managing and generally operating-on
real-time data processing data-structures.

"Streams, flumes, cascades and flows.."

"""
from __future__ import annotations
# from decimal import Decimal
from typing import (
    TYPE_CHECKING,
)

import tractor
import pendulum
import numpy as np

from ..accounting._mktinfo import (
    MktPair,
    Symbol,
)
from ._util import (
    log,
)
from .types import Struct
from ._sharedmem import (
    attach_shm_array,
    ShmArray,
    _Token,
)
# from .._profile import (
#     Profiler,
#     pg_profile_enabled,
# )

if TYPE_CHECKING:
    # from pyqtgraph import PlotItem
    from .feed import Feed


# TODO: ideas for further abstractions as per
# https://github.com/pikers/piker/issues/216 and
# https://github.com/pikers/piker/issues/270:
# - a ``Cascade`` would be the minimal "connection" of 2 ``Flumes``
#   as per circuit parlance:
#   https://en.wikipedia.org/wiki/Two-port_network#Cascade_connection
#     - could cover the combination of our `FspAdmin` and the
#       backend `.fsp._engine` related machinery to "connect" one flume
#       to another?
# - a (financial signal) ``Flow`` would be the a "collection" of such
#    minmial cascades. Some engineering based jargon concepts:
#     - https://en.wikipedia.org/wiki/Signal_chain
#     - https://en.wikipedia.org/wiki/Daisy_chain_(electrical_engineering)
#     - https://en.wikipedia.org/wiki/Audio_signal_flow
#     - https://en.wikipedia.org/wiki/Digital_signal_processing#Implementation
#     - https://en.wikipedia.org/wiki/Dataflow_programming
#     - https://en.wikipedia.org/wiki/Signal_programming
#     - https://en.wikipedia.org/wiki/Incremental_computing


class Flume(Struct):
    '''
    Composite reference type which points to all the addressing handles
    and other meta-data necessary for the read, measure and management
    of a set of real-time updated data flows.

    Can be thought of as a "flow descriptor" or "flow frame" which
    describes the high level properties of a set of data flows that can
    be used seamlessly across process-memory boundaries.

    Each instance's sub-components normally includes:
     - a msg oriented quote stream provided via an IPC transport
     - history and real-time shm buffers which are both real-time
       updated and backfilled.
     - associated startup indexing information related to both buffer
       real-time-append and historical prepend addresses.
     - low level APIs to read and measure the updated data and manage
       queuing properties.

    '''
    mkt: MktPair | Symbol
    first_quote: dict
    _rt_shm_token: _Token

    @property
    def symbol(self) -> MktPair | Symbol:
        log.warning(
            '`Flume.symbol` is deprecated!\n'
            'Use `.mkt: MktPair` instead!'
        )
        return self.mkt

    # optional since some data flows won't have a "downsampled" history
    # buffer/stream (eg. FSPs).
    _hist_shm_token: _Token | None = None

    # private shm refs loaded dynamically from tokens
    _hist_shm: ShmArray | None = None
    _rt_shm: ShmArray | None = None

    stream: tractor.MsgStream | None = None
    izero_hist: int = 0
    izero_rt: int = 0
    throttle_rate: int | None = None

    # TODO: do we need this really if we can pull the `Portal` from
    # ``tractor``'s internals?
    feed: Feed | None = None

    @property
    def rt_shm(self) -> ShmArray:

        if self._rt_shm is None:
            self._rt_shm = attach_shm_array(
                token=self._rt_shm_token,
                readonly=True,
            )

        return self._rt_shm

    @property
    def hist_shm(self) -> ShmArray:

        if self._hist_shm_token is None:
            raise RuntimeError(
                'No shm token has been set for the history buffer?'
            )

        if (
            self._hist_shm is None
        ):
            self._hist_shm = attach_shm_array(
                token=self._hist_shm_token,
                readonly=True,
            )

        return self._hist_shm

    async def receive(self) -> dict:
        return await self.stream.receive()

    def get_ds_info(
        self,
    ) -> tuple[float, float, float]:
        '''
        Compute the "downsampling" ratio info between the historical shm
        buffer and the real-time (HFT) one.

        Return a tuple of the fast sample period, historical sample
        period and ratio between them.

        '''
        times = self.hist_shm.array['time']
        end = pendulum.from_timestamp(times[-1])
        start = pendulum.from_timestamp(times[times != times[-1]][-1])
        hist_step_size_s = (end - start).seconds

        times = self.rt_shm.array['time']
        end = pendulum.from_timestamp(times[-1])
        start = pendulum.from_timestamp(times[times != times[-1]][-1])
        rt_step_size_s = (end - start).seconds

        ratio = hist_step_size_s / rt_step_size_s
        return (
            rt_step_size_s,
            hist_step_size_s,
            ratio,
        )

    # TODO: get native msgspec decoding for these workinn
    def to_msg(self) -> dict:

        msg = self.to_dict()
        msg['mkt'] = self.mkt.to_dict()

        # can't serialize the stream or feed objects, it's expected
        # you'll have a ref to it since this msg should be rxed on
        # a stream on whatever far end IPC..
        msg.pop('stream')
        msg.pop('feed')
        return msg

    @classmethod
    def from_msg(
        cls,
        msg: dict,

    ) -> dict:
        '''
        Load from an IPC msg presumably in either `dict` or
        `msgspec.Struct` form.

        '''
        mkt_msg = msg.pop('mkt')

        if 'dst' in mkt_msg:
            mkt = MktPair.from_msg(mkt_msg)

        else:
            # XXX NOTE: ``msgspec`` can encode `Decimal`
            # but it doesn't decide to it by default since
            # we aren't spec-cing these msgs as structs, SO
            # we have to ensure we do a struct type case (which `.copy()`
            # does) to ensure we get the right type!
            mkt = Symbol(**mkt_msg).copy()

        return cls(mkt=mkt, **msg)

    def get_index(
        self,
        time_s: float,
        array: np.ndarray,

    ) -> int | float:
        '''
        Return array shm-buffer index for for epoch time.

        '''
        times = array['time']
        first = np.searchsorted(
            times,
            time_s,
            side='left',
        )
        imx = times.shape[0] - 1
        return min(first, imx)
