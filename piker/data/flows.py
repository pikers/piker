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
abstractions for organizing, managing and generally operating-on
real-time data processing data-structures.

"Streams, flumes, cascades and flows.."

"""
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from functools import partial
from typing import (
    AsyncIterator,
    TYPE_CHECKING,
)

import tractor
from tractor.trionics import (
    maybe_open_context,
)
import pendulum
import numpy as np

from .types import Struct
from ._source import (
    Symbol,
)
from ._sharedmem import (
    attach_shm_array,
    ShmArray,
    _Token,
)
from ._sampling import (
    open_sample_stream,
)

if TYPE_CHECKING:
    from pyqtgraph import PlotItem
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
    symbol: Symbol
    first_quote: dict
    _rt_shm_token: _Token

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

    @acm
    async def index_stream(
        self,
        delay_s: float = 1,

    ) -> AsyncIterator[int]:

        if not self.feed:
            raise RuntimeError('This flume is not part of any ``Feed``?')

        # TODO: maybe a public (property) API for this in ``tractor``?
        portal = self.stream._ctx._portal
        assert portal

        # XXX: this should be singleton on a host,
        # a lone broker-daemon per provider should be
        # created for all practical purposes
        async with open_sample_stream(float(delay_s)) as stream:
            yield stream

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
        msg['symbol'] = msg['symbol'].to_dict()

        # can't serialize the stream or feed objects, it's expected
        # you'll have a ref to it since this msg should be rxed on
        # a stream on whatever far end IPC..
        msg.pop('stream')
        msg.pop('feed')
        return msg

    @classmethod
    def from_msg(cls, msg: dict) -> dict:
        symbol = Symbol(**msg.pop('symbol'))
        return cls(
            symbol=symbol,
            **msg,
        )

    def get_index(
        self,
        time_s: float,

    ) -> int:
        '''
        Return array shm-buffer index for for epoch time.

        '''
        array = self.rt_shm.array
        times = array['time']
        mask = (times >= time_s)

        if any(mask):
            return array['index'][mask][0]

        # just the latest index
        array['index'][-1]

    def slice_from_time(
        self,
        arr: np.ndarray,
        start_t: float,
        stop_t: float,

    ) -> tuple[
        slice,
        slice,
        np.ndarray | None,
    ]:
        '''
        Slice an input struct array to a time range and return the absolute
        and "readable" slices for that array as well as the indexing mask
        for the caller to use to slice the input array if needed.

        '''
        times = arr['time']
        index = arr['index']

        if (
            start_t < 0
            or start_t >= stop_t
        ):
            return (
                slice(
                    index[0],
                    index[-1],
                ),
                slice(
                    0,
                    len(arr),
                ),
                None,
            )

        # use advanced indexing to map the
        # time range to the index range.
        mask: np.ndarray = (
            (times >= start_t)
            &
            (times < stop_t)
        )

        # TODO: if we can ensure each time field has a uniform
        # step we can instead do some arithmetic to determine
        # the equivalent index like we used to?
        # return array[
        #     lbar - ifirst:
        #     (rbar - ifirst) + 1
        # ]

        i_by_t = index[mask]
        try:
            i_0 = i_by_t[0]
        except IndexError:
            if (
                start_t < times[0]
                or stop_t >= times[-1]
            ):
                return (
                    slice(
                        index[0],
                        index[-1],
                    ),
                    slice(
                        0,
                        len(arr),
                    ),
                    None,
                )

        abs_slc = slice(
            i_0,
            i_by_t[-1],
        )
        # slice data by offset from the first index
        # available in the passed datum set.
        read_slc = slice(
            0,
            i_by_t[-1] - i_0,
        )

        # also return the readable data from the timerange
        return (
            abs_slc,
            read_slc,
            mask,
        )

    def view_data(
        self,
        plot: PlotItem,
        timeframe_s: int = 1,

    ) -> np.ndarray:
        '''
        Return sliced-to-view source data along with absolute
        (``ShmArray._array['index']``) and read-relative
        (``ShmArray.array``) slices.

        '''
        # get far-side x-indices plot view
        vr = plot.viewRect()

        if timeframe_s > 1:
            arr = self.hist_shm.array
        else:
            arr = self.rt_shm.array

        (
            abs_slc,
            read_slc,
            mask,
        ) = self.slice_from_time(
            arr,
            start_t=vr.left(),
            stop_t=vr.right(),
            timeframe_s=timeframe_s,
        )
        return (
            abs_slc,
            read_slc,
            arr[mask] if mask is not None else arr,
        )
