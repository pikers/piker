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
(time-series) database middle ware layer.

- APIs for read, write, delete, replicate over multiple
  db systems.
- backend agnostic tick msg ingest machinery.
- broadcast systems for fan out of real-time ingested
  data to live consumers.
- test harness utilities for data-processing verification.

'''
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from datetime import datetime
# from pprint import pformat
from typing import (
    Union,
)

import tractor
import numpy as np
from anyio_marketstore import (
    Params,
)
import pendulum
import purerpc

from . import config
from ..service.marketstore import (
    MarketstoreClient,
    tf_in_1s,
    mk_tbk,
    _ohlcv_dt,
    MarketStoreError,
)
from ..data.feed import maybe_open_feed
from ..log import get_logger
from .._profile import Profiler


log = get_logger(__name__)


class Storage:
    '''
    High level storage api for both real-time and historical ingest.

    '''
    def __init__(
        self,
        client: MarketstoreClient,

    ) -> None:
        # TODO: eventually this should be an api/interface type that
        # ensures we can support multiple tsdb backends.
        self.client = client

        # series' cache from tsdb reads
        self._arrays: dict[str, np.ndarray] = {}

    async def list_keys(self) -> list[str]:
        return await self.client.list_symbols()

    async def search_keys(self, pattern: str) -> list[str]:
        '''
        Search for time series key in the storage backend.

        '''
        ...

    async def write_ticks(self, ticks: list) -> None:
        ...

    async def load(
        self,
        fqme: str,
        timeframe: int,

    ) -> tuple[
        np.ndarray,  # timeframe sampled array-series
        datetime | None,  # first dt
        datetime | None,  # last dt
    ]:

        first_tsdb_dt, last_tsdb_dt = None, None
        hist = await self.read_ohlcv(
            fqme,
            # on first load we don't need to pull the max
            # history per request size worth.
            limit=3000,
            timeframe=timeframe,
        )
        log.info(f'Loaded tsdb history {hist}')

        if len(hist):
            times = hist['Epoch']
            first, last = times[0], times[-1]
            first_tsdb_dt, last_tsdb_dt = map(
                pendulum.from_timestamp, [first, last]
            )

        return (
            hist,  # array-data
            first_tsdb_dt,  # start of query-frame
            last_tsdb_dt,  # most recent
        )

    async def read_ohlcv(
        self,
        fqme: str,
        timeframe: int | str,
        end: int | None = None,
        limit: int = int(800e3),

    ) -> np.ndarray:

        client = self.client
        syms = await client.list_symbols()

        if fqme not in syms:
            return {}

        # use the provided timeframe or 1s by default
        tfstr = tf_in_1s.get(timeframe, tf_in_1s[1])

        params = Params(
            symbols=fqme,
            timeframe=tfstr,
            attrgroup='OHLCV',
            end=end,
            # limit_from_start=True,

            # TODO: figure the max limit here given the
            # ``purepc`` msg size limit of purerpc: 33554432
            limit=limit,
        )

        for i in range(3):
            try:
                result = await client.query(params)
                break
            except purerpc.grpclib.exceptions.UnknownError as err:
                if 'snappy' in err.args:
                    await tractor.breakpoint()

                # indicate there is no history for this timeframe
                log.exception(
                    f'Unknown mkts QUERY error: {params}\n'
                    f'{err.args}'
                )
        else:
            return {}

        # TODO: it turns out column access on recarrays is actually slower:
        # https://jakevdp.github.io/PythonDataScienceHandbook/02.09-structured-data-numpy.html#RecordArrays:-Structured-Arrays-with-a-Twist
        # it might make sense to make these structured arrays?
        data_set = result.by_symbols()[fqme]
        array = data_set.array

        # XXX: ensure sample rate is as expected
        time = data_set.array['Epoch']
        if len(time) > 1:
            time_step = time[-1] - time[-2]
            ts = tf_in_1s.inverse[data_set.timeframe]

            if time_step != ts:
                log.warning(
                    f'MKTS BUG: wrong timeframe loaded: {time_step}'
                    'YOUR DATABASE LIKELY CONTAINS BAD DATA FROM AN OLD BUG'
                    f'WIPING HISTORY FOR {ts}s'
                )
                await self.delete_ts(fqme, timeframe)

                # try reading again..
                return await self.read_ohlcv(
                    fqme,
                    timeframe,
                    end,
                    limit,
                )

        return array

    async def delete_ts(
        self,
        key: str,
        timeframe: Union[int, str | None] = None,
        fmt: str = 'OHLCV',

    ) -> bool:

        client = self.client
        syms = await client.list_symbols()
        if key not in syms:
            await tractor.breakpoint()
            raise KeyError(f'`{key}` table key not found in\n{syms}?')

        tbk = mk_tbk((
            key,
            tf_in_1s.get(timeframe, tf_in_1s[60]),
            fmt,
        ))
        return await client.destroy(tbk=tbk)

    async def write_ohlcv(
        self,
        fqme: str,
        ohlcv: np.ndarray,
        timeframe: int,
        append_and_duplicate: bool = True,
        limit: int = int(800e3),

    ) -> None:
        # build mkts schema compat array for writing
        mkts_dt = np.dtype(_ohlcv_dt)
        mkts_array = np.zeros(
            len(ohlcv),
            dtype=mkts_dt,
        )
        # copy from shm array (yes it's this easy):
        # https://numpy.org/doc/stable/user/basics.rec.html#assignment-from-other-structured-arrays
        mkts_array[:] = ohlcv[[
            'time',
            'open',
            'high',
            'low',
            'close',
            'volume',
        ]]

        m, r = divmod(len(mkts_array), limit)

        tfkey = tf_in_1s[timeframe]
        for i in range(m, 1):
            to_push = mkts_array[i-1:i*limit]

            # write to db
            resp = await self.client.write(
                to_push,
                tbk=f'{fqme}/{tfkey}/OHLCV',

                # NOTE: will will append duplicates
                # for the same timestamp-index.
                # TODO: pre-deduplicate?
                isvariablelength=append_and_duplicate,
            )

            log.info(
                f'Wrote {mkts_array.size} datums to tsdb\n'
            )

            for resp in resp.responses:
                err = resp.error
                if err:
                    raise MarketStoreError(err)

        if r:
            to_push = mkts_array[m*limit:]

            # write to db
            resp = await self.client.write(
                to_push,
                tbk=f'{fqme}/{tfkey}/OHLCV',

                # NOTE: will will append duplicates
                # for the same timestamp-index.
                # TODO: pre deduplicate?
                isvariablelength=append_and_duplicate,
            )

            log.info(
                f'Wrote {mkts_array.size} datums to tsdb\n'
            )

            for resp in resp.responses:
                err = resp.error
                if err:
                    raise MarketStoreError(err)

    # XXX: currently the only way to do this is through the CLI:

    # sudo ./marketstore connect --dir ~/.config/piker/data
    # >> \show mnq.globex.20220617.ib/1Sec/OHLCV 2022-05-15
    # and this seems to block and use up mem..
    # >> \trim mnq.globex.20220617.ib/1Sec/OHLCV 2022-05-15

    # relevant source code for this is here:
    # https://github.com/alpacahq/marketstore/blob/master/cmd/connect/session/trim.go#L14
    # def delete_range(self, start_dt, end_dt) -> None:
    #     ...


@acm
async def open_storage_client(
    host: str,
    grpc_port: int,

) -> tuple[Storage, dict[str, np.ndarray]]:
    '''
    Load a series by key and deliver in ``numpy`` struct array format.

    '''
    from piker.service.marketstore import get_client

    async with (
        # eventually a storage backend endpoint
        get_client(
            host=host,
            port=grpc_port,
        ) as client,
    ):
        # slap on our wrapper api
        yield Storage(client)


# NOTE: pretty sure right now this is only being
# called by a CLI entrypoint?
@acm
async def open_tsdb_client(
    fqme: str,
) -> Storage:

    # TODO: real-time dedicated task for ensuring
    # history consistency between the tsdb, shm and real-time feed..

    # update sequence design notes:

    # - load existing highest frequency data from mkts
    #   * how do we want to offer this to the UI?
    #    - lazy loading?
    #    - try to load it all and expect graphics caching/diffing
    #      to  hide extra bits that aren't in view?

    # - compute the diff between latest data from broker and shm
    #   * use sql api in mkts to determine where the backend should
    #     start querying for data?
    #   * append any diff with new shm length
    #   * determine missing (gapped) history by scanning
    #   * how far back do we look?

    # - begin rt update ingest and aggregation
    #   * could start by always writing ticks to mkts instead of
    #     worrying about a shm queue for now.
    #   * we have a short list of shm queues worth groking:
    #     - https://github.com/pikers/piker/issues/107
    #   * the original data feed arch blurb:
    #     - https://github.com/pikers/piker/issues/98
    #
    profiler = Profiler(
        disabled=True,  # not pg_profile_enabled(),
        delayed=False,
    )

    # load any user service settings for connecting to
    rootconf, path = config.load(
        'conf',
        touch_if_dne=True,
    )
    tsdbconf = rootconf['network'].get('tsdb')
    # backend = tsdbconf.pop('backend')
    async with (
        open_storage_client(
            **tsdbconf,
        ) as storage,

        maybe_open_feed(
            [fqme],
            start_stream=False,

        ) as feed,
    ):
        profiler(f'opened feed for {fqme}')

        # to_append = feed.hist_shm.array
        # to_prepend = None

        if fqme:
            flume = feed.flumes[fqme]
            symbol = flume.mkt
            if symbol:
                fqme = symbol.fqme

            # diff db history with shm and only write the missing portions
            # ohlcv = flume.hist_shm.array

            # TODO: use pg profiler
            # for secs in (1, 60):
            #     tsdb_array = await storage.read_ohlcv(
            #         fqme,
            #         timeframe=timeframe,
            #     )
            #     # hist diffing:
            #     # these aren't currently used but can be referenced from
            #     # within the embedded ipython shell below.
            #     to_append = ohlcv[ohlcv['time'] > ts['Epoch'][-1]]
            #     to_prepend = ohlcv[ohlcv['time'] < ts['Epoch'][0]]

            # profiler('Finished db arrays diffs')

            _ = await storage.client.list_symbols()
            # log.info(f'Existing tsdb symbol set:\n{pformat(syms)}')
            # profiler(f'listed symbols {syms}')
            yield storage

        # for array in [to_append, to_prepend]:
        #     if array is None:
        #         continue

        #     log.info(
        #         f'Writing datums {array.size} -> to tsdb from shm\n'
        #     )
        #     await storage.write_ohlcv(fqme, array)

        # profiler('Finished db writes')
