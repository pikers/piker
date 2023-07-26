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
marketstore tsdb backend:
https://github.com/alpacahq/marketstore


We wrote an async gGRPC client:
https://github.com/pikers/anyio-marketstore

which is normally preferred minus the discovered issues
in https://github.com/pikers/piker/issues/443

Which is the main reason for us moving away from this
platform..

'''
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from datetime import datetime
# from pprint import pformat
from typing import (
    Union,
)

from bidict import bidict
import tractor
import numpy as np
from anyio_marketstore import (
    Params,
)
import pendulum
# import purerpc

from piker.service.marketstore import (
    MarketstoreClient,
    tf_in_1s,
    mk_tbk,
    _ohlcv_dt,
    MarketStoreError,
)
from anyio_marketstore import (  # noqa
    open_marketstore_client,
    MarketstoreClient,
    Params,
)
from piker.log import get_logger


log = get_logger(__name__)


class MktsStorageClient:
    '''
    High level storage api for both real-time and historical ingest.

    '''
    name: str = 'marketstore'

    def __init__(
        self,
        client: MarketstoreClient,
        config: dict,

    ) -> None:
        # TODO: eventually this should be an api/interface type that
        # ensures we can support multiple tsdb backends.
        self.client = client
        self._config = config

        # series' cache from tsdb reads
        self._arrays: dict[str, np.ndarray] = {}

    @property
    def address(self) -> str:
        conf = self._config
        return f'grpc://{conf["host"]}:{conf["port"]}'

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
            # breakpoint()
            times: np.ndarray = hist['Epoch']

            first, last = times[0], times[-1]
            first_tsdb_dt, last_tsdb_dt = map(
                pendulum.from_timestamp,
                [first, last]
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
        end: float | None = None,  # epoch or none
        limit: int = int(200e3),

    ) -> np.ndarray:

        client = self.client
        syms = await client.list_symbols()
        if fqme not in syms:
            return {}

        # ensure end time is in correct int format!
        if (
            end
            and not isinstance(end, float)
        ):
            end = int(float(end))
            # breakpoint()

        # use the provided timeframe or 1s by default
        tfstr = tf_in_1s.get(timeframe, tf_in_1s[1])

        import pymarketstore as pymkts
        sync_client = pymkts.Client()
        param = pymkts.Params(
            symbols=fqme,
            timeframe=tfstr,
            attrgroup='OHLCV',
            end=end,

            limit=limit,
            # limit_from_start=True,
        )
        try:
            reply = sync_client.query(param)
        except Exception as err:
            if 'no files returned from query parse: None' in err.args:
                return []

            raise

        data_set: pymkts.results.DataSet = reply.first()
        array: np.ndarray = data_set.array

        # params = Params(
        #     symbols=fqme,
        #     timeframe=tfstr,
        #     attrgroup='OHLCV',
        #     end=end,
        #     # limit_from_start=True,

        #     # TODO: figure the max limit here given the
        #     # ``purepc`` msg size limit of purerpc: 33554432
        #     limit=limit,
        # )

        # for i in range(3):
        #     try:
        #         result = await client.query(params)
        #         break
        #     except purerpc.grpclib.exceptions.UnknownError as err:
        #         if 'snappy' in err.args:
        #             await tractor.pause()

        #         # indicate there is no history for this timeframe
        #         log.exception(
        #             f'Unknown mkts QUERY error: {params}\n'
        #             f'{err.args}'
        #         )
        # else:
        #     return {}

        # # TODO: it turns out column access on recarrays is actually slower:
        # # https://jakevdp.github.io/PythonDataScienceHandbook/02.09-structured-data-numpy.html#RecordArrays:-Structured-Arrays-with-a-Twist
        # # it might make sense to make these structured arrays?
        # data_set = result.by_symbols()[fqme]
        # array = data_set.array

        # XXX: ensure sample rate is as expected
        time = data_set.array['Epoch']
        if len(time) > 1:
            time_step = time[-1] - time[-2]
            ts = tf_in_1s.inverse[data_set.timeframe]

            if time_step != ts:
                log.warning(
                    f'MKTS BUG: wrong timeframe loaded: {time_step}\n'
                    'YOUR DATABASE LIKELY CONTAINS BAD DATA FROM AN OLD BUG '
                    f'WIPING HISTORY FOR {ts}s'
                )
                await tractor.pause()
                # await self.delete_ts(fqme, timeframe)

                # try reading again..
                # return await self.read_ohlcv(
                #     fqme,
                #     timeframe,
                #     end,
                #     limit,
                # )

        return array

    async def delete_ts(
        self,
        key: str,
        timeframe: Union[int, str | None] = None,
        fmt: str = 'OHLCV',

    ) -> bool:

        client = self.client
        # syms = await client.list_symbols()
        # if key not in syms:
        #     raise KeyError(f'`{key}` table key not found in\n{syms}?')

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


ohlc_key_map = bidict({
    'Epoch': 'time',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
})


@acm
async def get_client(
    grpc_port: int = 5995,  # required
    host: str = 'localhost',

) -> MarketstoreClient:
    '''
    Load a ``anyio_marketstore`` grpc client connected
    to an existing ``marketstore`` server.

    '''
    async with open_marketstore_client(
        host or 'localhost',
        grpc_port,
    ) as client:
        yield MktsStorageClient(
            client,
            config={'host': host, 'port': grpc_port},
        )
