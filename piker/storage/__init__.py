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
from abc import abstractmethod
from contextlib import asynccontextmanager as acm
from functools import partial
from importlib import import_module
from datetime import datetime
from types import ModuleType
from typing import (
    # Callable,
    # Awaitable,
    # Any,
    # AsyncIterator,
    Protocol,
    # Generic,
    # TypeVar,
)

import numpy as np


from .. import config
from ..service import (
    check_for_service,
)
from ..log import (
    get_logger,
    get_console_log,
)
subsys: str = 'piker.storage'

log = get_logger(subsys)
get_console_log = partial(
    get_console_log,
    name=subsys,
)


__tsdbs__: list[str] = [
    'nativedb',
    # 'marketstore',
]


class StorageClient(
    Protocol,
):
    '''
    Api description that all storage backends must implement
    in order to suffice the historical data mgmt layer.

    '''
    name: str

    @abstractmethod
    async def list_keys(self) -> list[str]:
        ...

    @abstractmethod
    def search_keys(self) -> list[str]:
        ...

    # @abstractmethod
    # async def write_ticks(
    #     self,
    #     ticks: list,
    # ) -> ReceiveType:
    #     ...

    # ``trio.abc.AsyncResource`` methods
    @abstractmethod
    async def load(
        self,
        fqme: str,
        timeframe: int,

    ) -> tuple[
        np.ndarray,  # timeframe sampled array-series
        datetime | None,  # first dt
        datetime | None,  # last dt
    ]:
        ...

    @abstractmethod
    async def delete_ts(
        self,
        key: str,
        timeframe: int | str | None = None,
        fmt: str = 'OHLCV',

    ) -> bool:
        ...

    @abstractmethod
    async def read_ohlcv(
        self,
        fqme: str,
        timeframe: int | str,
        end: int | None = None,
        limit: int = int(800e3),

    ) -> np.ndarray:
        ...

    async def write_ohlcv(
        self,
        fqme: str,
        ohlcv: np.ndarray,
        timeframe: int,
        append_and_duplicate: bool = True,
        limit: int = int(800e3),

    ) -> None:
        ...


class TimeseriesNotFound(Exception):
    '''
    No timeseries entry can be found for this backend.

    '''


class StorageConnectionError(ConnectionError):
    '''
    Can't connect to the desired tsdb subsys/service.

    '''

def get_storagemod(name: str) -> ModuleType:
    mod: ModuleType = import_module(
        '.' + name,
        'piker.storage',
    )

    # we only allow monkeying because it's for internal keying
    mod.name = mod.__name__.split('.')[-1]
    return mod


@acm
async def open_storage_client(
    backend: str | None = None,

) -> tuple[ModuleType, StorageClient]:
    '''
    Load the ``StorageClient`` for named backend.

    '''
    def_backend: str = 'nativedb'
    tsdb_host: str = 'localhost'

    # load root config and any tsdb user defined settings
    conf, path = config.load(
        conf_name='conf',
        touch_if_dne=True,
    )

    # TODO: maybe not under a "network" section.. since
    # no more chitty `marketstore`..
    tsdbconf: dict = {}
    service_section = conf.get('service')
    if (
        not backend
        and service_section
    ):
        tsdbconf = service_section.get('tsdb')

        # lookup backend tsdb module by name and load any user service
        # settings for connecting to the tsdb service.
        backend: str = tsdbconf.pop(
            'name',
            def_backend,
        )
        tsdb_host: str = tsdbconf.get('maddrs', [])

    if backend is None:
        backend: str = def_backend

    # import and load storagemod by name
    mod: ModuleType = get_storagemod(backend)
    get_client = mod.get_client

    log.info('Scanning for existing `{tsbd_backend}`')
    if backend != def_backend:
        tsdb_is_up: bool = await check_for_service(f'{backend}d')
        if (
            tsdb_host == 'localhost'
            or tsdb_is_up
        ):
            log.info(f'Connecting to local: {backend}@{tsdbconf}')
        else:
            log.info(f'Attempting to connect to remote: {backend}@{tsdbconf}')
    else:
        log.info(f'Connecting to default storage: {backend}@{tsdbconf}')

    async with (
        get_client(**tsdbconf) as client,
    ):
        # slap on our wrapper api
        yield mod, client


# NOTE: pretty sure right now this is only being
# called by a CLI entrypoint?
@acm
async def open_tsdb_client(
    fqme: str,
) -> StorageClient:

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
    from ..toolz import Profiler
    profiler = Profiler(
        disabled=True,  # not pg_profile_enabled(),
        delayed=False,
    )
    from ..data.feed import maybe_open_feed

    async with (
        open_storage_client() as (_, storage),

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
