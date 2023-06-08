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
`nativedb`: a lulzy Apache-parquet file manager (that some might
            call a poor man's tsdb).

AKA a `piker`-native file-system native "time series database"
without needing an extra process and no standard TSDB features, YET!

'''
# TODO: like there's soo much..
# - better name like "parkdb" or "nativedb" (lel)? bundle this lib with
#   others to make full system:
#   - tractor for failover and reliablity?
#   - borg for replication and sync?
#
# - use `fastparquet` for appends:
#   https://fastparquet.readthedocs.io/en/latest/api.html#fastparquet.write
#   (presuming it's actually faster then overwrites and
#   makes sense in terms of impl?)
#
# - use `polars` support for lazy scanning, processing and schema
#   validation?
#   - https://pola-rs.github.io/polars-book/user-guide/io/parquet/#scan
#   - https://pola-rs.github.io/polars-book/user-guide/concepts/lazy-vs-eager/
# - consider delta writes for appends?
#   - https://github.com/pola-rs/polars/blob/main/py-polars/polars/dataframe/frame.py#L3232
# - consider multi-file appends with appropriate time-range naming?
#   - https://pola-rs.github.io/polars-book/user-guide/io/multiple/
#
# - use `borg` for replication?
#   - https://borgbackup.readthedocs.io/en/stable/quickstart.html#remote-repositories
#   - https://github.com/borgbackup/borg
#   - https://borgbackup.readthedocs.io/en/stable/faq.html#usage-limitations
#   - https://github.com/borgbackup/community
#   - https://github.com/spslater/borgapi
#   - https://nixos.wiki/wiki/ZFS
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from datetime import datetime
from pathlib import Path
import time

# from bidict import bidict
# import tractor
import numpy as np
import polars as pl
from pendulum import (
    from_timestamp,
)

from piker import config
from piker.data import def_iohlcv_fields
from piker.data import ShmArray
from piker.log import get_logger
# from .._profile import Profiler


log = get_logger('storage.nativedb')


# NOTE: thanks to this SO answer for the below conversion routines
# to go from numpy struct-arrays to polars dataframes and back:
# https://stackoverflow.com/a/72054819
def np2pl(array: np.ndarray) -> pl.DataFrame:
    return pl.DataFrame({
        field_name: array[field_name]
        for field_name in array.dtype.fields
    })


def pl2np(
    df: pl.DataFrame,
    dtype: np.dtype,

) -> np.ndarray:

    # Create numpy struct array of the correct size and dtype
    # and loop through df columns to fill in array fields.
    array = np.empty(
        df.height,
        dtype,
    )
    for field, col in zip(
        dtype.fields,
        df.columns,
    ):
        array[field] = df.get_column(col).to_numpy()

    return array


def detect_period(shm: ShmArray) -> float:
    '''
    Attempt to detect the series time step sampling period
    in seconds.

    '''
    # TODO: detect sample rate helper?
    # calc ohlc sample period for naming
    ohlcv: np.ndarray = shm.array
    times: np.ndarray = ohlcv['time']
    period: float = times[-1] - times[-2]
    if period == 0:
        # maybe just last sample is borked?
        period: float = times[-2] - times[-3]

    return period


def mk_ohlcv_shm_keyed_filepath(
    fqme: str,
    period: float,  # ow known as the "timeframe"
    datadir: Path,

) -> str:

    if period < 1.:
        raise ValueError('Sample period should be >= 1.!?')

    period_s: str = f'{period}s'
    path: Path = datadir / f'{fqme}.ohlcv{period_s}.parquet'
    return path


ohlc_key_map = None


class NativeStorageClient:
    '''
    High level storage api for OHLCV time series stored in
    a (modern) filesystem as apache parquet files B)

    Part of a grander scheme to use arrow and parquet as our main
    lowlevel data framework: https://arrow.apache.org/faq/.

    '''
    name: str = 'nativedb'

    def __init__(
        self,
        datadir: Path,

    ) -> None:
        self._datadir = datadir
        self._index: dict[str, dict] = {}

        # series' cache from tsdb reads
        self._dfs: dict[str, dict[str, pl.DataFrame]] = {}

    @property
    def address(self) -> str:
        return self._datadir.as_uri()

    @property
    def cardinality(self) -> int:
        return len(self._index)

    # @property
    # def compression(self) -> str:
    #     ...

    async def list_keys(self) -> list[str]:
        return list(self._index)

    def index_files(self):
        for path in self._datadir.iterdir():
            if 'borked' in path.name:
                continue

            key: str = path.name.rstrip('.parquet')
            fqme, _, descr = key.rpartition('.')
            prefix, _, suffix = descr.partition('ohlcv')
            period: int = int(suffix.strip('s'))

            # cache description data
            self._index[fqme] = {
                'path': path,
                'period': period,
            }

        return self._index


    # async def search_keys(self, pattern: str) -> list[str]:
    #     '''
    #     Search for time series key in the storage backend.

    #     '''
    #     ...

    # async def write_ticks(self, ticks: list) -> None:
    #     ...

    async def load(
        self,
        fqme: str,
        timeframe: int,

    ) -> tuple[
        np.ndarray,  # timeframe sampled array-series
        datetime | None,  # first dt
        datetime | None,  # last dt
    ] | None:
        try:
            array: np.ndarray = await self.read_ohlcv(
                fqme,
                timeframe,
            )
        except FileNotFoundError:
            return None

        times = array['time']
        return (
            array,
            from_timestamp(times[0]),
            from_timestamp(times[-1]),
        )

    def mk_path(
        self,
        fqme: str,
        period: float,
    ) -> Path:
        return mk_ohlcv_shm_keyed_filepath(
            fqme=fqme,
            period=period,
            datadir=self._datadir,
        )

    async def read_ohlcv(
        self,
        fqme: str,
        timeframe: int | str,
        end: float | None = None,  # epoch or none
        # limit: int = int(200e3),

    ) -> np.ndarray:
        path: Path = self.mk_path(fqme, period=int(timeframe))
        df: pl.DataFrame = pl.read_parquet(path)
        self._dfs.setdefault(timeframe, {})[fqme] = df

        # TODO: filter by end and limit inputs
        # times: pl.Series = df['time']
        array: np.ndarray = pl2np(
            df,
            dtype=np.dtype(def_iohlcv_fields),
        )
        return array

    async def as_df(
        self,
        fqme: str,
        period: int = 60,

    ) -> pl.DataFrame:
        try:
            return self._dfs[period][fqme]
        except KeyError:
            await self.read_ohlcv(fqme, period)
            return self._dfs[period][fqme]

    def _write_ohlcv(
        self,
        fqme: str,
        ohlcv: np.ndarray | pl.DataFrame,
        timeframe: int,

    ) -> Path:
        '''
        Sync version of the public interface meth, since we don't
        currently actually need or support an async impl.

        '''
        path: Path = mk_ohlcv_shm_keyed_filepath(
            fqme=fqme,
            period=timeframe,
            datadir=self._datadir,
        )
        if isinstance(ohlcv, np.ndarray):
            df: pl.DataFrame = np2pl(ohlcv)
        else:
            df = ohlcv

        # TODO: in terms of managing the ultra long term data
        # - use a proper profiler to measure all this IO and
        #   roundtripping!
        # - try out ``fastparquet``'s append writing:
        # https://fastparquet.readthedocs.io/en/latest/api.html#fastparquet.write
        start = time.time()
        df.write_parquet(path)
        delay: float = round(
            time.time() - start,
            ndigits=6,
        )
        print(
            f'parquet write took {delay} secs\n'
            f'file path: {path}'
        )
        return path


    async def write_ohlcv(
        self,
        fqme: str,
        ohlcv: np.ndarray,
        timeframe: int,

    ) -> Path:
        '''
        Write input ohlcv time series for fqme and sampling period
        to (local) disk.

        '''
        return self._write_ohlcv(
            fqme,
            ohlcv,
            timeframe,
        )

    async def delete_ts(
        self,
        key: str,
        timeframe: int | None = None,

    ) -> bool:
        path: Path = mk_ohlcv_shm_keyed_filepath(
            fqme=key,
            period=timeframe,
            datadir=self._datadir,
        )
        if path.is_file():
            path.unlink()
            log.warning(f'Deleting parquet entry:\n{path}')
        else:
            log.warning(f'No path exists:\n{path}')

        return path


@acm
async def get_client(

    # TODO: eventually support something something apache arrow
    # transport over ssh something..?
    # host: str | None = None,

    **kwargs,

) -> NativeStorageClient:
    '''
    Load a ``anyio_marketstore`` grpc client connected
    to an existing ``marketstore`` server.

    '''
    datadir: Path = config.get_conf_dir() / 'nativedb'
    if not datadir.is_dir():
        log.info(f'Creating `nativedb` director: {datadir}')
        datadir.mkdir()

    client = NativeStorageClient(datadir)
    client.index_files()
    yield client
