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
Storage middle-ware CLIs.

"""
from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pendulum
from rich.console import Console
import trio
# from rich.markdown import Markdown
import typer

from piker.service import open_piker_runtime
from piker.cli import cli
from . import (
    log,
)

if TYPE_CHECKING:
    from . import Storage


store = typer.Typer()


@store.command()
def ls(
    backends: list[str] = typer.Argument(
        default=None,
        help='Storage backends to query, default is all.'
    ),
):
    # from piker.service import open_piker_runtime
    from . import (
        __tsdbs__,
        open_storage_client,
    )
    from rich.table import Table

    if not backends:
        backends: list[str] = __tsdbs__

    table = Table(title=f'Table keys for backends {backends}:')
    console = Console()

    async def query_all():
        nonlocal backends

        async with (
            open_piker_runtime(
                'tsdb_storage',
                enable_modules=['piker.service._ahab'],
            ),
        ):
            for backend in backends:
                async with open_storage_client(name=backend) as (
                    mod,
                    client,
                ):
                    table.add_column(f'{mod.name} fqmes')
                    keys: list[str] = await client.list_keys()
                    for key in keys:
                        table.add_row(key)

            console.print(table)

    trio.run(query_all)


async def del_ts_by_timeframe(
    client: Storage,
    fqme: str,
    timeframe: int,

) -> None:

    path: Path = await client.delete_ts(fqme, timeframe)
    log.info(f'Deleted {path}')

    # TODO: encapsulate per backend errors..
    # - MEGA LOL, apparently the symbols don't
    # flush out until you refresh something or other
    # (maybe the WALFILE)... #lelandorlulzone, classic
    # alpaca(Rtm) design here ..
    # well, if we ever can make this work we
    # probably want to dogsplain the real reason
    # for the delete errurz..llululu
    # if fqme not in syms:
    #     log.error(f'Pair {fqme} dne in DB')
    # msgish = resp.ListFields()[0][1]
    # if 'error' in str(msgish):
    #     log.error(
    #         f'Deletion error:\n'
    #         f'backend: {client.name}\n'
    #         f'fqme: {fqme}\n'
    #         f'timeframe: {timeframe}s\n'
    #         f'Error msg:\n\n{msgish}\n',
    #     )


@store.command()
def delete(
    symbols: list[str],

    backend: str = typer.Option(
        default=None,
        help='Storage backend to update'
    ),

    # delete: bool = typer.Option(False, '-d'),
    # host: str = typer.Option(
    #     'localhost',
    #     '-h',
    # ),
    # port: int = typer.Option('5993', '-p'),
):
    '''
    Delete a storage backend's time series for (table) keys provided as
    ``symbols``.

    '''
    from . import open_storage_client

    async def main(symbols: list[str]):
        async with (
            open_piker_runtime(
                'tsdb_storage',
                enable_modules=['piker.service._ahab']
            ),
            open_storage_client(name=backend) as (_, storage),
            trio.open_nursery() as n,
        ):
            # spawn queries as tasks for max conc!
            for fqme in symbols:
                for tf in [1, 60]:
                    n.start_soon(
                        del_ts_by_timeframe,
                        storage,
                        fqme,
                        tf,
                    )

    trio.run(main, symbols)


@store.command()
def read(
    fqme: str,

    limit: int = int(800e3),
    # client_type: str = 'async',

) -> np.ndarray:

    # end: int | None = None
    # import tractor
    from .nativedb import get_client

    async def main():
        async with get_client() as client:
            syms: list[str] = await client.list_keys()

            (
                history,
                first_dt,
                last_dt,
            ) = await client.load(
                fqme,
                60,
            )
            assert first_dt < last_dt
            print(f'{fqme} SIZE -> {history.size}')
            breakpoint()
            # await tractor.breakpoint()

    trio.run(main)

    # if client_type == 'sync':
    #     import pymarketstore as pymkts
    #     cli = pymkts.Client()


    #     while end != 0:
    #         param = pymkts.Params(
    #             fqme,
    #             '1Min',
    #             'OHLCV',
    #             limit=limit,
    #             # limit_from_start=True,
    #             end=end,
    #         )
    #         if end is not None:
    #             breakpoint()
    #         reply = cli.query(param)
    #         ds: pymkts.results.DataSet = reply.first()
    #         array: np.ndarray = ds.array

    #         print(f'loaded {len(array)}-len array:\n{array}')

    #         times = array['Epoch']
    #         end: float = float(times[0])
    #         dt = pendulum.from_timestamp(end)
    #         # end: str = dt.isoformat('T')
    #         breakpoint()
    #         print(
    #             f'trying to load next {limit} datums frame starting @ {dt}'
    #         )
    # else:
    #     from anyio_marketstore import (  # noqa
    #         open_marketstore_client,
    #         MarketstoreClient,
    #         Params,
    #     )
    #     async def main():

    #         end: int | None = None

    #         async with open_marketstore_client(
    #             'localhost',
    #             5995,
    #         ) as client:

    #             while end != 0:
    #                 params = Params(
    #                     symbols=fqme,
    #                     # timeframe=tfstr,
    #                     timeframe='1Min',
    #                     attrgroup='OHLCV',
    #                     end=end,
    #                     # limit_from_start=True,

    #                     # TODO: figure the max limit here given the
    #                     # ``purepc`` msg size limit of purerpc: 33554432
    #                     limit=limit,
    #                 )

    #                 if end is not None:
    #                     breakpoint()
    #                 result = await client.query(params)
    #                 data_set = result.by_symbols()[fqme]
    #                 array = data_set.array
    #                 times = array['Epoch']
    #                 end: float = float(times[0])
    #                 dt = pendulum.from_timestamp(end)
    #                 breakpoint()
    #                 print(
    #                     f'trying to load next {limit} datums frame starting @ {dt}'
    #                 )

    #     trio.run(main)


@store.command()
def clone(
    fqme: str,
) -> None:
    import time
    from piker.config import get_conf_dir
    from piker.data import (
        maybe_open_shm_array,
        def_iohlcv_fields,
    )
    import polars as pl

    # open existing shm buffer for kucoin backend
    key: str = 'piker.brokerd[a9e7a4fe-39ae-44].btcusdt.binance.hist'
    shmpath: Path = Path('/dev/shm') / key
    assert shmpath.is_file()

    async def main():
        async with (
            open_piker_runtime(
                'polars_boi',
                enable_modules=['piker.data._sharedmem'],
            ),
        ):
            # attach to any shm buffer, load array into polars df,
            # write to local parquet file.
            shm, opened = maybe_open_shm_array(
                key=key,
                dtype=def_iohlcv_fields,
            )
            assert not opened
            ohlcv = shm.array

            start = time.time()

            # XXX: thanks to this SO answer for this conversion tip:
            # https://stackoverflow.com/a/72054819
            df = pl.DataFrame({
                field_name: ohlcv[field_name]
                for field_name in ohlcv.dtype.fields
            })
            delay: float = round(
                time.time() - start,
                ndigits=6,
            )
            print(
                f'numpy -> polars conversion took {delay} secs\n'
                f'polars df: {df}'
            )

            # compute ohlc properties for naming
            times: np.ndarray = ohlcv['time']
            secs: float = times[-1] - times[-2]
            if secs < 1.:
                breakpoint()
                raise ValueError(
                    f'Something is wrong with time period for {shm}:\n{ohlcv}'
                )

            timeframe: str = f'{secs}s'

            # write to parquet file
            datadir: Path = get_conf_dir() / 'parqdb'
            if not datadir.is_dir():
                datadir.mkdir()

            path: Path = datadir / f'{fqme}.{timeframe}.parquet'

            # write to fs
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

            # read back from fs
            start = time.time()
            read_df: pl.DataFrame = pl.read_parquet(path)
            delay: float = round(
                time.time() - start,
                ndigits=6,
            )
            print(
                f'parquet read took {delay} secs\n'
                f'polars df: {read_df}'
            )

    trio.run(main)


typer_click_object = typer.main.get_command(store)
cli.add_command(typer_click_object, 'store')
