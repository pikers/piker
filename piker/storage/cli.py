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
# from typing import TYPE_CHECKING

import polars as pl
import numpy as np
# import pendulum
from rich.console import Console
import trio
# from rich.markdown import Markdown
import typer

from piker.service import open_piker_runtime
from piker.cli import cli
from . import (
    log,
)
from . import (
    __tsdbs__,
    open_storage_client,
)


store = typer.Typer()


@store.command()
def ls(
    backends: list[str] = typer.Argument(
        default=None,
        help='Storage backends to query, default is all.'
    ),
):
    from rich.table import Table

    if not backends:
        backends: list[str] = __tsdbs__

    console = Console()

    async def query_all():
        nonlocal backends

        async with (
            open_piker_runtime(
                'tsdb_storage',
                enable_modules=['piker.service._ahab'],
            ),
        ):
            for i, backend in enumerate(backends):
                table = Table()
                try:
                    async with open_storage_client(backend=backend) as (
                        mod,
                        client,
                    ):
                        table.add_column(f'{mod.name}@{client.address}')
                        keys: list[str] = await client.list_keys()
                        for key in keys:
                            table.add_row(key)

                    console.print(table)
                except Exception:
                    log.error(f'Unable to connect to storage engine: `{backend}`')

    trio.run(query_all)


@store.command()
def delete(
    symbols: list[str],

    backend: str = typer.Option(
        default=None,
        help='Storage backend to update'
    ),
    # TODO: expose this as flagged multi-option?
    timeframes: list[int] = [1, 60],
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
            open_storage_client(backend) as (_, client),
            trio.open_nursery() as n,
        ):
            # spawn queries as tasks for max conc!
            for fqme in symbols:
                for tf in timeframes:
                    n.start_soon(
                        client.delete_ts,
                        fqme,
                        tf,
                    )

    trio.run(main, symbols)


@store.command()
def anal(
    fqme: str,
    period: int = 60,

) -> np.ndarray:

    # import tractor

    async def main():
        async with open_storage_client() as (mod, client):
            syms: list[str] = await client.list_keys()
            print(f'{len(syms)} FOUND for {mod.name}')

            (
                history,
                first_dt,
                last_dt,
            ) = await client.load(
                fqme,
                period,
            )
            assert first_dt < last_dt

            src_df = await client.as_df(fqme, period)
            df = mod.with_dts(src_df)
            gaps: pl.DataFrame = mod.detect_time_gaps(df)

            # TODO: something better with tab completion..
            # is there something more minimal but nearly as
            # functional as ipython?
            import code
            code.interact(
                f'df: {df}\ngaps: {gaps}\n',
                local=locals()
            )

    trio.run(main)


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

    # TODO: actually look up an existing shm buf (set) from
    # an fqme and file name parsing..
    # open existing shm buffer for kucoin backend
    key: str = 'piker.brokerd[3595d316-3c15-46].xmrusdt.kucoin.hist'
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
