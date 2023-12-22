# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of pikers)

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
# from datetime import datetime
from pathlib import Path
import time

import polars as pl
import numpy as np
import tractor
# import pendulum
from rich.console import Console
import trio
# from rich.markdown import Markdown
import typer

from piker.service import open_piker_runtime
from piker.cli import cli
from piker.config import get_conf_dir
from piker.data import (
    ShmArray,
)
from piker import tsp
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


# TODO: like ls but takes in a pattern and matches
# @store.command()
# def search(
#     patt: str,
#     backends: list[str] = typer.Argument(
#         default=None,
#         help='Storage backends to query, default is all.'
#     ),
# ):
#     ...


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
    pdb: bool = False,

) -> np.ndarray:
    '''
    Anal-ysis is when you take the data do stuff to it.

    NOTE: This ONLY loads the offline timeseries data (by default
    from a parquet file) NOT the in-shm version you might be seeing
    in a chart.

    '''
    async def main():
        async with (
            open_piker_runtime(
                # are you a bear or boi?
                'tsdb_polars_anal',
                debug_mode=pdb,
            ),
            open_storage_client() as (
                mod,
                client,
            ),
        ):
            syms: list[str] = await client.list_keys()
            log.info(f'{len(syms)} FOUND for {mod.name}')

            history: ShmArray  # np buffer format
            (
                history,
                first_dt,
                last_dt,
            ) = await client.load(
                fqme,
                period,
            )
            assert first_dt < last_dt

            null_segs: tuple = tsp.get_null_segs(
                frame=history,
                period=period,
            )
            # TODO: do tsp queries to backcend to fill i missing
            # history and then prolly write it to tsdb!

            shm_df: pl.DataFrame = await client.as_df(
                fqme,
                period,
            )

            df: pl.DataFrame  # with dts
            deduped: pl.DataFrame  # deduplicated dts
            (
                df,
                gaps,
                deduped,
                diff,
            ) = tsp.dedupe(shm_df)

            write_edits: bool = True
            if (
                write_edits
                and (
                    diff
                    or null_segs
                )
            ):
                await tractor.pause()

                await client.write_ohlcv(
                    fqme,
                    ohlcv=deduped,
                    timeframe=period,
                )

            else:
                # TODO: something better with tab completion..
                # is there something more minimal but nearly as
                # functional as ipython?
                await tractor.pause()

    trio.run(main)


@store.command()
def ldshm(
    fqme: str,
    write_parquet: bool = False,

) -> None:
    '''
    Linux ONLY: load any fqme file name matching shm buffer from
    /dev/shm/ into an OHLCV numpy array and polars DataFrame,
    optionally write to offline storage via `.parquet` file.

    '''
    async def main():
        async with (
            open_piker_runtime(
                'polars_boi',
                enable_modules=['piker.data._sharedmem'],
                debug_mode=True,
            ),
        ):
            df: pl.DataFrame | None = None
            for shmfile, shm, shm_df in tsp.iter_dfs_from_shms(fqme):

                # compute ohlc properties for naming
                times: np.ndarray = shm.array['time']
                period_s: float = float(times[-1] - times[-2])
                if period_s < 1.:
                    raise ValueError(
                        f'Something is wrong with time period for {shm}:\n{times}'
                    )

                # over-write back to shm?
                df: pl.DataFrame  # with dts
                deduped: pl.DataFrame  # deduplicated dts
                (
                    df,
                    gaps,
                    deduped,
                    diff,
                ) = tsp.dedupe(shm_df)

                null_segs: tuple = tsp.get_null_segs(
                    frame=shm.array,
                    period=period_s,
                )

                # TODO: maybe only optionally enter this depending
                # on some CLI flags and/or gap detection?
                if (
                    not gaps.is_empty()
                    or null_segs
                ):
                    from piker.ui._remote_ctl import (
                        open_annot_ctl,
                        AnnotCtl,
                    )
                    annot_ctl: AnnotCtl
                    async with open_annot_ctl() as annot_ctl:
                        for i in range(gaps.height):

                            row: pl.DataFrame = gaps[i]

                            # TODO: can we eventually remove this
                            # once we figure out why the epoch cols
                            # don't match?
                            iend: int = row['index'][0]
                            # dt: datetime = row['dt'][0]
                            # dt_prev: datetime = row['dt_prev'][0]

                            # the gap's right-most bar's OPEN value
                            # at that time (sample) step.
                            # dt_end_t: float = dt.timestamp()

                            # TODO: FIX HOW/WHY these aren't matching
                            # and are instead off by 4hours (EST
                            # vs. UTC?!?!)
                            # end_t: float = row['time']
                            # assert (
                            #     dt.timestamp()
                            #     ==
                            #     end_t
                            # )

                            # the gap's left-most bar's CLOSE value
                            # at that time (sample) step.

                            prev_r: pl.DataFrame = df.filter(
                                pl.col('index') == gaps[0]['index'] - 1
                            )
                            istart: int = prev_r['index'][0]
                            # dt_start_t: float = dt_prev.timestamp()

                            # start_t: float = prev_r['time']
                            # assert (
                            #     dt_start_t
                            #     ==
                            #     start_t
                            # )

                            # TODO: implement px-col width measure
                            # and ensure at least as many px-cols
                            # shown per rect as configured by user.
                            gap_w: float = abs((iend - istart))
                            # await tractor.pause()
                            if gap_w < 6:
                                margin: float = 6
                                iend += margin
                                istart -= margin

                            ro: tuple[float, float] = (
                                # dt_end_t,
                                iend,
                                row['open'][0],
                            )
                            lc: tuple[float, float] = (
                                # dt_start_t,
                                istart,
                                prev_r['close'][0],
                            )

                            aid: int = await annot_ctl.add_rect(
                                fqme=fqme,
                                timeframe=period_s,
                                start_pos=lc,
                                end_pos=ro,
                            )
                            assert aid
                            await tractor.pause()

                # write to parquet file?
                if write_parquet:
                    timeframe: str = f'{period_s}s'

                    datadir: Path = get_conf_dir() / 'nativedb'
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
                    log.info(
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

            if df is None:
                log.error(f'No matching shm buffers for {fqme} ?')

    trio.run(main)


typer_click_object = typer.main.get_command(store)
cli.add_command(typer_click_object, 'store')
