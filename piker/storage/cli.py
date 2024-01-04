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
# from contextlib import (
#     AsyncExitStack,
# )
from pathlib import Path
from math import copysign
import time
from types import ModuleType
from typing import (
    Any,
    TYPE_CHECKING,
)

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
from piker.data import (
    ShmArray,
)
from piker import tsp
from piker.data._formatters import BGM
from . import log
from . import (
    __tsdbs__,
    open_storage_client,
    StorageClient,
)

if TYPE_CHECKING:
    from piker.ui._remote_ctl import AnnotCtl


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
                deduped,
                diff,
            ) = tsp.dedupe(
                shm_df,
                period=period,
            )

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
                assert not null_segs

    trio.run(main)


async def markup_gaps(
    fqme: str,
    timeframe: float,
    actl: AnnotCtl,
    wdts: pl.DataFrame,
    gaps: pl.DataFrame,

) -> dict[int, dict]:
    '''
    Remote annotate time-gaps in a dt-fielded ts (normally OHLC)
    with rectangles.

    '''
    aids: dict[int] = {}
    for i in range(gaps.height):

        row: pl.DataFrame = gaps[i]

        # the gap's RIGHT-most bar's OPEN value
        # at that time (sample) step.
        iend: int = row['index'][0]
        # dt: datetime = row['dt'][0]
        # dt_prev: datetime = row['dt_prev'][0]
        # dt_end_t: float = dt.timestamp()


        # TODO: can we eventually remove this
        # once we figure out why the epoch cols
        # don't match?
        # TODO: FIX HOW/WHY these aren't matching
        # and are instead off by 4hours (EST
        # vs. UTC?!?!)
        # end_t: float = row['time']
        # assert (
        #     dt.timestamp()
        #     ==
        #     end_t
        # )

        # the gap's LEFT-most bar's CLOSE value
        # at that time (sample) step.
        prev_r: pl.DataFrame = wdts.filter(
            pl.col('index') == iend - 1
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
        # gap_w: float = abs((iend - istart))
        # if gap_w < 6:
        #     margin: float = 6
        #     iend += margin
        #     istart -= margin

        rect_gap: float = BGM*3/8
        opn: float = row['open'][0]
        ro: tuple[float, float] = (
            # dt_end_t,
            iend + rect_gap + 1,
            opn,
        )
        cls: float = prev_r['close'][0]
        lc: tuple[float, float] = (
            # dt_start_t,
            istart - rect_gap, # + 1 ,
            cls,
        )

        color: str = 'dad_blue'
        diff: float = cls - opn
        sgn: float = copysign(1, diff)
        color: str = {
            -1: 'buy_green',
            1: 'sell_red',
        }[sgn]

        rect_kwargs: dict[str, Any] = dict(
            fqme=fqme,
            timeframe=timeframe,
            start_pos=lc,
            end_pos=ro,
            color=color,
        )

        aid: int = await actl.add_rect(**rect_kwargs)
        assert aid
        aids[aid] = rect_kwargs

    # tell chart to redraw all its
    # graphics view layers Bo
    await actl.redraw(
        fqme=fqme,
        timeframe=timeframe,
    )
    return aids


@store.command()
def ldshm(
    fqme: str,
    write_parquet: bool = True,
    reload_parquet_to_shm: bool = True,

) -> None:
    '''
    Linux ONLY: load any fqme file name matching shm buffer from
    /dev/shm/ into an OHLCV numpy array and polars DataFrame,
    optionally write to offline storage via `.parquet` file.

    '''
    async def main():
        from piker.ui._remote_ctl import (
            open_annot_ctl,
        )
        actl: AnnotCtl
        mod: ModuleType
        client: StorageClient
        async with (
            open_piker_runtime(
                'polars_boi',
                enable_modules=['piker.data._sharedmem'],
                debug_mode=True,
            ),
            open_storage_client() as (
                mod,
                client,
            ),
            open_annot_ctl() as actl,
        ):
            shm_df: pl.DataFrame | None = None
            for (
                shmfile,
                shm,
                # parquet_path,
                shm_df,
            ) in tsp.iter_dfs_from_shms(fqme):

                times: np.ndarray = shm.array['time']
                d1: float = float(times[-1] - times[-2])
                d2: float = float(times[-2] - times[-3])
                med: float = np.median(np.diff(times))
                if (
                    d1 < 1.
                    and d2 < 1.
                    and med < 1.
                ):
                    raise ValueError(
                        f'Something is wrong with time period for {shm}:\n{times}'
                    )

                period_s: float = float(max(d1, d2, med))

                # over-write back to shm?
                wdts: pl.DataFrame  # with dts
                deduped: pl.DataFrame  # deduplicated dts
                (
                    wdts,
                    deduped,
                    diff,
                ) = tsp.dedupe(
                    shm_df,
                    period=period_s,
                )

                null_segs: tuple = tsp.get_null_segs(
                    frame=shm.array,
                    period=period_s,
                )

                # detect gaps from in expected (uniform OHLC) sample period
                step_gaps: pl.DataFrame = tsp.detect_time_gaps(
                    wdts,
                    expect_period=period_s,
                )

                # TODO: by default we always want to mark these up
                # with rects showing up/down gaps Bo
                venue_gaps: pl.DataFrame = tsp.detect_time_gaps(
                    wdts,
                    expect_period=period_s,

                    # TODO: actually pull the exact duration
                    # expected for each venue operational period?
                    gap_dt_unit='days',
                    gap_thresh=1,
                )

                # TODO: call null-seg fixer somehow?
                if null_segs:
                    await tractor.pause()
                #     async with (
                #         trio.open_nursery() as tn,
                #         mod.open_history_client(
                #             mkt,
                #         ) as (get_hist, config),
                #     ):
                #         nulls_detected: trio.Event = await tn.start(partial(
                #             tsp.maybe_fill_null_segments,

                #             shm=shm,
                #             timeframe=timeframe,
                #             get_hist=get_hist,
                #             sampler_stream=sampler_stream,
                #             mkt=mkt,
                #         ))

                # TODO: find the disjoint set of step gaps from
                # venue (closure) set!
                # -[ ] do a set diff by checking for the unique
                #    gap set only in the step_gaps?
                if (
                    not venue_gaps.is_empty()
                    # and not step_gaps.is_empty()
                ):
                    do_markup_gaps: bool = True
                    if do_markup_gaps:
                        aids: dict = await markup_gaps(
                            fqme,
                            period_s,
                            actl,
                            wdts,
                            step_gaps,
                        )
                        assert aids

                    # write repaired ts to parquet-file?
                    if write_parquet:
                        start: float = time.time()
                        path: Path = await client.write_ohlcv(
                            fqme,
                            ohlcv=deduped,
                            timeframe=period_s,
                        )
                        write_delay: float = round(
                            time.time() - start,
                            ndigits=6,
                        )

                        # read back from fs
                        start: float = time.time()
                        read_df: pl.DataFrame = pl.read_parquet(path)
                        read_delay: float = round(
                            time.time() - start,
                            ndigits=6,
                        )
                        log.info(
                            f'parquet write took {write_delay} secs\n'
                            f'file path: {path}'
                            f'parquet read took {read_delay} secs\n'
                            f'polars df: {read_df}'
                        )

                        if reload_parquet_to_shm:
                            new = tsp.pl2np(
                                deduped,
                                dtype=shm.array.dtype,
                            )
                            # since normally readonly
                            shm._array.setflags(
                                write=int(1),
                            )
                            # last chance manual overwrites in REPL
                            await tractor.pause()
                            shm.push(
                                new,
                                prepend=True,
                                start=new['index'][-1],
                                update_first=False,  # don't update ._first
                            )


                else:
                    # allow interaction even when no ts problems.
                    await tractor.pause()
                    assert not diff


            if shm_df is None:
                log.error(
                    f'No matching shm buffers for {fqme} ?'

                )

    trio.run(main)


typer_click_object = typer.main.get_command(store)
cli.add_command(typer_click_object, 'store')
