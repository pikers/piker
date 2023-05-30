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
from typing import TYPE_CHECKING
import trio
from rich.console import Console
# from rich.markdown import Markdown
import typer

from ..cli import cli
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
    from piker.service import open_piker_runtime
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

    resp = await client.delete_ts(fqme, timeframe)

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
    msgish = resp.ListFields()[0][1]
    if 'error' in str(msgish):
        log.error(
            f'Deletion error:\n'
            f'backend: {client.name}\n'
            f'fqme: {fqme}\n'
            f'timeframe: {timeframe}s\n'
            f'Error msg:\n\n{msgish}\n',
        )


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
    from piker.service import open_piker_runtime
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


typer_click_object = typer.main.get_command(store)
cli.add_command(typer_click_object, 'store')
