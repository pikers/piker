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
marketstore cli.

"""
from __future__ import annotations
from typing import TYPE_CHECKING
# import tractor
import trio
# import click
from rich.console import Console
# from rich.markdown import Markdown
import typer

from ..service.marketstore import (
    # get_client,
    # stream_quotes,
    ingest_quote_stream,
    # _url,
    # _tick_tbk_ids,
    # mk_tbk,
)
from ..cli import cli
from .. import watchlists as wl
from . import (
    log,
)


if TYPE_CHECKING:
    from . import Storage

store = typer.Typer()

# @cli.command()
# @click.option(
#     '--url',
#     default='ws://localhost:5993/ws',
#     help='HTTP URL of marketstore instance'
# )
# @click.argument('names', nargs=-1)
# @click.pass_obj
# def ms_stream(
#     config: dict,
#     names: list[str],
#     url: str,
# ) -> None:
#     '''
#     Connect to a marketstore time bucket stream for (a set of) symbols(s)
#     and print to console.

#     '''
#     async def main():
#         # async for quote in stream_quotes(symbols=names):
#         #    log.info(f"Received quote:\n{quote}")
#         ...

#     trio.run(main)


# @cli.command()
# @click.option(
#     '--url',
#     default=_url,
#     help='HTTP URL of marketstore instance'
# )
# @click.argument('names', nargs=-1)
# @click.pass_obj
# def ms_destroy(config: dict, names: list[str], url: str) -> None:
#     """Destroy symbol entries in the local marketstore instance.
#     """
#     async def main():
#         nonlocal names
#         async with get_client(url) as client:
#
#             if not names:
#                 names = await client.list_symbols()
#
#             # default is to wipe db entirely.
#             answer = input(
#                 "This will entirely wipe you local marketstore db @ "
#                 f"{url} of the following symbols:\n {pformat(names)}"
#                 "\n\nDelete [N/y]?\n")
#
#             if answer == 'y':
#                 for sym in names:
#                     # tbk = _tick_tbk.format(sym)
#                     tbk = tuple(sym, *_tick_tbk_ids)
#                     print(f"Destroying {tbk}..")
#                     await client.destroy(mk_tbk(tbk))
#             else:
#                 print("Nothing deleted.")
#
#     tractor.run(main)


# @cli.command()
# @click.option(
#     '--tsdb_host',
#     default='localhost'
# )
# @click.option(
#     '--tsdb_port',
#     default=5993
# )
# @click.argument('symbols', nargs=-1)
# @click.pass_obj
# def storesh(
#     config,
#     tl,
#     host,
#     port,
#     symbols: list[str],
# ):
#     '''
#     Start an IPython shell ready to query the local marketstore db.

#     '''
#     from piker.storage import open_tsdb_client
#     from piker.service import open_piker_runtime

#     async def main():
#         nonlocal symbols

#         async with open_piker_runtime(
#             'storesh',
#             enable_modules=['piker.service._ahab'],
#         ):
#             symbol = symbols[0]

#             async with open_tsdb_client(symbol):
#                 # TODO: ask if user wants to write history for detected
#                 # available shm buffers?
#                 from tractor.trionics import ipython_embed
#                 await ipython_embed()

#     trio.run(main)


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

# @cli.command()
# @click.option('--test-file', '-t', help='Test quote stream file')
# @click.option('--tl', is_flag=True, help='Enable tractor logging')
# @click.argument('name', nargs=1, required=True)
# @click.pass_obj
# def ingest(config, name, test_file, tl):
#     '''
#     Ingest real-time broker quotes and ticks to a marketstore instance.

#     '''
#     # global opts
#     loglevel = config['loglevel']
#     tractorloglevel = config['tractorloglevel']
#     # log = config['log']

#     watchlist_from_file = wl.ensure_watchlists(config['wl_path'])
#     watchlists = wl.merge_watchlist(watchlist_from_file, wl._builtins)
#     symbols = watchlists[name]

#     grouped_syms = {}
#     for sym in symbols:
#         symbol, _, provider = sym.rpartition('.')
#         if provider not in grouped_syms:
#             grouped_syms[provider] = []

#         grouped_syms[provider].append(symbol)

#     async def entry_point():
#         async with tractor.open_nursery() as n:
#             for provider, symbols in grouped_syms.items():
#                 await n.run_in_actor(
#                     ingest_quote_stream,
#                     name='ingest_marketstore',
#                     symbols=symbols,
#                     brokername=provider,
#                     tries=1,
#                     actorloglevel=loglevel,
#                     loglevel=tractorloglevel
#                 )

#     tractor.run(entry_point)

# if __name__ == "__main__":
#     store()  # this is called from ``>> ledger <accountname>``
