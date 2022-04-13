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
from typing import List
from functools import partial
from pprint import pformat

import trio
import tractor
import click

from .marketstore import (
    get_client,
    stream_quotes,
    ingest_quote_stream,
    _url,
    _tick_tbk_ids,
    mk_tbk,
)
from ..cli import cli
from .. import watchlists as wl
from ..log import get_logger


log = get_logger(__name__)


@cli.command()
@click.option(
    '--url',
    default='ws://localhost:5993/ws',
    help='HTTP URL of marketstore instance'
)
@click.argument('names', nargs=-1)
@click.pass_obj
def ms_stream(config: dict, names: List[str], url: str):
    """Connect to a marketstore time bucket stream for (a set of) symbols(s)
    and print to console.
    """
    async def main():
        async for quote in stream_quotes(symbols=names):
            log.info(f"Received quote:\n{quote}")

    trio.run(main)


@cli.command()
@click.option(
    '--url',
    default=_url,
    help='HTTP URL of marketstore instance'
)
@click.argument('names', nargs=-1)
@click.pass_obj
def ms_destroy(config: dict, names: List[str], url: str) -> None:
    """Destroy symbol entries in the local marketstore instance.
    """
    async def main():
        nonlocal names
        async with get_client(url) as client:

            if not names:
                names = await client.list_symbols()

            # default is to wipe db entirely.
            answer = input(
                "This will entirely wipe you local marketstore db @ "
                f"{url} of the following symbols:\n {pformat(names)}"
                "\n\nDelete [N/y]?\n")

            if answer == 'y':
                for sym in names:
                    # tbk = _tick_tbk.format(sym)
                    tbk = tuple(sym, *_tick_tbk_ids)
                    print(f"Destroying {tbk}..")
                    await client.destroy(mk_tbk(tbk))
            else:
                print("Nothing deleted.")

    tractor.run(main)


@cli.command()
@click.option(
    '--tl',
    is_flag=True,
    help='Enable tractor logging')
@click.option(
    '--url',
    default=_url,
    help='HTTP URL of marketstore instance'
)
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def ms_shell(config, name, tl, url):
    """Start an IPython shell ready to query the local marketstore db.
    """
    async def main():
        async with get_client(url) as client:
            query = client.query  # noqa
            # TODO: write magics to query marketstore
            from IPython import embed
            embed()

    tractor.run(main)


@cli.command()
@click.option('--test-file', '-t', help='Test quote stream file')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option(
    '--url',
    default=_url,
    help='HTTP URL of marketstore instance'
)
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def ingest(config, name, test_file, tl, url):
    """Ingest real-time broker quotes and ticks to a marketstore instance.
    """
    # global opts
    brokermods = config['brokermods']
    loglevel = config['loglevel']
    tractorloglevel = config['tractorloglevel']
    # log = config['log']

    watchlist_from_file = wl.ensure_watchlists(config['wl_path'])
    watchlists = wl.merge_watchlist(watchlist_from_file, wl._builtins)
    symbols = watchlists[name]

    grouped_syms = {}
    for sym in symbols:
        symbol, _, provider = sym.rpartition('.')
        if provider not in grouped_syms:
            grouped_syms[provider] = []

        grouped_syms[provider].append(symbol)

    async def entry_point():
        async with tractor.open_nursery() as n:
            for provider, symbols in grouped_syms.items(): 
                await n.run_in_actor(
                    ingest_quote_stream,
                    name='ingest_marketstore',
                    symbols=symbols,
                    brokername=provider,
                    tries=1,
                    actorloglevel=loglevel,
                    loglevel=tractorloglevel
                )

    tractor.run(entry_point)
