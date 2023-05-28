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
import trio
import tractor
import click

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
from ._util import (
    log,
)


@cli.command()
@click.option(
    '--url',
    default='ws://localhost:5993/ws',
    help='HTTP URL of marketstore instance'
)
@click.argument('names', nargs=-1)
@click.pass_obj
def ms_stream(
    config: dict,
    names: list[str],
    url: str,
) -> None:
    '''
    Connect to a marketstore time bucket stream for (a set of) symbols(s)
    and print to console.

    '''
    async def main():
        # async for quote in stream_quotes(symbols=names):
        #    log.info(f"Received quote:\n{quote}")
        ...

    trio.run(main)


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


@cli.command()
@click.option(
    '--tsdb_host',
    default='localhost'
)
@click.option(
    '--tsdb_port',
    default=5993
)
@click.argument('symbols', nargs=-1)
@click.pass_obj
def storesh(
    config,
    tl,
    host,
    port,
    symbols: list[str],
):
    '''
    Start an IPython shell ready to query the local marketstore db.

    '''
    from piker.data.marketstore import open_tsdb_client
    from piker.service import open_piker_runtime

    async def main():
        nonlocal symbols

        async with open_piker_runtime(
            'storesh',
            enable_modules=['piker.service._ahab'],
        ):
            symbol = symbols[0]

            async with open_tsdb_client(symbol):
                # TODO: ask if user wants to write history for detected
                # available shm buffers?
                from tractor.trionics import ipython_embed
                await ipython_embed()

    trio.run(main)


@cli.command()
@click.option(
    '--host',
    default='localhost'
)
@click.option(
    '--port',
    default=5993
)
@click.option(
    '--delete',
    '-d',
    is_flag=True,
    help='Delete history (1 Min) for symbol(s)',
)
@click.argument('symbols', nargs=-1)
@click.pass_obj
def storage(
    config,
    host,
    port,
    symbols: list[str],
    delete: bool,

):
    '''
    Start an IPython shell ready to query the local marketstore db.

    '''
    from piker.service.marketstore import open_tsdb_client
    from piker.service import open_piker_runtime

    async def main():
        nonlocal symbols

        async with open_piker_runtime(
            'tsdb_storage',
            enable_modules=['piker.service._ahab'],
        ):
            symbol = symbols[0]
            async with open_tsdb_client(symbol) as storage:
                if delete:
                    for fqme in symbols:
                        syms = await storage.client.list_symbols()

                        resp60s = await storage.delete_ts(fqme, 60)

                        msgish = resp60s.ListFields()[0][1]
                        if 'error' in str(msgish):

                            # TODO: MEGA LOL, apparently the symbols don't
                            # flush out until you refresh something or other
                            # (maybe the WALFILE)... #lelandorlulzone, classic
                            # alpaca(Rtm) design here ..
                            # well, if we ever can make this work we
                            # probably want to dogsplain the real reason
                            # for the delete errurz..llululu
                            if fqme not in syms:
                                log.error(f'Pair {fqme} dne in DB')

                            log.error(f'Deletion error: {fqme}\n{msgish}')

                        resp1s = await storage.delete_ts(fqme, 1)
                        msgish = resp1s.ListFields()[0][1]
                        if 'error' in str(msgish):
                            log.error(f'Deletion error: {fqme}\n{msgish}')

    trio.run(main)


@cli.command()
@click.option('--test-file', '-t', help='Test quote stream file')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def ingest(config, name, test_file, tl):
    '''
    Ingest real-time broker quotes and ticks to a marketstore instance.

    '''
    # global opts
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
