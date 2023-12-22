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
Console interface to UI components.

"""
import os
import click
import tractor

from ..cli import cli
from .. import watchlists as wl
from ..service import maybe_spawn_brokerd


_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')


def _kivy_import_hack():
    # Command line hacks to make it work.
    # See the pkg mod.
    from .kivy import kivy  # noqa


@cli.command()
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--rate', '-r', default=3, help='Quote rate limit')
@click.option('--test', '-t', help='Test quote stream file')
@click.option('--dhost', '-dh', default='127.0.0.1',
              help='Daemon host address to connect to')
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def monitor(config, rate, name, dhost, test, tl):
    '''
    Start a real-time watchlist UI

    '''
    # global opts
    brokermod = config['brokermods'][0]
    loglevel = config['loglevel']
    log = config['log']

    watchlist_from_file = wl.ensure_watchlists(_watchlists_data_path)
    watchlists = wl.merge_watchlist(watchlist_from_file, wl._builtins)
    tickers = watchlists[name]
    if not tickers:
        log.error(f"No symbols found for watchlist `{name}`?")
        return

    _kivy_import_hack()
    from .kivy.monitor import _async_main

    async def main():
        async with maybe_spawn_brokerd(
            brokername=brokermod.name,
            loglevel=loglevel
        ) as portal:
            # run app "main"
            await _async_main(
                name,
                portal,
                tickers,
                brokermod,
                rate,
                test=test,
            )

    tractor.run(
        main,
        name='monitor',
        loglevel=loglevel if tl else None,
        rpc_module_paths=['piker.ui.kivy.monitor'],
        debug_mode=True,
    )


@cli.command()
# @click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--date', '-d', help='Contracts expiry date')
@click.option('--test', '-t', help='Test quote stream file')
@click.option('--rate', '-r', default=1, help='Logging level')
@click.argument('symbol', required=True)
@click.pass_obj
def optschain(
    config,
    symbol,
    date,
    rate,
    test,
):
    '''
    Start an option chain UI

    '''
    # global opts
    loglevel = config['loglevel']
    brokername = config['broker']

    _kivy_import_hack()
    from .kivy.option_chain import _async_main

    async def main():
        async with maybe_spawn_brokerd(
            loglevel=loglevel
        ):
            # run app "main"
            await _async_main(
                symbol,
                brokername,
                rate=rate,
                loglevel=loglevel,
                test=test,
            )

    tractor.run(
        main,
        name='kivy-options-chain',
    )


@cli.command()
@click.option(
    '--profile',
    # '-p',
    default=None,
    help='Enable pyqtgraph profiling'
)
# @click.option(
#     '--pdb',
#     is_flag=True,
#     help='Enable tractor debug mode'
# )
@click.argument('symbols', nargs=-1, required=True)
# @click.pass_context
@click.pass_obj
def chart(
    config,
    # ctx: click.Context,
    symbols: list[str],
    profile,
):
    '''
    Run chart UI app, spawning service daemons dynamically as
    needed if not discovered via [network] config.

    '''
    # eg. ``--profile 3`` reports profiling for anything slower then 3 ms.
    if profile is not None:
        from .. import _profile
        _profile._pg_profile = True
        _profile.ms_slower_then = float(profile)

    # Qt UI entrypoint
    from ._app import _main

    for symbol in symbols:
        if '.' not in symbol:
            click.echo(click.style(
                f'symbol: {symbol} must have a {symbol}.<provider> suffix',
                fg='red',
            ))
            return

    # global opts
    brokernames = config['brokers']
    brokermods = config['brokermods']
    assert brokermods
    tractorloglevel = config['tractorloglevel']
    pikerloglevel = config['loglevel']

    maddrs: list[tuple[str, int]] = config.get(
        'maddrs',
        [],
    )

    # if maddrs:
    #     from tractor._multiaddr import parse_maddr
    #     for addr in maddrs:
    #         breakpoint()
    #         layers: dict = parse_maddr(addr)

    regaddrs: list[tuple[str, int]] = config.get(
        'registry_addrs',
        [],
    )

    from ..config import load
    conf, _ = load(
        conf_name='conf',
    )
    network: dict = conf.get('network')
    if network:
        from ..cli import load_trans_eps
        eps: dict = load_trans_eps(
            network,
            maddrs,
        )
        for layers in eps['pikerd']:
            regaddrs.append((
                layers['ipv4']['addr'],
                layers['tcp']['port'],
            ))

    from tractor.devx import maybe_open_crash_handler
    pdb: bool = config['pdb']
    with maybe_open_crash_handler(pdb=pdb):
        _main(
            syms=symbols,
            brokermods=brokermods,
            piker_loglevel=pikerloglevel,
            tractor_kwargs={
                'debug_mode': pdb,
                'loglevel': tractorloglevel,
                'name': 'chart',
                'registry_addrs': list(set(regaddrs)),
                'enable_modules': [

                    # remote data-view annotations Bo
                    'piker.ui._remote_ctl',
                ],
            },
        )
