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
from ..data import maybe_spawn_brokerd


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
    """Start a real-time watchlist UI
    """
    # global opts
    brokermod = config['brokermod']
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
                name, portal, tickers,
                brokermod, rate, test=test,
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
def optschain(config, symbol, date, rate, test):
    """Start an option chain UI
    """
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
    is_flag=True,
    help='Enable pyqtgraph profiling'
)
@click.argument('symbol', required=True)
@click.pass_obj
def chart(config, symbol, profile):
    """Start a real-time chartng UI
    """
    from .. import _profile
    from ._chart import _main

    # toggle to enable profiling
    _profile._pg_profile = profile

    # global opts
    brokername = config['broker']
    tractorloglevel = config['tractorloglevel']
    pikerloglevel = config['loglevel']

    _main(
        sym=symbol,
        brokername=brokername,
        piker_loglevel=pikerloglevel,
        tractor_kwargs={
            'debug_mode': True,
            'loglevel': tractorloglevel,
            'name': 'chart',
            'enable_modules': [
                'piker.clearing._client'
            ],
        },
    )
