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
Console interface to broker client/daemons.
"""
import os
from functools import partial
from operator import attrgetter
from operator import itemgetter

import click
import pandas as pd
import trio
import tractor

from .. import config as broker_conf
from ..cli import cli
from .. import watchlists as wl
from ..log import get_console_log, colorize_json, get_logger
from .._daemon import maybe_spawn_brokerd, maybe_open_pikerd
from ..brokers import core, get_brokermod, data

log = get_logger('cli')
DEFAULT_BROKER = 'questrade'

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')


@cli.command()
@click.option('--keys', '-k', multiple=True,
            help='Return results only for these keys')
@click.argument('meth', nargs=1)
@click.argument('kwargs', nargs=-1)
@click.pass_obj
def api(config, meth, kwargs, keys):
    """Make a broker-client API method call
    """
    # global opts
    broker = config['brokers'][0]

    _kwargs = {}
    for kwarg in kwargs:
        if '=' not in kwarg:
            log.error(f"kwarg `{kwarg}` must be of form <key>=<value>")
        else:
            key, _, value = kwarg.partition('=')
            _kwargs[key] = value

    data = trio.run(
        partial(core.api, broker, meth, **_kwargs)
    )

    if keys:
        # filter to requested keys
        filtered = []
        if meth in data:  # often a list of dicts
            for item in data[meth]:
                filtered.append({key: item[key] for key in keys})

        else:  # likely just a dict
            filtered.append({key: data[key] for key in keys})
        data = filtered

    click.echo(colorize_json(data))


@cli.command()
@click.option('--df-output', '-df', flag_value=True,
              help='Output in `pandas.DataFrame` format')
@click.argument('tickers', nargs=-1, required=True)
@click.pass_obj
def quote(config, tickers, df_output):
    """Print symbol quotes to the console
    """
    # global opts
    brokermod = config['brokermods'][0]

    quotes = trio.run(partial(core.stocks_quote, brokermod, tickers))
    if not quotes:
        log.error(f"No quotes could be found for {tickers}?")
        return

    if len(quotes) < len(tickers):
        syms = tuple(map(itemgetter('symbol'), quotes))
        for ticker in tickers:
            if ticker not in syms:
                brokermod.log.warn(f"Could not find symbol {ticker}?")

    if df_output:
        cols = next(filter(bool, quotes)).copy()
        cols.pop('symbol')
        df = pd.DataFrame(
            (quote or {} for quote in quotes),
            columns=cols,
        )
        click.echo(df)
    else:
        click.echo(colorize_json(quotes))


@cli.command()
@click.option('--df-output', '-df', flag_value=True,
              help='Output in `pandas.DataFrame` format')
@click.option('--count', '-c', default=1000,
              help='Number of bars to retrieve')
@click.argument('symbol', required=True)
@click.pass_obj
def bars(config, symbol, count, df_output):
    """Retreive 1m bars for symbol and print on the console
    """
    # global opts
    brokermod = config['brokermods'][0]

    # broker backend should return at the least a
    # list of candle dictionaries
    bars = trio.run(
        partial(
            core.bars,
            brokermod,
            symbol,
            count=count,
            as_np=df_output
        )
    )

    if not len(bars):
        log.error(f"No quotes could be found for {symbol}?")
        return

    if df_output:
        click.echo(pd.DataFrame(bars))
    else:
        click.echo(colorize_json(bars))


@cli.command()
@click.option('--rate', '-r', default=5, help='Logging level')
@click.option('--filename', '-f', default='quotestream.jsonstream',
              help='Logging level')
@click.option('--dhost', '-dh', default='127.0.0.1',
              help='Daemon host address to connect to')
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def record(config, rate, name, dhost, filename):
    """Record client side quotes to a file on disk
    """
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

    async def main(tries):
        async with maybe_spawn_brokerd(
            tries=tries, loglevel=loglevel
        ) as portal:
            # run app "main"
            return await data.stream_to_file(
                name, filename,
                portal, tickers,
                brokermod, rate,
            )

    filename = tractor.run(partial(main, tries=1), name='data-feed-recorder')
    click.echo(f"Data feed recording saved to {filename}")


# options utils

@cli.command()
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--ids', flag_value=True, help='Include numeric ids in output')
@click.argument('symbol', required=True)
@click.pass_context
def contracts(ctx, loglevel, broker, symbol, ids):
    """Get list of all option contracts for symbol
    """
    brokermod = get_brokermod(broker)
    get_console_log(loglevel)

    contracts = trio.run(partial(core.contracts, brokermod, symbol))
    if not ids:
        # just print out expiry dates which can be used with
        # the option_chain_quote cmd
        output = tuple(map(attrgetter('expiry'), contracts))
    else:
        output = tuple(contracts.items())

    # TODO: need a cli test to verify
    click.echo(colorize_json(output))


@cli.command()
@click.option('--df-output', '-df', flag_value=True,
              help='Output in `pandas.DataFrame` format')
@click.option('--date', '-d', help='Contracts expiry date')
@click.argument('symbol', required=True)
@click.pass_obj
def optsquote(config, symbol, df_output, date):
    """Retreive symbol option quotes on the console
    """
    # global opts
    brokermod = config['brokermods'][0]

    quotes = trio.run(
        partial(
            core.option_chain, brokermod, symbol, date
        )
    )
    if not quotes:
        log.error(f"No option quotes could be found for {symbol}?")
        return

    if df_output:
        df = pd.DataFrame(
            (quote.values() for quote in quotes),
            columns=quotes[0].keys(),
        )
        click.echo(df)
    else:
        click.echo(colorize_json(quotes))


@cli.command()
@click.argument('tickers', nargs=-1, required=True)
@click.pass_obj
def symbol_info(config, tickers):
    """Print symbol quotes to the console
    """
    # global opts
    brokermod = config['brokermods'][0]

    quotes = trio.run(partial(core.symbol_info, brokermod, tickers))
    if not quotes:
        log.error(f"No quotes could be found for {tickers}?")
        return

    if len(quotes) < len(tickers):
        syms = tuple(map(itemgetter('symbol'), quotes))
        for ticker in tickers:
            if ticker not in syms:
                brokermod.log.warn(f"Could not find symbol {ticker}?")

    click.echo(colorize_json(quotes))


@cli.command()
@click.argument('pattern', required=True)
@click.pass_obj
def search(config, pattern):
    """Search for symbols from broker backend(s).
    """
    # global opts
    brokermods = config['brokermods']

    # define tractor entrypoint
    async def main(func):

        async with maybe_open_pikerd(
            loglevel=config['loglevel'],
        ):
            return await func()

    quotes = trio.run(
        main,
        partial(
            core.symbol_search,
            brokermods,
            pattern,
        ),
    )

    if not quotes:
        log.error(f"No matches could be found for {pattern}?")
        return

    click.echo(colorize_json(quotes))


@cli.command()
@click.argument('section', required=False)
@click.argument('value', required=False)
@click.option('--delete', '-d', flag_value=True, help='Delete section')
@click.pass_obj
def brokercfg(config, section, value, delete):
    """If invoked with no arguments, open an editor to edit broker configs file
    or get / update an individual section.
    """

    if section:
        conf, path = broker_conf.load()

        if not delete:
            if value:
                broker_conf.set_value(conf, section, value)

            click.echo(
                colorize_json(
                    broker_conf.get_value(conf, section))
            )
        else:
            broker_conf.del_value(conf, section)

        broker_conf.write(config=conf)

    else:
        conf, path = broker_conf.load(raw=True)
        broker_conf.write(
            raw=click.edit(text=conf)
        )
