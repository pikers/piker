"""
Console interface to broker client/daemons.
"""
from functools import partial
import json
import os
from operator import attrgetter
from operator import itemgetter

import click
import pandas as pd
import trio
import tractor

from . import watchlists as wl
from .log import get_console_log, colorize_json, get_logger
from .brokers import core, get_brokermod, data, config
from .brokers.core import maybe_spawn_brokerd_as_subactor, _data_mods

log = get_logger('cli')
DEFAULT_BROKER = 'questrade'

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')
_context_defaults = dict(
    default_map={
        'monitor': {
            'rate': 3,
        },
        'optschain': {
            'rate': 1,
        },
    }
)


@click.command()
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--host', '-h', default='127.0.0.1', help='Host address to bind')
def pikerd(loglevel, host, tl):
    """Spawn the piker broker-daemon.
    """
    get_console_log(loglevel)
    tractor.run_daemon(
        rpc_module_paths=_data_mods,
        name='brokerd',
        loglevel=loglevel if tl else None,
    )


@click.group(context_settings=_context_defaults)
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--configdir', '-c', help='Configuration directory')
@click.pass_context
def cli(ctx, broker, loglevel, configdir):
    if configdir is not None:
        assert os.path.isdir(configdir), f"`{configdir}` is not a valid path"
        config._override_config_dir(configdir)

    # ensure that ctx.obj exists even though we aren't using it (yet)
    ctx.ensure_object(dict)
    ctx.obj.update({
        'broker': broker,
        'brokermod': get_brokermod(broker),
        'loglevel': loglevel,
        'log': get_console_log(loglevel),
    })


@cli.command()
@click.option('--keys', '-k', multiple=True,
              help='Return results only for these keys')
@click.argument('meth', nargs=1)
@click.argument('kwargs', nargs=-1)
@click.pass_obj
def api(config, meth, kwargs, keys):
    """client for testing broker API methods with pretty printing of output.
    """
    # global opts
    broker = config['broker']

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
    """Retreive symbol quotes on the console in either json or dataframe
    format.
    """
    # global opts
    brokermod = config['brokermod']

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
        cols = next(filter(bool, quotes.values())).copy()
        cols.pop('symbol')
        df = pd.DataFrame(
            (quote or {} for quote in quotes.values()),
            index=quotes.keys(),
            columns=cols,
        )
        click.echo(df)
    else:
        click.echo(colorize_json(quotes))


@cli.command()
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--rate', '-r', default=3, help='Quote rate limit')
@click.option('--test', '-t', help='Test quote stream file')
@click.option('--dhost', '-dh', default='127.0.0.1',
              help='Daemon host address to connect to')
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def monitor(config, rate, name, dhost, test, tl):
    """Spawn a real-time watchlist.
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

    from .ui.monitor import _async_main

    async def main(tries):
        async with maybe_spawn_brokerd_as_subactor(
            tries=tries, loglevel=loglevel
        ) as portal:
            # run app "main"
            await _async_main(
                name, portal, tickers,
                brokermod, rate, test=test,
            )

    tractor.run(
        partial(main, tries=1),
        name='monitor',
        loglevel=loglevel if tl else None,
        rpc_module_paths=['piker.ui.monitor'],
    )


@cli.command()
@click.option('--rate', '-r', default=5, help='Logging level')
@click.option('--filename', '-f', default='quotestream.jsonstream',
              help='Logging level')
@click.option('--dhost', '-dh', default='127.0.0.1',
              help='Daemon host address to connect to')
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def record(config, rate, name, dhost, filename):
    """Record client side quotes to file
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

    async def main(tries):
        async with maybe_spawn_brokerd_as_subactor(
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


@cli.group()
@click.option('--config_dir', '-d', default=_watchlists_data_path,
              help='Path to piker configuration directory')
@click.pass_context
def watchlists(ctx, config_dir):
    """Watchlists commands and operations
    """
    loglevel = ctx.parent.params['loglevel']
    get_console_log(loglevel)  # activate console logging

    wl.make_config_dir(_config_dir)
    ctx.ensure_object(dict)
    ctx.obj = {'path': config_dir,
               'watchlist': wl.ensure_watchlists(config_dir)}


@watchlists.command(help='show watchlist')
@click.argument('name', nargs=1, required=False)
@click.pass_context
def show(ctx, name):
    watchlist = wl.merge_watchlist(ctx.obj['watchlist'], wl._builtins)
    click.echo(colorize_json(
               watchlist if name is None else watchlist[name]))


@watchlists.command(help='load passed in watchlist')
@click.argument('data', nargs=1, required=True)
@click.pass_context
def load(ctx, data):
    try:
        wl.write_to_file(json.loads(data), ctx.obj['path'])
    except (json.JSONDecodeError, IndexError):
        click.echo('You have passed an invalid text respresentation of a '
                   'JSON object. Try again.')


@watchlists.command(help='add ticker to watchlist')
@click.argument('name', nargs=1, required=True)
@click.argument('ticker_names', nargs=-1, required=True)
@click.pass_context
def add(ctx, name, ticker_names):
    for ticker in ticker_names:
        watchlist = wl.add_ticker(
            name, ticker, ctx.obj['watchlist'])
    wl.write_to_file(watchlist, ctx.obj['path'])


@watchlists.command(help='remove ticker from watchlist')
@click.argument('name', nargs=1, required=True)
@click.argument('ticker_name', nargs=1, required=True)
@click.pass_context
def remove(ctx, name, ticker_name):
    try:
        watchlist = wl.remove_ticker(name, ticker_name, ctx.obj['watchlist'])
    except KeyError:
        log.error(f"No watchlist with name `{name}` could be found?")
    except ValueError:
        if name in wl._builtins and ticker_name in wl._builtins[name]:
            log.error(f"Can not remove ticker `{ticker_name}` from built-in "
                      f"list `{name}`")
        else:
            log.error(f"Ticker `{ticker_name}` not found in list `{name}`")
    else:
        wl.write_to_file(watchlist, ctx.obj['path'])


@watchlists.command(help='delete watchlist group')
@click.argument('name', nargs=1, required=True)
@click.pass_context
def delete(ctx, name):
    watchlist = wl.delete_group(name, ctx.obj['watchlist'])
    wl.write_to_file(watchlist, ctx.obj['path'])


@watchlists.command(help='merge a watchlist from another user')
@click.argument('watchlist_to_merge', nargs=1, required=True)
@click.pass_context
def merge(ctx, watchlist_to_merge):
    merged_watchlist = wl.merge_watchlist(json.loads(watchlist_to_merge),
                                          ctx.obj['watchlist'])
    wl.write_to_file(merged_watchlist, ctx.obj['path'])


@watchlists.command(help='dump text respresentation of a watchlist to console')
@click.argument('name', nargs=1, required=False)
@click.pass_context
def dump(ctx, name):
    click.echo(json.dumps(ctx.obj['watchlist']))


# options utils

@cli.command()
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--ids', flag_value=True, help='Include numeric ids in output')
@click.argument('symbol', required=True)
@click.pass_context
def contracts(ctx, loglevel, broker, symbol, ids):

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
    """Retreive symbol quotes on the console in either
    json or dataframe format.
    """
    # global opts
    brokermod = config['brokermod']

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
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--date', '-d', help='Contracts expiry date')
@click.option('--test', '-t', help='Test quote stream file')
@click.option('--rate', '-r', default=1, help='Logging level')
@click.argument('symbol', required=True)
@click.pass_obj
def optschain(config, symbol, date, tl, rate, test):
    """Start the real-time option chain UI.
    """
    # global opts
    loglevel = config['loglevel']
    brokername = config['broker']

    from .ui.option_chain import _async_main

    async def main(tries):
        async with maybe_spawn_brokerd_as_subactor(
            tries=tries, loglevel=loglevel
        ) as portal:
            # run app "main"
            await _async_main(
                symbol,
                brokername,
                rate=rate,
                loglevel=loglevel,
                test=test,
            )

    tractor.run(
        partial(main, tries=1),
        name='kivy-options-chain',
        loglevel=loglevel if tl else None,
    )
