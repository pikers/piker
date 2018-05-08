"""
Console interface to broker client/daemons.
"""
from functools import partial
from multiprocessing import Process
import json
import os

import click
import pandas as pd
import trio

from . import watchlists as wl
from .brokers import core, get_brokermod
from .brokers.core import _daemon_main, Client
from .log import get_console_log, colorize_json, get_logger

log = get_logger('cli')
DEFAULT_BROKER = 'robinhood'

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')


def run(main, loglevel='info'):
    get_console_log(loglevel)
    return trio.run(main)


@click.command()
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--host', '-h', default='127.0.0.1', help='Host address to bind')
def pikerd(loglevel, host):
    """Spawn the piker daemon.
    """
    run(partial(_daemon_main, host), loglevel)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--keys', '-k', multiple=True,
              help='Return results only for these keys')
@click.argument('meth', nargs=1)
@click.argument('kwargs', nargs=-1)
def api(meth, kwargs, loglevel, broker, keys):
    """client for testing broker API methods with pretty printing of output.
    """
    log = get_console_log(loglevel)
    brokermod = get_brokermod(broker)

    _kwargs = {}
    for kwarg in kwargs:
        if '=' not in kwarg:
            log.error(f"kwarg `{kwarg}` must be of form <key>=<value>")
        else:
            key, _, value = kwarg.partition('=')
            _kwargs[key] = value

    data = run(
        partial(core.api, brokermod, meth, **_kwargs), loglevel=loglevel)

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
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--df-output', '-df', flag_value=True,
              help='Ouput in `pandas.DataFrame` format')
@click.argument('tickers', nargs=-1, required=True)
def quote(loglevel, broker, tickers, df_output):
    """client for testing broker API methods with pretty printing of output.
    """
    brokermod = get_brokermod(broker)
    quotes = run(partial(core.quote, brokermod, tickers), loglevel=loglevel)
    if not quotes:
        log.error(f"No quotes could be found for {tickers}?")
        return

    cols = next(filter(bool, quotes.values())).copy()
    cols.pop('symbol')
    if df_output:
        df = pd.DataFrame(
            (quote or {} for quote in quotes.values()),
            index=quotes.keys(),
            columns=cols,
        )
        click.echo(df)
    else:
        click.echo(colorize_json(quotes))


@cli.command()
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--rate', '-r', default=5, help='Logging level')
@click.option('--dhost', '-dh', default='127.0.0.1',
              help='Daemon host address to connect to')
@click.argument('name', nargs=1, required=True)
def watch(loglevel, broker, rate, name, dhost):
    """Spawn a real-time watchlist.
    """
    from .ui.watchlist import _async_main
    log = get_console_log(loglevel)  # activate console logging
    brokermod = get_brokermod(broker)
    watchlist_from_file = wl.ensure_watchlists(_watchlists_data_path)
    watchlists = wl.merge_watchlist(watchlist_from_file, wl._builtins)
    tickers = watchlists[name]

    async def launch_client(sleep=0.5, tries=10):

        async def subscribe(client):
            # initial request for symbols price streams
            await client.send((brokermod.name, tickers))

        client = Client((dhost, 1616), subscribe)
        for _ in range(tries):  # try for 5 seconds
            try:
                await client.connect()
                break
            except OSError as oserr:
                await trio.sleep(sleep)
        else:
            # will raise indicating child proc should be spawned
            await client.connect()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                _async_main, name, client, tickers,
                brokermod, rate
            )

        # signal exit of stream handler task
        await client.aclose()

    try:
        trio.run(partial(launch_client, tries=1))
    except OSError as oserr:
        log.warn("No broker daemon could be found")
        log.warn(oserr)
        log.warning("Spawning local broker-daemon...")
        child = Process(
            target=run,
            args=(partial(_daemon_main, dhost), loglevel),
            daemon=True,
        )
        child.start()
        trio.run(launch_client, 5)
        child.join()


@cli.group()
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--config_dir', '-d', default=_watchlists_data_path,
              help='Path to piker configuration directory')
@click.pass_context
def watchlists(ctx, loglevel, config_dir):
    """Watchlists commands and operations
    """
    get_console_log(loglevel)  # activate console logging
    wl.make_config_dir(_config_dir)
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
