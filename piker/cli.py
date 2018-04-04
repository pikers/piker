"""
Console interface to broker client/daemons.
"""
from functools import partial
from importlib import import_module
import os
from collections import defaultdict
import json

import click
import trio
import pandas as pd


from .log import get_console_log, colorize_json, get_logger
from . import watchlists as wl
from .brokers import core, get_brokermod

log = get_logger('cli')
DEFAULT_BROKER = 'robinhood'

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')


def run(main, loglevel='info'):
    log = get_console_log(loglevel)

    # main sandwich
    try:
        return trio.run(main)
    except Exception as err:
        log.exception(err)
    finally:
        log.debug("Exiting piker")


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
@click.argument('name', nargs=1, required=True)
def watch(loglevel, broker, rate, name):
    """Spawn a watchlist.
    """
    from .ui.watchlist import _async_main
    log = get_console_log(loglevel)  # activate console logging
    brokermod = get_brokermod(broker)

    watchlists_base = {
        'cannabis': [
            'EMH.VN', 'LEAF.TO', 'HVT.VN', 'HMMJ.TO', 'APH.TO',
            'CBW.VN', 'TRST.CN', 'VFF.TO', 'ACB.TO', 'ABCN.VN',
            'APH.TO', 'MARI.CN', 'WMD.VN', 'LEAF.TO', 'THCX.VN',
            'WEED.TO', 'NINE.VN', 'RTI.VN', 'SNN.CN', 'ACB.TO',
            'OGI.VN', 'IMH.VN', 'FIRE.VN', 'EAT.CN',
            'WMD.VN', 'HEMP.VN', 'CALI.CN', 'RQB.CN', 'MPX.CN',
            'SEED.TO', 'HMJR.TO', 'CMED.TO', 'PAS.VN',
            'CRON',
        ],
        'dad': ['GM', 'TSLA', 'DOL.TO', 'CIM', 'SPY', 'SHOP.TO'],
        'pharma': ['ATE.VN'],
        'indexes': ['SPY', 'DAX', 'QQQ', 'DIA'],
    }
    watchlist_from_file = wl.ensure_watchlists(_watchlists_data_path)
    watchlists = wl.merge_watchlist(watchlist_from_file, watchlists_base)
    # broker_conf_path = os.path.join(
    #     click.get_app_dir('piker'), 'watchlists.json')
    # from piker.testing import _quote_streamer as brokermod
    broker_limit = getattr(brokermod, '_rate_limit', float('inf'))
    if broker_limit < rate:
        rate = broker_limit
        log.warn(f"Limiting {brokermod.__name__} query rate to {rate}/sec")
    trio.run(_async_main, name, watchlists[name], brokermod, rate)
    # broker_conf_path = os.path.join(
    #     click.get_app_dir('piker'), 'watchlists.json')
    # from piker.testing import _quote_streamer as brokermod


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
    watchlist = ctx.obj['watchlist']
    click.echo(colorize_json(
               watchlist if name is None else watchlist[name]))


@watchlists.command(help='load passed in watchlist')
@click.argument('data', nargs=1, required=True)
@click.pass_context
def load(ctx, data):
    try:
        wl.write_sorted_json(json.loads(data), ctx.obj['path'])
    except (json.JSONDecodeError, IndexError):
        click.echo('You have passed an invalid text respresentation of a '
                   'JSON object. Try again.')


@watchlists.command(help='add ticker to watchlist')
@click.argument('name', nargs=1, required=True)
@click.argument('ticker_name', nargs=1, required=True)
@click.pass_context
def add(ctx, name, ticker_name):
    watchlist = wl.add_ticker(name, ticker_name,
                              ctx.obj['watchlist'])
    wl.write_sorted_json(watchlist, ctx.obj['path'])


@watchlists.command(help='remove ticker from watchlist')
@click.argument('name', nargs=1, required=True)
@click.argument('ticker_name', nargs=1, required=True)
@click.pass_context
def remove(ctx, name, ticker_name):
    watchlist = wl.remove_ticker(name, ticker_name, ctx.obj['watchlist'])
    wl.write_sorted_json(watchlist, ctx.obj['path'])


@watchlists.command(help='delete watchlist group')
@click.argument('name', nargs=1, required=True)
@click.pass_context
def delete(ctx, name):
    watchlist = wl.delete_group(name, ctx.obj['watchlist'])
    wl.write_sorted_json(watchlist, ctx.obj['path'])


@watchlists.command(help='merge a watchlist from another user')
@click.argument('watchlist_to_merge', nargs=1, required=True)
@click.pass_context
def merge(ctx, watchlist_to_merge):
    merged_watchlist = wl.merge_watchlist(json.loads(watchlist_to_merge),
                                          ctx.obj['watchlist'])
    wl.write_sorted_json(merged_watchlist, ctx.obj['path'])


@watchlists.command(help='dump text respresentation of a watchlist to console')
@click.argument('name', nargs=1, required=False)
@click.pass_context
def dump(ctx, name):
    click.echo(json.dumps(ctx.obj['watchlist']))
