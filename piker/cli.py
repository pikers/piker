"""
Console interface to broker client/daemons.
"""
from functools import partial
from importlib import import_module

import click
import trio
import pandas as pd

from .log import get_console_log, colorize_json, get_logger
from .brokers import core

log = get_logger('cli')


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
@click.option('--broker', '-b', default='questrade',
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
    brokermod = import_module('.' + broker, 'piker.brokers')

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
@click.option('--broker', '-b', default='questrade',
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--df-output', '-df', flag_value=True,
              help='Ouput in `pandas.DataFrame` format')
@click.argument('tickers', nargs=-1)
def quote(loglevel, broker, tickers, df_output):
    """client for testing broker API methods with pretty printing of output.
    """
    brokermod = import_module('.' + broker, 'piker.brokers')
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
@click.option('--broker', '-b', default='questrade',
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.argument('name', nargs=1, required=True)
def watch(loglevel, broker, name):
    """Spawn a watchlist.
    """
    from .ui.watchlist import _async_main
    get_console_log(loglevel)  # activate console logging
    brokermod = import_module('.' + broker, 'piker.brokers')

    watchlists = {
        'cannabis': [
            'EMH.VN', 'LEAF.TO', 'HVT.VN', 'HMMJ.TO', 'APH.TO',
            'CBW.VN', 'TRST.CN', 'VFF.TO', 'ACB.TO', 'ABCN.VN',
            'APH.TO', 'MARI.CN', 'WMD.VN', 'LEAF.TO', 'THCX.VN',
            'WEED.TO', 'NINE.VN', 'RTI.VN', 'SNN.CN', 'ACB.TO',
            'OGI.VN', 'IMH.VN', 'FIRE.VN', 'EAT.CN', 'NUU.VN',
            'WMD.VN', 'HEMP.VN', 'CALI.CN', 'RQB.CN', 'MPX.CN',
            'SEED.TO', 'HMJR.TO', 'CMED.TO', 'PAS.VN',
            'CRON',
        ],
        'dad': [
            'GM', 'TSLA', 'DOL.TO', 'CIM', 'SPY',
            'SHOP.TO',
        ],
        'pharma': [
            'ATE.VN'
        ],
    }
    # broker_conf_path = os.path.join(
    #     click.get_app_dir('piker'), 'watchlists.json')
    # from piker.testing import _quote_streamer as brokermod
    trio.run(_async_main, name, watchlists[name], brokermod)
