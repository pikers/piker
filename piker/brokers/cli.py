"""
Console interface to broker client/daemons.
"""
from functools import partial
from importlib import import_module

import click
import trio

from ..log import get_console_log, colorize_json


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
@click.option('--broker', default='questrade', help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.argument('meth', nargs=1)
@click.argument('kwargs', nargs=-1)
def api(meth, kwargs, loglevel, broker):
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

    data = run(partial(brokermod.api, meth, **_kwargs), loglevel=loglevel)
    if data:
        click.echo(colorize_json(data))


@cli.command()
@click.option('--broker', default='questrade', help='Broker backend to use')
@click.option('--loglevel', '-l', default='info', help='Logging level')
@click.argument('tickers', nargs=-1)
def stream(broker, loglevel, tickers):
    # import broker module daemon entry point
    bm = import_module('.' + broker, 'piker.brokers')
    run(
        partial(bm.serve_forever, [
            partial(bm.poll_tickers, tickers=tickers)
        ]),
        loglevel
    )
