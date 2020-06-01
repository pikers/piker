
"""
Watchlist management commands.
"""
import os
import json

import click

from .. import watchlists as wl
from ..cli import cli
from ..log import get_console_log, colorize_json, get_logger

log = get_logger('watchlist-cli')

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')


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
