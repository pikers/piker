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
from types import ModuleType

import click
import trio
import tractor

from ..cli import cli
from .. import watchlists as wl
from ..log import (
    colorize_json,
)
from ._util import (
    log,
    get_console_log,
)
from ..service import (
    maybe_spawn_brokerd,
    maybe_open_pikerd,
)
from ..brokers import (
    core,
    get_brokermod,
    data,
)
DEFAULT_BROKER = 'binance'

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')


OK = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'


def print_ok(s: str, **kwargs):
    print(OK + s + ENDC, **kwargs)


def print_error(s: str, **kwargs):
    print(FAIL + s + ENDC, **kwargs)


def get_method(client, meth_name: str):
    print(f'checking client for method \'{meth_name}\'...', end='', flush=True)
    method = getattr(client, meth_name, None)
    assert method
    print_ok('found!.')
    return method


async def run_method(client, meth_name: str, **kwargs):
    method = get_method(client, meth_name)
    print('running...', end='', flush=True)
    result = await method(**kwargs)
    print_ok(f'done! result: {type(result)}')
    return result


async def run_test(broker_name: str):
    brokermod = get_brokermod(broker_name)
    total = 0
    passed = 0
    failed = 0

    print('getting client...', end='', flush=True)
    if not hasattr(brokermod, 'get_client'):
        print_error('fail! no \'get_client\' context manager found.')
        return

    async with brokermod.get_client(is_brokercheck=True) as client:
        print_ok('done! inside client context.')

        # check for methods present on brokermod
        method_list = [
            'backfill_bars',
            'get_client',
            'trades_dialogue',
            'open_history_client',
            'open_symbol_search',
            'stream_quotes',

        ]

        for method in method_list:
            print(
                f'checking brokermod for method \'{method}\'...',
                end='', flush=True)
            if not hasattr(brokermod, method):
                print_error(f'fail! method \'{method}\' not found.')
                failed += 1
            else:
                print_ok('done!')
                passed += 1

            total += 1

        # check for methods present con brokermod.Client and their
        # results

        # for private methods only check is present
        method_list = [
            'get_balances',
            'get_assets',
            'get_trades',
            'get_xfers',
            'submit_limit',
            'submit_cancel',
            'search_symbols',
        ]

        for method_name in method_list:
            try:
                get_method(client, method_name)
                passed += 1

            except AssertionError:
                print_error(f'fail! method \'{method_name}\' not found.')
                failed += 1

            total += 1

        # check for methods present con brokermod.Client and their
        # results

        syms = await run_method(client, 'symbol_info')
        total += 1

        if len(syms) == 0:
            raise BaseException('Empty Symbol list?')

        passed += 1

        first_sym = tuple(syms.keys())[0]

        method_list = [
            ('cache_symbols', {}),
            ('search_symbols', {'pattern': first_sym[:-1]}),
            ('bars', {'symbol': first_sym})
        ]

        for method_name, method_kwargs in method_list:
            try:
                await run_method(client, method_name, **method_kwargs)
                passed += 1

            except AssertionError:
                print_error(f'fail! method \'{method_name}\' not found.')
                failed += 1

            total += 1

        print(f'total: {total}, passed: {passed}, failed: {failed}')


@cli.command()
@click.argument('broker', nargs=1, required=True)
@click.pass_obj
def brokercheck(config, broker):
    '''
    Test broker apis for completeness.

    '''
    async def bcheck_main():
        async with maybe_spawn_brokerd(broker) as portal:
            await portal.run(run_test, broker)
            await portal.cancel_actor()

    trio.run(run_test, broker)


@cli.command()
@click.option('--keys', '-k', multiple=True,
              help='Return results only for these keys')
@click.argument('meth', nargs=1)
@click.argument('kwargs', nargs=-1)
@click.pass_obj
def api(config, meth, kwargs, keys):
    '''
    Make a broker-client API method call

    '''
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
@click.argument('tickers', nargs=-1, required=True)
@click.pass_obj
def quote(config, tickers):
    '''
    Print symbol quotes to the console

    '''
    # global opts
    brokermod = list(config['brokermods'].values())[0]

    quotes = trio.run(partial(core.stocks_quote, brokermod, tickers))
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
@click.option('--count', '-c', default=1000,
              help='Number of bars to retrieve')
@click.argument('symbol', required=True)
@click.pass_obj
def bars(config, symbol, count):
    '''
    Retreive 1m bars for symbol and print on the console

    '''
    # global opts
    brokermod = list(config['brokermods'].values())[0]

    # broker backend should return at the least a
    # list of candle dictionaries
    bars = trio.run(
        partial(
            core.bars,
            brokermod,
            symbol,
            count=count,
            as_np=False,
        )
    )

    if not len(bars):
        log.error(f"No quotes could be found for {symbol}?")
        return

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
    '''
    Record client side quotes to a file on disk

    '''
    # global opts
    brokermod = list(config['brokermods'].values())[0]
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
    '''
    Get list of all option contracts for symbol

    '''
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
@click.option('--date', '-d', help='Contracts expiry date')
@click.argument('symbol', required=True)
@click.pass_obj
def optsquote(config, symbol, date):
    '''
    Retreive symbol option quotes on the console

    '''
    # global opts
    brokermod = list(config['brokermods'].values())[0]

    quotes = trio.run(
        partial(
            core.option_chain, brokermod, symbol, date
        )
    )
    if not quotes:
        log.error(f"No option quotes could be found for {symbol}?")
        return

    click.echo(colorize_json(quotes))


@cli.command()
@click.argument('tickers', nargs=-1, required=True)
@click.pass_obj
def mkt_info(
    config: dict,
    tickers: list[str],
):
    '''
    Print symbol quotes to the console

    '''
    from msgspec.json import encode, decode
    from ..accounting import MktPair
    from ..service import (
        open_piker_runtime,
    )

    # global opts
    brokermods: dict[str, ModuleType] = config['brokermods']

    mkts: list[MktPair] = []
    async def main():

        async with open_piker_runtime(
            name='mkt_info_query',
            # loglevel=loglevel,
            debug_mode=True,

        ) as (_, _):
            for fqme in tickers:
                bs_fqme, _, broker = fqme.rpartition('.')
                brokermod: ModuleType = brokermods[broker]
                mkt, bs_pair = await core.mkt_info(
                    brokermod,
                    bs_fqme,
                )
                mkts.append((mkt, bs_pair))

    trio.run(main)

    if not mkts:
        log.error(
            f'No market info could be found for {tickers}'
        )
        return

    if len(mkts) < len(tickers):
        syms = tuple(map(itemgetter('fqme'), mkts))
        for ticker in tickers:
            if ticker not in syms:
                log.warn(f"Could not find symbol {ticker}?")


    # TODO: use ``rich.Table`` intead here!
    for mkt, bs_pair in mkts:
        click.echo(
            '\n'
            '----------------------------------------------------\n'
            f'{type(bs_pair)}\n'
            '----------------------------------------------------\n'
            f'{colorize_json(bs_pair.to_dict())}\n'
            '----------------------------------------------------\n'
            f'as piker `MktPair` with fqme: {mkt.fqme}\n'
            '----------------------------------------------------\n'
            # NOTE: roundtrip to json codec for console print
            f'{colorize_json(decode(encode(mkt)))}'
        )


@cli.command()
@click.argument('pattern', required=True)
@click.pass_obj
def search(config, pattern):
    '''
    Search for symbols from broker backend(s).

    '''
    # global opts
    brokermods = list(config['brokermods'].values())

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


# @cli.command()
# @click.argument('section', required=True)
# @click.argument('value', required=False)
# @click.option('--delete', '-d', flag_value=True, help='Delete section')
# @click.pass_obj
# def brokercfg(config, section, value, delete):
#     from .. import config
#     conf, path = config.load()

#     # XXX: Recursive getting & setting

#     def get_value(_dict, _section):
#         subs = _section.split('.')
#         if len(subs) > 1:
#             return get_value(
#                 _dict[subs[0]],
#                 '.'.join(subs[1:]),
#             )

#         else:
#             return _dict[_section]

#     def set_value(_dict, _section, val):
#         subs = _section.split('.')
#         if len(subs) > 1:
#             if subs[0] not in _dict:
#                 _dict[subs[0]] = {}

#             return set_value(
#                 _dict[subs[0]],
#                 '.'.join(subs[1:]),
#                 val
#             )

#         else:
#             _dict[_section] = val

#     def del_value(_dict, _section):
#         subs = _section.split('.')
#         if len(subs) > 1:
#             if subs[0] not in _dict:
#                 return

#             return del_value(
#                 _dict[subs[0]],
#                 '.'.join(subs[1:])
#             )

#         else:
#             if _section not in _dict:
#                 return

#             del _dict[_section]

#     if not delete:
#         if value:
#             set_value(conf, section, value)

#         click.echo(colorize_json(get_value(conf, section)))
#     else:
#         del_value(conf, section)

#     broker_conf.write(conf)
