"""
``marketstore`` integration.
"""
from pprint import pformat
from typing import Dict, Any
from functools import partial

import numpy as np
import pandas as pd
import pymarketstore as pymkts
import click
import tractor

from ..cli import cli
from .. import watchlists as wl
from ..brokers.data import DataFeed
from ..log import get_logger
from ..brokers.core import maybe_spawn_brokerd_as_subactor


log = get_logger(__name__)

_tick_tbk_ids = ('1Sec', 'TICK')
_tick_tbk = '{}/' + '/'.join(_tick_tbk_ids)


_quote_dt = [
    ('Epoch', 'i8'),
    ('Nanoseconds', 'i4'),
    ('Tick', 'i4'),
    # ('fill_time', 'f4'),
    ('Last', 'f4'),
    ('Bid', 'f4'),
    ('Bsize', 'i8'),
    ('Asize', 'i8'),
    ('Ask', 'f4'),
    ('Size', 'i8'),
    ('Volume', 'i8'),
    # ('VWAP', 'f4')
]


_tick_map = {
    'Up': 1,
    'Equal': 0,
    'Down': -1
}


class MarketStoreError(Exception):
    "Generic marketstore client error"


def err_on_resp(response: dict) -> None:
    """Raise any errors found in responses from client request.
    """
    responses = response['responses']
    if responses is not None:
        for r in responses:
            err = r['error']
            if err:
                raise MarketStoreError(err)


def quote_to_marketstore_structarray(
    quote: Dict[str, Any],
    last_fills: Dict[str, str],
) -> np.array:
    """Return marketstore writeable recarray from quote ``dict``.
    """
    # pack into List[Tuple[str, Any]]
    array_input = []

    # this should get inserted by the broker-client to subtract from
    # IPC latency
    now = timestamp(pd.Timestamp.now())

    sym = quote['symbol']
    last_fill_time = quote['fill_time']
    if last_fills.get(sym) != last_fill_time:
        # new fill
        now = timestamp(last_fill_time)
        last_fills[sym] = last_fill_time

    secs, ns = now / 10**9, now % 10**9
    # insert 'Epoch' entry first
    array_input.append(int(secs))

    # insert 'Nanoseconds' field
    array_input.append(int(ns))

    # tick mapping to int
    array_input.append(_tick_map[quote['tick']])

    # append remaining fields
    for name, dt in _quote_dt[3:]:
        array_input.append(quote[name.casefold()])

    return np.array([tuple(array_input)], dtype=_quote_dt)


def timestamp(datestr: str) -> int:
    """Return marketstore compatible 'Epoch' integer in nanoseconds.
    """
    return int(pd.Timestamp(datestr).value)


@cli.command()
@click.option('--test-file', '-t', help='Test quote stream file')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option(
    '--url',
    default='http://localhost:5993/rpc',
    help='HTTP URL of marketstore instance'
)
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def ingest(config, name, test_file, tl, url):
    """Ingest real-time broker quotes and ticks to a marketstore instance.
    """
    # global opts
    brokermod = config['brokermod']
    loglevel = config['loglevel']
    log = config['log']

    watchlist_from_file = wl.ensure_watchlists(config['wl_path'])
    watchlists = wl.merge_watchlist(watchlist_from_file, wl._builtins)
    symbols = watchlists[name]

    async def main(tries):
        async with maybe_spawn_brokerd_as_subactor(
            tries=tries,
            loglevel=loglevel
        ) as portal:
            # connect to broker data feed
            feed = DataFeed(portal, brokermod)
            qstream, quotes = await feed.open_stream(
                symbols,
                'stock',
                rate=3,
                test=test_file,
            )

            first_quotes, _ = feed.format_quotes(quotes)

            if first_quotes[0].get('last') is None:
                log.error("Broker API is down temporarily")
                return

            client = pymkts.Client(endpoint=url)

            # keep track of last executed fill for each symbol
            last_fills = {}

            # start ingest to marketstore
            async for quotes in qstream:
                for symbol, quote in quotes.items():
                    breakpoint()
                    fmt_quote, _ = brokermod.format_stock_quote(
                        quote,
                        feed._symbol_data_cache
                    )
                    a = quote_to_marketstore_structarray(fmt_quote, last_fills)
                    # start = time.time()
                    # err_on_resp(client.write(
                    #     a, _tick_tbk.format(symbol), isvariablelength=True)
                    # )
                    # log.trace(
                    #     f"{symbol} write time (s): {time.time() - start}")

    tractor.run(
        partial(main, tries=1),
        name='ingest_marketstore',
        loglevel=loglevel if tl else None,
        # start_method='forkserver',
    )


@cli.command()
@click.option(
    '--tl',
    is_flag=True,
    help='Enable tractor logging')
@click.option(
    '--url',
    default='http://localhost:5993/rpc',
    help='HTTP URL of marketstore instance'
)
@click.argument('name', nargs=1, required=True)
@click.pass_obj
def ms_shell(config, name, tl, url):
    """Start an IPython shell ready to query the local marketstore db.
    """
    client = pymkts.Client(url)

    def query(name):
        return client.query(
            pymkts.Params(name, *_tick_tbk_ids)).first().df()

    # causes crash
    # client.query(pymkts.Params(symbol, '*', 'OHCLV'

    from IPython import embed
    embed()


@cli.command()
@click.option(
    '--url',
    default='http://localhost:5993/rpc',
    help='HTTP URL of marketstore instance'
)
@click.argument('names', nargs=-1)
@click.pass_obj
def marketstore_destroy(config, names, url):
    """Destroy symbol entries in the local marketstore instance.
    """
    client = pymkts.Client(url)
    if not names:
        names = client.list_symbols()

    # default is to wipe db entirely.
    answer = input(
        "This will entirely wipe you local marketstore db @ "
        f"{url} of the following symbols:\n {pformat(names)}"
        "\n\nDelete [N/y]?\n")

    if answer == 'y':
        for sym in names:
            tbk = _tick_tbk.format(sym)
            print(f"Destroying {tbk}..")
            err_on_resp(client.destroy(_tick_tbk.format(sym)))
    else:
        print("Nothing deleted.")


@cli.command()
@click.option(
    '--url',
    default='ws://localhost:5993/ws',
    help='HTTP URL of marketstore instance'
)
@click.argument('names', nargs=-1)
@click.pass_obj
def marketstore_stream(config, names, url):
    """Destroy symbol entries in the local marketstore instance.
    """
    symbol = 'APHA'
    conn = pymkts.StreamConn('ws://localhost:5993/ws')

    @conn.on(r'^{}/'.format(symbol))
    def on_tsla(conn, msg):
        print(f'received {symbol}', msg['data'])

    conn.run(['APHA/*/*'])  # runs until exception
