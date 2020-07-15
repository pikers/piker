"""
``marketstore`` integration.

- TICK data ingest routines
- websocket client for subscribing to write triggers
- docker container management automation
"""
from pprint import pformat
from typing import Dict, Any, List
from functools import partial
import time

import msgpack
import numpy as np
import pandas as pd
import pymarketstore as pymkts
import click
import trio
import tractor
from trio_websocket import open_websocket_url

from . import maybe_spawn_brokerd
from ..cli import cli
from .. import watchlists as wl
from ..brokers.data import DataFeed
from ..log import get_logger


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
_quote_tmp = {}.fromkeys(dict(_quote_dt).keys(), np.nan)


_tick_map = {
    'Up': 1,
    'Equal': 0,
    'Down': -1,
    None: np.nan,
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
    last_fill: str,
) -> np.array:
    """Return marketstore writeable structarray from quote ``dict``.
    """
    if last_fill:
        # new fill bby
        now = timestamp(last_fill)
    else:
        # this should get inserted upstream by the broker-client to
        # subtract from IPC latency
        now = timestamp(pd.Timestamp.now())

    secs, ns = now / 10**9, now % 10**9

    # pack into List[Tuple[str, Any]]
    array_input = []

    # insert 'Epoch' entry first
    array_input.append(int(secs))
    # insert 'Nanoseconds' field
    array_input.append(int(ns))

    # append remaining fields
    for name, dt in _quote_dt[2:]:
        if 'f' in dt:
            none = np.nan
        else:
            none = 0
        val = quote.get(name.casefold(), none)
        array_input.append(val)

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
        async with maybe_spawn_brokerd(
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

            quote_cache = {quote['symbol']: quote for quote in first_quotes}

            client = pymkts.Client(endpoint=url)

            # start ingest to marketstore
            async for quotes in qstream:
                for symbol, quote in quotes.items():
                    fmt_quote, _ = brokermod.format_stock_quote(
                        quote,
                        feed._symbol_data_cache
                    )

                    # remap tick strs to ints
                    fmt_quote['tick'] = _tick_map[
                        fmt_quote.get('tick', 'Equal')
                    ]

                    # check for volume update (i.e. did trades happen
                    # since last quote)
                    new_vol = fmt_quote.get('volume', None)
                    if new_vol is None:
                        log.debug(f"No fills for {symbol}")
                        if new_vol == quote_cache.get('volume'):
                            log.error(
                                f"{symbol}: got same volume as last quote?")

                    quote_cache.update(fmt_quote)

                    a = quote_to_marketstore_structarray(
                        fmt_quote,
                        # TODO: check this closer to the broker query api
                        last_fill=fmt_quote.get('last_fill', '')
                    )
                    start = time.time()
                    err_on_resp(client.write(
                        a, _tick_tbk.format(symbol), isvariablelength=True)
                    )
                    log.trace(
                        f"{symbol} write time (s): {time.time() - start}")

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

    def query(name, tbk=_tick_tbk_ids):
        return client.query(
            pymkts.Params(name, *tbk)).first().df()

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
def marketstore_destroy(config: dict, names: List[str], url: str) -> None:
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


async def open_quote_stream(
    tbks: List[str],
    host: str = 'localhost',
    port: int = 5993
) -> None:
    """Open a symbol stream from a running instance of marketstore and
    log to console.
    """
    async with open_websocket_url(f'ws://{host}:{port}/ws') as ws:
        # send subs topics to server
        await ws.send_message(msgpack.dumps({'streams': tbks}))

        async def recv() -> Dict[str, Any]:
            return msgpack.loads((await ws.get_message()), encoding='utf-8')

        streams = (await recv())['streams']
        log.info(f"Subscribed to {streams}")

        while True:
            msg = await recv()
            log.info(f"Received quote:\n{msg}")


@cli.command()
@click.option(
    '--url',
    default='ws://localhost:5993/ws',
    help='HTTP URL of marketstore instance'
)
@click.argument('names', nargs=-1)
@click.pass_obj
def marketstore_stream(config: dict, names: List[str], url: str):
    """Connect to a marketstore time bucket stream for (a set of) symbols(s)
    and print to console.
    """
    trio.run(open_quote_stream, names)
