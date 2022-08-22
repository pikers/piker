# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for piker0)

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

'''
Deribit backend.

'''

import asyncio
from async_generator import aclosing
from contextlib import asynccontextmanager as acm
from datetime import datetime
from typing import Any, Optional, List, Callable
import time

import trio
from trio_typing import TaskStatus
import pendulum
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor
from tractor import to_asyncio

from piker._cacheables import open_cached_client
from piker.log import get_logger, get_console_log
from piker.data import ShmArray
from piker.brokers._util import (
    BrokerError,
    DataUnavailable,
)

from cryptofeed import FeedHandler

from cryptofeed.defines import (
    DERIBIT, L1_BOOK, TRADES, OPTION, CALL, PUT
)
from cryptofeed.symbols import Symbol

from .api import Client, Trade, get_config

_spawn_kwargs = {
    'infect_asyncio': True,
}


log = get_logger(__name__)


_url = 'https://www.deribit.com'


def str_to_cb_sym(name: str) -> Symbol:
    base, strike_price, expiry_date, option_type = name.split('-')

    quote = base

    if option_type == 'put':
        option_type = PUT 
    elif option_type  == 'call':
        option_type = CALL
    else:
        raise BaseException("Couldn\'t parse option type")

    return Symbol(
        base, quote,
        type=OPTION,
        strike_price=strike_price,
        option_type=option_type,
        expiry_date=expiry_date,
        expiry_normalize=False)



def piker_sym_to_cb_sym(name: str) -> Symbol:
    base, expiry_date, strike_price, option_type = tuple(
        name.upper().split('-'))

    quote = base

    if option_type == 'P':
        option_type = PUT 
    elif option_type  == 'C':
        option_type = CALL
    else:
        raise BaseException("Couldn\'t parse option type")

    return Symbol(
        base, quote,
        type=OPTION,
        strike_price=strike_price,
        option_type=option_type,
        expiry_date=expiry_date.upper())


def cb_sym_to_deribit_inst(sym: Symbol):
    # cryptofeed normalized
    cb_norm = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

    # deribit specific 
    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

    exp = sym.expiry_date

    # YYMDD
    # 01234
    year, month, day = (
        exp[:2], months[cb_norm.index(exp[2:3])], exp[3:])

    otype = 'C' if sym.option_type == CALL else 'P'

    return f'{sym.base}-{day}{month}{year}-{sym.strike_price}-{otype}'


# inside here we are in an asyncio context
async def open_aio_cryptofeed_relay(
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,
    instruments: List[str] = []
) -> None:

    instruments = [piker_sym_to_cb_sym(i) for i in instruments]

    async def trade_cb(data: dict, receipt_timestamp):
        to_trio.send_nowait(('trade', {
            'symbol': cb_sym_to_deribit_inst(
                str_to_cb_sym(data.symbol)).lower(),
            'last': data,
            'broker_ts': time.time(),
            'data': data.to_dict(),
            'receipt': receipt_timestamp
        }))

    async def l1_book_cb(data: dict, receipt_timestamp):
        to_trio.send_nowait(('l1', {
            'symbol': cb_sym_to_deribit_inst(
                str_to_cb_sym(data.symbol)).lower(),
            'ticks': [
                {'type': 'bid',
                    'price': float(data.bid_price), 'size': float(data.bid_size)},
                {'type': 'bsize',
                    'price': float(data.bid_price), 'size': float(data.bid_size)},
                {'type': 'ask',
                    'price': float(data.ask_price), 'size': float(data.ask_size)},
                {'type': 'asize',
                    'price': float(data.ask_price), 'size': float(data.ask_size)}
            ]
        }))

    fh = FeedHandler(config=get_config())
    fh.run(start_loop=False)

    fh.add_feed(
        DERIBIT,
        channels=[L1_BOOK],
        symbols=instruments,
        callbacks={L1_BOOK: l1_book_cb})

    fh.add_feed(
        DERIBIT,
        channels=[TRADES],
        symbols=instruments,
        callbacks={TRADES: trade_cb})

    # sync with trio
    to_trio.send_nowait(None)

    try:
        await asyncio.sleep(float('inf'))

    except asyncio.exceptions.CancelledError:
        ...


@acm
async def open_cryptofeeds(

    instruments: List[str]

) -> trio.abc.ReceiveStream:

    async with to_asyncio.open_channel_from(
        open_aio_cryptofeed_relay,
        instruments=instruments,
    ) as (first, chan):
        yield chan


@acm
async def open_history_client(
    instrument: str,
) -> tuple[Callable, int]:

    # TODO implement history getter for the new storage layer.
    async with open_cached_client('deribit') as client:

        async def get_ohlc(
            end_dt: Optional[datetime] = None,
            start_dt: Optional[datetime] = None,

        ) -> tuple[
            np.ndarray,
            datetime,  # start
            datetime,  # end
        ]:

            array = await client.bars(
                instrument,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            if len(array) == 0:
                raise DataUnavailable

            start_dt = pendulum.from_timestamp(array[0]['time'])
            end_dt = pendulum.from_timestamp(array[-1]['time'])
            
            return array, start_dt, end_dt

        yield get_ohlc, {'erlangs': 3, 'rate': 3}


async def backfill_bars(
    symbol: str,
    shm: ShmArray,  # type: ignore # noqa
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Fill historical bars into shared mem / storage afap.
    """
    instrument = symbol
    with trio.CancelScope() as cs:
        async with open_cached_client('deribit') as client:
            bars = await client.bars(instrument)
            shm.push(bars)
            task_status.started(cs)


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    sym = symbols[0]

    async with (
        open_cached_client('deribit') as client,
        send_chan as send_chan,
        open_cryptofeeds(symbols) as stream 
    ):

        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            sym: {
                'symbol_info': {
                    'asset_type': 'option',
                    'price_tick_size': 0.0005
                },
                'shm_write_opts': {'sum_tick_vml': False},
                'fqsn': sym,
            },
        }

        nsym = piker_sym_to_cb_sym(sym)

        # keep client cached for real-time section
        cache = await client.cache_symbols()

        async with aclosing(stream):
            last_trades = (await client.last_trades(
                cb_sym_to_deribit_inst(nsym), count=1)).trades

            if len(last_trades) == 0:
                async for typ, quote in stream:
                    if typ == 'trade':
                        last_trade = Trade(**quote['data'])

            else:
                last_trade = Trade(**last_trades[0])

            first_quote = {
                'symbol': sym,
                'last': last_trade.price,
                'brokerd_ts': last_trade.timestamp,
                'ticks': [{
                    'type': 'trade',
                    'price': last_trade.price,
                    'size': last_trade.amount,
                    'broker_ts': last_trade.timestamp
                }]
            }
            task_status.started((init_msgs,  first_quote))

            feed_is_live.set()

            async for typ, quote in stream:
                topic = quote['symbol']
                await send_chan.send({topic: quote})


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> Client:
    async with open_cached_client('deribit') as client:

        # load all symbols locally for fast search
        cache = await client.cache_symbols()
        await ctx.started()

        async with ctx.open_stream() as stream:

            async for pattern in stream:
                # repack in dict form
                await stream.send(
                    await client.search_symbols(pattern))
