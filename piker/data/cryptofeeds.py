# piker: trading gear for hackers
# Copyright (C) Jared Goldman (in stewardship for piker0)

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
"""
from os import walk
from contextlib import asynccontextmanager as acm
from datetime import datetime
from types import ModuleType
from typing import Any, Literal, Optional, Callable
import time
from functools import partial

import trio
from trio_typing import TaskStatus
from tractor.trionics import broadcast_receiver, maybe_open_context
import pendulum
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor
from tractor import to_asyncio
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK
from cryptofeed.symbols import Symbol
from cryptofeed.types import OrderBook
import asyncio

from piker._cacheables import open_cached_client
from piker.log import get_logger, get_console_log
from piker.data import ShmArray
from piker.brokers._util import (
    BrokerError,
    DataUnavailable,
)
from piker.pp import config

_spawn_kwargs = {
    "infect_asyncio": True,
}

log = get_logger(__name__)


def fqsn_to_cb_sym(pair_data: Symbol) -> Symbol:
    return Symbol(base=pair_data["baseCurrency"], quote=pair_data["quoteCurrency"])


def fqsn_to_cf_sym(fqsn: str, pairs: dict[str, Symbol]) -> str:
    pair_data = pairs[fqsn]
    return pair_data["baseCurrency"] + "-" + pair_data["quoteCurrency"]


def pair_data_to_cf_sym(sym_data: Symbol):
    return sym_data["baseCurrency"] + "-" + sym_data["quoteCurrency"]


def cf_sym_to_fqsn(sym: str) -> str:
    return sym.lower().replace("-", "")


def get_config(exchange: str) -> dict[str, Any]:
    conf, path = config.load()

    section = conf.get(exchange.lower())

    # TODO: document why we send this, basically because logging params for cryptofeed
    conf["log"] = {}
    conf["log"]["disabled"] = True

    if section is None:
        log.warning(f"No config section found for deribit in {exchange}")

    return conf


async def mk_stream_quotes(
    exchange: str,
    channels: list[str],
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
        open_cached_client(exchange.lower()) as client, 
        send_chan as send_chan
    ):
        pairs = await client.cache_pairs()

        pair_data = pairs[sym]

        async with maybe_open_price_feed(pair_data, exchange, channels) as stream:

            init_msgs = {
                sym: {
                    "symbol_info": {"asset_type": "crypto", "price_tick_size": 0.0005},
                    "shm_write_opts": {"sum_tick_vml": False},
                    "fqsn": sym,
                },
            }
            quote_msg = {"symbol": pair_data["name"], "last": 0, "ticks": []}

            task_status.started((init_msgs, quote_msg))

            feed_is_live.set()
            # try:
            #     async for typ, quote in stream:
            #         print(f'streaming {typ} quote: {quote}')
            #         topic = quote["symbobl"]
            #         await send_chan.send({topic: quote})
            # finally: 
            #     breakpoint()
            
            while True:
                with trio.move_on_after(4) as cancel_scope:
                    log.warning(f'WAITING FOR MESSAGE')
                    msg = await stream.receive()
                    log.warning(f'RECEIVED MSG: {msg}')
                    topic = msg["symbol"]
                    await send_chan.send({topic: msg})
                    log.warning(f'SENT TO CHAN')
                if cancel_scope.cancelled_caught:
                    await tractor.breakpoint()

@acm
async def maybe_open_price_feed(
    pair_data: Symbol, exchange: str, channels
) -> trio.abc.ReceiveStream:
    # TODO: add a predicate to maybe_open_context
    # TODO: ensure we can dynamically pass down args here
    async with maybe_open_context(
        acm_func=open_price_feed,
        kwargs={
            "pair_data": pair_data,
            "exchange": exchange,
            "channels": channels,
        },
        key=pair_data["name"],
    ) as (cache_hit, feed):
        yield feed


@acm
async def open_price_feed(
    pair_data: Symbol, exchange, channels
) -> trio.abc.ReceiveStream:
    async with maybe_open_feed_handler(exchange) as fh:
        async with to_asyncio.open_channel_from(
            partial(aio_price_feed_relay, pair_data, exchange, channels, fh)
        ) as (first, chan):
            yield chan


@acm
async def maybe_open_feed_handler(exchange: str) -> trio.abc.ReceiveStream:
    async with maybe_open_context(
        acm_func=open_feed_handler,
        kwargs={
            "exchange": exchange,
        },
        key="feedhandler",
    ) as (cache_hit, fh):
        yield fh


@acm
async def open_feed_handler(exchange: str):
    fh = FeedHandler(config=get_config(exchange))
    yield fh
    await to_asyncio.run_task(fh.stop_async)


async def aio_price_feed_relay(
    pair_data: Symbol,
    exchange: str,
    channels: list[str],
    fh: FeedHandler,
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,
) -> None:
    async def _trade(data: dict, receipt_timestamp):
        data = data.to_dict()
        message = (
                "trade",
                {
                    "symbol": cf_sym_to_fqsn(data['symbol']),
                    "last": float(data['price']),
                    "broker_ts": time.time(),
                    "ticks": [{
                        'type': 'trade',
                        'price': float(data['price']),
                        'size': float(data['amount']),
                        'broker_ts': receipt_timestamp
                    }],
                },
            )
        print(f'trade message: {message}')
        # try:
        to_trio.send_nowait(message)
        # except trio.WouldBlock as e:
            #breakpoint()

    async def _l1(data: dict, receipt_timestamp):
        bid = data.book.to_dict()['bid']
        ask = data.book.to_dict()['ask']
        l1_ask_price, l1_ask_size = next(iter(ask.items()))
        l1_bid_price, l1_bid_size = next(iter(bid.items()))
        message =  ( 
                "l1",
                {
                    "symbol": cf_sym_to_fqsn(data.symbol),
                    "broker_ts": time.time(),
                    "ticks": [
                        {
                            "type": "bid",
                            "price": float(l1_bid_price),
                            "size": float(l1_bid_size),
                        },
                        {
                            "type": "bsize",
                            "price": float(l1_bid_price),
                            "size": float(l1_bid_size),
                        },
                        {
                            "type": "ask",
                            "price": float(l1_ask_price),
                            "size": float(l1_ask_size),
                        },
                        {
                            "type": "asize",
                            "price": float(l1_ask_price),
                            "size": float(l1_ask_size),
                        },
                    ]
                }
            )
        try:
            to_trio.send_nowait(message)
        except trio.WouldBlock as e:
            print(e)

    fh.add_feed(
        exchange,
        channels=channels,
        symbols=[pair_data_to_cf_sym(pair_data)],
        callbacks={TRADES: _trade, L2_BOOK: _l1}
    )

    if not fh.running:
        fh.run(start_loop=False, install_signal_handlers=False)
       
    # sync with trio
    to_trio.send_nowait(None)

    await asyncio.sleep(float("inf"))
