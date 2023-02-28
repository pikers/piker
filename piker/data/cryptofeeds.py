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


def deribit_timestamp(when):
    return int((when.timestamp() * 1000) + (when.microsecond / 1000))


# def str_to_cb_sym(name: str) -> Symbol:
#     base, strike_price, expiry_date, option_type = name.split("-")
#
#     quote = base
#
#     if option_type == "put":
#         option_type = PUT
#     elif option_type == "call":
#         option_type = CALL
#     else:
#         raise Exception("Couldn't parse option type")
#
#     return Symbol(
#         base,
#         quote,
#         type=OPTION,
#         strike_price=strike_price,
#         option_type=option_type,
#         expiry_date=expiry_date,
#         expiry_normalize=False,
#     )
#


def piker_sym_to_cb_sym(symbol) -> Symbol:
    return Symbol(
        base=symbol['baseCurrency'],
        quote=symbol['quoteCurrency']
    )


def cb_sym_to_deribit_inst(sym: Symbol):
    # cryptofeed normalized
    cb_norm = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

    # deribit specific
    months = [
        "JAN",
        "FEB",
        "MAR",
        "APR",
        "MAY",
        "JUN",
        "JUL",
        "AUG",
        "SEP",
        "OCT",
        "NOV",
        "DEC",
    ]

    exp = sym.expiry_date

    # YYMDD
    # 01234
    year, month, day = (exp[:2], months[cb_norm.index(exp[2:3])], exp[3:])

    otype = "C" if sym.option_type == CALL else "P"

    return f"{sym.base}-{day}{month}{year}-{sym.strike_price}-{otype}"


def get_config(exchange: str) -> dict[str, Any]:
    conf, path = config.load()

    section = conf.get(exchange.lower())
    breakpoint()

    # TODO: document why we send this, basically because logging params for cryptofeed
    conf["log"] = {}
    conf["log"]["disabled"] = True

    if section is None:
        log.warning(f"No config section found for deribit in {path}")

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

    async with (open_cached_client(exchange.lower()) as client, send_chan as send_chan):
        # create init message here

        cache = await client.cache_symbols()

        cf_syms = {}
        for key, value in cache.items():
            cf_sym = key.lower().replace('-', '')
            cf_syms[cf_sym] = value 

        cf_sym = cf_syms[sym]

        async with maybe_open_price_feed(cf_sym, exchange, channels) as stream:

            init_msgs = {
                # pass back token, and bool, signalling if we're the writer
                # and that history has been written
                sym: {
                    'symbol_info': {
                        'asset_type': 'crypto',
                        'price_tick_size': 0.0005
                    },
                    'shm_write_opts': {'sum_tick_vml': False},
                    'fqsn': sym,
                },
            }

            # broker schemas to validate symbol data
            quote_msg = {"symbol": cf_sym["name"], "last": 0, "ticks": []}

            task_status.started((init_msgs, quote_msg))

            feed_is_live.set()

            async for typ, quote in stream:
                topic = quote["symbol"]
                await send_chan.send({topic: quote})

@acm
async def maybe_open_price_feed(
    symbol, exchange, channels
) -> trio.abc.ReceiveStream:
    # TODO: add a predicate to maybe_open_context
    # TODO: ensure we can dynamically pass down args here
    async with maybe_open_context(
        acm_func=open_price_feed,
        kwargs={
            "symbol": symbol,
            "exchange": exchange,
            "channels": channels,
        },
        key=symbol['name'],
    ) as (cache_hit, feed):
        if cache_hit:
            yield broadcast_receiver(feed, 10)
        else:
            yield feed


@acm
async def open_price_feed(symbol: str, exchange, channels) -> trio.abc.ReceiveStream:
    async with maybe_open_feed_handler(exchange) as fh:
        async with to_asyncio.open_channel_from(
            partial(aio_price_feed_relay, exchange, channels, fh, symbol)
        ) as (first, chan):
            yield chan


@acm
async def maybe_open_feed_handler(exchange: str) -> trio.abc.ReceiveStream:
    async with maybe_open_context(
        acm_func=open_feed_handler,
        kwargs={
            'exchange': exchange,
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
    exchange: str,
    channels: list[str],
    fh: FeedHandler,
    symbol: Symbol,
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,
) -> None:
    async def _trade(data: dict, receipt_timestamp):
        breakpoint()
        # to_trio.send_nowait(
        #     (
        #         "trade",
        #         {
        #             "symbol": cb_sym_to_deribit_inst(
        #                 str_to_cb_sym(data.symbol)
        #             ).lower(),
        #             "last": data,
        #             "broker_ts": time.time(),
        #             "data": data.to_dict(),
        #             "receipt": receipt_timestamp,
        #         },
        #     )
        # )

    async def _l1(data: dict, receipt_timestamp):
        breakpoint()
        # to_trio.send_nowait(
        #     (
        #         "l1",
        #         {
        #             "symbol": cb_sym_to_deribit_inst(
        #                 str_to_cb_sym(data.symbol)
        #             ).lower(),
        #             "ticks": [
        #                 {
        #                     "type": "bid",
        #                     "price": float(data.bid_price),
        #                     "size": float(data.bid_size),
        #                 },
        #                 {
        #                     "type": "bsize",
        #                     "price": float(data.bid_price),
        #                     "size": float(data.bid_size),
        #                 },
        #                 {
        #                     "type": "ask",
        #                     "price": float(data.ask_price),
        #                     "size": float(data.ask_size),
        #                 },
        #                 {
        #                     "type": "asize",
        #                     "price": float(data.ask_price),
        #                     "size": float(data.ask_size),
        #                 },
        #             ],
        #         },
        #     )
        # )
    fh.add_feed(
        exchange,
        channels=channels,
        symbols=[piker_sym_to_cb_sym(symbol)],
        callbacks={TRADES: _trade, L2_BOOK: _l1},
    )

    if not fh.running:
        fh.run(start_loop=False, install_signal_handlers=False)

    # sync with trio
    to_trio.send_nowait(None)

    await asyncio.sleep(float("inf"))
