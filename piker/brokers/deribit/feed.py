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
from contextlib import asynccontextmanager as acm
from datetime import datetime
from typing import Any, Optional, Callable
import time

import trio
from trio_typing import TaskStatus
import pendulum
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor

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

from .api import (
    Client, Trade,
    get_config,
    str_to_cb_sym, piker_sym_to_cb_sym, cb_sym_to_deribit_inst,
    maybe_open_price_feed
)

_spawn_kwargs = {
    'infect_asyncio': True,
}


log = get_logger(__name__)


@acm
async def open_history_client(
    mkt: MktPair,
) -> tuple[Callable, int]:

    fnstrument: str = mkt.bs_fqme
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
        send_chan as send_chan
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

        async with maybe_open_price_feed(sym) as stream:

            cache = await client.cache_symbols()

            last_trades = (await client.last_trades(
                cb_sym_to_deribit_inst(nsym), count=1)).trades

            if len(last_trades) == 0:
                last_trade = None
                async for typ, quote in stream:
                    if typ == 'trade':
                        last_trade = Trade(**(quote['data']))
                        break

            else:
                last_trade = Trade(**(last_trades[0]))

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
