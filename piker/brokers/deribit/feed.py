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
from typing import (
    Callable,
)

import trio
from trio_typing import TaskStatus
import pendulum
import numpy as np
import tractor

from piker._cacheables import open_cached_client
from piker.log import get_logger, get_console_log
from piker.brokers._util import (
    DataUnavailable,
)


from .api import (
    Client,
    Trade,
    maybe_open_price_feed
)

log = get_logger(__name__)


@acm
async def open_history_client(
    instrument: str,
) -> tuple[Callable, int]:

    # TODO implement history getter for the new storage layer.
    async with open_cached_client('deribit') as client:

        async def get_ohlc(
            timeframe: float,
            end_dt: datetime | None = None,
            start_dt: datetime | None = None,

        ) -> tuple[
            np.ndarray,
            datetime,  # start
            datetime,  # end
        ]:
            if timeframe != 60:
                raise DataUnavailable('Only 1m bars are supported')

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

    async with open_cached_client('deribit') as client:

        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            sym: {
                'symbol_info': {
                    'asset_type': 'option',
                    'price_tick_size': 0.0005,
                    'lot_tick_size': 0.1
                },
                'shm_write_opts': {'sum_tick_vml': False},
                'fqsn': sym,
            },
        }


        last_trades = (await client.last_trades(sym, count=1)).trades

        async with maybe_open_price_feed(sym) as stream:

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
        await client.cache_symbols()
        await ctx.started()

        async with ctx.open_stream() as stream:

            async for pattern in stream:
                # repack in dict form
                await stream.send(
                    await client.search_symbols(pattern))
