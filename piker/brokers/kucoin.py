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

from typing import Any, Optional, Literal
from contextlib import asynccontextmanager as acm

import asks
import tractor
import trio
from trio_typing import TaskStatus
from fuzzywuzzy import process as fuzzy
from cryptofeed.defines import (
    KUCOIN,
    TRADES,
    L2_BOOK
)
from piker.data.cryptofeeds import mk_stream_quotes
from piker._cacheables import open_cached_client
from piker.log import get_logger
from ._util import SymbolNotFound

_spawn_kwargs = {
    "infect_asyncio": True,
}

log = get_logger(__name__)

class Client:
    def __init__(self) -> None:
        self._pairs: dict[str, Any] = None
        # TODO" Shouldn't have to write kucoin twice here

        # config = get_config('kucoin').get('kucoin', {})
        #
        # if ('key_id' in config) and ('key_secret' in config):
        #     self._key_id = config['key_id']
        #     self._key_secret = config['key_secret']
        #
        # else:
        #     self._key_id = None
        #     self._key_secret = None

    async def symbol_info(
        self,
        sym: str = None,  
    ) -> dict[str, Any]:

        if self._pairs:
            return self._pairs

        entries = await self.request("GET", "/symbols")
        if not entries:
            raise SymbolNotFound(f'{sym} not found')
        
        syms = {item['name']: item for item in entries}
        return syms
 

    async def request(self, action: Literal["POST", "GET", "PUT", "DELETE"], route: str):
        api_url = f"https://api.kucoin.com/api/v2{route}"
        res = await asks.request(action, api_url)
        return res.json()['data']

    async def cache_symbols(
        self,
    ) -> dict:
        if not self._pairs:
            self._pairs = await self.symbol_info()

        return self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = 30,
    ) -> dict[str, Any]:
        data = await self.symbol_info()

        matches = fuzzy.extractBests(pattern, data, score_cutoff=35, limit=limit)
        # repack in dict form
        return {item[0]["instrument_name"].lower(): item[0] for item in matches}


@acm
async def get_client():
    client = Client()
    # Do we need to open a nursery here?
    await client.cache_symbols()
    yield client


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
):
    async with open_cached_client("kucoin") as client:
        # load all symbols locally for fast search
        cache = await client.cache_symbols()
        await ctx.started()

        # async with ctx.open_stream() as stream:
        #     async for pattern in stream:
        #         # repack in dict form
        #         await stream.send(await client.search_symbols(pattern))


async def stream_quotes(
    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,
    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,
):
    return await mk_stream_quotes(
        KUCOIN,
        [L2_BOOK],
        send_chan,
        symbols,
        feed_is_live,
        loglevel,
        task_status,
    )
