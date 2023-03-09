# piker: trading gear for hackers
# Copyright (C) Jared Goldman (in stewardship for pikers)

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

from logging import warning
from typing import Any, Optional, Literal
from contextlib import asynccontextmanager as acm
from datetime import datetime
import time
import math
import base64
import hmac
import hashlib

import asks
import tractor
import trio
from trio_typing import TaskStatus
from fuzzywuzzy import process as fuzzy
import pendulum
import numpy as np

from piker._cacheables import open_cached_client
from piker.log import get_logger
from ._util import DataUnavailable
from piker.pp import config

_spawn_kwargs = {
    "infect_asyncio": True,
}

log = get_logger(__name__)
_ohlc_dtype = [
    ("index", int),
    ("time", int),
    ("open", float),
    ("high", float),
    ("low", float),
    ("close", float),
    ("volume", float),
    ("bar_wap", float),  # will be zeroed by sampler if not filled
]


class Client:
    def __init__(self) -> None:
        self._pairs: dict[str, any] = {}
        self._bars: list[list] = []
        self._key_id: str
        self._key_secret: str
        self._key_passphrase: str
        self._authenticated: bool = False

        config = get_config()
        breakpoint()
        if ("key_id" in config) and \
            ("key_secret" in config) and \
                ("key_passphrase" in config):
            self._authenticated = True
            self._key_id = config["key_id"]
            self._key_secret = config["key_secret"]
            self._key_passphrase = config["key_passphrase"]

    async def _request(
        self,
        action: Literal["POST", "GET", "PUT", "DELETE"],
        endpoint: str,
        api_v: str = "v2",
    ) -> Any:

        now = int(time.time() * 1000)
        path = f'/api/{api_v}{endpoint}'
        str_to_sign = str(now) + action + path
        headers = {}

        # Add headers to request if authenticated
        if self._authenticated:
            signature = base64.b64encode(
                hmac.new(
                    self._key_secret.encode('utf-8'),
                    str_to_sign.encode('utf-8'),
                    hashlib.sha256
                ).digest()
            )

            passphrase = base64.b64encode(
                hmac.new(
                    self._key_secret.encode('utf-8'),
                    self._key_passphrase.encode('utf-8'),
                    hashlib.sha256
                ).digest()
            )

            headers = {
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": str(now),
                "KC-API-KEY": self._key_id,
                "KC-API-PASSPHRASE": passphrase,
                "KC-API-KEY-VERSION": "2"
            }

        api_url = f"https://api.kucoin.com{path}"
        res = await asks.request(action, api_url, headers=headers)
        # breakpoint()
        if "data" in res.json():
            return res.json()["data"]
        else:
            print(f'KUCOIN ERROR: {res.json()["msg"]}')
            breakpoint()

    async def get_pairs(
        self,
    ) -> dict[str, Any]:
        if self._pairs:
            return self._pairs

        entries = await self._request("GET", "/symbols")
        syms = {item["name"]: item for item in entries}
        return syms

    async def cache_pairs(
        self,
        normalize: bool = True,
    ) -> dict[str, any]:
        if not self._pairs:
            self._pairs = await self.get_pairs()
        if normalize:
            self._pairs = self.normalize_pairs(self._pairs)
        return self._pairs

    def normalize_pairs(self, pairs: dict[str, any]) -> dict[str, any]:
        """
        Map crypfeeds symbols to fqsn strings

        """
        norm_pairs = {}

        for key, value in pairs.items():
            fqsn = key.lower().replace("-", "")
            norm_pairs[fqsn] = value

        return norm_pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = 30,
    ) -> dict[str, Any]:
        data = await self.get_pairs()

        matches = fuzzy.extractBests(pattern, data, score_cutoff=35, limit=limit)
        # repack in dict form
        return {item[0]["name"].lower(): item[0] for item in matches}

    async def last_trades(self, sym: str):
        trades = await self._request("GET", f"/accounts/ledgers?currency={sym}", "v1")
        return trades.items

    async def get_bars(
        self,
        fqsn: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        limit: int = 1000,
        as_np: bool = True,
        type: str = "1min",
    ):
        if len(self._bars):
            return self._bars

        if end_dt is None:
            end_dt = pendulum.now("UTC").add(minutes=1)

        if start_dt is None:
            start_dt = end_dt.start_of("minute").subtract(minutes=limit)

        # Format datetime to unix
        start_dt = math.trunc(time.mktime(start_dt.timetuple()))
        end_dt = math.trunc(time.mktime(end_dt.timetuple()))
        kucoin_sym = fqsn_to_cf_sym(fqsn, self._pairs)
        url = f"/market/candles?type={type}&symbol={kucoin_sym}&startAt={start_dt}&endAt={end_dt}"

        bars = await self._request(
            "GET",
            url,
            api_v="v1",
        )

        new_bars = []
        for i, bar in enumerate(bars[::-1]):
            # TODO: implement struct/typecasting/validation here

            data = {
                "index": i,
                "time": bar[0],
                "open": bar[1],
                "close": bar[2],
                "high": bar[3],
                "low": bar[4],
                "volume": bar[5],
                "amount": bar[6],
                "bar_wap": 0.0,
            }

            row = []
            for j, (field_name, field_type) in enumerate(_ohlc_dtype):
                value = data[field_name]
                match field_name:
                    case "index" | "time":
                        row.append(int(value))
                    # case 'time':
                    # dt_from_unix_ts = datetime.utcfromtimestamp(int(value))
                    # # convert unix time to epoch seconds
                    # row.append(int(dt_from_unix_ts.timestamp()))
                    case _:
                        row.append(float(value))

            new_bars.append(tuple(row))

        self._bars = array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
        return array


def fqsn_to_cf_sym(fqsn: str, pairs: dict[str, any]) -> str:
    pair_data = pairs[fqsn]
    return pair_data["baseCurrency"] + "-" + pair_data["quoteCurrency"]


def get_config() -> dict[str, Any]:
    conf, path = config.load()

    section = conf.get('kucoin')

    # TODO: document why we send this, basically because logging params for cryptofeed
    conf["log"] = {}
    conf["log"]["disabled"] = True

    if section is None:
        log.warning("No config section found for deribit in kucoin")

    return section


@acm
async def get_client():
    client = Client()
    # Do we need to open a nursery here?
    await client.cache_pairs()
    yield client


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
):
    async with open_cached_client("kucoin") as client:
        # load all symbols locally for fast search
        await client.cache_pairs()
        await ctx.started()

        async with ctx.open_stream() as stream:
            async for pattern in stream:
                # repack in dict form
                await stream.send(await client.search_symbols(pattern))


async def stream_quotes(
    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,
    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,
):
    sym = symbols[0]

    async with open_cached_client("kucoin") as client:
        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            sym: {
                "symbol_info": {
                    "asset_type": "option",
                    "price_tick_size": 0.0005,
                    "lot_tick_size": 0.1,
                },
                "shm_write_opts": {"sum_tick_vml": False},
                "fqsn": sym,
            },
        }

        last_trades = await client.last_trades(sym)


@acm
async def open_history_client(
    symbol: str,
    type: str = "1m",
):
    async with open_cached_client("kucoin") as client:
        # call bars on kucoin
        async def get_ohlc_history(
            timeframe: float,
            end_dt: datetime | None = None,
            start_dt: datetime | None = None,
        ) -> tuple[np.ndarray, datetime | None, datetime | None,]:  # start  # end
            if timeframe != 60:
                raise DataUnavailable("Only 1m bars are supported")

            array = await client.get_bars(
                symbol,
                start_dt=start_dt,
                end_dt=end_dt,
            )

            times = array["time"]

            if end_dt is None:
                inow = round(time.time())
                print(
                    f"difference in time between load and processing {inow - times[-1]}"
                )
                if (inow - times[-1]) > 60:
                    await tractor.breakpoint()

            start_dt = pendulum.from_timestamp(times[0])
            end_dt = pendulum.from_timestamp(times[-1])
            return array, start_dt, end_dt

        yield get_ohlc_history, {"erlangs": 3, "rate": 3}
