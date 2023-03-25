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
Kucoin broker backend

"""

from random import randint
from typing import Any, Callable, Optional, Literal, AsyncGenerator
from contextlib import asynccontextmanager as acm
from datetime import datetime
import time
import math
import base64
import hmac
import hashlib
import wsproto
from uuid import uuid4

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
from ..data.types import Struct
from ..data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)

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


class KucoinMktPair(Struct, frozen=True):
    '''
    Kucoin's pair format

    '''
    baseCurrency: str
    baseIncrement: float
    baseMaxSize: float
    baseMinSize: float
    enableTrading: bool
    feeCurrency: str
    isMarginEnabled: bool
    market: str
    minFunds: float
    name: str
    priceIncrement: float
    priceLimitRate: float
    quoteCurrency: str
    quoteIncrement: float
    quoteMaxSize: float
    quoteMinSize: float
    symbol: str


class AccountTrade(Struct, frozen=True):
    '''
    Historical trade format

    '''
    id: str
    currency: str
    amount: float
    fee: float
    balance: float
    accountType: str
    bizType: str
    direction: Literal["in", "out"]
    createdAt: float
    context: list[str]


class AccountResponse(Struct, frozen=True):
    currentPage: int
    pageSize: int
    totalNum: int
    totalPage: int
    items: list[AccountTrade]


class KucoinTrade(Struct, frozen=True):
    '''
    Real-time trade format

    '''
    bestAsk: float
    bestAskSize: float
    bestBid: float
    bestBidSize: float
    price: float
    sequence: float
    size: float
    time: float


class BrokerConfig(Struct, frozen=True):
    key_id: str
    key_secret: str
    key_passphrase: str


class KucoinTradeMsg(Struct, frozen=True):
    type: str
    topic: str
    subject: str
    data: list[KucoinTrade]


def get_config() -> BrokerConfig | None:
    conf, path = config.load()

    section = conf.get("kucoin")

    if section is None:
        log.warning("No config section found for kucoin in config")
        return None

    return BrokerConfig(**section)


class Client:
    def __init__(self) -> None:
        self._pairs: dict[str, KucoinMktPair] = {}
        self._bars: list[list[float]] = []
        self._key_id: str
        self._key_secret: str
        self._key_passphrase: str
        self._authenticated: bool = False

        config: BrokerConfig | None = get_config()

        if (
            config and float(config.key_id) and config.key_secret and config.key_passphrase
        ):
            self._authenticated = True
            self._key_id = config.key_id
            self._key_secret = config.key_secret
            self._key_passphrase = config.key_passphrase

    def _gen_auth_req_headers(
        self,
        action: Literal["POST", "GET"],
        endpoint: str,
        api_v: str = "v2",
    ) -> dict[str, str | bytes]:
        '''
        Generate authenticated request headers
        https://docs.kucoin.com/#authentication

        '''
        now = int(time.time() * 1000)
        path = f"/api/{api_v}{endpoint}"
        str_to_sign = str(now) + action + path

        signature = base64.b64encode(
            hmac.new(
                self._key_secret.encode("utf-8"),
                str_to_sign.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        )

        passphrase = base64.b64encode(
            hmac.new(
                self._key_secret.encode("utf-8"),
                self._key_passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        )

        return {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": str(now),
            "KC-API-KEY": self._key_id,
            "KC-API-PASSPHRASE": passphrase,
            # XXX: Even if using the v1 api - this stays the same
            "KC-API-KEY-VERSION": "2",
        }

    async def _request(
        self,
        action: Literal["POST", "GET"],
        endpoint: str,
        api_v: str = "v2",
        headers: dict = {},
    ) -> Any:
        '''
        Generic request wrapper for Kucoin API

        '''
        if self._authenticated:
            headers = self._gen_auth_req_headers(action, endpoint, api_v)

        api_url = f"https://api.kucoin.com/api/{api_v}{endpoint}"

        res = await asks.request(action, api_url, headers=headers)

        if "data" in res.json():
            return res.json()["data"]
        else:
            log.error(f'Error making request to {api_url} -> {res.json()["msg"]}')
            return res.json()["msg"]

    async def _get_ws_token(
        self,
        private: bool = False
    ) -> tuple[str, int] | None:
        '''
        Fetch ws token needed for sub access

        '''
        token_type = "private" if private else "public"
        data: dict[str, Any] | None = await self._request(
            "POST",
            f"/bullet-{token_type}",
            "v1"
        )

        if data and "token" in data:
            ping_interval: int = data["instanceServers"][0]["pingInterval"]
            return data["token"], ping_interval
        elif data:
            log.error(
                f'Error making request for Kucoin ws token -> {data.json()["msg"]}'
            )

    async def _get_pairs(
        self,
    ) -> dict[str, KucoinMktPair]:
        if self._pairs:
            return self._pairs

        entries = await self._request("GET", "/symbols")
        syms = {item["name"]: KucoinMktPair(**item) for item in entries}
        return syms

    async def cache_pairs(
        self,
        normalize: bool = True,
    ) -> dict[str, KucoinMktPair]:
        '''
        Get cached pairs and convert keyed symbols into fqsns if ya want

        '''
        if not self._pairs:
            self._pairs = await self._get_pairs()
        if normalize:
            self._pairs = self._normalize_pairs(self._pairs)
        return self._pairs

    def _normalize_pairs(
        self, pairs: dict[str, KucoinMktPair]
    ) -> dict[str, KucoinMktPair]:
        """
        Map kucoin pairs to fqsn strings

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
    ) -> dict[str, KucoinMktPair]:
        data = await self._get_pairs()

        matches = fuzzy.extractBests(pattern, data, score_cutoff=35, limit=limit)
        # repack in dict form
        return {kucoin_sym_to_fqsn(item[0].name): item[0] for item in matches}

    async def last_trades(self, sym: str) -> list[AccountTrade]:
        trades = await self._request("GET", f"/accounts/ledgers?currency={sym}", "v1")
        trades = AccountResponse(**trades)
        return trades.items

    async def _get_bars(
        self,
        fqsn: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        limit: int = 1000,
        as_np: bool = True,
        type: str = "1min",
    ) -> np.ndarray:
        '''
        Get OHLC data and convert to numpy array for perffff

        '''
        # Generate generic end and start time if values not passed
        if end_dt is None:
            end_dt = pendulum.now("UTC").add(minutes=1)

        if start_dt is None:
            start_dt = end_dt.start_of("minute").subtract(minutes=limit)

        # Format datetime to unix timestamp
        start_dt = math.trunc(time.mktime(start_dt.timetuple()))
        end_dt = math.trunc(time.mktime(end_dt.timetuple()))
        kucoin_sym = fqsn_to_kucoin_sym(fqsn, self._pairs)

        url = f"/market/candles?type={type}&symbol={kucoin_sym}&startAt={start_dt}&endAt={end_dt}"
        bars = []

        for i in range(10):

            data = await self._request(
                "GET",
                url,
                api_v="v1",
            )

            if not isinstance(data, list):
                # Do a gradual backoff if Kucoin is rate limiting us
                backoff_interval = i + (randint(0, 1000) / 1000)
                log.warn(f'History call failed, backing off for {backoff_interval}s')
                await trio.sleep(backoff_interval)
            else:
                bars = data
                break

        # Map to OHLC values to dict then to np array
        new_bars = []
        for i, bar in enumerate(bars[::-1]):

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
                    case _:
                        row.append(float(value))

            new_bars.append(tuple(row))

        self._bars = array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
        return array


def fqsn_to_kucoin_sym(
    fqsn: str,
    pairs: dict[str, KucoinMktPair]


) -> str:
    pair_data = pairs[fqsn]
    return pair_data.baseCurrency + "-" + pair_data.quoteCurrency


def kucoin_sym_to_fqsn(sym: str) -> str:
    return sym.lower().replace("-", "")


@ acm
async def get_client() -> AsyncGenerator[Client, None]:

    client = Client()
    await client.cache_pairs()

    yield client


@ tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> None:
    async with open_cached_client("kucoin") as client:
        # load all symbols locally for fast search
        await client.cache_pairs()
        await ctx.started()

        async with ctx.open_stream() as stream:
            async for pattern in stream:
                await stream.send(await client.search_symbols(pattern))
                log.info("Kucoin symbol search opened")


async def stream_quotes(
    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,
    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,
) -> None:
    '''
    Required piker api to stream real-time data.
    Where the rubber hits the road baby

    '''
    connect_id = str(uuid4())

    async with open_cached_client("kucoin") as client:
        log.info("Starting up quote stream")
        # loop through symbols and sub to feedz
        for sym in symbols:

            token, ping_interval = await client._get_ws_token()
            pairs = await client.cache_pairs()
            kucoin_sym = pairs[sym].symbol

            init_msgs = {
                # pass back token, and bool, signalling if we're the writer
                # and that history has been written
                sym: {
                    "symbol_info": {
                        "asset_type": "crypto",
                        "price_tick_size": 0.0005,
                        "lot_tick_size": 0.1,
                    },
                    "shm_write_opts": {"sum_tick_vml": False},
                    "fqsn": sym,
                },
            }

            @acm
            async def subscribe(ws: wsproto.WSConnection):

                @acm
                async def open_ping_task(ws: wsproto.WSConnection):
                    '''
                    Spawn a non-blocking task that pings the ws 
                    server every ping_interval so Kucoin doesn't drop 
                    our connection

                    '''
                    async with trio.open_nursery() as n:

                        async def ping_server():
                            while True:
                                await trio.sleep((ping_interval - 1000) / 1000)
                                await ws.send_msg({"id": connect_id, "type": "ping"})

                        n.start_soon(ping_server)

                        yield ws

                        n.cancel_scope.cancel()

                # Spawn the ping task here
                async with open_ping_task(ws) as ws:
                    # subscribe to market feedz here
                    log.info(f'Subscribing to {kucoin_sym} feed')
                    l1_sub = make_sub(kucoin_sym, connect_id)
                    await ws.send_msg(l1_sub)

                    yield

                    # unsub
                    if ws.connected():
                        log.info(f'Unsubscribing to {kucoin_sym} feed')
                        await ws.send_msg(
                            {
                                "id": connect_id,
                                "type": "unsubscribe",
                                "topic": f"/market/ticker:{sym}",
                                "privateChannel": False,
                                "response": True,
                            }
                        )

            async with open_autorecon_ws(
                f"wss://ws-api-spot.kucoin.com/?token={token}&[connectId={connect_id}]",
                fixture=subscribe,
            ) as ws:
                msg_gen = stream_messages(ws, sym)
                typ, quote = await msg_gen.__anext__()
                #
                while typ != "trade":
                    # TODO: use ``anext()`` when it lands in 3.10!
                    typ, quote = await msg_gen.__anext__()

                task_status.started((init_msgs, quote))
                feed_is_live.set()

                async for typ, msg in msg_gen:
                    await send_chan.send({sym: msg})


def make_sub(sym, connect_id) -> dict[str, str | bool]:
    return {
        "id": connect_id,
        "type": "subscribe",
        "topic": f"/market/ticker:{sym}",
        "privateChannel": False,
        "response": True,
    }


async def stream_messages(
    ws: NoBsWs,
    sym: str
) -> AsyncGenerator[NoBsWs, dict]:

    timeouts = 0

    while True:
        with trio.move_on_after(3) as cs:
            msg = await ws.recv_msg()
        if cs.cancelled_caught:
            timeouts += 1
            if timeouts > 2:
                log.error("kucoin feed is sh**ing the bed... rebooting...")
                await ws._connect()

            continue

        if "subject" in msg and msg["subject"] == "trade.ticker":

            trade_msg = KucoinTradeMsg(**msg)
            trade_data = KucoinTrade(**trade_msg.data)

            yield "trade", {
                "symbol": sym,
                "last": trade_data.price,
                "brokerd_ts": trade_data.time,
                "ticks": [
                    {
                        "type": "trade",
                        "price": float(trade_data.price),
                        "size": float(trade_data.size),
                        "broker_ts": trade_data.time,
                    }
                ],
            }

        else:
            continue


@acm
async def open_history_client(
    symbol: str,
    type: str = "1m",
) -> AsyncGenerator[Callable, None]:
    async with open_cached_client("kucoin") as client:
        log.info("Attempting to open kucoin history client")

        async def get_ohlc_history(

            timeframe: float,
            end_dt: datetime | None = None,
            start_dt: datetime | None = None,

        ) -> tuple[np.ndarray, datetime | None, datetime | None]:  # start  # end

            if timeframe != 60:
                raise DataUnavailable("Only 1m bars are supported")

            array = await client._get_bars(
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
            log.info('History succesfully fetched baby')
            return array, start_dt, end_dt

        yield get_ohlc_history, {"erlangs": 3, "rate": 3}
