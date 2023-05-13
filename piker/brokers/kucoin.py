# Copyright (C) (in stewardship for pikers)
# - Jared Goldman
# - Tyler Goodlet

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
Kucoin broker backend

'''

from contextlib import (
    asynccontextmanager as acm,
    aclosing,
)
from datetime import datetime
from decimal import Decimal
import base64
import hmac
import hashlib
import time
from functools import partial
from pprint import pformat
from typing import (
    Any,
    Callable,
    Literal,
    AsyncGenerator,
)
import wsproto
from uuid import uuid4

from fuzzywuzzy import process as fuzzy
from trio_typing import TaskStatus
import asks
from bidict import bidict
import numpy as np
import pendulum
import tractor
import trio

from piker.accounting._mktinfo import (
    Asset,
    digits_to_dec,
    MktPair,
)
from piker.data.validate import FeedInit
from piker import config
from piker._cacheables import (
    open_cached_client,
    async_lifo_cache,
)
from piker.log import get_logger
from piker.data.types import Struct
from piker.data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)
from ._util import DataUnavailable

log = get_logger(__name__)

_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('bar_wap', float),  # will be zeroed by sampler if not filled
]


class KucoinMktPair(Struct, frozen=True):
    '''
    Kucoin's pair format:
    https://docs.kucoin.com/#get-symbols-list

    '''
    baseCurrency: str
    baseIncrement: float

    @property
    def price_tick(self) -> Decimal:
        return Decimal(str(self.baseIncrement))

    baseMaxSize: float
    baseMinSize: float

    @property
    def size_tick(self) -> Decimal:
        return Decimal(str(self.baseMinSize))

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
    symbol: str  # our bs_mktid, kucoin's internal id


class AccountTrade(Struct, frozen=True):
    '''
    Historical trade format:
    https://docs.kucoin.com/#get-account-ledgers

    '''
    id: str
    currency: str
    amount: float
    fee: float
    balance: float
    accountType: str
    bizType: str
    direction: Literal['in', 'out']
    createdAt: float
    context: list[str]


class AccountResponse(Struct, frozen=True):
    '''
    https://docs.kucoin.com/#get-account-ledgers

    '''
    currentPage: int
    pageSize: int
    totalNum: int
    totalPage: int
    items: list[AccountTrade]


class KucoinTrade(Struct, frozen=True):
    '''
    Real-time trade format:
    https://docs.kucoin.com/#symbol-ticker

    '''
    bestAsk: float
    bestAskSize: float
    bestBid: float
    bestBidSize: float
    price: float
    sequence: float
    size: float
    time: float


class KucoinL2(Struct, frozen=True):
    '''
    Real-time L2 order book format:
    https://docs.kucoin.com/#level2-5-best-ask-bid-orders

    '''

    asks: list[list[float]]
    bids: list[list[float]]
    timestamp: float


class KucoinMsg(Struct, frozen=True):
    '''
    Generic outer-wrapper for any Kucoin ws msg

    '''
    type: str
    topic: str
    subject: str
    data: list[KucoinTrade | KucoinL2]


class Currency(Struct, frozen=True):
    '''
    Currency (asset) info:
    https://docs.kucoin.com/#get-currencies

    '''
    currency: str
    name: str
    fullName: str
    precision: int
    confirms: int
    contractAddress: str
    withdrawalMinSize: str
    withdrawalMinFee: str
    isWithdrawEnabled: bool
    isDepositEnabled: bool
    isMarginEnabled: bool
    isDebitEnabled: bool


class BrokerConfig(Struct, frozen=True):
    key_id: str
    key_secret: str
    key_passphrase: str


def get_config() -> BrokerConfig | None:
    conf, _ = config.load()

    section = conf.get('kucoin')

    if section is None:
        log.warning('No config section found for kucoin in config')
        return None

    return BrokerConfig(**section).copy()


class Client:
    def __init__(self) -> None:
        self._config: BrokerConfig | None = get_config()
        self._pairs: dict[str, KucoinMktPair] = {}
        self._fqmes2mktids: bidict[str, str] = bidict()
        self._bars: list[list[float]] = []
        self._currencies: dict[str, Currency] = {}

    def _gen_auth_req_headers(
        self,
        action: Literal['POST', 'GET'],
        endpoint: str,
        api: str = 'v2',

    ) -> dict[str, str | bytes]:
        '''
        Generate authenticated request headers
        https://docs.kucoin.com/#authentication

        '''

        if not self._config:
            raise ValueError(
                'No config found when trying to send authenticated request')

        str_to_sign = (
            str(int(time.time() * 1000))
            + action + f'/api/{api}/{endpoint.lstrip("/")}'
        )

        signature = base64.b64encode(
            hmac.new(
                self._config.key_secret.encode('utf-8'),
                str_to_sign.encode('utf-8'),
                hashlib.sha256,
            ).digest()
        )

        passphrase = base64.b64encode(
            hmac.new(
                self._config.key_secret.encode('utf-8'),
                self._config.key_passphrase.encode('utf-8'),
                hashlib.sha256,
            ).digest()
        )

        return {
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': str(pendulum.now().int_timestamp * 1000),
            'KC-API-KEY': self._config.key_id,
            'KC-API-PASSPHRASE': passphrase,
            # XXX: Even if using the v1 api - this stays the same
            'KC-API-KEY-VERSION': '2',
        }

    async def _request(
        self,
        action: Literal['POST', 'GET'],
        endpoint: str,
        api: str = 'v2',
        headers: dict = {},
    ) -> Any:
        '''
        Generic request wrapper for Kucoin API

        '''
        if self._config:
            headers = self._gen_auth_req_headers(
                action,
                endpoint,
                api,
            )

        api_url = f'https://api.kucoin.com/api/{api}/{endpoint}'

        res = await asks.request(action, api_url, headers=headers)

        json = res.json()
        if 'data' in json:
            return json['data']
        else:
            log.error(
                f'Error making request to {api_url} ->\n'
                f'{pformat(res)}'
            )
            return json['msg']

    async def _get_ws_token(
        self,
        private: bool = False,
    ) -> tuple[str, int] | None:
        '''
        Fetch ws token needed for sub access:
        https://docs.kucoin.com/#apply-connect-token
        returns a token and the interval we must ping
        the server at to keep the connection alive

        '''
        token_type = 'private' if private else 'public'
        try:
            data: dict[str, Any] | None = await self._request(
                'POST',
                endpoint=f'bullet-{token_type}',
                api='v1'
            )
        except Exception as e:
            log.error(
                f'Error making request for Kucoin ws token -> {str(e)}')
            return None

        if data and 'token' in data:
            # ping_interval is in ms
            ping_interval: int = data['instanceServers'][0]['pingInterval']
            return data['token'], ping_interval
        elif data:
            log.error(
                'Error making request for Kucoin ws token'
                f'{data.json()["msg"]}'
            )

    async def get_currencies(
        self,
        update: bool = False,
    ) -> dict[str, Currency]:
        '''
        Retrieve all "currency" info:
        https://docs.kucoin.com/#get-currencies

        We use this for creating piker-interal ``Asset``s.

        '''
        if (
            not self._currencies
            or update
        ):
            currencies: dict[str, Currency] = {}
            entries: list[dict] = await self._request(
                'GET',
                api='v1',
                endpoint='currencies',
            )
            for entry in entries:
                curr = Currency(**entry).copy()
                currencies[curr.name] = curr

            self._currencies.update(currencies)

        return self._currencies

    async def _get_pairs(
        self,
    ) -> tuple[
        dict[str, KucoinMktPair],
        bidict[str, KucoinMktPair],
    ]:
        entries = await self._request('GET', 'symbols')
        log.info(f' {len(entries)} Kucoin market pairs fetched')

        pairs: dict[str, KucoinMktPair] = {}
        fqmes2mktids: bidict[str, str] = bidict()
        for item in entries:
            pair = pairs[item['name']] = KucoinMktPair(**item)
            fqmes2mktids[
                item['name'].lower().replace('-', '')
            ] = pair.name

        return pairs, fqmes2mktids

    async def cache_pairs(
        self,
        update: bool = False,

    ) -> dict[str, KucoinMktPair]:
        '''
        Get request all market pairs and store in a local cache.

        Also create a table of piker style fqme -> kucoin symbols.

        '''
        if (
            not self._pairs
            or update
        ):
            pairs, fqmes = await self._get_pairs()
            self._pairs.update(pairs)
            self._fqmes2mktids.update(fqmes)

        return self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = 30,

    ) -> dict[str, KucoinMktPair]:
        '''
        Use fuzzy search to match against all market names.

        '''
        data = await self.cache_pairs()

        matches = fuzzy.extractBests(
            pattern, data, score_cutoff=35, limit=limit
        )
        # repack in dict form
        return {item[0].name: item[0] for item in matches}

    async def last_trades(self, sym: str) -> list[AccountTrade]:
        trades = await self._request(
            'GET',
            endpoint=f'accounts/ledgers?currency={sym}',
            api='v1'
        )
        trades = AccountResponse(**trades)
        return trades.items

    async def _get_bars(
        self,
        fqme: str,

        start_dt: datetime | None = None,
        end_dt: datetime | None = None,
        limit: int = 1000,
        as_np: bool = True,
        type: str = '1min',

    ) -> np.ndarray:
        '''
        Get OHLC data and convert to numpy array for perffff:
        https://docs.kucoin.com/#get-klines

        Kucoin bar data format:
        [
             '1545904980',             //Start time of the candle cycle 0
             '0.058',                  //opening price 1
             '0.049',                  //closing price 2
             '0.058',                  //highest price 3
             '0.049',                  //lowest price 4
             '0.018',                  //Transaction volume 5
             '0.000945'                //Transaction amount 6
        ],

        piker ohlc numpy array format:
        [
            ('index', int),
            ('time', int),
            ('open', float),
            ('high', float),
            ('low', float),
            ('close', float),
            ('volume', float),
            ('bar_wap', float),  # will be zeroed by sampler if not filled
        ]

        '''
        # Generate generic end and start time if values not passed
        # Currently gives us 12hrs of data
        if end_dt is None:
            end_dt = pendulum.now('UTC').add(minutes=1)

        if start_dt is None:
            start_dt = end_dt.start_of(
                'minute').subtract(minutes=limit)

        start_dt = int(start_dt.timestamp())
        end_dt = int(end_dt.timestamp())

        kucoin_sym = self._fqmes2mktids[fqme]

        url = (
            f'market/candles?type={type}'
            f'&symbol={kucoin_sym}'
            f'&startAt={start_dt}'
            f'&endAt={end_dt}'
        )

        for i in range(10):
            data: list[list[str]] | dict = await self._request(
                'GET',
                url,
                api='v1',
            )

            if not isinstance(data, list):
                # Do a gradual backoff if Kucoin is rate limiting us
                backoff_interval = i
                log.warn(
                    f'History call failed, backing off for {backoff_interval}s'
                )
                await trio.sleep(backoff_interval)
            else:
                bars: list[list[str]] = data
                break

        new_bars = []
        reversed_bars = bars[::-1]

        # Convert from kucoin format to piker format
        for i, bar in enumerate(reversed_bars):
            new_bars.append(
                (
                    # index
                    i,
                    # time
                    int(bar[0]),
                    # open
                    float(bar[1]),
                    # high
                    float(bar[3]),
                    # low
                    float(bar[4]),
                    # close
                    float(bar[2]),
                    # volume
                    float(bar[5]),
                    # bar_wap
                    0.0,
                )
            )

        array = np.array(
            new_bars, dtype=_ohlc_dtype) if as_np else bars
        return array


def fqme_to_kucoin_sym(
    fqme: str,
    pairs: dict[str, KucoinMktPair],

) -> str:
    pair_data = pairs[fqme]
    return pair_data.baseCurrency + '-' + pair_data.quoteCurrency


@acm
async def get_client() -> AsyncGenerator[Client, None]:
    client = Client()

    async with trio.open_nursery() as n:
        n.start_soon(client.cache_pairs)
        await client.get_currencies()

    yield client


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> None:
    async with open_cached_client('kucoin') as client:
        # load all symbols locally for fast search
        await client.cache_pairs()
        await ctx.started()

        async with ctx.open_stream() as stream:
            async for pattern in stream:
                await stream.send(await client.search_symbols(pattern))
                log.info('Kucoin symbol search opened')


@acm
async def open_ping_task(
    ws: wsproto.WSConnection,
    ping_interval, connect_id
) -> AsyncGenerator[None, None]:
    '''
    Spawn a non-blocking task that pings the ws
    server every ping_interval so Kucoin doesn't drop
    our connection

    '''
    async with trio.open_nursery() as n:
        # TODO: cache this task so it's only called once
        async def ping_server():
            while True:
                await trio.sleep((ping_interval - 1000) / 1000)
                await ws.send_msg({'id': connect_id, 'type': 'ping'})

        log.info('Starting ping task for kucoin ws connection')
        n.start_soon(ping_server)

        yield

        n.cancel_scope.cancel()


@async_lifo_cache()
async def get_mkt_info(
    fqme: str,

) -> tuple[MktPair, KucoinMktPair]:
    '''
    Query for and return a `MktPair` and `KucoinMktPair`.

    '''
    async with open_cached_client('kucoin') as client:
        # split off any fqme broker part
        bs_fqme, _, broker = fqme.partition('.')

        pairs: dict[str, KucoinMktPair] = await client.cache_pairs()
        bs_mktid: str = client._fqmes2mktids[bs_fqme]
        pair: KucoinMktPair = pairs[bs_mktid]
        assert bs_mktid == pair.symbol

        # pair: KucoinMktPair = await client.pair_info(pair_str)
        assets: dict[str, Currency] = client._currencies

        # TODO: maybe just do this processing in
        # a .get_assets() method (see kraken)?
        src: Currency = assets[pair.quoteCurrency]
        src_asset = Asset(
            name=src.name,
            atype='crypto_currency',
            tx_tick=digits_to_dec(src.precision),
            info=src.to_dict(),
        )
        dst: Currency = assets[pair.baseCurrency]
        dst_asset = Asset(
            name=dst.name,
            atype='crypto_currency',
            tx_tick=digits_to_dec(dst.precision),
            info=dst.to_dict(),
        )

        mkt = MktPair(
            dst=dst_asset,
            src=src_asset,

            price_tick=pair.price_tick,
            size_tick=pair.size_tick,
            bs_mktid=bs_mktid,

            broker='kucoin',
        )
        return mkt, pair


async def stream_quotes(
    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,

    task_status: TaskStatus[
        tuple[dict, dict]
    ] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Required piker api to stream real-time data.
    Where the rubber hits the road baby

    '''
    init_msgs: list[FeedInit] = []

    async with open_cached_client('kucoin') as client:

        log.info(f'Starting up quote stream(s) for {symbols}')
        for sym_str in symbols:
            mkt, pair = await get_mkt_info(sym_str)
            init_msgs.append(
                FeedInit(
                    mkt_info=mkt,
                    shm_write_opts={
                        'sum_tick_vml': False,
                    },
                )
            )

        ws: NoBsWs
        token, ping_interval = await client._get_ws_token()
        connect_id = str(uuid4())
        async with (
            open_autorecon_ws(
                (
                    f'wss://ws-api-spot.kucoin.com/?'
                    f'token={token}&[connectId={connect_id}]'
                ),
                fixture=partial(
                    subscribe,
                    connect_id=connect_id,
                    bs_mktid=pair.symbol,
                ),
            ) as ws,
            open_ping_task(ws, ping_interval, connect_id),
            aclosing(stream_messages(ws, sym_str)) as msg_gen,
        ):
            typ, quote = await anext(msg_gen)

            while typ != 'trade':
                # take care to not unblock here until we get a real
                # trade quote
                typ, quote = await anext(msg_gen)

            task_status.started((init_msgs, quote))
            feed_is_live.set()

            async for typ, msg in msg_gen:
                await send_chan.send({sym_str: msg})


@acm
async def subscribe(
    ws: NoBsWs,
    connect_id,
    bs_mktid,

    # subs are filled in with `bs_mktid` from avbove
    topics: list[str] = [
        '/market/ticker:{bs_mktid}',  # clearing events
        '/spotMarket/level2Depth5:{bs_mktid}',  # level 2
    ],

) -> AsyncGenerator[None, None]:

    eps: list[str] = []
    for topic in topics:
        ep: str = topic.format(bs_mktid=bs_mktid)
        eps.append(ep)
        await ws.send_msg(
            {
                'id': connect_id,
                'type': 'subscribe',
                'topic': ep,
                # 'topic': f'/spotMarket/level2Depth5:{bs_mktid}',
                'privateChannel': False,
                'response': True,
            }
        )

    for _ in topics:
        ack_msg = await ws.recv_msg()
        log.info(f'Sub ACK: {ack_msg}')

    yield

    # unsub
    if ws.connected():
        log.info(f'Unsubscribing to {bs_mktid} feed')
        for ep in eps:
            await ws.send_msg(
                {
                    'id': connect_id,
                    'type': 'unsubscribe',
                    'topic': ep,
                    'privateChannel': False,
                    'response': True,
                }
            )


async def stream_messages(
    ws: NoBsWs,
    sym: str,

) -> AsyncGenerator[tuple[str, dict], None]:
    '''
    Core (live) feed msg handler: relay market events
    to the piker-ized tick-stream format.

    '''
    last_trade_ts: float = 0

    async for dict_msg in ws:
        if 'subject' not in dict_msg:
            log.warn(f'Unhandled message: {dict_msg}')
            continue

        msg = KucoinMsg(**dict_msg)
        match msg:
            case KucoinMsg(
                subject='trade.ticker',
            ):
                trade_data = KucoinTrade(**msg.data)

                # XXX: Filter for duplicate messages as ws feed will
                # send duplicate market state
                # https://docs.kucoin.com/#level2-5-best-ask-bid-orders
                if trade_data.time == last_trade_ts:
                    continue

                last_trade_ts = trade_data.time

                yield 'trade', {
                    'symbol': sym,
                    'last': trade_data.price,
                    'brokerd_ts': last_trade_ts,
                    'ticks': [
                        {
                            'type': 'trade',
                            'price': float(trade_data.price),
                            'size': float(trade_data.size),
                            'broker_ts': last_trade_ts,
                        }
                    ],
                }

            case KucoinMsg(
                subject='level2',
            ):
                l2_data = KucoinL2(**msg.data)
                first_ask = l2_data.asks[0]
                first_bid = l2_data.bids[0]
                yield 'l1', {
                    'symbol': sym,
                    'ticks': [
                        {
                            'type': 'bid',
                            'price': float(first_bid[0]),
                            'size': float(first_bid[1]),
                        },
                        {
                            'type': 'bsize',
                            'price': float(first_bid[0]),
                            'size': float(first_bid[1]),
                        },
                        {
                            'type': 'ask',
                            'price': float(first_ask[0]),
                            'size': float(first_ask[1]),
                        },
                        {
                            'type': 'asize',
                            'price': float(first_ask[0]),
                            'size': float(first_ask[1]),
                        },
                    ],
                }

            case _:
                log.warn(f'Unhandled message: {msg}')


@acm
async def open_history_client(
    symbol: str,
) -> AsyncGenerator[Callable, None]:
    async with open_cached_client('kucoin') as client:
        log.info('Attempting to open kucoin history client')

        async def get_ohlc_history(
            timeframe: float,
            end_dt: datetime | None = None,
            start_dt: datetime | None = None,
        ) -> tuple[
            np.ndarray, datetime
            | None, datetime
            | None
        ]:  # start  # end
            if timeframe != 60:
                raise DataUnavailable('Only 1m bars are supported')

            array = await client._get_bars(
                symbol,
                start_dt=start_dt,
                end_dt=end_dt,
            )

            times = array['time']

            if end_dt is None:
                inow = round(time.time())

                print(
                    f'difference in time between load and processing'
                    f'{inow - times[-1]}'
                )

            start_dt = pendulum.from_timestamp(times[0])
            end_dt = pendulum.from_timestamp(times[-1])

            log.info('History succesfully fetched baby')

            return array, start_dt, end_dt

        yield get_ohlc_history, {}
