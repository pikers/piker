# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
Real-time and historical data feed endpoints.

'''
from contextlib import (
    asynccontextmanager as acm,
    aclosing,
)
from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Optional,
)
import time

from fuzzywuzzy import process as fuzzy
import numpy as np
import pendulum
from trio_typing import TaskStatus
import tractor
import trio

from piker.accounting._mktinfo import (
    Asset,
    MktPair,
)
from piker.brokers import (
    open_cached_client,
)
from piker._cacheables import (
    async_lifo_cache,
)
from piker.brokers._util import (
    BrokerError,
    DataThrottle,
    DataUnavailable,
)
from piker.data.types import Struct
from piker.data.validate import FeedInit
from piker.data._web_bs import open_autorecon_ws, NoBsWs
from .api import (
    log,
    Client,
    Pair,
)


class OHLC(Struct):
    '''
    Description of the flattened OHLC quote format.

    For schema details see:
        https://docs.kraken.com/websockets/#message-ohlc

    '''
    chan_id: int  # internal kraken id
    chan_name: str  # eg. ohlc-1  (name-interval)
    pair: str  # fx pair
    time: float  # Begin time of interval, in seconds since epoch
    etime: float  # End time of interval, in seconds since epoch
    open: float  # Open price of interval
    high: float  # High price within interval
    low: float  # Low price within interval
    close: float  # Close price of interval
    vwap: float  # Volume weighted average price within interval
    volume: float  # Accumulated volume **within interval**
    count: int  # Number of trades within interval
    # (sampled) generated tick data
    ticks: list[Any] = []


async def stream_messages(
    ws: NoBsWs,
):
    '''
    Message stream parser and heartbeat handler.

    Deliver ws subscription messages as well as handle heartbeat logic
    though a single async generator.

    '''
    last_hb: float = 0

    async for msg in ws:
        match msg:
            case {'event': 'heartbeat'}:
                now = time.time()
                delay = now - last_hb
                last_hb = now

                # XXX: why tf is this not printing without --tl flag?
                log.debug(f"Heartbeat after {delay}")
                # print(f"Heartbeat after {delay}")

                continue

            case _:
                # passthrough sub msgs
                yield msg


async def process_data_feed_msgs(
    ws: NoBsWs,
):
    '''
    Parse and pack data feed messages.

    '''
    async with aclosing(stream_messages(ws)) as ws_stream:
        async for msg in ws_stream:
            match msg:
                case {
                    'errorMessage': errmsg
                }:
                    raise BrokerError(errmsg)

                case {
                    'event': 'subscriptionStatus',
                } as sub:
                    log.info(
                        'WS subscription is active:\n'
                        f'{sub}'
                    )
                    continue

                case [
                    chan_id,
                    *payload_array,
                    chan_name,
                    pair
                ]:
                    if 'ohlc' in chan_name:
                        ohlc = OHLC(
                            chan_id,
                            chan_name,
                            pair,
                            *payload_array[0]
                        )
                        ohlc.typecast()
                        yield 'ohlc', ohlc

                    elif 'spread' in chan_name:

                        bid, ask, ts, bsize, asize = map(
                            float, payload_array[0])

                        # TODO: really makes you think IB has a horrible API...
                        quote = {
                            'symbol': pair.replace('/', ''),
                            'ticks': [
                                {'type': 'bid', 'price': bid, 'size': bsize},
                                {'type': 'bsize', 'price': bid, 'size': bsize},

                                {'type': 'ask', 'price': ask, 'size': asize},
                                {'type': 'asize', 'price': ask, 'size': asize},
                            ],
                        }
                        yield 'l1', quote

                    # elif 'book' in msg[-2]:
                    #     chan_id, *payload_array, chan_name, pair = msg
                    #     print(msg)

                case {
                    'connectionID': conid,
                    'event': 'systemStatus',
                    'status': 'online',
                    'version': ver,
                }:
                    log.info(
                        f'Established {ver} ws connection with id: {conid}'
                    )
                    continue

                case _:
                    print(f'UNHANDLED MSG: {msg}')
                    # yield msg


def normalize(
    ohlc: OHLC,

) -> dict:
    quote = ohlc.to_dict()
    quote['broker_ts'] = quote['time']
    quote['brokerd_ts'] = time.time()
    quote['symbol'] = quote['pair'] = quote['pair'].replace('/', '')
    quote['last'] = quote['close']
    quote['bar_wap'] = ohlc.vwap

    # seriously eh? what's with this non-symmetry everywhere
    # in subscription systems...
    # XXX: piker style is always lowercases symbols.
    topic = quote['pair'].replace('/', '').lower()

    # print(quote)
    return topic, quote


@acm
async def open_history_client(
    mkt: MktPair,

) -> AsyncGenerator[Callable, None]:

    symbol: str = mkt.bs_fqme

    # TODO implement history getter for the new storage layer.
    async with open_cached_client('kraken') as client:

        # lol, kraken won't send any more then the "last"
        # 720 1m bars.. so we have to just ignore further
        # requests of this type..
        queries: int = 0

        async def get_ohlc(
            timeframe: float,
            end_dt: Optional[datetime] = None,
            start_dt: Optional[datetime] = None,

        ) -> tuple[
            np.ndarray,
            datetime,  # start
            datetime,  # end
        ]:

            nonlocal queries
            if (
                queries > 0
                or timeframe != 60
            ):
                raise DataUnavailable(
                    'Only a single query for 1m bars supported')

            count = 0
            while count <= 3:
                try:
                    array = await client.bars(
                        symbol,
                        since=end_dt,
                    )
                    count += 1
                    queries += 1
                    break
                except DataThrottle:
                    log.warning(f'kraken OHLC throttle for {symbol}')
                    await trio.sleep(1)

            start_dt = pendulum.from_timestamp(array[0]['time'])
            end_dt = pendulum.from_timestamp(array[-1]['time'])
            return array, start_dt, end_dt

        yield get_ohlc, {'erlangs': 1, 'rate': 1}


@async_lifo_cache()
async def get_mkt_info(
    fqme: str,

) -> tuple[MktPair, Pair]:
    '''
    Query for and return a `MktPair` and backend-native `Pair` (or
    wtv else) info.

    If more then one fqme is provided return a ``dict`` of native
    key-strs to `MktPair`s.

    '''
    async with open_cached_client('kraken') as client:

        # uppercase since kraken bs_mktid is always upper
        bs_fqme, _, broker = fqme.partition('.')
        pair_str: str = bs_fqme.upper()
        bs_mktid: str = Client.normalize_symbol(pair_str)
        pair: Pair = await client.pair_info(pair_str)

        assets = client.assets
        dst_asset: Asset = assets[pair.base]
        src_asset: Asset = assets[pair.quote]

        mkt = MktPair(
            dst=dst_asset,
            src=src_asset,

            price_tick=pair.price_tick,
            size_tick=pair.size_tick,
            bs_mktid=bs_mktid,

            broker='kraken',
        )
        return mkt, pair


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # backend specific
    sub_type: str = 'ohlc',

    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol>/<fiat_symbol>.

    '''

    ws_pairs: list[str] = []
    init_msgs: list[FeedInit] = []

    async with (
        send_chan as send_chan,
    ):
        for sym_str in symbols:
            mkt, pair = await get_mkt_info(sym_str)
            init_msgs.append(
                FeedInit(mkt_info=mkt)
            )

            ws_pairs.append(pair.wsname)

        @acm
        async def subscribe(ws: NoBsWs):

            # XXX: setup subs
            # https://docs.kraken.com/websockets/#message-subscribe
            # specific logic for this in kraken's sync client:
            # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
            ohlc_sub = {
                'event': 'subscribe',
                'pair': ws_pairs,
                'subscription': {
                    'name': 'ohlc',
                    'interval': 1,
                },
            }

            # TODO: we want to eventually allow unsubs which should
            # be completely fine to request from a separate task
            # since internally the ws methods appear to be FIFO
            # locked.
            await ws.send_msg(ohlc_sub)

            # trade data (aka L1)
            l1_sub = {
                'event': 'subscribe',
                'pair': ws_pairs,
                'subscription': {
                    'name': 'spread',
                    # 'depth': 10}
                },
            }

            # pull a first quote and deliver
            await ws.send_msg(l1_sub)

            yield

            # unsub from all pairs on teardown
            if ws.connected():
                await ws.send_msg({
                    'pair': ws_pairs,
                    'event': 'unsubscribe',
                    'subscription': ['ohlc', 'spread'],
                })

                # XXX: do we need to ack the unsub?
                # await ws.recv_msg()

        # see the tips on reconnection logic:
        # https://support.kraken.com/hc/en-us/articles/360044504011-WebSocket-API-unexpected-disconnections-from-market-data-feeds
        ws: NoBsWs
        async with (
            open_autorecon_ws(
                'wss://ws.kraken.com/',
                fixture=subscribe,
                reset_after=20,
            ) as ws,

            # avoid stream-gen closure from breaking trio..
            # NOTE: not sure this actually works XD particularly
            # if we call `ws._connect()` manally in the streaming
            # async gen..
            aclosing(process_data_feed_msgs(ws)) as msg_gen,
        ):
            # pull a first quote and deliver
            typ, ohlc_last = await anext(msg_gen)
            topic, quote = normalize(ohlc_last)

            task_status.started((init_msgs,  quote))
            feed_is_live.set()

            # keep start of last interval for volume tracking
            last_interval_start = ohlc_last.etime

            # start streaming
            async for typ, ohlc in msg_gen:

                if typ == 'ohlc':

                    # TODO: can get rid of all this by using
                    # ``trades`` subscription...

                    # generate tick values to match time & sales pane:
                    # https://trade.kraken.com/charts/KRAKEN:BTC-USD?period=1m
                    volume = ohlc.volume

                    # new OHLC sample interval
                    if ohlc.etime > last_interval_start:
                        last_interval_start = ohlc.etime
                        tick_volume = volume

                    else:
                        # this is the tick volume *within the interval*
                        tick_volume = volume - ohlc_last.volume

                    ohlc_last = ohlc
                    last = ohlc.close

                    if tick_volume:
                        ohlc.ticks.append({
                            'type': 'trade',
                            'price': last,
                            'size': tick_volume,
                        })

                    topic, quote = normalize(ohlc)

                elif typ == 'l1':
                    quote = ohlc
                    topic = quote['symbol'].lower()

                await send_chan.send({topic: quote})


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,

) -> Client:
    async with open_cached_client('kraken') as client:

        # load all symbols locally for fast search
        cache = await client.cache_symbols()
        await ctx.started(cache)

        async with ctx.open_stream() as stream:

            async for pattern in stream:

                matches = fuzzy.extractBests(
                    pattern,
                    cache,
                    score_cutoff=50,
                )
                # repack in dict form
                await stream.send({
                    pair[0].altname: pair[0]
                    for pair in matches
                })
