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
from __future__ import annotations
from contextlib import (
    asynccontextmanager as acm,
    aclosing,
)
from datetime import datetime
from functools import (
    partial,
)
import itertools
from pprint import pformat
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Generator,
)
import time

import trio
from trio_typing import TaskStatus
from pendulum import (
    from_timestamp,
)
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor

from piker.brokers import (
    open_cached_client,
)
from piker._cacheables import (
    async_lifo_cache,
)
from piker.accounting import (
    Asset,
    DerivTypes,
    MktPair,
    unpack_fqme,
)
from piker.data.types import Struct
from piker.data.validate import FeedInit
from piker.data._web_bs import (
    open_autorecon_ws,
    NoBsWs,
)
from piker.brokers._util import (
    DataUnavailable,
    get_logger,
)

from .api import (
    Client,
)
from .venues import (
    Pair,
    FutesPair,
    get_api_eps,
)

log = get_logger('piker.brokers.binance')


class L1(Struct):
    # https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams

    update_id: int
    sym: str

    bid: float
    bsize: float
    ask: float
    asize: float


# validation type
class AggTrade(Struct, frozen=True):
    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    a: int  # Aggregate trade ID
    p: float  # Price
    q: float  # Quantity
    f: int  # First trade ID
    l: int  # noqa Last trade ID
    T: int  # Trade time
    m: bool  # Is the buyer the market maker?
    M: bool | None = None  # Ignore


async def stream_messages(
    ws: NoBsWs,
) -> AsyncGenerator[NoBsWs, dict]:

    # TODO: match syntax here!
    msg: dict[str, Any]
    async for msg in ws:
        match msg:
            # for l1 streams binance doesn't add an event type field so
            # identify those messages by matching keys
            # https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams
            case {
                # NOTE: this is never an old value it seems, so
                # they are always sending real L1 spread updates.
                'u': upid,  # update id
                's': sym,
                'b': bid,
                'B': bsize,
                'a': ask,
                'A': asize,
            }:
                # TODO: it would be super nice to have a `L1` piker type
                # which "renders" incremental tick updates from a packed
                # msg-struct:
                # - backend msgs after packed into the type such that we
                #   can reduce IPC usage but without each backend having
                #   to do that incremental update logic manually B)
                # - would it maybe be more efficient to use this instead?
                #   https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
                l1 = L1(
                    update_id=upid,
                    sym=sym,
                    bid=bid,
                    bsize=bsize,
                    ask=ask,
                    asize=asize,
                )
                # for speed probably better to only specifically
                # cast fields we need in numerical form?
                # l1.typecast()

                # repack into piker's tick-quote format
                yield 'l1', {
                    'symbol': l1.sym,
                    'ticks': [
                        {
                            'type': 'bid',
                            'price': float(l1.bid),
                            'size': float(l1.bsize),
                        },
                        {
                            'type': 'bsize',
                            'price': float(l1.bid),
                            'size': float(l1.bsize),
                        },
                        {
                            'type': 'ask',
                            'price': float(l1.ask),
                            'size': float(l1.asize),
                        },
                        {
                            'type': 'asize',
                            'price': float(l1.ask),
                            'size': float(l1.asize),
                        }
                    ]
                }

            # https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
            case {
                'e': 'aggTrade',
            }:
                # NOTE: this is purely for a definition,
                # ``msgspec.Struct`` does not runtime-validate until you
                # decode/encode, see:
                # https://jcristharif.com/msgspec/structs.html#type-validation
                msg = AggTrade(**msg)  # TODO: should we .copy() ?
                piker_quote: dict = {
                    'symbol': msg.s,
                    'last': float(msg.p),
                    'brokerd_ts': time.time(),
                    'ticks': [{
                        'type': 'trade',
                        'price': float(msg.p),
                        'size': float(msg.q),
                        'broker_ts': msg.T,
                    }],
                }
                yield 'trade', piker_quote


def make_sub(pairs: list[str], sub_name: str, uid: int) -> dict[str, str]:
    '''
    Create a request subscription packet dict.

    - spot:
      https://binance-docs.github.io/apidocs/spot/en/#live-subscribing-unsubscribing-to-streams

    - futes:
      https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams

    '''
    return {
        'method': 'SUBSCRIBE',
        'params': [
            f'{pair.lower()}@{sub_name}'
            for pair in pairs
        ],
        'id': uid
    }


@acm
async def open_history_client(
    mkt: MktPair,

) -> tuple[Callable, int]:

    # TODO implement history getter for the new storage layer.
    async with open_cached_client('binance') as client:

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

            # TODO: better wrapping for venue / mode?
            # - eventually logic for usd vs. coin settled futes
            #   based on `MktPair.src` type/value?
            # - maybe something like `async with
            # Client.use_venue('usdtm_futes')`
            if mkt.type_key in DerivTypes:
                client.mkt_mode = 'usdtm_futes'
            else:
                client.mkt_mode = 'spot'

            # NOTE: always query using their native symbology!
            mktid: str = mkt.bs_mktid
            array = await client.bars(
                mktid,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            times = array['time']
            if (
                end_dt is None
            ):
                inow = round(time.time())
                if (inow - times[-1]) > 60:
                    await tractor.breakpoint()

            start_dt = from_timestamp(times[0])
            end_dt = from_timestamp(times[-1])

            return array, start_dt, end_dt

        yield get_ohlc, {'erlangs': 3, 'rate': 3}


@async_lifo_cache()
async def get_mkt_info(
    fqme: str,

) -> tuple[MktPair, Pair] | None:

    # uppercase since kraken bs_mktid is always upper
    if 'binance' not in fqme.lower():
        fqme += '.binance'

    mkt_mode: str = ''
    broker, mkt_ep, venue, expiry = unpack_fqme(fqme)

    # NOTE: we always upper case all tokens to be consistent with
    # binance's symbology style for pairs, like `BTCUSDT`, but in
    # theory we could also just keep things lower case; as long as
    # we're consistent and the symcache matches whatever this func
    # returns, always!
    expiry: str = expiry.upper()
    venue: str = venue.upper()
    venue_lower: str = venue.lower()

    # XXX TODO: we should change the usdtm_futes name to just
    # usdm_futes (dropping the tether part) since it turns out that
    # there are indeed USD-tokens OTHER THEN tether being used as
    # the margin assets.. it's going to require a wholesale
    # (variable/key) rename as well as file name adjustments to any
    # existing tsdb set..
    if 'usd' in venue_lower:
        mkt_mode: str = 'usdtm_futes'

    # NO IDEA what these contracts (some kinda DEX-ish futes?) are
    # but we're masking them for now..
    elif (
        'defi' in venue_lower

        # TODO: handle coinm futes which have a margin asset that
        # is some crypto token!
        # https://binance-docs.github.io/apidocs/delivery/en/#exchange-information
        or 'btc' in venue_lower
    ):
        return None

    else:
        # NOTE: see the `FutesPair.bs_fqme: str` implementation
        # to understand the reverse market info lookup below.
        mkt_mode = venue_lower or 'spot'

    if (
        venue
        and 'spot' not in venue_lower

        # XXX: catch all in case user doesn't know which
        # venue they want (usdtm vs. coinm) and we can choose
        # a default (via config?) once we support coin-m APIs.
        or 'perp' in venue_lower
    ):
        if not mkt_mode:
            mkt_mode: str = f'{venue_lower}_futes'

    async with open_cached_client(
        'binance',
    ) as client:

        assets: dict[str, Asset] = await client.get_assets()
        pair_str: str = mkt_ep.upper()

        # switch venue-mode depending on input pattern parsing
        # since we want to use a particular endpoint (set) for
        # pair info lookup!
        client.mkt_mode = mkt_mode

        pair: Pair = await client.exch_info(
            pair_str,
            venue=mkt_mode,  # explicit
            expiry=expiry,
        )

        if 'futes' in mkt_mode:
            assert isinstance(pair, FutesPair)

        dst: Asset | None = assets.get(pair.bs_dst_asset)
        if (
            not dst
            # TODO: a known asset DNE list?
            # and pair.baseAsset == 'DEFI'
        ):
            log.warning(
                f'UNKNOWN {venue} asset {pair.baseAsset} from,\n'
                f'{pformat(pair.to_dict())}'
            )

            # XXX UNKNOWN missing "asset", though no idea why?
            # maybe it's only avail in the margin venue(s): /dapi/ ?
            return None

        mkt = MktPair(
            dst=dst,
            src=assets[pair.bs_src_asset],
            price_tick=pair.price_tick,
            size_tick=pair.size_tick,
            bs_mktid=pair.symbol,
            expiry=expiry,
            venue=venue,
            broker='binance',

            # NOTE: sectype is always taken from dst, see
            # `MktPair.type_key` and `Client._cache_pairs()`
            # _atype=sectype,
        )
        return mkt, pair


@acm
async def subscribe(
    ws: NoBsWs,
    symbols: list[str],

    # defined once at import time to keep a global state B)
    iter_subids: Generator[int, None, None] = itertools.count(),

):
    # setup subs

    subid: int = next(iter_subids)

    # trade data (aka L1)
    # https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker
    l1_sub = make_sub(symbols, 'bookTicker', subid)
    await ws.send_msg(l1_sub)

    # aggregate (each order clear by taker **not** by maker)
    # trades data:
    # https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
    agg_trades_sub = make_sub(symbols, 'aggTrade', subid)
    await ws.send_msg(agg_trades_sub)

    # might get ack from ws server, or maybe some
    # other msg still in transit..
    res = await ws.recv_msg()
    subid: str | None = res.get('id')
    if subid:
        assert res['id'] == subid

    yield

    subs = []
    for sym in symbols:
        subs.append("{sym}@aggTrade")
        subs.append("{sym}@bookTicker")

    # unsub from all pairs on teardown
    if ws.connected():
        await ws.send_msg({
            "method": "UNSUBSCRIBE",
            "params": subs,
            "id": subid,
        })

        # XXX: do we need to ack the unsub?
        # await ws.recv_msg()


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,

) -> None:

    async with (
        send_chan as send_chan,
        open_cached_client('binance') as client,
    ):
        init_msgs: list[FeedInit] = []
        for sym in symbols:
            mkt, pair = await get_mkt_info(sym)

            # build out init msgs according to latest spec
            init_msgs.append(
                FeedInit(mkt_info=mkt)
            )

        wss_url: str = get_api_eps(client.mkt_mode)[1]  # 2nd elem is wss url

        # TODO: for sanity, but remove eventually Xp
        if 'future' in mkt.type_key:
            assert 'fstream' in wss_url

        async with (
            open_autorecon_ws(
                url=wss_url,
                fixture=partial(
                    subscribe,
                    symbols=[mkt.bs_mktid],
                ),
            ) as ws,

            # avoid stream-gen closure from breaking trio..
            aclosing(stream_messages(ws)) as msg_gen,
        ):
            # log.info('WAITING ON FIRST LIVE QUOTE..')
            typ, quote = await anext(msg_gen)

            # pull a first quote and deliver
            while typ != 'trade':
                typ, quote = await anext(msg_gen)

            task_status.started((init_msgs, quote))

            # signal to caller feed is ready for consumption
            feed_is_live.set()

            # import time
            # last = time.time()

            # XXX NOTE: can't include the `.binance` suffix
            # or the sampling loop will not broadcast correctly
            # since `bus._subscribers.setdefault(bs_fqme, set())`
            # is used inside `.data.open_feed_bus()` !!!
            topic: str = mkt.bs_fqme

            # start streaming
            async for typ, quote in msg_gen:

                # period = time.time() - last
                # hz = 1/period if period else float('inf')
                # if hz > 60:
                #     log.info(f'Binance quotez : {hz}')
                await send_chan.send({topic: quote})
                # last = time.time()


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,
) -> Client:

    # NOTE: symbology tables are loaded as part of client
    # startup in ``.api.get_client()`` and in this case
    # are stored as `Client._pairs`.
    async with open_cached_client('binance') as client:

        # TODO: maybe we should deliver the cache
        # so that client's can always do a local-lookup-first
        # style try and then update async as (new) match results
        # are delivered from here?
        await ctx.started()

        async with ctx.open_stream() as stream:

            pattern: str
            async for pattern in stream:
                matches = fuzzy.extractBests(
                    pattern,
                    client._pairs,
                    score_cutoff=50,
                )

                # repack in dict form
                await stream.send({
                    item[0].bs_fqme: item[0]
                    for item in matches
                })
