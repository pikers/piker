# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
Kraken backend.

'''
from contextlib import asynccontextmanager
from dataclasses import asdict, field
from typing import Dict, List, Tuple, Any, Optional, AsyncIterator
import time

from trio_typing import TaskStatus
import trio
import arrow
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
import tractor
from pydantic.dataclasses import dataclass
from pydantic import BaseModel
import wsproto
import itertools
import urllib.parse
import hashlib
import hmac
import base64

from .. import config
from .._cacheables import open_cached_client
from ._util import resproc, SymbolNotFound, BrokerError
from ..log import get_logger, get_console_log
from ..data import ShmArray
from ..data._web_bs import open_autorecon_ws, NoBsWs
from ..clearing._paper_engine import PaperBoi
from ..clearing._messages import (
    BrokerdPosition, BrokerdOrder, BrokerdStatus,
    BrokerdOrderAck, BrokerdError, BrokerdCancel,
    BrokerdFill,
)


log = get_logger(__name__)


# <uri>/<version>/
_url = 'https://api.kraken.com/0'


# Broker specific ohlc schema which includes a vwap field
_ohlc_dtype = [
    ('index', int),
    ('time', int),
    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('count', int),
    ('bar_wap', float),
]

# UI components allow this to be declared such that additional
# (historical) fields can be exposed.
ohlc_dtype = np.dtype(_ohlc_dtype)

_show_wap_in_history = True


_symbol_info_translation: Dict[str, str] = {
    'tick_decimals': 'pair_decimals',
}


# https://www.kraken.com/features/api#get-tradable-pairs
class Pair(BaseModel):
    altname: str  # alternate pair name
    wsname: str  # WebSocket pair name (if available)
    aclass_base: str  # asset class of base component
    base: str  # asset id of base component
    aclass_quote: str  # asset class of quote component
    quote: str  # asset id of quote component
    lot: str  # volume lot size

    pair_decimals: int  # scaling decimal places for pair
    lot_decimals: int  # scaling decimal places for volume

    # amount to multiply lot volume by to get currency volume
    lot_multiplier: float

    # array of leverage amounts available when buying
    leverage_buy: List[int]
    # array of leverage amounts available when selling
    leverage_sell: List[int]

    # fee schedule array in [volume, percent fee] tuples
    fees: List[Tuple[int, float]]

    # maker fee schedule array in [volume, percent fee] tuples (if on
    # maker/taker)
    fees_maker: List[Tuple[int, float]]

    fee_volume_currency: str  # volume discount currency
    margin_call: str  # margin call level
    margin_stop: str  # stop-out/liquidation margin level
    ordermin: float  # minimum order volume for pair


class Trade(BaseModel):
    '''
    Trade class that helps parse and validate ownTrades stream

    '''
    reqid: str  # kraken order transaction id
    action: str  # buy or sell
    price: str  # price of asset
    size: str  # vol of asset
    broker_time: str  # e.g GTC, GTD


@dataclass
class OHLC:
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
    ticks: List[Any] = field(default_factory=list)


def get_config() -> dict[str, Any]:

    conf, path = config.load()

    section = conf.get('kraken')

    if section is None:
        log.warning(f'No config section found for kraken in {path}')
        return {}

    return section


def get_kraken_signature(
    urlpath: str,
    data: Dict[str, Any],
    secret: str
) -> str:
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()


class InvalidKey(ValueError):
    '''
    EAPI:Invalid key
    This error is returned when the API key used for the call is
    either expired or disabled, please review the API key in your
    Settings -> API tab of account management or generate a new one
    and update your application.

    '''


class Client:

    def __init__(
        self,
        name: str = '',
        api_key: str = '',
        secret: str = ''
    ) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url
        self._sesh.headers.update({
            'User-Agent':
                'krakenex/2.1.0 (+https://github.com/veox/python3-krakenex)'
        })
        self._pairs: list[str] = []
        self._name = name
        self._api_key = api_key
        self._secret = secret

    @property
    def pairs(self) -> Dict[str, Any]:
        if self._pairs is None:
            raise RuntimeError(
                "Make sure to run `cache_symbols()` on startup!"
            )
            # retreive and cache all symbols

        return self._pairs

    async def _public(
        self,
        method: str,
        data: dict,
    ) -> Dict[str, Any]:
        resp = await self._sesh.post(
            path=f'/public/{method}',
            json=data,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def _private(
        self,
        method: str,
        data: dict,
        uri_path: str
    ) -> Dict[str, Any]:
        headers = {
            'Content-Type':
                'application/x-www-form-urlencoded',
            'API-Key':
                self._api_key,
            'API-Sign':
                get_kraken_signature(uri_path, data, self._secret)
        }
        resp = await self._sesh.post(
            path=f'/private/{method}',
            data=data,
            headers=headers,
            timeout=float('inf')
        )
        return resproc(resp, log)

    async def endpoint(
        self,
        method: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        uri_path = f'/0/private/{method}'
        data['nonce'] = str(int(1000*time.time()))
        return await self._private(method, data, uri_path)

    async def get_trades(
        self,
        data: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        data['ofs'] = 0
        # Grab all trade history
        # https://docs.kraken.com/rest/#operation/getTradeHistory
        # Kraken uses 'ofs' to refer to the offset
        while True:
            resp = await self.endpoint('TradesHistory', data)
            # grab the first 50 trades
            if data['ofs'] == 0:
                trades = resp['result']['trades']
            # load the next 50 trades using dict constructor
            # for speed
            elif data['ofs'] == 50:
                trades = dict(trades, **resp['result']['trades'])
            # catch the end of the trades
            elif resp['result']['trades'] == {}:
                count = resp['result']['count']
                break
            # update existing dict if num trades exceeds 100
            else:
                trades.update(resp['result']['trades'])
            # increment the offset counter
            data['ofs'] += 50
            # To avoid exceeding API rate limit in case of a lot of trades
            await trio.sleep(1)

        # make sure you grabbed all the trades
        assert count == len(trades.values())

        return trades

    async def submit_limit(
        self,
        oid: str,
        symbol: str,
        price: float,
        action: str,
        size: float,
        reqid: int = None,
    ) -> int:
        '''
        Place an order and return integer request id provided by client.

        '''
        # Build order data for kraken api
        data = {
            "userref": reqid,
            "ordertype": "limit",
            "type": action,
            "volume": str(size),
            "pair": symbol,
            "price": str(price),
            # set to True test AddOrder call without a real submission
            "validate": False
        }
        return await self.endpoint('AddOrder', data)

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:
        '''
        Send cancel request for order id ``reqid``.

        '''
        # txid is a transaction id given by kraken
        return await self.endpoint('CancelOrder', {"txid": reqid})

    async def symbol_info(
        self,
        pair: Optional[str] = None,
    ):
        if pair is not None:
            pairs = {'pair': pair}
        else:
            pairs = None  # get all pairs

        resp = await self._public('AssetPairs', pairs)
        err = resp['error']
        if err:
            symbolname = pairs['pair'] if pair else None
            raise SymbolNotFound(f'{symbolname}.kraken')

        pairs = resp['result']

        if pair is not None:
            _, data = next(iter(pairs.items()))
            return data
        else:
            return pairs

    async def cache_symbols(
        self,
    ) -> dict:
        if not self._pairs:
            self._pairs = await self.symbol_info()

        return self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,
    ) -> Dict[str, Any]:
        if self._pairs is not None:
            data = self._pairs
        else:
            data = await self.symbol_info()

        matches = fuzzy.extractBests(
            pattern,
            data,
            score_cutoff=50,
        )
        # repack in dict form
        return {item[0]['altname']: item[0] for item in matches}

    async def bars(
        self,
        symbol: str = 'XBTUSD',
        # UTC 2017-07-02 12:53:20
        since: int = None,
        count: int = 720,  # <- max allowed per query
        as_np: bool = True,
    ) -> dict:
        if since is None:
            since = arrow.utcnow().floor('minute').shift(
                minutes=-count).timestamp()

        # UTC 2017-07-02 12:53:20 is oldest seconds value
        since = str(max(1499000000, since))
        json = await self._public(
            'OHLC',
            data={
                'pair': symbol,
                'since': since,
            },
        )
        try:
            res = json['result']
            res.pop('last')
            bars = next(iter(res.values()))

            new_bars = []

            first = bars[0]
            last_nz_vwap = first[-3]
            if last_nz_vwap == 0:
                # use close if vwap is zero
                last_nz_vwap = first[-4]

            # convert all fields to native types
            for i, bar in enumerate(bars):
                # normalize weird zero-ed vwap values..cmon kraken..
                # indicates vwap didn't change since last bar
                vwap = float(bar.pop(-3))
                if vwap != 0:
                    last_nz_vwap = vwap
                if vwap == 0:
                    vwap = last_nz_vwap

                # re-insert vwap as the last of the fields
                bar.append(vwap)

                new_bars.append(
                    (i,) + tuple(
                        ftype(bar[j]) for j, (name, ftype) in enumerate(
                            _ohlc_dtype[1:]
                        )
                    )
                )
            array = np.array(new_bars, dtype=_ohlc_dtype) if as_np else bars
            return array
        except KeyError:
            raise SymbolNotFound(json['error'][0] + f': {symbol}')


@asynccontextmanager
async def get_client() -> Client:

    section = get_config()
    if section:
        client = Client(
            name=section['key_descr'],
            api_key=section['api_key'],
            secret=section['secret']
        )
    else:
        client = Client()

    # at startup, load all symbols locally for fast search
    await client.cache_symbols()

    yield client


def pack_positions(
    acc: str,
    trades: dict
) -> list[Any]:
    positions: dict[str, float] = {}
    vols: dict[str, float] = {}
    costs: dict[str, float] = {}
    position_msgs: list[Any] = []

    for trade in trades.values():
        sign = -1 if trade['type'] == 'sell' else 1
        pair = trade['pair']
        vol = float(trade['vol'])
        vols[pair] = vols.get(pair, 0) + sign * vol
        costs[pair] = costs.get(pair, 0) + sign * float(trade['cost'])
        positions[pair] = costs[pair] / vols[pair] if vols[pair] else 0

    for ticker, pos in positions.items():
        vol = float(vols[ticker])
        if not vol:
            continue
        norm_sym = normalize_symbol(ticker)
        msg = BrokerdPosition(
            broker='kraken',
            account=acc,
            symbol=norm_sym,
            currency=norm_sym[-3:],
            size=vol,
            avg_price=float(pos),
        )
        position_msgs.append(msg.dict())

    return position_msgs


def normalize_symbol(
    ticker: str
) -> str:
    # This is to convert symbol names from what kraken
    # uses to the traditional 3x3 pair symbol syntax
    symlen = len(ticker)
    if symlen == 6:
        return ticker.lower()
    else:
        for sym in ['XXBT', 'XXMR', 'ZEUR']:
            if sym in ticker:
                ticker = ticker.replace(sym, sym[1:])
        return ticker.lower()


def make_auth_sub(data: Dict[str, Any]) -> Dict[str, str]:
    '''
    Create a request subscription packet dict.

    ## TODO: point to the auth urls
    https://docs.kraken.com/websockets/#message-subscribe

    '''
    # eg. specific logic for this in kraken's sync client:
    # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
    return {
        'event': 'subscribe',
        'subscription': data,
    }


async def handle_order_requests(

        client: Client,
        ems_order_stream: tractor.MsgStream,

) -> None:

    request_msg: dict
    order: BrokerdOrder
    userref_counter = itertools.count()

    async for request_msg in ems_order_stream:
        log.info(f'Received order request {request_msg}')

        action = request_msg['action']

        if action in {'buy', 'sell'}:

            account = request_msg['account']
            if account != 'kraken.spot':
                log.error(
                    'This is a kraken account, \
                    only a `kraken.spot` selection is valid'
                )
                await ems_order_stream.send(BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],
                    reason=f'Kraken only, No account found: `{account}` ?',
                ).dict())
                continue

            # validate
            temp_id = next(userref_counter)
            order = BrokerdOrder(**request_msg)

            # call our client api to submit the order
            resp = await client.submit_limit(
                oid=order.oid,
                symbol=order.symbol,
                price=order.price,
                action=order.action,
                size=order.size,
                reqid=temp_id,
            )

            err = resp['error']
            if err:
                oid = order.oid
                log.error(f'Failed to submit order: {oid}')

                await ems_order_stream.send(
                    BrokerdError(
                        oid=order.oid,
                        reqid=temp_id,
                        symbol=order.symbol,
                        reason="Failed order submission",
                        broker_details=resp
                    ).dict()
                )
            else:
                # TODO: handle multiple cancels
                #       txid is an array of strings
                reqid = resp['result']['txid'][0]
                # deliver ack that order has been submitted to broker routing
                await ems_order_stream.send(
                    BrokerdOrderAck(

                        # ems order request id
                        oid=order.oid,

                        # broker specific request id
                        reqid=reqid,

                        # account the made the order
                        account=order.account

                    ).dict()
                )

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)

            # Send order cancellation to kraken
            resp = await client.submit_cancel(
                reqid=msg.reqid
            )

            # Check to make sure there was no error returned by
            # the kraken endpoint. Assert one order was cancelled.
            try:
                result = resp['result']
                count = result['count']

            # check for 'error' key if we received no 'result'
            except KeyError:
                error = resp.get('error')

                await ems_order_stream.send(
                    BrokerdError(
                        oid=order.oid,
                        reqid=temp_id,
                        symbol=order.symbol,
                        reason="Failed order cancel",
                        broker_details=resp
                    ).dict()
                )

                if not error:
                    raise BrokerError(f'Unknown order cancel response: {resp}')

            else:
                if not count:  # no orders were cancelled?

                    # XXX: what exactly is this from and why would we care?
                    # there doesn't seem to be any docs here?
                    # https://docs.kraken.com/rest/#operation/cancelOrder

                    # Check to make sure the cancellation is NOT pending,
                    # then send the confirmation to the ems order stream
                    pending = result.get('pending')
                    if pending:
                        log.error(f'Order {oid} cancel was not yet successful')

                        await ems_order_stream.send(
                            BrokerdError(
                                oid=order.oid,
                                reqid=temp_id,
                                symbol=order.symbol,
                                reason="Order cancel is still pending?",
                                broker_details=resp
                            ).dict()
                        )

                else:  # order cancel success case.

                    await ems_order_stream.send(
                        BrokerdStatus(
                            reqid=msg.reqid,
                            account=msg.account,
                            time_ns=time.time_ns(),
                            status='cancelled',
                            reason='Order cancelled',
                            broker_details={'name': 'kraken'}
                        ).dict()
                    )
    else:
        log.error(f'Unknown order command: {request_msg}')


@tractor.context
async def trades_dialogue(
    ctx: tractor.Context,
    loglevel: str = None,
) -> AsyncIterator[Dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    @asynccontextmanager
    async def subscribe(ws: wsproto.WSConnection, token: str):
        # XXX: setup subs
        # https://docs.kraken.com/websockets/#message-subscribe
        # specific logic for this in kraken's shitty sync client:
        # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
        trades_sub = make_auth_sub(
            {'name': 'ownTrades', 'token': token}
        )

        # TODO: we want to eventually allow unsubs which should
        # be completely fine to request from a separate task
        # since internally the ws methods appear to be FIFO
        # locked.
        await ws.send_msg(trades_sub)

        yield

        # unsub from all pairs on teardown
        await ws.send_msg({
            'event': 'unsubscribe',
            'subscription': ['ownTrades'],
        })

        # XXX: do we need to ack the unsub?
        # await ws.recv_msg()

    # Authenticated block
    async with get_client() as client:
        if not client._api_key:
            log.error('Missing Kraken API key: Trades WS connection failed')
            await ctx.started(({}, ['paper']))

            async with (
                ctx.open_stream() as ems_stream,
                trio.open_nursery() as n,
            ):

                client = PaperBoi(
                    'kraken',
                    ems_stream,
                    _buys={},
                    _sells={},

                    _reqids={},

                    # TODO: load paper positions from ``positions.toml``
                    _positions={},
                )

                # TODO: maybe add multiple accounts
                n.start_soon(handle_order_requests, client, ems_stream)

        acc_name = 'kraken.' + client._name
        trades = await client.get_trades()

        position_msgs = pack_positions(acc_name, trades)

        await ctx.started((position_msgs, (acc_name,)))

        # Get websocket token for authenticated data stream
        # Assert that a token was actually received.
        resp = await client.endpoint('GetWebSocketsToken', {})
        assert resp['error'] == []
        token = resp['result']['token']

        async with (
            ctx.open_stream() as ems_stream,
            trio.open_nursery() as n,
        ):
            # TODO: maybe add multiple accounts
            n.start_soon(handle_order_requests, client, ems_stream)

            # Process trades msg stream of ws
            async with open_autorecon_ws(
                'wss://ws-auth.kraken.com/',
                fixture=subscribe,
                token=token,
            ) as ws:
                async for msg in process_trade_msgs(ws):
                    for trade in msg:
                        # check the type of packaged message
                        assert type(trade) == Trade

                        # prepare and send a filled status update
                        filled_msg = BrokerdStatus(
                            reqid=trade.reqid,
                            time_ns=time.time_ns(),

                            account='kraken.spot',
                            status='filled',
                            filled=float(trade.size),
                            reason='Order filled by kraken',
                            broker_details={
                                'name': 'kraken',
                                'broker_time': trade.broker_time
                            },

                            # TODO: figure out if kraken gives a count
                            # of how many units of underlying were
                            # filled. Alternatively we can decrement
                            # this value ourselves by associating and
                            # calcing from the diff with the original
                            # client-side request, see:
                            # https://github.com/pikers/piker/issues/296
                            remaining=0,
                        )

                        await ems_stream.send(filled_msg.dict())

                        # send a fill msg for gui update
                        fill_msg = BrokerdFill(
                            reqid=trade.reqid,
                            time_ns=time.time_ns(),

                            action=trade.action,
                            size=float(trade.size),
                            price=float(trade.price),
                            # TODO: maybe capture more msg data i.e fees?
                            broker_details={'name': 'kraken'},
                            broker_time=float(trade.broker_time)
                        )

                        await ems_stream.send(fill_msg.dict())


async def stream_messages(
    ws: NoBsWs,
):
    '''
    Message stream parser and heartbeat handler.

    Deliver ws subscription messages as well as handle heartbeat logic
    though a single async generator.

    '''
    too_slow_count = last_hb = 0

    while True:

        with trio.move_on_after(5) as cs:
            msg = await ws.recv_msg()

        # trigger reconnection if heartbeat is laggy
        if cs.cancelled_caught:

            too_slow_count += 1

            if too_slow_count > 20:
                log.warning(
                    "Heartbeat is too slow, resetting ws connection")

                await ws._connect()
                too_slow_count = 0
                continue

        if isinstance(msg, dict):
            if msg.get('event') == 'heartbeat':

                now = time.time()
                delay = now - last_hb
                last_hb = now

                # XXX: why tf is this not printing without --tl flag?
                log.debug(f"Heartbeat after {delay}")
                # print(f"Heartbeat after {delay}")

                continue

            err = msg.get('errorMessage')
            if err:
                raise BrokerError(err)
        else:
            yield msg


async def process_data_feed_msgs(
    ws: NoBsWs,
):
    '''
    Parse and pack data feed messages.

    '''
    async for msg in stream_messages(ws):

        chan_id, *payload_array, chan_name, pair = msg

        if 'ohlc' in chan_name:

            yield 'ohlc', OHLC(chan_id, chan_name, pair, *payload_array[0])

        elif 'spread' in chan_name:

            bid, ask, ts, bsize, asize = map(float, payload_array[0])

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

        else:
            print(f'UNHANDLED MSG: {msg}')
            yield msg


async def process_trade_msgs(
    ws: NoBsWs,
):
    '''
    Parse and pack data feed messages.

    '''
    sequence_counter = 0
    async for msg in stream_messages(ws):

        try:
            # check that we are on the ownTrades stream and that msgs
            # are arriving in sequence with kraken For clarification the
            # kraken ws api docs for this stream:
            # https://docs.kraken.com/websockets/#message-ownTrades
            assert msg[1] == 'ownTrades'
            assert msg[2]['sequence'] > sequence_counter
            sequence_counter += 1
            raw_msgs = msg[0]
            trade_msgs = []

            # Check that we are only processing new trades
            if msg[2]['sequence'] != 1:
                # check if its a new order or an update msg
                for trade_msg in raw_msgs:
                    trade = list(trade_msg.values())[0]
                    order_msg = Trade(
                        reqid=trade['ordertxid'],
                        action=trade['type'],
                        price=trade['price'],
                        size=trade['vol'],
                        broker_time=trade['time']
                    )
                    trade_msgs.append(order_msg)

            yield trade_msgs

        except AssertionError:
            print(f'UNHANDLED MSG: {msg}')
            yield msg


def normalize(
    ohlc: OHLC,

) -> dict:
    quote = asdict(ohlc)
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


def make_sub(pairs: List[str], data: Dict[str, Any]) -> Dict[str, str]:
    '''
    Create a request subscription packet dict.

    https://docs.kraken.com/websockets/#message-subscribe

    '''
    # eg. specific logic for this in kraken's sync client:
    # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
    return {
        'pair': pairs,
        'event': 'subscribe',
        'subscription': data,
    }


async def backfill_bars(

    sym: str,
    shm: ShmArray,  # type: ignore # noqa
    count: int = 10,  # NOTE: any more and we'll overrun the underlying buffer
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Fill historical bars into shared mem / storage afap.
    '''
    with trio.CancelScope() as cs:
        async with open_cached_client('kraken') as client:
            bars = await client.bars(symbol=sym)
            shm.push(bars)
            task_status.started(cs)


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: List[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # backend specific
    sub_type: str = 'ohlc',

    # startup sync
    task_status: TaskStatus[Tuple[Dict, Dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol>/<fiat_symbol>.

    '''
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    ws_pairs = {}
    sym_infos = {}

    async with open_cached_client('kraken') as client, send_chan as send_chan:

        # keep client cached for real-time section
        for sym in symbols:

            # transform to upper since piker style is always lower
            sym = sym.upper()

            si = Pair(**await client.symbol_info(sym))  # validation
            syminfo = si.dict()
            syminfo['price_tick_size'] = 1 / 10**si.pair_decimals
            syminfo['lot_tick_size'] = 1 / 10**si.lot_decimals
            syminfo['asset_type'] = 'crypto'
            sym_infos[sym] = syminfo
            ws_pairs[sym] = si.wsname

        symbol = symbols[0].lower()

        init_msgs = {
            # pass back token, and bool, signalling if we're the writer
            # and that history has been written
            symbol: {
                'symbol_info': sym_infos[sym],
                'shm_write_opts': {'sum_tick_vml': False},
            },
        }

        @asynccontextmanager
        async def subscribe(ws: wsproto.WSConnection):
            # XXX: setup subs
            # https://docs.kraken.com/websockets/#message-subscribe
            # specific logic for this in kraken's shitty sync client:
            # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
            ohlc_sub = make_sub(
                list(ws_pairs.values()),
                {'name': 'ohlc', 'interval': 1}
            )

            # TODO: we want to eventually allow unsubs which should
            # be completely fine to request from a separate task
            # since internally the ws methods appear to be FIFO
            # locked.
            await ws.send_msg(ohlc_sub)

            # trade data (aka L1)
            l1_sub = make_sub(
                list(ws_pairs.values()),
                {'name': 'spread'}  # 'depth': 10}
            )

            # pull a first quote and deliver
            await ws.send_msg(l1_sub)

            yield

            # unsub from all pairs on teardown
            await ws.send_msg({
                'pair': list(ws_pairs.values()),
                'event': 'unsubscribe',
                'subscription': ['ohlc', 'spread'],
            })

            # XXX: do we need to ack the unsub?
            # await ws.recv_msg()

        # see the tips on reconnection logic:
        # https://support.kraken.com/hc/en-us/articles/360044504011-WebSocket-API-unexpected-disconnections-from-market-data-feeds
        ws: NoBsWs
        async with open_autorecon_ws(
            'wss://ws.kraken.com/',
            fixture=subscribe,
        ) as ws:

            # pull a first quote and deliver
            msg_gen = process_data_feed_msgs(ws)

            # TODO: use ``anext()`` when it lands in 3.10!
            typ, ohlc_last = await msg_gen.__anext__()

            topic, quote = normalize(ohlc_last)

            first_quote = {topic: quote}
            task_status.started((init_msgs,  first_quote))

            # lol, only "closes" when they're margin squeezing clients ;P
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
                await stream.send(
                    {item[0]['altname']: item[0]
                     for item in matches}
                )
