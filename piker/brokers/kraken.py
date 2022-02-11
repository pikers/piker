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

"""
Kraken backend.

"""
from contextlib import asynccontextmanager
from dataclasses import asdict, field
from typing import List, Dict, Any, Tuple, Optional, AsyncIterator
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
from itertools import count

from .. import config
from .._cacheables import open_cached_client
from ._util import resproc, SymbolNotFound, BrokerError
from ..log import get_logger, get_console_log
from ..data import ShmArray
from ..data._web_bs import open_autorecon_ws, NoBsWs
from ..clearing._messages import (
    BrokerdPosition, BrokerdOrder, BrokerdStatus,
    BrokerdOrderAck, BrokerdError, BrokerdCancel,
    BrokerdFill,
)

import urllib.parse
import hashlib
import hmac
import base64


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


@dataclass
class OHLC:
    """Description of the flattened OHLC quote format.

    For schema details see:
        https://docs.kraken.com/websockets/#message-ohlc
    """
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
    """EAPI:Invalid key
        This error is returned when the API key used for the call is
        either expired or disabled, please review the API key in your
        Settings -> API tab of account management or generate a new one
        and update your application."""


class Client:

    def __init__(self) -> None:
        self._sesh = asks.Session(connections=4)
        self._sesh.base_location = _url
        self._sesh.headers.update({
            'User-Agent':
                'krakenex/2.1.0 (+https://github.com/veox/python3-krakenex)'
        })
        self._pairs: list[str] = []
        self._name = ''
        self._api_key = ''
        self._secret = ''

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

    async def kraken_endpoint(
        self,
        method: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        uri_path = f'/0/private/{method}'
        data['nonce'] = str(int(1000*time.time()))
        resp = await self._private(method, data, uri_path)
        return resp

    async def get_positions(
        self,
        data: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        resp = await self.kraken_endpoint('Balance', data)
        balances = resp['result']
        ## TODO: grab all entries, not just first 50
        resp = await self.kraken_endpoint('TradesHistory', data)
        traders = resp['result']
        positions = {}
        vols = {}
        
        # positions
        ## TODO: Make sure to add option to include fees in positions calc
        for trade in traders['trades'].values():
            sign = -1 if trade['type'] == 'sell' else 1
            try:
                positions[trade['pair']] += sign * float(trade['cost'])
                vols[trade['pair']] += sign * float(trade['vol'])
            except KeyError:
                positions[trade['pair']] = sign * float(trade['cost'])
                vols[trade['pair']] = sign * float(trade['vol'])
    
        for pair in positions.keys():
            asset_balance = vols[pair]
            if asset_balance == 0:
                positions[pair] = 0
            else:
                positions[pair] /= asset_balance

        return positions, vols

    async def submit_limit(
        self,
        oid: str,
        symbol: str,
        price: float,
        action: str,
        size: float,
#        account: str,
        reqid: int = None,
    ) -> int:
        """Place an order and return integer request id provided by client.

        """
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
        resp = await self.kraken_endpoint('AddOrder', data)
        return resp

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:
        """Send cancel request for order id ``reqid``.

        """
        # txid is a transaction id given by kraken
        data = {"txid": reqid}
        resp = await self.kraken_endpoint('CancelOrder', data)
        return resp

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
    client = Client()

    ## TODO: maybe add conditional based on section
    section = get_config()
    client._name = section['key_descr']
    client._api_key = section['api_key']
    client._secret = section['secret']
    ## TODO: Add a client attribute to hold this info
    #data = {
    #    # add non-nonce and non-ofs vars
    #}

    # at startup, load all symbols locally for fast search
    await client.cache_symbols()

    yield client


def pack_position(
    acc: str,
    symkey: str,
    pos: float,
    vol: float
) -> dict[str, Any]:

    return BrokerdPosition(
        broker='kraken',
        account=acc,
        symbol=symkey,
        currency=symkey[-3:],
        size=float(vol),
        avg_price=float(pos),
    )


def normalize_symbol(
    ticker: str
) -> str:
    symlen = len(ticker)
    if symlen == 6:
        return ticker.lower()
    else:
        for sym in ['XXBT', 'XXMR', 'ZEUR']:
            if sym in ticker:
                ticker = ticker.replace(sym, sym[1:])
        return ticker.lower()


def make_auth_sub(data: Dict[str, Any]) -> Dict[str, str]:
    """Create a request subscription packet dict.

    ## TODO: point to the auth urls
    https://docs.kraken.com/websockets/#message-subscribe

    """
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
    userref_counter = count()
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
                ## XXX: how do I handle new orders
                reqid=temp_id,
            )

            err = resp['error']
            if err:
                log.error(f'Failed to submit order')
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
                ## TODO: handle multiple cancels
                ##       txid is an array of strings
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

            try:
                # Check to make sure there was no error returned by
                # the kraken endpoint. Assert one order was cancelled
                assert resp['error'] == []
                assert resp['result']['count'] == 1

                ## TODO: Change this code using .get
                try:
                    pending = resp['result']['pending']
                # Check to make sure the cancellation is NOT pending,
                # then send the confirmation to the ems order stream
                except KeyError:
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
            except AssertionError:
                log.error(f'Order cancel was not successful')

        else:
            log.error(f'Unknown order command: {request_msg}')


@tractor.context
async def trades_dialogue(
    ctx: tractor.Context,
    loglevel: str = None,
) -> AsyncIterator[Dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    # Generate

    @asynccontextmanager
    async def subscribe(ws: wsproto.WSConnection, token: str):
        ## TODO: Fix docs and points to right urls
        # XXX: setup subs
        # https://docs.kraken.com/websockets/#message-subscribe
        # specific logic for this in kraken's shitty sync client:
        # https://github.com/krakenfx/kraken-wsclient-py/blob/master/kraken_wsclient_py/kraken_wsclient_py.py#L188
        trades_sub = make_auth_sub(
            {'name': 'openOrders', 'token': token}
        )

        # TODO: we want to eventually allow unsubs which should
        # be completely fine to request from a separate task
        # since internally the ws methods appear to be FIFO
        # locked.
        await ws.send_msg(trades_sub)

        ## trade data (aka L1)
        #l1_sub = make_sub(
        #    list(ws_pairs.values()),
        #    {'name': 'spread'}  # 'depth': 10}
        #)

        ## pull a first quote and deliver
        #await ws.send_msg(l1_sub)

        yield

        # unsub from all pairs on teardown
        await ws.send_msg({
            'event': 'unsubscribe',
            'subscription': ['openOrders'],
        })

        # XXX: do we need to ack the unsub?
        # await ws.recv_msg()


    # Authenticated block
    async with get_client() as client:
        acc_name = 'kraken.' + client._name
        positions, vols = await client.get_positions()

        all_positions = []

        for ticker, pos in positions.items():
            norm_sym = normalize_symbol(ticker)
            if float(vols[ticker]) != 0:
                msg = pack_position(acc_name, norm_sym, pos, vols[ticker])
                all_positions.append(msg.dict())

        open_orders = await client.kraken_endpoint('OpenOrders', {})
        #await tractor.breakpoint()

        await ctx.started((all_positions, (acc_name,)))

        #await trio.sleep_forever()

        # Get websocket token for authenticated data stream
        # Assert that a token was actually received 
        resp = await client.kraken_endpoint('GetWebSocketsToken', {})
        assert resp['error'] == []
        token = resp['result']['token']

        async with (
            ctx.open_stream() as ems_stream,
            trio.open_nursery() as n,
        ):
            ## TODO: maybe add multiple accounts
            n.start_soon(handle_order_requests, client, ems_stream)

            async with open_autorecon_ws(
                'wss://ws-auth.kraken.com/',
                fixture=subscribe,
                token=token,
            ) as ws:
                from pprint import pprint
                async for msg in process_order_msgs(ws):
                    pprint(msg)


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


async def process_order_msgs(
    ws: NoBsWs,
):
    '''
    Parse and pack data feed messages.

    '''
    async for msg in stream_messages(ws):

        # TODO: write your order event parser here!
        # HINT: create a ``pydantic.BaseModel`` to parse and validate
        # and then in the caller recast to our native ``BrokerdX`` msg types.

        # form of order msgs:
        # [{'OIZACU-HB2JZ-YA2QEF': {'lastupdated': '1644595511.768544',
        #     'status': 'canceled', 'vol_exec': '0.00000000', 'cost':
        #     '0.00000000', 'fee': '0.00000000', 'avg_price':
        #     '0.00000000', 'userref': 1, 'cancel_reason': 'User
        #     requested'}}], 'openOrders', {'sequence': 4}]

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
    """Create a request subscription packet dict.

    https://docs.kraken.com/websockets/#message-subscribe

    """
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
    """Fill historical bars into shared mem / storage afap.
    """
    with trio.CancelScope() as cs:
        async with open_cached_client('kraken') as client:
            bars = await client.bars(symbol=sym)
            shm.push(bars)
            task_status.started(cs)


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: List[str],
    shm: ShmArray,
    feed_is_live: trio.Event,
    loglevel: str = None,

    # backend specific
    sub_type: str = 'ohlc',

    # startup sync
    task_status: TaskStatus[Tuple[Dict, Dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    """Subscribe for ohlc stream of quotes for ``pairs``.

    ``pairs`` must be formatted <crypto_symbol>/<fiat_symbol>.
    """
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
