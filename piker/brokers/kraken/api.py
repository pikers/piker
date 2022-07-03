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
Kraken web API wrapping.

'''
from contextlib import asynccontextmanager as acm
from dataclasses import field
from datetime import datetime
import itertools
from typing import (
    Any,
    Optional,
    Union,
)
import time

# import trio
# import tractor
import pendulum
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
from pydantic.dataclasses import dataclass
import urllib.parse
import hashlib
import hmac
import base64

from piker import config
from piker.brokers._util import (
    resproc,
    SymbolNotFound,
    BrokerError,
    DataThrottle,
)
from piker.log import get_logger

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
_symbol_info_translation: dict[str, str] = {
    'tick_decimals': 'pair_decimals',
}


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
    ticks: list[Any] = field(default_factory=list)


def get_config() -> dict[str, Any]:

    conf, path = config.load()
    section = conf.get('kraken')

    if section is None:
        log.warning(f'No config section found for kraken in {path}')
        return {}

    return section


def get_kraken_signature(
    urlpath: str,
    data: dict[str, Any],
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
    def pairs(self) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
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
        data: dict[str, Any]

    ) -> dict[str, Any]:
        uri_path = f'/0/private/{method}'
        data['nonce'] = str(int(1000*time.time()))
        return await self._private(method, data, uri_path)

    async def get_trades(
        self,

    ) -> dict[str, Any]:
        '''
        Get the trades (aka cleared orders) history from the rest endpoint:
        https://docs.kraken.com/rest/#operation/getTradeHistory

        '''
        ofs = 0
        trades_by_id: dict[str, Any] = {}

        for i in itertools.count():

            # increment 'ofs' pagination offset
            ofs = i*50

            resp = await self.endpoint(
                'TradesHistory',
                {'ofs': ofs},
            )
            by_id = resp['result']['trades']
            trades_by_id.update(by_id)

            # we can get up to 50 results per query
            if (
                len(by_id) < 50
            ):
                err = resp.get('error')
                if err:
                    raise BrokerError(err)

                # we know we received the max amount of
                # trade results so there may be more history.
                # catch the end of the trades
                count = resp['result']['count']
                break

        # santity check on update
        assert count == len(trades_by_id.values())
        return trades_by_id

    async def submit_limit(
        self,
        symbol: str,
        price: float,
        action: str,
        size: float,
        reqid: str = None,
        validate: bool = False  # set True test call without a real submission

    ) -> dict:
        '''
        Place an order and return integer request id provided by client.

        '''
        # Build common data dict for common keys from both endpoints
        data = {
            "pair": symbol,
            "price": str(price),
            "validate": validate
        }
        if reqid is None:
            # Build order data for kraken api
            data |= {
                "ordertype": "limit",
                "type": action,
                "volume": str(size),
            }
            return await self.endpoint('AddOrder', data)
        else:
            # Edit order data for kraken api
            data["txid"] = reqid
            return await self.endpoint('EditOrder', data)

    async def submit_cancel(
        self,
        reqid: str,
    ) -> dict:
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
    ) -> dict[str, Any]:
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
        since: Optional[Union[int, datetime]] = None,
        count: int = 720,  # <- max allowed per query
        as_np: bool = True,

    ) -> dict:

        if since is None:
            since = pendulum.now('UTC').start_of('minute').subtract(
                minutes=count).timestamp()

        elif isinstance(since, int):
            since = pendulum.from_timestamp(since).timestamp()

        else:  # presumably a pendulum datetime
            since = since.timestamp()

        # UTC 2017-07-02 12:53:20 is oldest seconds value
        since = str(max(1499000000, int(since)))
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
            errmsg = json['error'][0]

            if 'not found' in errmsg:
                raise SymbolNotFound(errmsg + f': {symbol}')

            elif 'Too many requests' in errmsg:
                raise DataThrottle(f'{symbol}')

            else:
                raise BrokerError(errmsg)


@acm
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


def normalize_symbol(
    ticker: str
) -> str:
    '''
    Normalize symbol names to to a 3x3 pair.

    '''
    remap = {
        'XXBTZEUR': 'XBTEUR',
        'XXMRZEUR': 'XMREUR',

        # ws versions? pretty weird..
        'XBT/EUR': 'XBTEUR',
        'XMR/EUR': 'XMREUR',
    }
    symlen = len(ticker)
    if symlen != 6:
        ticker = remap[ticker]
    else:
        raise ValueError(f'Unhandled symbol: {ticker}')

    return ticker.lower()
