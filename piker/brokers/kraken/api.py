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
from datetime import datetime
from decimal import Decimal
import itertools
from typing import (
    Any,
    Optional,
    Union,
)
import time

from bidict import bidict
import pendulum
import asks
from fuzzywuzzy import process as fuzzy
import numpy as np
import urllib.parse
import hashlib
import hmac
import base64
import trio

from piker import config
from piker.data.types import Struct
from piker.accounting._mktinfo import (
    Asset,
    digits_to_dec,
    MktPair,
    Symbol,
)
from piker.brokers._util import (
    resproc,
    SymbolNotFound,
    BrokerError,
    DataThrottle,
)
from piker.accounting import Transaction
from . import log

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


# https://www.kraken.com/features/api#get-tradable-pairs
class Pair(Struct):
    altname: str  # alternate pair name
    wsname: str  # WebSocket pair name (if available)
    aclass_base: str  # asset class of base component
    base: str  # asset id of base component
    aclass_quote: str  # asset class of quote component
    quote: str  # asset id of quote component
    lot: str  # volume lot size

    cost_decimals: int
    costmin: float
    pair_decimals: int  # scaling decimal places for pair
    lot_decimals: int  # scaling decimal places for volume

    # amount to multiply lot volume by to get currency volume
    lot_multiplier: float

    # array of leverage amounts available when buying
    leverage_buy: list[int]
    # array of leverage amounts available when selling
    leverage_sell: list[int]

    # fee schedule array in [volume, percent fee] tuples
    fees: list[tuple[int, float]]

    # maker fee schedule array in [volume, percent fee] tuples (if on
    # maker/taker)
    fees_maker: list[tuple[int, float]]

    fee_volume_currency: str  # volume discount currency
    margin_call: str  # margin call level
    margin_stop: str  # stop-out/liquidation margin level
    ordermin: float  # minimum order volume for pair
    tick_size: float  # min price step size
    status: str

    short_position_limit: float = 0
    long_position_limit: float = float('inf')


class Client:

    # global symbol normalization table
    _ntable: dict[str, str] = {}
    _atable: bidict[str, str] = bidict()
    _pairs: dict[str, Pair] = {}

    def __init__(
        self,
        config: dict[str, str],
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
        self._name = name
        self._api_key = api_key
        self._secret = secret

        self.conf: dict[str, str] = config
        self.assets: dict[str, Asset] = {}

    @property
    def pairs(self) -> dict[str, Pair]:
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

    async def get_balances(
        self,
    ) -> dict[str, float]:
        '''
        Return the set of asset balances for this account
        by symbol.

        '''
        resp = await self.endpoint(
            'Balance',
            {},
        )
        by_bsmktid = resp['result']

        # TODO: we need to pull out the "asset" decimals
        # data and return a `decimal.Decimal` instead here!
        # using the underlying Asset
        return {
            self._atable[sym].lower(): float(bal)
            for sym, bal in by_bsmktid.items()
        }

    async def get_assets(self) -> dict[str, dict]:
        '''
        Get all assets available for trading and xfer.

        https://docs.kraken.com/rest/#tag/Market-Data/operation/getAssetInfo

        return msg:
            "asset1": {
                "aclass": "string",
                "altname": "string",
                "decimals": 0,
                "display_decimals": 0,
                "collateral_value": 0,
                "status": "string"
            }

        '''
        resp = await self._public('Assets', {})
        return resp['result']

    async def cache_assets(self) -> None:
        '''
        Load and cache all asset infos and pack into
        our native ``Asset`` struct.

        '''
        assets = await self.get_assets()
        for bs_mktid, info in assets.items():

            aname = self._atable[bs_mktid] = info['altname']
            aclass = info['aclass']

            self.assets[bs_mktid] = Asset(
                name=aname.lower(),
                atype=f'crypto_{aclass}',
                tx_tick=digits_to_dec(info['decimals']),
                info=info,
            )

    async def get_trades(
        self,
        fetch_limit: int | None = None,

    ) -> dict[str, Any]:
        '''
        Get the trades (aka cleared orders) history from the rest endpoint:
        https://docs.kraken.com/rest/#operation/getTradeHistory

        '''
        ofs = 0
        trades_by_id: dict[str, Any] = {}

        for i in itertools.count():
            if (
                fetch_limit
                and i >= fetch_limit
            ):
                break

            # increment 'ofs' pagination offset
            ofs = i*50

            resp = await self.endpoint(
                'TradesHistory',
                {'ofs': ofs},
            )
            by_id = resp['result']['trades']
            trades_by_id.update(by_id)

            # can get up to 50 results per query, see:
            # https://docs.kraken.com/rest/#tag/User-Data/operation/getTradeHistory
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

    async def get_xfers(
        self,
        asset: str,
        src_asset: str = '',

    ) -> dict[str, Transaction]:
        '''
        Get asset balance transfer transactions.

        Currently only withdrawals are supported.

        '''
        resp = await self.endpoint(
            'WithdrawStatus',
            {'asset': asset},
        )
        try:
            xfers: list[dict] = resp['result']
        except KeyError:
            log.exception(f'Kraken suxxx: {resp}')
            return []

        # eg. resp schema:
        # 'result': [{'method': 'Bitcoin', 'aclass': 'currency', 'asset':
        #     'XXBT', 'refid': 'AGBJRMB-JHD2M4-NDI3NR', 'txid':
        #     'b95d66d3bb6fd76cbccb93f7639f99a505cb20752c62ea0acc093a0e46547c44',
        #     'info': 'bc1qc8enqjekwppmw3g80p56z5ns7ze3wraqk5rl9z',
        #     'amount': '0.00300726', 'fee': '0.00001000', 'time':
        #     1658347714, 'status': 'Success'}]}

        trans: dict[str, Transaction] = {}
        for entry in xfers:

            # look up the normalized name and asset info
            asset_key = entry['asset']
            asset = self.assets[asset_key]
            asset_key = self._atable[asset_key].lower()

            # XXX: this is in the asset units (likely) so it isn't
            # quite the same as a commisions cost necessarily..)
            cost = float(entry['fee'])

            fqsn = asset_key + '.kraken'

            # pair = MktPair(
            #     src=asset,
            #     dst=asset,
            #     broker='kraken',
            # )

            # pairinfo = Symbol.from_fqsn(
            #     fqsn,
            #     info={
            #         'asset_type': 'crypto',
            #         'lot_tick_size': asset.tx_tick,
            #     },
            # )

            tran = Transaction(
                fqsn=fqsn,
                sym=asset,
                tid=entry['txid'],
                dt=pendulum.from_timestamp(entry['time']),
                bs_mktid=f'{asset_key}{src_asset}',
                size=-1*(
                    float(entry['amount'])
                    +
                    cost
                ),
                # since this will be treated as a "sell" it
                # shouldn't be needed to compute the be price.
                price='NaN',

                # XXX: see note above
                cost=cost,
            )
            trans[tran.tid] = tran

        return trans

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

    ) -> dict[str, Pair] | Pair:

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
            return Pair(**data)
        else:
            return {key: Pair(**data) for key, data in pairs.items()}

    async def cache_symbols(self) -> dict:
        '''
        Load all market pair info build and cache it for downstream use.

        A ``._ntable: dict[str, str]`` is available for mapping the
        websocket pair name-keys and their http endpoint API (smh)
        equivalents to the "alternative name" which is generally the one
        we actually want to use XD

        '''
        if not self._pairs:
            self._pairs.update(await self.symbol_info())

            # table of all ws and rest keys to their alt-name values.
            ntable: dict[str, str] = {}

            for rest_key in list(self._pairs.keys()):

                pair: Pair = self._pairs[rest_key]
                altname = pair.altname
                wsname = pair.wsname
                ntable[rest_key] = ntable[wsname] = altname

                # register the pair under all monikers, a giant flat
                # surjection of all possible names to each info obj.
                self._pairs[altname] = self._pairs[wsname] = pair

            self._ntable.update(ntable)

        return self._pairs

    async def search_symbols(
        self,
        pattern: str,
        limit: int = None,

    ) -> dict[str, Any]:
        '''
        Search for a symbol by "alt name"..

        It is expected that the ``Client._pairs`` table
        gets populated before conducting the underlying fuzzy-search
        over the pair-key set.

        '''
        if not len(self._pairs):
            await self.cache_symbols()
            assert self._pairs, '`Client.cache_symbols()` was never called!?'

        matches = fuzzy.extractBests(
            pattern,
            self._pairs,
            score_cutoff=50,
        )
        # repack in dict form
        return {item[0].altname: item[0] for item in matches}

    async def bars(
        self,
        symbol: str = 'XBTUSD',

        # UTC 2017-07-02 12:53:20
        since: Union[int, datetime] | None = None,
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

    @classmethod
    def normalize_symbol(
        cls,
        ticker: str
    ) -> tuple[str, Pair]:
        '''
        Normalize symbol names to to a 3x3 pair from the global
        definition map which we build out from the data retreived from
        the 'AssetPairs' endpoint, see methods above.

        '''
        ticker = cls._ntable[ticker]
        return ticker.lower(), cls._pairs[ticker]


@acm
async def get_client() -> Client:

    conf = get_config()
    if conf:
        client = Client(
            conf,
            name=conf['key_descr'],
            api_key=conf['api_key'],
            secret=conf['secret']
        )
    else:
        client = Client({})

    # at startup, load all symbols, and asset info in
    # batch requests.
    async with trio.open_nursery() as nurse:
        nurse.start_soon(client.cache_assets)
        await client.cache_symbols()

    yield client
