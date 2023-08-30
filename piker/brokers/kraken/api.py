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
Core (web) API client

'''
from contextlib import asynccontextmanager as acm
from datetime import datetime
import itertools
from typing import (
    Any,
    Union,
)
import time

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
from piker.data import def_iohlcv_fields
from piker.accounting._mktinfo import (
    Asset,
    digits_to_dec,
    dec_digits,
)
from piker.brokers._util import (
    resproc,
    SymbolNotFound,
    BrokerError,
    DataThrottle,
)
from piker.accounting import Transaction
from piker.log import get_logger
from .symbols import Pair

log = get_logger('piker.brokers.kraken')

# <uri>/<version>/
_url = 'https://api.kraken.com/0'
# TODO: this is the only backend providing this right?
# in which case we should drop it from the defaults and
# instead make a custom fields descr in this module!
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


class Client:

    # assets and mkt pairs are key-ed by kraken's ReST response
    # symbol-bs_mktids (we call them "X-keys" like fricking
    # "XXMRZEUR"). these keys used directly since ledger endpoints
    # return transaction sets keyed with the same set!
    _Assets: dict[str, Asset] = {}
    _AssetPairs: dict[str, Pair] = {}

    # offer lookup tables for all .altname and .wsname
    # to the equivalent .xname so that various symbol-schemas
    # can be mapped to `Pair`s in the tables above.
    _altnames: dict[str, str] = {}
    _wsnames: dict[str, str] = {}

    # key-ed by `Pair.bs_fqme: str`, and thus used for search
    # allowing for lookup using piker's own FQME symbology sys.
    _pairs: dict[str, Pair] = {}
    _assets: dict[str, Asset] = {}

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

    @property
    def pairs(self) -> dict[str, Pair]:

        if self._pairs is None:
            raise RuntimeError(
                "Client didn't run `.get_mkt_pairs()` on startup?!"
            )

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
        by_bsmktid: dict[str, dict] = resp['result']

        balances: dict = {}
        for xname, bal in by_bsmktid.items():
            asset: Asset = self._Assets[xname]

            # TODO: which KEY should we use? it's used to index
            # the `Account.pps: dict` ..
            key: str = asset.name.lower()
            # TODO: should we just return a `Decimal` here
            # or is the rounded version ok?
            balances[key] = round(
                float(bal),
                ndigits=dec_digits(asset.tx_tick)
            )

        return balances

    async def get_assets(
        self,
        reload: bool = False,

    ) -> dict[str, Asset]:
        '''
        Load and cache all asset infos and pack into
        our native ``Asset`` struct.

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
        if (
            not self._assets
            or reload
        ):
            resp = await self._public('Assets', {})
            assets: dict[str, dict] = resp['result']

            for bs_mktid, info in assets.items():

                altname: str = info['altname']
                aclass: str = info['aclass']
                asset = Asset(
                    name=altname,
                    atype=f'crypto_{aclass}',
                    tx_tick=digits_to_dec(info['decimals']),
                    info=info,
                )
                # NOTE: yes we keep 2 sets since kraken insists on
                # keeping 3 frickin sets bc apparently they have
                # no sane data engineers whol all like different
                # keys for their fricking symbology sets..
                self._Assets[bs_mktid] = asset
                self._assets[altname.lower()] = asset
                self._assets[altname] = asset

        # we return the "most native" set merged with our preferred
        # naming (which i guess is the "altname" one) since that's
        # what the symcache loader will be storing, and we need the
        # keys that are easiest to match against in any trade
        # records.
        return self._Assets | self._assets

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

        if xfers:
            import tractor
            await tractor.pp()

        trans: dict[str, Transaction] = {}
        for entry in xfers:
            # look up the normalized name and asset info
            asset_key: str = entry['asset']
            asset: Asset = self._Assets[asset_key]
            asset_key: str = asset.name.lower()

            # XXX: this is in the asset units (likely) so it isn't
            # quite the same as a commisions cost necessarily..)
            # TODO: also round this based on `Pair` cost precision info?
            cost = float(entry['fee'])
            # fqme: str = asset_key + '.kraken'

            tx = Transaction(
                fqme=asset_key,  # this must map to an entry in .assets!
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

                # not a trade but a withdrawal or deposit on the
                # asset (chain) system.
                etype='transfer',

            )
            trans[tx.tid] = tx

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

    async def asset_pairs(
        self,
        pair_patt: str | None = None,

    ) -> dict[str, Pair] | Pair:
        '''
        Query for a tradeable asset pair (info), or all if no input
        pattern is provided.

        https://docs.kraken.com/rest/#tag/Market-Data/operation/getTradableAssetPairs

        '''
        if not self._AssetPairs:
            # get all pairs by default, or filter
            # to whatever pattern is provided as input.
            req_pairs: dict[str, str] | None = None
            if pair_patt is not None:
                req_pairs = {'pair': pair_patt}

            resp = await self._public(
                'AssetPairs',
                req_pairs,
            )
            err = resp['error']
            if err:
                raise SymbolNotFound(pair_patt)

            # NOTE: we try to key pairs by our custom defined
            # `.bs_fqme` field since we want to offer search over
            # this pattern set, callers should fill out lookup
            # tables for kraken's bs_mktid keys to map to these
            # keys!
            # XXX: FURTHER kraken's data eng team decided to offer
            # 3 frickin market-pair-symbol key sets depending on
            # which frickin API is being used.
            # Example for the trading pair 'LTC<EUR'
            # - the "X-key" from rest eps 'XLTCZEUR'
            # - the "websocket key" from ws msgs is 'LTC/EUR'
            # - the "altname key" also delivered in pair info is 'LTCEUR'
            for xkey, data in resp['result'].items():

                # NOTE: always cache in pairs tables for faster lookup
                pair = Pair(xname=xkey, **data)

                # register the above `Pair` structs for all
                # key-sets/monikers: a set of 4 (frickin) tables
                # acting as a combined surjection of all possible
                # (and stupid) kraken names to their `Pair` obj.
                self._AssetPairs[xkey] = pair
                self._pairs[pair.bs_fqme] = pair
                self._altnames[pair.altname] = pair
                self._wsnames[pair.wsname] = pair

        if pair_patt is not None:
            return next(iter(self._pairs.items()))[1]

        return self._AssetPairs

    async def get_mkt_pairs(
        self,
        reload: bool = False,
    ) -> dict:
        '''
        Load all market pair info build and cache it for downstream
        use.

        Multiple pair info lookup tables (like ``._altnames:
        dict[str, str]``) are created for looking up the
        piker-native `Pair`-struct from any input of the three
        (yes, it's that idiotic..) available symbol/pair-key-sets
        that kraken frickin offers depending on the API including
        the .altname, .wsname and the weird ass default set they
        return in ReST responses .xname..

        '''
        if (
            not self._pairs
            or reload
        ):
            await self.asset_pairs()

        return self._AssetPairs

    async def search_symbols(
        self,
        pattern: str,

    ) -> dict[str, Any]:
        '''
        Search for a symbol by "alt name"..

        It is expected that the ``Client._pairs`` table
        gets populated before conducting the underlying fuzzy-search
        over the pair-key set.

        '''
        if not len(self._pairs):
            await self.get_mkt_pairs()
            assert self._pairs, '`Client.get_mkt_pairs()` was never called!?'

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
                            def_iohlcv_fields[1:]
                        )
                    )
                )
            array = np.array(new_bars, dtype=def_iohlcv_fields) if as_np else bars
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
    def to_bs_fqme(
        cls,
        pair_str: str
    ) -> str:
        '''
        Normalize symbol names to to a 3x3 pair from the global
        definition map which we build out from the data retreived from
        the 'AssetPairs' endpoint, see methods above.

        '''
        try:
            return cls._altnames[pair_str.upper()].bs_fqme
        except KeyError as ke:
            raise SymbolNotFound(f'kraken has no {ke.args[0]}')


@acm
async def get_client() -> Client:

    conf = get_config()
    if conf:
        client = Client(
            conf,

            # TODO: don't break these up and just do internal
            # conf lookups instead..
            name=conf['key_descr'],
            api_key=conf['api_key'],
            secret=conf['secret']
        )
    else:
        client = Client({})

    # at startup, load all symbols, and asset info in
    # batch requests.
    async with trio.open_nursery() as nurse:
        nurse.start_soon(client.get_assets)
        await client.get_mkt_pairs()

    yield client
