"""
Questrade API backend.
"""
import time
from datetime import datetime
from functools import partial
import configparser
from operator import itemgetter
from typing import List, Tuple, Dict, Any, Iterator, NamedTuple

import trio
from async_generator import asynccontextmanager

from ..calc import humanize, percent_change
from . import config
from ._util import resproc, BrokerError
from ..log import get_logger, colorize_json
from .._async_utils import async_lifo_cache

# TODO: move to urllib3/requests once supported
import asks
asks.init('trio')

log = get_logger(__name__)

_refresh_token_ep = 'https://login.questrade.com/oauth2/'
_version = 'v1'

# stock queries/sec
# it seems 4 rps is best we can do total
_rate_limit = 4


class QuestradeError(Exception):
    "Non-200 OK response code"


class ContractsKey(NamedTuple):
    symbol: str
    id: int
    expiry: datetime


class _API:
    """Questrade API endpoints exposed as methods and wrapped with an
    http session.
    """
    def __init__(self, session: asks.Session):
        self._sess = session

    async def _request(self, path: str, params=None) -> dict:
        resp = await self._sess.get(path=f'/{path}', params=params)
        return resproc(resp, log)

    async def accounts(self) -> dict:
        return await self._request('accounts')

    async def time(self) -> dict:
        return await self._request('time')

    async def markets(self) -> dict:
        return await self._request('markets')

    async def search(self, prefix: str) -> dict:
        return await self._request(
            'symbols/search', params={'prefix': prefix})

    async def symbols(self, ids: str = '', names: str = '') -> dict:
        log.debug(f"Symbol lookup for {ids or names}")
        return await self._request(
            'symbols', params={'ids': ids, 'names': names})

    async def quotes(self, ids: str) -> dict:
        quotes = (await self._request(
            'markets/quotes', params={'ids': ids}))['quotes']
        for quote in quotes:
            quote['key'] = quote['symbol']
        return quotes

    async def candles(self, id: str, start: str, end, interval) -> dict:
        return await self._request(f'markets/candles/{id}', params={})

    async def balances(self, id: str) -> dict:
        return await self._request(f'accounts/{id}/balances')

    async def postions(self, id: str) -> dict:
        return await self._request(f'accounts/{id}/positions')

    async def option_contracts(self, symbol_id: str) -> dict:
        "Retrieve all option contract API ids with expiry -> strike prices."
        contracts = await self._request(f'symbols/{symbol_id}/options')
        return contracts['optionChain']

    async def option_quotes(
        self,
        contracts: Dict[ContractsKey, Dict[int, dict]],
        option_ids: List[int] = [],  # if you don't want them all
    ) -> dict:
        """Retrieve option chain quotes for all option ids or by filter(s).
        """
        filters = [
            {
                "underlyingId": int(symbol_id),
                "expiryDate": str(expiry),
            }
            # every expiry per symbol id
            for (symbol, symbol_id, expiry), bystrike in contracts.items()
        ]
        resp = await self._sess.post(
            path=f'/markets/quotes/options',
            json={'filters': filters, 'optionIds': option_ids}
        )
        return resproc(resp, log)['optionQuotes']


class Client:
    """API client suitable for use as a long running broker daemon or
    single api requests.

    Provides a high-level api which wraps the underlying endpoint calls.
    """
    def __init__(self, config: configparser.ConfigParser):
        self._sess = asks.Session()
        self.api = _API(self._sess)
        self._conf = config
        self.access_data = {}
        self._reload_config(config)
        self._symbol_cache: Dict[str, int] = {}
        self._contracts2expiries = {}
        self._optids2contractinfo = {}

    def _reload_config(self, config=None, **kwargs):
        log.warn("Reloading access config data")
        self._conf = config or get_config(**kwargs)
        self.access_data = dict(self._conf['questrade'])

    async def _new_auth_token(self) -> dict:
        """Request a new api authorization ``refresh_token``.

        Gain api access using either a user provided or existing token.
        See the instructions::

        http://www.questrade.com/api/documentation/getting-started
        http://www.questrade.com/api/documentation/security
        """
        resp = await self._sess.get(
            _refresh_token_ep + 'token',
            params={'grant_type': 'refresh_token',
                    'refresh_token': self.access_data['refresh_token']}
        )
        data = resproc(resp, log)
        self.access_data.update(data)

        return data

    def _prep_sess(self) -> None:
        """Fill http session with auth headers and a base url.
        """
        data = self.access_data
        # set access token header for the session
        self._sess.headers.update({
            'Authorization': (f"{data['token_type']} {data['access_token']}")})
        # set base API url (asks shorthand)
        self._sess.base_location = self.access_data['api_server'] + _version

    async def _revoke_auth_token(self) -> None:
        """Revoke api access for the current token.
        """
        token = self.access_data['refresh_token']
        log.debug(f"Revoking token {token}")
        resp = await asks.post(
            _refresh_token_ep + 'revoke',
            headers={'token': token}
        )
        return resp

    async def ensure_access(self, force_refresh: bool = False) -> dict:
        """Acquire new ``access_token`` and/or ``refresh_token`` if necessary.

        Checks if the locally cached (file system) ``access_token`` has expired
        (based on a ``expires_at`` time stamp stored in the brokers.ini config)
        expired (normally has a lifetime of 3 days). If ``false is set then
        and refreshs token if necessary using the ``refresh_token``. If the
        ``refresh_token`` has expired a new one needs to be provided by the
        user.
        """
        access_token = self.access_data.get('access_token')
        expires = float(self.access_data.get('expires_at', 0))
        expires_stamp = datetime.fromtimestamp(
            expires).strftime('%Y-%m-%d %H:%M:%S')
        if not access_token or (expires < time.time()) or force_refresh:
            log.debug(
                f"Refreshing access token {access_token} which expired at"
                f" {expires_stamp}")
            try:
                data = await self._new_auth_token()
            except BrokerError as qterr:
                if "We're making some changes" in str(qterr.args[0]):
                    # API service is down
                    raise QuestradeError("API is down for maintenance")
                elif qterr.args[0].decode() == 'Bad Request':
                    # likely config ``refresh_token`` is expired but may
                    # be updated in the config file via another piker process
                    self._reload_config()
                    try:
                        data = await self._new_auth_token()
                    except BrokerError as qterr:
                        if qterr.args[0].decode() == 'Bad Request':
                            # actually expired; get new from user
                            self._reload_config(force_from_user=True)
                            data = await self._new_auth_token()
                        else:
                            raise QuestradeError(qterr)
                else:
                    raise qterr

            # store absolute token expiry time
            self.access_data['expires_at'] = time.time() + float(
                data['expires_in'])
            # write to config on disk
            write_conf(self)
        else:
            log.debug(f"\nCurrent access token {access_token} expires at"
                      f" {expires_stamp}\n")

        self._prep_sess()
        return self.access_data

    async def tickers2ids(self, tickers):
        """Helper routine that take a sequence of ticker symbols and returns
        their corresponding QT numeric symbol ids.

        Cache any symbol to id lookups for later use.
        """
        cache = self._symbol_cache
        symbols2ids = {}
        for symbol in tickers:
            id = cache.get(symbol)
            if id is not None:
                symbols2ids[symbol] = id

        # still missing uncached values - hit the server
        to_lookup = list(set(tickers) - set(symbols2ids))
        if to_lookup:
            data = await self.api.symbols(names=','.join(to_lookup))
            for ticker, symbol in zip(to_lookup, data['symbols']):
                name = symbol['symbol']
                assert name == ticker
                cache[name] = symbols2ids[name] = str(symbol['symbolId'])

        return symbols2ids

    async def symbol_data(self, tickers: List[str]):
        """Return symbol data for ``tickers``.
        """
        t2ids = await self.tickers2ids(tickers)
        ids = ','.join(t2ids.values())
        symbols = {}
        for pkt in (await self.api.symbols(ids=ids))['symbols']:
            symbols[pkt['symbol']] = pkt

        return symbols

    async def quote(self, tickers: [str]):
        """Return stock quotes for each ticker in ``tickers``.
        """
        t2ids = await self.tickers2ids(tickers)
        quotes = []
        if t2ids:
            ids = ','.join(t2ids.values())
            quotes = (await self.api.quotes(ids=ids))

        return quotes

    async def symbol2contracts(
        self,
        symbol: str
    ) -> Dict[Tuple[str, int, datetime], dict]:
        """Return option contract for the given symbol.

        The most useful part is the expiries which can be passed to the option
        chain endpoint but specifc contract ids can be pulled here as well.
        """
        id = int((await self.tickers2ids([symbol]))[symbol])
        contracts = await self.api.option_contracts(id)
        return {
            ContractsKey(
                symbol=symbol,
                id=id,
                # convert to native datetime objs for sorting
                expiry=datetime.fromisoformat(item['expiryDate'])):
                    item for item in contracts
        }

    async def get_all_contracts(
        self,
        symbols: Iterator[str],
        # {symbol_id: {dt_iso_contract: {strike_price: {contract_id: id}}}}
    ) -> Dict[int, Dict[str, Dict[int, Any]]]:
        """Look up all contracts for each symbol in ``symbols`` and return the
        of symbol ids to contracts by further organized by expiry and strike
        price.

        This routine is a bit slow doing all the contract lookups (a request
        per symbol) and thus the return values should be cached for use with
        ``option_chains()``.
        """
        by_key = {}
        for symbol in symbols:
            contracts = await self.symbol2contracts(symbol)
            # FIXME: chainPerRoot here is probably why in some UIs
            # you see a second chain with a (1) suffixed; should
            # probably handle this eventually.
            for key, byroot in sorted(
                # sort by datetime
                contracts.items(),
                key=lambda item: item[0].expiry
            ):
                by_key[
                    ContractsKey(
                        key.symbol,
                        key.id,
                        # converting back - maybe just do this initially?
                        key.expiry.isoformat(timespec='microseconds'),
                    )
                ] = {
                    item['strikePrice']: item for item in
                    byroot['chainPerRoot'][0]['chainPerStrikePrice']
                }

        # fill out contract id to strike expiry map
        for tup, bystrikes in by_key.items():
            for strike, ids in bystrikes.items():
                for key, contract_type in (
                    ('callSymbolId', 'call'), ('putSymbolId', 'put')
                ):
                    self._optids2contractinfo[
                        ids[key]] = {
                            'strike': strike,
                            'expiry': tup.expiry,
                            'contract_type': contract_type,
                        }
        return by_key

    async def option_chains(
        self,
        # see dict output from ``get_all_contracts()``
        contracts: dict,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Return option chain snap quote for each ticker in ``symbols``.
        """
        batch = []
        for key, bystrike in contracts.items():
            quotes = await self.api.option_quotes({key: bystrike})
            for quote in quotes:
                # index by .symbol, .expiry since that's what
                # a subscriber (currently) sends initially
                quote['key'] = (key[0], key[2])
                # update with expiry and strike (Obviously the
                # QT api designers are using some kind of severely
                # stupid disparate table system where they keep
                # contract info in a separate table from the quote format
                # keys. I'm really not surprised though - windows shop..)
                quote.update(self._optids2contractinfo[quote['symbolId']])
            batch.extend(quotes)

        return batch


async def token_refresher(client):
    """Coninually refresh the ``access_token`` near its expiry time.
    """
    while True:
        await trio.sleep(
            float(client.access_data['expires_at']) - time.time() - .1)
        await client.ensure_access(force_refresh=True)


def _token_from_user(conf: 'configparser.ConfigParser') -> None:
    """Get API token from the user on the console.
    """
    refresh_token = input("Please provide your Questrade access token: ")
    conf['questrade'] = {'refresh_token': refresh_token}


def get_config(force_from_user=False) -> "configparser.ConfigParser":
    conf, path = config.load()
    if not conf.has_section('questrade') or (
        not conf['questrade'].get('refresh_token') or (
            force_from_user)
    ):
        log.warn(
            f"No valid refresh token could be found in {path}")
        _token_from_user(conf)

    return conf


def write_conf(client):
    """Save access creds to config file.
    """
    client._conf['questrade'] = client.access_data
    config.write(client._conf)


@asynccontextmanager
async def get_client() -> Client:
    """Spawn a broker client.

    A client must adhere to the method calls in ``piker.broker.core``.
    """
    conf = get_config()
    log.debug(f"Loaded config:\n{colorize_json(dict(conf['questrade']))}")
    client = Client(conf)
    await client.ensure_access()

    try:
        log.debug("Check time to ensure access token is valid")
        try:
            # await client.api.time()
            quote = await client.quote(['RY.TO'])
        except Exception as err:
            # import pdb; pdb.set_trace()
            # access token is likely no good
            log.warn(f"Access token {client.access_data['access_token']} seems"
                     f" expired, forcing refresh")
            await client.ensure_access(force_refresh=True)
            await client.api.time()

        accounts = await client.api.accounts()
        log.info(f"Available accounts:\n{colorize_json(accounts)}")
        yield client
    finally:
        write_conf(client)


async def stock_quoter(client: Client, tickers: List[str]):
    """Stock quoter context.

    Yeah so fun times..QT has this symbol to ``int`` id lookup system that you
    have to use to get any quotes. That means we try to be smart and maintain
    a cache of this map lazily as requests from in for new tickers/symbols.
    Most of the closure variables here are to deal with that.
    """
    @async_lifo_cache(maxsize=128)
    async def get_symbol_id_seq(symbols: Tuple[str]):
        """For each tuple ``(symbol_1, symbol_2, ... , symbol_n)``
        return a symbol id sequence string ``'id_1,id_2, ... , id_n'``.
        """
        return ','.join(map(str, (await client.tickers2ids(symbols)).values()))

    async def get_quote(tickers):
        """Query for quotes using cached symbol ids.
        """
        if not tickers:
            # don't hit the network
            return {}

        ids = await get_symbol_id_seq(tuple(tickers))

        try:
            quotes_resp = await client.api.quotes(ids=ids)
        except (QuestradeError, BrokerError) as qterr:
            if "Access token is invalid" not in str(qterr.args[0]):
                raise
            # out-of-process piker actor may have
            # renewed already..
            client._reload_config()
            try:
                quotes_resp = await client.api.quotes(ids=ids)
            except BrokerError as qterr:
                if "Access token is invalid" in str(qterr.args[0]):
                    # TODO: this will crash when run from a sub-actor since
                    # STDIN can't be acquired. The right way to handle this
                    # is to make a request to the parent actor (i.e.
                    # spawner of this) to call this
                    # `client.ensure_access()` locally thus blocking until
                    # the user provides an API key on the "client side"
                    await client.ensure_access(force_refresh=True)
                    quotes_resp = await client.api.quotes(ids=ids)

        # post-processing
        for quote in quotes_resp:
            if quote.get('delay', 0) > 0:
                log.warn(f"Delayed quote:\n{quote}")

        return quotes_resp

    return get_quote


async def option_quoter(client: Client, tickers: List[str]):
    """Option quoter context.
    """
    # sanity
    if isinstance(tickers[0], tuple):
        datetime.fromisoformat(tickers[0][1])
    else:
        raise ValueError(f'Option subscription format is (symbol, expiry)')

    @async_lifo_cache(maxsize=128)
    async def get_contract_by_date(sym_date_pairs: Tuple[Tuple[str, str]]):
        """For each tuple,
        ``(symbol_date_1, symbol_date_2, ... , symbol_date_n)``
        return a contract dict.
        """
        symbols = map(itemgetter(0), sym_date_pairs)
        dates = map(itemgetter(1), sym_date_pairs)
        contracts = await client.get_all_contracts(symbols)
        selected = {}
        for key, val in contracts.items():
            if key.expiry in dates:
                selected[key] = val

        return selected

    async def get_quote(symbol_date_pairs):
        """Query for quotes using cached symbol ids.
        """
        contracts = await get_contract_by_date(
            tuple(symbol_date_pairs))
        try:
            quotes = await client.option_chains(contracts)
        except (QuestradeError, BrokerError) as qterr:
            if "Access token is invalid" not in str(qterr.args[0]):
                raise
            # out-of-process piker actor may have
            # renewed already..
            client._reload_config()
            try:
                quotes = await client.option_chains(contracts)
            except BrokerError as qterr:
                if "Access token is invalid" in str(qterr.args[0]):
                    # TODO: this will crash when run from a sub-actor since
                    # STDIN can't be acquired. The right way to handle this
                    # is to make a request to the parent actor (i.e.
                    # spawner of this) to call this
                    # `client.ensure_access()` locally thus blocking until
                    # the user provides an API key on the "client side"
                    await client.ensure_access(force_refresh=True)
                    quotes = await client.option_chains(contracts)

        return quotes

    return get_quote


# Questrade column order / value conversion
# XXX: keys-values in this map define the final column values which will
# be "displayable" but not necessarily used for "data processing"
# (i.e. comparisons for sorting purposes or other calculations).
_qt_stock_keys = {
    'symbol': 'symbol',  # done manually in qtconvert
    '%': '%',
    'lastTradePrice': 'last',
    'askPrice': 'ask',
    'bidPrice': 'bid',
    'lastTradeSize': 'size',
    'bidSize': 'bsize',
    'askSize': 'asize',
    'VWAP': ('VWAP', partial(round, ndigits=3)),
    'mktcap': ('mktcap', humanize),
    '$ vol': ('$ vol', humanize),
    'volume': ('vol', humanize),
    'close': 'close',
    'openPrice': 'open',
    'lowPrice': 'low',
    'highPrice': 'high',
    # 'low52w': 'low52w',  # put in info widget
    # 'high52w': 'high52w',
    # "lastTradePriceTrHrs": 7.99,
    # 'lastTradeTime': ('time', datetime.fromisoformat),
    # "lastTradeTick": "Equal",
    # "symbolId": 3575753,
    # "tier": "",
    # 'isHalted': 'halted',  # as subscript 'h'
    # 'delay': 'delay',  # as subscript 'p'
}

# BidAskLayout columns which will contain three cells the first stacked on top
# of the other 2
_stock_bidasks = {
    'last': ['bid', 'ask'],
    'size': ['bsize', 'asize'],
    'VWAP': ['low', 'high'],
    'vol': ['mktcap', '$ vol'],
}


def format_stock_quote(
    quote: dict,
    symbol_data: dict,
    keymap: dict = _qt_stock_keys,
) -> Tuple[dict, dict]:
    """Remap a list of quote dicts ``quotes`` using the mapping of old keys
    -> new keys ``keymap`` returning 2 dicts: one with raw data and the other
    for display.

    Returns 2 dicts: first is the original values mapped by new keys,
    and the second is the same but with all values converted to a
    "display-friendly" string format.
    """
    last = quote['lastTradePrice']
    symbol = quote['symbol']
    previous = symbol_data[symbol]['prevDayClosePrice']
    change = percent_change(previous, last)
    share_count = symbol_data[symbol].get('outstandingShares', None)
    mktcap = share_count * last if (last and share_count) else 0
    computed = {
        'symbol': quote['symbol'],
        '%': round(change, 3),
        'mktcap': mktcap,
        # why QT do you have to be an asshole shipping null values!!!
        '$ vol': round((quote['VWAP'] or 0) * (quote['volume'] or 0), 3),
        'close': previous,
    }
    new = {}
    displayable = {}

    for key, new_key in keymap.items():
        display_value = value = computed.get(key) or quote.get(key)

        # API servers can return `None` vals when markets are closed (weekend)
        value = 0 if value is None else value

        # convert values to a displayble format using available formatting func
        if isinstance(new_key, tuple):
            new_key, func = new_key
            display_value = func(value) if value else value

        new[new_key] = value
        displayable[new_key] = display_value

    return new, displayable


_qt_option_keys = {
    "lastTradePrice": 'last',
    "askPrice": 'ask',
    "bidPrice": 'bid',
    "lastTradeSize": 'size',
    "bidSize": 'bsize',
    "askSize": 'asize',
    'VWAP': ('VWAP', partial(round, ndigits=3)),
    "lowPrice": 'low',
    "highPrice": 'high',
    # "expiry": "expiry",
    # "delay": 0,
    "delta": ('delta', partial(round, ndigits=3)),
    # "gamma": ('gama', partial(round, ndigits=3)),
    # "rho": ('rho', partial(round, ndigits=3)),
    # "theta": ('theta', partial(round, ndigits=3)),
    # "vega": ('vega', partial(round, ndigits=3)),
    '$ vol': ('$ vol', humanize),
    'volume': ('vol', humanize),
    # "2021-01-15T00:00:00.000000-05:00",
    # "isHalted": false,
    # "key": [
    #     "APHA.TO",
    #     "2021-01-15T00:00:00.000000-05:00"
    # ],
    # "lastTradePriceTrHrs": null,
    # "lastTradeTick": 'tick',
    "lastTradeTime": 'time',
    "openInterest": 'oi',
    "openPrice": 'open',
    # "strike": 'strike',
    # "symbol": "APHA15Jan21P8.00.MX",
    # "symbolId": 23881868,
    # "underlying": "APHA.TO",
    # "underlyingId": 8297492,
    "symbol": 'symbol',
    "contract_type": 'contract_type',
    "volatility": (
        'volatility',
        lambda v: '{}%'.format(round(v, ndigits=2))
    ),
    "strike": 'strike',
}

_option_bidasks = {
    'last': ['bid', 'ask'],
    'size': ['bsize', 'asize'],
    'VWAP': ['low', 'high'],
    'vol': ['oi', '$ vol'],
}


def format_option_quote(
    quote: dict,
    symbol_data: dict,
    keymap: dict = _qt_option_keys,
) -> Tuple[dict, dict]:
    """Remap a list of quote dicts ``quotes`` using the mapping of old keys
    -> new keys ``keymap`` returning 2 dicts: one with raw data and the other
    for display.

    Returns 2 dicts: first is the original values mapped by new keys,
    and the second is the same but with all values converted to a
    "display-friendly" string format.
    """
    # TODO: need historical data..
    # (cause why would QT keep their quote structure consistent across
    # assets..)
    # previous = symbol_data[symbol]['prevDayClosePrice']
    # change = percent_change(previous, last)
    computed = {
        # why QT do you have to be an asshole shipping null values!!!
        '$ vol': round((quote['VWAP'] or 0) * (quote['volume'] or 0), 3),
        # '%': round(change, 3),
        # 'close': previous,
    }
    new = {}
    displayable = {}

    # structuring and normalization
    for key, new_key in keymap.items():
        display_value = value = computed.get(key) or quote.get(key)

        # API servers can return `None` vals when markets are closed (weekend)
        value = 0 if value is None else value

        # convert values to a displayble format using available formatting func
        if isinstance(new_key, tuple):
            new_key, func = new_key
            display_value = func(value) if value else value

        new[new_key] = value
        displayable[new_key] = display_value

    return new, displayable
