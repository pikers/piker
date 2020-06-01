"""
Questrade API backend.
"""
from __future__ import annotations
import inspect
import time
from datetime import datetime
from functools import partial
from itertools import chain
import configparser
from typing import List, Tuple, Dict, Any, Iterator, NamedTuple

import arrow
import trio
from async_generator import asynccontextmanager
import pandas as pd
import numpy as np
import wrapt
import asks

from ..calc import humanize, percent_change
from . import config
from ._util import resproc, BrokerError, SymbolNotFound
from ..log import get_logger, colorize_json
from .._async_utils import async_lifo_cache

log = get_logger(__name__)

_use_practice_account = False
_refresh_token_ep = 'https://{}login.questrade.com/oauth2/'
_version = 'v1'

# stock queries/sec
# it seems 4 rps is best we can do total
_rate_limit = 4

_time_frames = {
    '1m': 'OneMinute',
    '2m': 'TwoMinutes',
    '3m': 'ThreeMinutes',
    '4m': 'FourMinutes',
    '5m': 'FiveMinutes',
    '10m': 'TenMinutes',
    '15m': 'FifteenMinutes',
    '20m': 'TwentyMinutes',
    '30m': 'HalfHour',
    '1h': 'OneHour',
    '2h': 'TwoHours',
    '4h': 'FourHours',
    'D': 'OneDay',
    'W': 'OneWeek',
    'M': 'OneMonth',
    'Y': 'OneYear',
}


class QuestradeError(Exception):
    "Non-200 OK response code"


class ContractsKey(NamedTuple):
    symbol: str
    id: int
    expiry: datetime


def refresh_token_on_err(tries=3):
    """`_API` method decorator which locks the client and refreshes tokens
    before unlocking access to the API again.

    QT's service end can't handle concurrent requests to multiple
    endpoints reliably without choking up and confusing their interal
    servers.
    """

    @wrapt.decorator
    async def wrapper(wrapped, api, args, kwargs):
        assert inspect.iscoroutinefunction(wrapped)
        client = api.client

        if not client._has_access.is_set():
            log.warning("Waiting on access lock")
            await client._has_access.wait()

        for i in range(1, tries):
            try:
                try:
                    client._request_not_in_progress = trio.Event()
                    return await wrapped(*args, **kwargs)
                finally:
                    client._request_not_in_progress.set()
            except (QuestradeError, BrokerError) as qterr:
                if "Access token is invalid" not in str(qterr.args[0]):
                    raise
                # TODO: this will crash when run from a sub-actor since
                # STDIN can't be acquired (ONLY WITH MP). The right way
                # to handle this is to make a request to the parent
                # actor (i.e.  spawner of this) to call this
                # `client.ensure_access()` locally thus blocking until
                # the user provides an API key on the "client side"
                log.warning(f"Tokens are invalid refreshing try {i}..")
                await client.ensure_access(force_refresh=True)
                if i == tries - 1:
                    raise
    return wrapper


class _API:
    """Questrade API endpoints exposed as methods and wrapped with an
    http session.
    """
    def __init__(
        self,
        client: Client,
    ):
        self.client = client
        self._sess: asks.Session = client._sess

    @refresh_token_on_err()
    async def _get(self, path: str, params=None) -> dict:
        """Get an endpoint "reliably" by ensuring access on failure.
        """
        resp = await self._sess.get(path=f'/{path}', params=params)
        return resproc(resp, log)

    async def _new_auth_token(
        self,
        refresh_token: str,
    ) -> dict:
        """Request a new api authorization ``refresh_token``.

        Gain api access using either a user provided or existing token.
        See the instructions::

        http://www.questrade.com/api/documentation/getting-started
        http://www.questrade.com/api/documentation/security
        """
        resp = await self._sess.get(
            self.client._auth_ep + 'token',
            params={'grant_type': 'refresh_token',
                    'refresh_token': refresh_token}
        )
        return resproc(resp, log)

    async def _revoke_auth_token(
        self,
        practise: bool = False,
    ) -> None:
        """Revoke api access for the current token.
        """
        token = self.access_data['refresh_token']
        log.debug(f"Revoking token {token}")
        resp = await asks.post(
            self.client._auth_ep + 'revoke',
            headers={'token': token}
        )
        return resp

    # accounts end points

    async def accounts(self) -> dict:
        return await self._get('accounts')

    async def time(self) -> dict:
        return await self._get('time')

    async def balances(self, id: str) -> dict:
        return await self._get(f'accounts/{id}/balances')

    async def postions(self, id: str) -> dict:
        return await self._get(f'accounts/{id}/positions')

    # market end points

    async def markets(self) -> dict:
        return await self._get('markets')

    async def search(self, prefix: str) -> dict:
        return await self._get(
            'symbols/search', params={'prefix': prefix})

    async def symbols(self, ids: str = '', names: str = '') -> dict:
        log.debug(f"Symbol lookup for {ids or names}")
        return await self._get(
            'symbols', params={'ids': ids, 'names': names})

    async def quotes(self, ids: str) -> dict:
        quotes = (await self._get(
            'markets/quotes', params={'ids': ids}))['quotes']
        for quote in quotes:
            quote['key'] = quote['symbol']
        return quotes

    async def candles(
        self, symbol_id:
        str, start: str,
        end: str,
        interval: str
    ) -> List[Dict[str, float]]:
        """Retrieve historical candles for provided date range.
        """
        return (await self._get(
            f'markets/candles/{symbol_id}',
            params={'startTime': start, 'endTime': end, 'interval': interval},
        ))['candles']

    async def option_contracts(self, symbol_id: str) -> dict:
        "Retrieve all option contract API ids with expiry -> strike prices."
        contracts = await self._get(f'symbols/{symbol_id}/options')
        return contracts['optionChain']

    @refresh_token_on_err()
    async def option_quotes(
        self,
        contracts: Dict[ContractsKey, Dict[int, dict]] = {},
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
            path='/markets/quotes/options',
            # XXX: b'{"code":1024,"message":"The size of the array requested
            #         is not valid: optionIds"}'
            # ^ what I get when trying to use too many ids manually...
            json={'filters': filters, 'optionIds': option_ids}
        )
        return resproc(resp, log)['optionQuotes']


class Client:
    """API client suitable for use as a long running broker daemon or
    single api requests.

    Provides a high-level api which wraps the underlying endpoint calls.
    """
    def __init__(
        self,
        config: dict,
    ):
        # use 2 connections per streaming endpoint (stocks, opts)
        # TODO: when we have more then one account key then this should scale
        # linearly with that.
        self._sess = asks.Session(connections=4)
        self.api = _API(self)
        self._conf = config
        self._is_practice = _use_practice_account or (
            config['questrade'].get('is_practice', False)
        )
        self._auth_ep = _refresh_token_ep.format(
            'practice' if self._is_practice else '')
        self.access_data = {}
        self._reload_config(config=config)
        self._symbol_cache: Dict[str, int] = {}
        self._optids2contractinfo = {}
        self._contract2ids = {}
        # for blocking during token refresh
        self._has_access = trio.Event()
        self._has_access.set()
        self._request_not_in_progress = trio.Event()
        self._request_not_in_progress.set()
        self._mutex = trio.StrictFIFOLock()

    def _reload_config(self, config=None, **kwargs):
        if config:
            self._conf = config
        else:
            self._conf, _ = get_config(**kwargs)
        self.access_data = dict(self._conf['questrade'])

    def write_config(self):
        """Save access creds to config file.
        """
        self._conf['questrade'] = self.access_data
        config.write(self._conf)

    async def ensure_access(
        self,
        force_refresh: bool = False,
        ask_user: bool = True,
    ) -> dict:
        """Acquire a new token set (``access_token`` and ``refresh_token``).

        Checks if the locally cached (file system) ``access_token`` has expired
        (based on a ``expires_at`` time stamp stored in the brokers.ini config)
        expired (normally has a lifetime of 3 days). If ``false is set then
        and refreshs token if necessary using the ``refresh_token``. If the
        ``refresh_token`` has expired a new one needs to be provided by the
        user.
        """
        # wait for ongoing requests to clear (API can't handle
        # concurrent endpoint requests alongside a token refresh)
        await self._request_not_in_progress.wait()

        # block api access to tall other tasks
        # XXX: this is limitation of the API when using a single
        # token whereby their service can't handle concurrent requests
        # to differnet end points (particularly the auth ep) which
        # causes hangs and premature token invalidation issues.
        self._has_access = trio.Event()
        try:
            # don't allow simultaneous token refresh requests
            async with self._mutex:
                access_token = self.access_data.get('access_token')
                expires = float(self.access_data.get('expires_at', 0))
                expires_stamp = datetime.fromtimestamp(
                    expires).strftime('%Y-%m-%d %H:%M:%S')
                if not access_token or (
                    expires < time.time()
                ) or force_refresh:
                    log.info("Refreshing API tokens")
                    log.debug(
                        f"Refreshing access token {access_token} which expired"
                        f" at {expires_stamp}")
                    try:
                        data = await self.api._new_auth_token(
                            self.access_data['refresh_token'])
                    except BrokerError as qterr:

                        def get_err_msg(err):
                            # handle str and bytes...
                            msg = err.args[0]
                            return msg.decode() if msg.isascii() else msg

                        msg = get_err_msg(qterr)

                        if "We're making some changes" in msg:
                            # API service is down
                            raise QuestradeError("API is down for maintenance")

                        elif msg == 'Bad Request':
                            # likely config ``refresh_token`` is expired but
                            # may be updated in the config file via
                            # another actor
                            self._reload_config()
                            try:
                                data = await self.api._new_auth_token(
                                    self.access_data['refresh_token'])
                            except BrokerError as qterr:
                                if get_err_msg(qterr) == 'Bad Request' and (
                                    ask_user
                                ):
                                    # actually expired; get new from user
                                    self._reload_config(force_from_user=True)
                                    data = await self.api._new_auth_token(
                                        self.access_data['refresh_token'])
                                else:
                                    raise QuestradeError(qterr)
                        else:
                            raise qterr

                    self.access_data.update(data)
                    log.debug(f"Updated tokens:\n{data}")
                    # store an absolute access token expiry time
                    self.access_data['expires_at'] = time.time() + float(
                        data['expires_in'])

                    # write to config to disk
                    self.write_config()
                else:
                    log.debug(
                        f"\nCurrent access token {access_token} expires at"
                        f" {expires_stamp}\n")

                # set access token header for the session
                data = self.access_data
                self._sess.headers.update({
                    'Authorization':
                        (f"{data['token_type']} {data['access_token']}")}
                )
                # set base API url (asks shorthand)
                self._sess.base_location = data['api_server'] + _version
        finally:
            self._has_access.set()

        return data

    async def tickers2ids(
        self,
        tickers: Iterator[str]
    ) -> Dict[str, int]:
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

        # still missing uncached values - hit the api server
        to_lookup = list(set(tickers) - set(symbols2ids))
        if to_lookup:
            data = await self.api.symbols(names=','.join(to_lookup))
            for symbol in data['symbols']:
                name = symbol['symbol']
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
                for chain in byroot['chainPerRoot']:
                    optroot = chain['optionRoot']

                    # handle QTs "adjusted contracts" (aka adjusted for
                    # the underlying in some way; usually has a '(1)' in
                    # the expiry key in their UI)
                    adjusted_contracts = optroot not in key.symbol
                    tail = optroot[len(key.symbol):]
                    suffix = '-' + tail if adjusted_contracts else ''

                    by_key[
                        ContractsKey(
                            key.symbol + suffix,
                            key.id,
                            # converting back - maybe just do this initially?
                            key.expiry.isoformat(timespec='microseconds'),
                        )
                    ] = {
                        item['strikePrice']: item for item in
                        chain['chainPerStrikePrice']
                    }

        # fill out contract id to strike expiry map
        for tup, bystrikes in by_key.items():
            for strike, ids in bystrikes.items():
                for key, contract_type in (
                    ('callSymbolId', 'call'), ('putSymbolId', 'put')
                ):
                    contract_int_id = ids[key]
                    self._optids2contractinfo[contract_int_id] = {
                        'strike': strike,
                        'expiry': tup.expiry,
                        'contract_type': contract_type,
                        'contract_key': tup,
                    }
                    # store ids per contract
                    self._contract2ids.setdefault(
                        tup, set()).add(contract_int_id)
        return by_key

    async def option_chains(
        self,
        # see dict output from ``get_all_contracts()``
        contracts: dict,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Return option chain snap quote for each ticker in ``symbols``.
        """
        quotes = await self.api.option_quotes(contracts=contracts)
        # XXX the below doesn't work so well due to the symbol count
        # limit per quote request
        # quotes = await self.api.option_quotes(option_ids=list(contract_ids))
        for quote in quotes:
            id = quote['symbolId']
            contract_info = self._optids2contractinfo[id].copy()
            key = contract_info.pop('contract_key')

            # XXX TODO: this currently doesn't handle adjusted contracts
            # (i.e. ones that we stick a '(1)' after)

            # index by .symbol, .expiry since that's what
            # a subscriber (currently) sends initially
            quote['key'] = (key.symbol, key.expiry)

            # update with expiry and strike (Obviously the
            # QT api designers are using some kind of severely
            # stupid disparate table system where they keep
            # contract info in a separate table from the quote format
            # keys. I'm really not surprised though - windows shop..)
            # quote.update(self._optids2contractinfo[quote['symbolId']])
            quote.update(contract_info)

        return quotes

    async def bars(
        self,
        symbol: str,
        # EST in ISO 8601 format is required... below is EPOCH
        start_date: str = "1970-01-01T00:00:00.000000-05:00",
        time_frame: str = '1m',
        count: float = 20e3,
        is_paid_feed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Retreive OHLCV bars for a symbol over a range to the present.

        .. note::
            The candles endpoint only allows "2000" points per query
            however tests here show that it is 20k candles per query.
        """
        # fix case
        if symbol.islower():
            symbol = symbol.swapcase()

        sids = await self.tickers2ids([symbol])
        if not sids:
            raise SymbolNotFound(symbol)

        sid = sids[symbol]

        # get last market open end time
        est_end = now = arrow.utcnow().to('US/Eastern').floor('minute')
        # on non-paid feeds we can't retreive the first 15 mins
        wd = now.isoweekday()
        if wd > 5:
            quotes = await self.quote([symbol])
            est_end = arrow.get(quotes[0]['lastTradeTime'])
            if est_end.hour == 0:
                # XXX don't bother figuring out extended hours for now
                est_end = est_end.replace(hour=17)

        if not is_paid_feed:
            est_end = est_end.shift(minutes=-15)

        est_start = est_end.shift(minutes=-count)

        start = time.time()
        bars = await self.api.candles(
            sid,
            start=est_start.isoformat(),
            end=est_end.isoformat(),
            interval=_time_frames[time_frame],
        )
        log.debug(
            f"Took {time.time() - start} seconds to retreive {len(bars)} bars")
        return bars


# marketstore TSD compatible numpy dtype for bar
_qt_bars_dt = [
    ('Epoch', 'i8'),
    # ('start', 'S40'),
    # ('end', 'S40'),
    ('low', 'f4'),
    ('high', 'f4'),
    ('open', 'f4'),
    ('close', 'f4'),
    ('volume', 'i8'),
    # ('VWAP', 'f4')
]


def get_OHLCV(
    bar: Dict[str, Any]
) -> Tuple[str, Any]:
    """Return a marketstore key-compatible OHCLV dictionary.
    """
    del bar['end']
    del bar['VWAP']
    bar['start'] = pd.Timestamp(bar['start']).value/10**9
    return tuple(bar.values())


def bars_to_marketstore_structarray(
    bars: List[Dict[str, Any]]
) -> np.array:
    """Return marketstore writeable recarray from sequence of bars
    retrieved via the ``candles`` endpoint.
    """
    return np.array(list(map(get_OHLCV, bars)), dtype=_qt_bars_dt)


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


def get_config(
    config_path: str = None,
    force_from_user: bool = False,
    ask_user_on_failure: bool = False,
) -> "configparser.ConfigParser":
    """Load the broker config from disk.

    By default this is the file:

        ~/.config/piker/brokers.ini

    though may be different depending on your OS.
    """
    log.debug("Reloading access config data")
    conf, path = config.load(config_path)

    # check if the current config has a token
    section = conf.get('questrade')
    has_token = section.get('refresh_token') if section else False

    if force_from_user or ask_user_on_failure and not (section or has_token):
        log.warn("Forcing manual token auth from user")
        _token_from_user(conf)
    else:
        if not section:
            raise ValueError(f"No `questrade` section found in {path}")
        if not has_token:
            raise ValueError(f"No refresh token found in {path}")

    return conf, path


@asynccontextmanager
async def get_client(
    config_path: str = None,
    ask_user: bool = True
) -> Client:
    """Spawn a broker client for making requests to the API service.
    """
    conf, path = get_config(config_path, ask_user_on_failure=ask_user)
    log.debug(f"Loaded config:\n{colorize_json(dict(conf['questrade']))}")
    client = Client(conf)
    await client.ensure_access(ask_user=ask_user)
    try:
        log.debug("Check time to ensure access token is valid")
        # XXX: the `time()` end point requires acc_read Oauth access.
        # In order to use a client you need at least one key with this
        # access enabled in order to do symbol searches and id lookups.
        await client.api.time()
    except Exception:
        raise
        # access token is likely no good
        log.warn(f"Access tokens {client.access_data} seem"
                 f" expired, forcing refresh")
        await client.ensure_access(force_refresh=True, ask_user=ask_user)
        await client.api.time()
    try:
        yield client
    except trio.Cancelled:
        # only write config if we didn't bail out
        client.write_config()
        raise


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
        raise ValueError('Option subscription format is (symbol, expiry)')

    @async_lifo_cache(maxsize=128)
    async def get_contract_by_date(
        sym_date_pairs: Tuple[Tuple[str, str]],
    ):
        """For each tuple,
        ``(symbol_date_1, symbol_date_2, ... , symbol_date_n)``
        return a contract dict.
        """
        symbols, dates = zip(*sym_date_pairs)
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
        return await client.option_chains(contracts)

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
    'MC': ('MC', humanize),
    '$ vol': ('$ vol', humanize),
    'volume': ('volume', humanize),
    # 'close': 'close',
    # 'openPrice': 'open',
    'lowPrice': 'low',
    'highPrice': 'high',
    # 'low52w': 'low52w',  # put in info widget
    # 'high52w': 'high52w',
    # "lastTradePriceTrHrs": 7.99,
    'lastTradeTime': ('fill_time', datetime.fromisoformat),
    "lastTradeTick": 'tick',  # ("Equal", "Up", "Down")
    # "symbolId": 3575753,
    # "tier": "",
    # 'isHalted': 'halted',  # as subscript 'h'
    # 'delay': 'delay',  # as subscript 'p'
}

# BidAskLayout columns which will contain three cells the first stacked on top
# of the other 2 (this is a UI layout instruction)
_stock_bidasks = {
    'last': ['bid', 'ask'],
    'size': ['bsize', 'asize'],
    'VWAP': ['low', 'high'],
    'volume': ['MC', '$ vol'],
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
    symbol = quote['symbol']
    previous = symbol_data[symbol]['prevDayClosePrice']

    computed = {'symbol': symbol}
    last = quote.get('lastTradePrice')
    if last:
        change = percent_change(previous, last)
        share_count = symbol_data[symbol].get('outstandingShares', None)
        mktcap = share_count * last if (last and share_count) else 0
        computed.update({
            # 'symbol': quote['symbol'],
            '%': round(change, 3),
            'MC': mktcap,
            # why questrade do you have to be an asshole shipping null values!!!
            # '$ vol': round((quote['VWAP'] or 0) * (quote['volume'] or 0), 3),
            'close': previous,
        })

    vwap = quote.get('VWAP')
    volume = quote.get('volume')
    if volume is not None:  # could be 0
        # why questrade do you have to be an asshole shipping null values!!!
        computed['$ vol'] = round((vwap or 0) * (volume or 0), 3)

    new = {}
    displayable = {}

    for key, value in chain(quote.items(), computed.items()):
        new_key = keymap.get(key)
        if not new_key:
            continue

        # API servers can return `None` vals when markets are closed (weekend)
        value = 0 if value is None else value

        display_value = value

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
        'IV %',
        lambda v: '{}'.format(round(v, ndigits=2))
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
    # (cause why would questrade keep their quote structure consistent across
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
