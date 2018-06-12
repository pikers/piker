"""
Questrade API backend.
"""
import time
import datetime
from functools import partial

import trio
from async_generator import asynccontextmanager

from ..calc import humanize, percent_change
from . import config
from ._util import resproc, BrokerError
from ..log import get_logger, colorize_json

# TODO: move to urllib3/requests once supported
import asks
asks.init('trio')

log = get_logger(__name__)

_refresh_token_ep = 'https://login.questrade.com/oauth2/'
_version = 'v1'
_rate_limit = 3  # queries/sec


class QuestradeError(Exception):
    "Non-200 OK response code"


class Client:
    """API client suitable for use as a long running broker daemon or
    single api requests.

    Provides a high-level api which wraps the underlying endpoint calls.
    """
    def __init__(self, config: 'configparser.ConfigParser'):
        self._sess = asks.Session()
        self.api = _API(self._sess)
        self._conf = config
        self.access_data = {}
        self.user_data = {}
        self._reload_config(config)

    def _reload_config(self, config=None, **kwargs):
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
        expires_stamp = datetime.datetime.fromtimestamp(
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
        their corresponding QT symbol ids.
        """
        data = await self.api.symbols(names=','.join(tickers))
        symbols2ids = {}
        for ticker, symbol in zip(tickers, data['symbols']):
            symbols2ids[symbol['symbol']] = symbol['symbolId']

        return symbols2ids

    async def quote(self, tickers: [str]):
        """Return quotes for each ticker in ``tickers``.
        """
        t2ids = await self.tickers2ids(tickers)
        ids = ','.join(map(str, t2ids.values()))
        results = (await self.api.quotes(ids=ids))['quotes']
        quotes = {quote['symbol']: quote for quote in results}

        # set None for all symbols not found
        if len(t2ids) < len(tickers):
            for ticker in tickers:
                if ticker not in quotes:
                    quotes[ticker] = None

        return quotes

    async def symbol_data(self, tickers: [str]):
        """Return symbol data for ``tickers``.
        """
        t2ids = await self.tickers2ids(tickers)
        ids = ','.join(map(str, t2ids.values()))
        symbols = {}
        for pkt in (await self.api.symbols(ids=ids))['symbols']:
            symbols[pkt['symbol']] = pkt

        return symbols


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
        log.debug(f"Symbol lookup for {ids}")
        return await self._request(
            'symbols', params={'ids': ids, 'names': names})

    async def quotes(self, ids: str) -> dict:
        return await self._request('markets/quotes', params={'ids': ids})

    async def candles(self, id: str, start: str, end, interval) -> dict:
        return await self._request(f'markets/candles/{id}', params={})

    async def balances(self, id: str) -> dict:
        return await self._request(f'accounts/{id}/balances')

    async def postions(self, id: str) -> dict:
        return await self._request(f'accounts/{id}/positions')


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
            await client.api.time()
        except Exception as err:
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


async def quoter(client: Client, tickers: [str]):
    """Quoter context.
    """
    t2ids = {}
    ids = ''

    def filter_symbols(quotes_dict):
        nonlocal t2ids
        for symbol, quote in quotes_dict.items():
            if quote['low52w'] is None:
                log.warn(
                    f"{symbol} seems to be defunct discarding from tickers")
                t2ids.pop(symbol)

    async def get_quote(tickers):
        """Query for quotes using cached symbol ids.
        """
        if not tickers:
            return {}
        nonlocal ids, t2ids
        new, current = set(tickers), set(t2ids.keys())
        if new != current:
            # update ticker ids cache
            log.debug(f"Tickers set changed {new - current}")
            t2ids = await client.tickers2ids(tickers)
            ids = ','.join(map(str, t2ids.values()))

        try:
            quotes_resp = await client.api.quotes(ids=ids)
        except QuestradeError as qterr:
            if "Access token is invalid" in str(qterr.args[0]):
                # out-of-process piker may have renewed already
                client._reload_config()
                quotes_resp = await client.api.quotes(ids=ids)
            else:
                raise

        # dict packing and post-processing
        quotes = {}
        for quote in quotes_resp['quotes']:
            quotes[quote['symbol']] = quote

            if quote.get('delay', 0) > 0:
                log.warn(f"Delayed quote:\n{quote}")

        return quotes

    first_quotes_dict = await get_quote(tickers)
    filter_symbols(first_quotes_dict)

    # re-save symbol ids cache
    ids = ','.join(map(str, t2ids.values()))

    return get_quote


# Questrade key conversion / column order
_qt_keys = {
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
    # "lastTradeTick": "Equal",
    # "lastTradeTime": "2018-01-30T18:28:23.434000-05:00",
    # "symbolId": 3575753,
    # "tier": "",
    # 'isHalted': 'halted',  # as subscript 'h'
    # 'delay': 'delay',  # as subscript 'p'
}

_bidasks = {
    'last': ['bid', 'ask'],
    'size': ['bsize', 'asize'],
    'VWAP': ['low', 'high'],
}


def format_quote(
    quote: dict,
    symbol_data: dict,
    keymap: dict = _qt_keys,
) -> (dict, dict):
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
    mktcap = share_count * last if (last and share_count) else 'NA'
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
