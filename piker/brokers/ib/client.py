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

"""
``ib`` core API client machinery; mostly sane wrapping around
``ib_insync``.

"""
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from contextlib import AsyncExitStack
from dataclasses import asdict, astuple
from datetime import datetime
from functools import partial
import itertools
from math import isnan
from typing import (
    Any,
    Optional,
    AsyncIterator,
    Union,
)
import asyncio
from pprint import pformat
import inspect
import time
from types import SimpleNamespace


import trio
from trio_typing import TaskStatus
import tractor
from tractor import to_asyncio
from ib_insync.wrapper import RequestError
from ib_insync.contract import Contract, ContractDetails, Option
from ib_insync.order import Order, Trade, OrderStatus
from ib_insync.objects import Fill, Execution
from ib_insync.ticker import Ticker
from ib_insync.objects import Position
import ib_insync as ibis
from ib_insync.wrapper import Wrapper
from ib_insync.client import Client as ib_Client
import numpy as np


from piker import config
from piker.log import get_logger, get_console_log
from piker.data._source import base_ohlc_dtype
from piker.clearing._messages import (
    BrokerdOrder, BrokerdOrderAck, BrokerdStatus,
    BrokerdPosition, BrokerdCancel,
    BrokerdFill, BrokerdError,
)


log = get_logger(__name__)


_time_units = {
    's': ' sec',
    'm': ' mins',
    'h': ' hours',
}

_time_frames = {
    '1s': '1 Sec',
    '5s': '5 Sec',
    '30s': '30 Sec',
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

_show_wap_in_history: bool = False

# optional search config the backend can register for
# it's symbol search handling (in this case we avoid
# accepting patterns before the kb has settled more then
# a quarter second).
_search_conf = {
    'pause_period': 6 / 16,
}


# overrides to sidestep pretty questionable design decisions in
# ``ib_insync``:
class NonShittyWrapper(Wrapper):
    def tcpDataArrived(self):
        """Override time stamps to be floats for now.
        """
        # use a ns int to store epoch time instead of datetime
        self.lastTime = time.time_ns()

        for ticker in self.pendingTickers:
            ticker.rtTime = None
            ticker.ticks = []
            ticker.tickByTicks = []
            ticker.domTicks = []
        self.pendingTickers = set()

    def execDetails(
        self,
        reqId: int,
        contract: Contract,
        execu,
    ):
        """
        Get rid of datetime on executions.
        """
        # this is the IB server's execution time supposedly
        # https://interactivebrokers.github.io/tws-api/classIBApi_1_1Execution.html#a2e05cace0aa52d809654c7248e052ef2
        execu.time = execu.time.timestamp()
        return super().execDetails(reqId, contract, execu)


class NonShittyIB(ibis.IB):
    """The beginning of overriding quite a few decisions in this lib.

    - Don't use datetimes
    - Don't use named tuples
    """
    def __init__(self):

        # override `ib_insync` internal loggers so we can see wtf
        # it's doing..
        self._logger = get_logger(
            'ib_insync.ib',
        )
        self._createEvents()

        # XXX: just to override this wrapper
        self.wrapper = NonShittyWrapper(self)
        self.client = ib_Client(self.wrapper)
        self.client._logger = get_logger(
            'ib_insync.client',
        )

        # self.errorEvent += self._onError
        self.client.apiEnd += self.disconnectedEvent


# map of symbols to contract ids
_adhoc_cmdty_data_map = {
    # https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Conid%20Info&wlId=IB&conid=69067924

    # NOTE: some cmdtys/metals don't have trade data like gold/usd:
    # https://groups.io/g/twsapi/message/44174
    'XAUUSD': ({'conId': 69067924}, {'whatToShow': 'MIDPOINT'}),
}

_futes_venues = (
    'GLOBEX',
    'NYMEX',
    'CME',
    'CMECRYPTO',
)

_adhoc_futes_set = {

    # equities
    'nq.globex',
    'mnq.globex',

    'es.globex',
    'mes.globex',

    # cypto$
    'brr.cmecrypto',
    'ethusdrr.cmecrypto',

    # agriculture
    'he.globex',  # lean hogs
    'le.globex',  # live cattle (geezers)
    'gf.globex',  # feeder cattle (younguns)

    # raw
    'lb.globex',  # random len lumber

    # metals
    'xauusd.cmdty',  # gold spot
    'gc.nymex',
    'mgc.nymex',

    'xagusd.cmdty',  # silver spot
    'ni.nymex',  # silver futes
    'qi.comex',  # mini-silver futes
}

# exchanges we don't support at the moment due to not knowing
# how to do symbol-contract lookup correctly likely due
# to not having the data feeds subscribed.
_exch_skip_list = {
    'ASX',  # aussie stocks
    'MEXI',  # mexican stocks
    'VALUE',  # no idea
}

# https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Conid%20Info&wlId=IB&conid=69067924

_enters = 0


def bars_to_np(bars: list) -> np.ndarray:
    '''
    Convert a "bars list thing" (``BarsList`` type from ibis)
    into a numpy struct array.

    '''
    # TODO: maybe rewrite this faster with ``numba``
    np_ready = []
    for bardata in bars:
        ts = bardata.date.timestamp()
        t = astuple(bardata)[:7]
        np_ready.append((ts, ) + t[1:7])

    nparr = np.array(
        np_ready,
        dtype=base_ohlc_dtype,
    )
    assert nparr['time'][0] == bars[0].date.timestamp()
    assert nparr['time'][-1] == bars[-1].date.timestamp()
    return nparr


class Client:
    '''
    IB wrapped for our broker backend API.

    Note: this client requires running inside an ``asyncio`` loop.

    '''
    _contracts: dict[str, Contract] = {}

    def __init__(
        self,

        ib: ibis.IB,

    ) -> None:
        self.ib = ib
        self.ib.RaiseRequestErrors = True

        # contract cache
        self._feeds: dict[str, trio.abc.SendChannel] = {}

        # NOTE: the ib.client here is "throttled" to 45 rps by default

    async def trades(
        self,
        # api_only: bool = False,

    ) -> dict[str, Any]:

        # orders = await self.ib.reqCompletedOrdersAsync(
        #     apiOnly=api_only
        # )
        fills = await self.ib.reqExecutionsAsync()
        norm_fills = []
        for fill in fills:
            fill = fill._asdict()  # namedtuple
            for key, val in fill.copy().items():
                if isinstance(val, Contract):
                    fill[key] = asdict(val)

            norm_fills.append(fill)

        return norm_fills

    async def bars(
        self,
        fqsn: str,

        # EST in ISO 8601 format is required... below is EPOCH
        start_dt: Union[datetime, str] = "1970-01-01T00:00:00.000000-05:00",
        end_dt: Union[datetime, str] = "",

        sample_period_s: str = 1,  # ohlc sample period
        period_count: int = int(2e3),  # <- max per 1s sample query

    ) -> list[dict[str, Any]]:
        '''
        Retreive OHLCV bars for a fqsn over a range to the present.

        '''
        bars_kwargs = {'whatToShow': 'TRADES'}

        global _enters
        # log.info(f'REQUESTING BARS {_enters} @ end={end_dt}')
        print(f'REQUESTING BARS {_enters} @ end={end_dt}')

        if not end_dt:
            end_dt = ''

        _enters += 1

        contract = await self.find_contract(fqsn)
        bars_kwargs.update(getattr(contract, 'bars_kwargs', {}))

        # _min = min(2000*100, count)
        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_dt,
            formatDate=2,

            # time history length values format:
            # ``durationStr=integer{SPACE}unit (S|D|W|M|Y)``

            # OHLC sampling values:
            # 1 secs, 5 secs, 10 secs, 15 secs, 30 secs, 1 min, 2 mins,
            # 3 mins, 5 mins, 10 mins, 15 mins, 20 mins, 30 mins,
            # 1 hour, 2 hours, 3 hours, 4 hours, 8 hours, 1 day, 1W, 1M
            # barSizeSetting='1 secs',

            # durationStr='{count} S'.format(count=15000 * 5),
            # durationStr='{count} D'.format(count=1),
            # barSizeSetting='5 secs',

            durationStr='{count} S'.format(count=period_count),
            # barSizeSetting='5 secs',
            barSizeSetting='1 secs',

            # barSizeSetting='1 min',

            # always use extended hours
            useRTH=False,

            # restricted per contract type
            **bars_kwargs,
            # whatToShow='MIDPOINT',
            # whatToShow='TRADES',
        )
        if not bars:
            # TODO: raise underlying error here
            raise ValueError(f"No bars retreived for {fqsn}?")

        nparr = bars_to_np(bars)
        return bars, nparr

    async def con_deats(
        self,
        contracts: list[Contract],

    ) -> dict[str, ContractDetails]:

        futs = []
        for con in contracts:
            if con.primaryExchange not in _exch_skip_list:
                futs.append(self.ib.reqContractDetailsAsync(con))

        # batch request all details
        results = await asyncio.gather(*futs)

        # one set per future result
        details = {}
        for details_set in results:

            # XXX: if there is more then one entry in the details list
            # then the contract is so called "ambiguous".
            for d in details_set:
                con = d.contract

                key = '.'.join([
                    con.symbol,
                    con.primaryExchange or con.exchange,
                ])
                expiry = con.lastTradeDateOrContractMonth
                if expiry:
                    key += f'.{expiry}'

                # nested dataclass we probably don't need and that
                # won't IPC serialize..
                d.secIdList = ''

                details[key] = d

        return details

    async def search_stocks(
        self,
        pattern: str,
        upto: int = 3,  # how many contracts to search "up to"

    ) -> dict[str, ContractDetails]:
        '''
        Search for stocks matching provided ``str`` pattern.

        Return a dictionary of ``upto`` entries worth of contract details.

        '''
        descriptions = await self.ib.reqMatchingSymbolsAsync(pattern)

        if descriptions is None:
            return {}

        # limit
        descrs = descriptions[:upto]
        return await self.con_deats([d.contract for d in descrs])

    async def search_symbols(
        self,
        pattern: str,
        # how many contracts to search "up to"
        upto: int = 3,
        asdicts: bool = True,

    ) -> dict[str, ContractDetails]:

        # TODO add search though our adhoc-locally defined symbol set
        # for futes/cmdtys/
        results = await self.search_stocks(
            pattern,
            upto=upto,
        )

        for key, deats in results.copy().items():

            tract = deats.contract
            sym = tract.symbol
            sectype = tract.secType

            if sectype == 'IND':
                results[f'{sym}.IND'] = tract
                results.pop(key)
                exch = tract.exchange

                if exch in _futes_venues:
                    # try get all possible contracts for symbol as per,
                    # https://interactivebrokers.github.io/tws-api/basic_contracts.html#fut
                    con = ibis.Future(
                        symbol=sym,
                        exchange=exch,
                    )
                    try:
                        all_deats = await self.con_deats([con])
                        results |= all_deats

                    except RequestError as err:
                        log.warning(err.message)

        return results

    async def get_fute(
        self,
        symbol: str,
        exchange: str,
        expiry: str = '',
        front: bool = False,

    ) -> Contract:
        '''
        Get an unqualifed contract for the current "continous" future.

        '''
        # it's the "front" contract returned here
        if front:
            con = (await self.ib.qualifyContractsAsync(
                ibis.ContFuture(symbol, exchange=exchange)
            ))[0]
        else:
            con = (await self.ib.qualifyContractsAsync(
                ibis.Future(
                    symbol,
                    exchange=exchange,
                    lastTradeDateOrContractMonth=expiry,
                )
            ))[0]

        return con

    async def find_contract(
        self,
        pattern: str,
        currency: str = 'USD',
        **kwargs,

    ) -> Contract:

        # TODO: we can't use this currently because
        # ``wrapper.starTicker()`` currently cashes ticker instances
        # which means getting a singel quote will potentially look up
        # a quote for a ticker that it already streaming and thus run
        # into state clobbering (eg. list: Ticker.ticks). It probably
        # makes sense to try this once we get the pub-sub working on
        # individual symbols...

        # XXX UPDATE: we can probably do the tick/trades scraping
        # inside our eventkit handler instead to bypass this entirely?

        if '.ib' in pattern:
            from ..data._source import unpack_fqsn
            broker, symbol, expiry = unpack_fqsn(pattern)
        else:
            symbol = pattern

        # try:
        #     # give the cache a go
        #     return self._contracts[symbol]
        # except KeyError:
        #     log.debug(f'Looking up contract for {symbol}')
        expiry: str = ''
        if symbol.count('.') > 1:
            symbol, _, expiry = symbol.rpartition('.')

        # use heuristics to figure out contract "type"
        sym, exch = symbol.upper().rsplit('.', maxsplit=1)

        qualify: bool = True

        # futes
        if exch in _futes_venues:
            if expiry:
                # get the "front" contract
                contract = await self.get_fute(
                    symbol=sym,
                    exchange=exch,
                    expiry=expiry,
                )

            else:
                # get the "front" contract
                contract = await self.get_fute(
                    symbol=sym,
                    exchange=exch,
                    front=True,
                )

            qualify = False

        elif exch in ('FOREX'):
            currency = ''
            symbol, currency = sym.split('/')
            con = ibis.Forex(
                symbol=symbol,
                currency=currency,
            )
            con.bars_kwargs = {'whatToShow': 'MIDPOINT'}

        # commodities
        elif exch == 'CMDTY':  # eg. XAUUSD.CMDTY
            con_kwargs, bars_kwargs = _adhoc_cmdty_data_map[sym]
            con = ibis.Commodity(**con_kwargs)
            con.bars_kwargs = bars_kwargs

        # stonks
        else:
            # TODO: metadata system for all these exchange rules..
            primaryExchange = ''

            if exch in ('PURE', 'TSE'):  # non-yankee
                currency = 'CAD'
                # stupid ib...
                primaryExchange = exch
                exch = 'SMART'

            else:
                exch = 'SMART'
                primaryExchange = exch

            con = ibis.Stock(
                symbol=sym,
                exchange=exch,
                primaryExchange=primaryExchange,
                currency=currency,
            )
        try:
            exch = 'SMART' if not exch else exch
            if qualify:
                contract = (await self.ib.qualifyContractsAsync(con))[0]
            else:
                assert contract

        except IndexError:
            raise ValueError(f"No contract could be found {con}")

        self._contracts[pattern] = contract

        # add an aditional entry with expiry suffix if available
        conexp = contract.lastTradeDateOrContractMonth
        if conexp:
            self._contracts[pattern + f'.{conexp}'] = contract

        return contract

    async def get_head_time(
        self,
        contract: Contract,
    ) -> datetime:
        """Return the first datetime stamp for ``contract``.

        """
        return await self.ib.reqHeadTimeStampAsync(
            contract,
            whatToShow='TRADES',
            useRTH=False,
            formatDate=2,  # timezone aware UTC datetime
        )

    async def get_sym_details(
        self,
        symbol: str,
    ) -> tuple[Contract, Ticker, ContractDetails]:

        contract = await self.find_contract(symbol)
        ticker: Ticker = self.ib.reqMktData(
            contract,
            snapshot=True,
        )
        details_fute = self.ib.reqContractDetailsAsync(contract)
        details = (await details_fute)[0]

        return contract, ticker, details

    async def get_quote(
        self,
        symbol: str,

    ) -> tuple[Contract, Ticker, ContractDetails]:
        '''
        Return a single quote for symbol.

        '''
        contract, ticker, details = await self.get_sym_details(symbol)

        ready = ticker.updateEvent

        # ensure a last price gets filled in before we deliver quote
        for _ in range(100):
            if isnan(ticker.last):

                done, pending = await asyncio.wait(
                    [ready],
                    timeout=0.1,
                )
                if ready in done:
                    break
                else:
                    log.warning(
                        f'Quote for {symbol} timed out: market is closed?'
                    )

            else:
                log.info(f'Got first quote for {symbol}')
                break
        else:
            log.warning(
                f'Symbol {symbol} is not returning a quote '
                'it may be outside trading hours?')

        return contract, ticker, details

    # async to be consistent for the client proxy, and cuz why not.
    def submit_limit(
        self,
        # ignored since ib doesn't support defining your
        # own order id
        oid: str,
        symbol: str,
        price: float,
        action: str,
        size: int,
        account: str,  # if blank the "default" tws account is used

        # XXX: by default 0 tells ``ib_insync`` methods that there is no
        # existing order so ask the client to create a new one (which it
        # seems to do by allocating an int counter - collision prone..)
        reqid: int = None,

    ) -> int:
        '''
        Place an order and return integer request id provided by client.

        '''
        try:
            contract = self._contracts[symbol]
        except KeyError:
            # require that the symbol has been previously cached by
            # a data feed request - ensure we aren't making orders
            # against non-known prices.
            raise RuntimeError("Can not order {symbol}, no live feed?")

        try:
            trade = self.ib.placeOrder(
                contract,
                Order(
                    orderId=reqid or 0,  # stupid api devs..
                    action=action.upper(),  # BUY/SELL
                    # lookup the literal account number by name here.
                    account=account,
                    orderType='LMT',
                    lmtPrice=price,
                    totalQuantity=size,
                    outsideRth=True,

                    optOutSmartRouting=True,
                    routeMarketableToBbo=True,
                    designatedLocation='SMART',
                ),
            )
        except AssertionError:  # errrg insync..
            log.warning(f'order for {reqid} already complete?')
            # will trigger an error in ems request handler task.
            return None

        # ib doesn't support setting your own id outside
        # their own weird client int counting ids..
        return trade.order.orderId

    def submit_cancel(
        self,
        reqid: str,
    ) -> None:
        """Send cancel request for order id ``oid``.

        """
        self.ib.cancelOrder(
            Order(
                orderId=reqid,
                clientId=self.ib.client.clientId,
            )
        )

    def inline_errors(
        self,
        to_trio: trio.abc.SendChannel,

    ) -> None:
        '''
        Setup error relay to the provided ``trio`` mem chan such that
        trio tasks can retreive and parse ``asyncio``-side API request
        errors.

        '''
        def push_err(
            reqId: int,
            errorCode: int,
            errorString: str,
            contract: Contract,

        ) -> None:

            reason = errorString

            if reqId == -1:
                # it's a general event?
                key = 'event'
                log.info(errorString)

            else:
                key = 'error'
                log.error(errorString)

            try:
                to_trio.send_nowait((
                    key,

                    # error "object"
                    {
                        'type': key,
                        'reqid': reqId,
                        'reason': reason,
                        'error_code': errorCode,
                        'contract': contract,
                    }
                ))
            except trio.BrokenResourceError:
                # XXX: eventkit's ``Event.emit()`` for whatever redic
                # reason will catch and ignore regular exceptions
                # resulting in tracebacks spammed to console..
                # Manually do the dereg ourselves.
                log.exception('Disconnected from errorEvent updates')
                self.ib.errorEvent.disconnect(push_err)

        self.ib.errorEvent.connect(push_err)

    def positions(
        self,
        account: str = '',

    ) -> list[Position]:
        """
        Retrieve position info for ``account``.
        """
        return self.ib.positions(account=account)


async def recv_trade_updates(

    client: Client,
    to_trio: trio.abc.SendChannel,

) -> None:
    """Stream a ticker using the std L1 api.
    """
    client.inline_errors(to_trio)

    # sync with trio task
    to_trio.send_nowait(None)

    def push_tradesies(eventkit_obj, obj, fill=None):
        """Push events to trio task.

        """
        if fill is not None:
            # execution details event
            item = ('fill', (obj, fill))

        elif eventkit_obj.name() == 'positionEvent':
            item = ('position', obj)

        else:
            item = ('status', obj)

        log.info(f'eventkit event ->\n{pformat(item)}')

        try:
            to_trio.send_nowait(item)
        except trio.BrokenResourceError:
            log.exception(f'Disconnected from {eventkit_obj} updates')
            eventkit_obj.disconnect(push_tradesies)

    # hook up to the weird eventkit object - event stream api
    for ev_name in [
        'orderStatusEvent',  # all order updates
        'execDetailsEvent',  # all "fill" updates
        'positionEvent',  # avg price updates per symbol per account

        # 'commissionReportEvent',
        # XXX: ugh, it is a separate event from IB and it's
        # emitted as follows:
        # self.ib.commissionReportEvent.emit(trade, fill, report)

        # XXX: not sure yet if we need these
        # 'updatePortfolioEvent',

        # XXX: these all seem to be weird ib_insync intrernal
        # events that we probably don't care that much about
        # given the internal design is wonky af..
        # 'newOrderEvent',
        # 'orderModifyEvent',
        # 'cancelOrderEvent',
        # 'openOrderEvent',
    ]:
        eventkit_obj = getattr(client.ib, ev_name)
        handler = partial(push_tradesies, eventkit_obj)
        eventkit_obj.connect(handler)

    # let the engine run and stream
    await client.ib.disconnectedEvent

# per-actor API ep caching
_client_cache: dict[tuple[str, int], Client] = {}
_scan_ignore: set[tuple[str, int]] = set()


def get_config() -> dict[str, Any]:

    conf, path = config.load()

    section = conf.get('ib')

    if section is None:
        log.warning(f'No config section found for ib in {path}')
        return {}

    return section


_accounts2clients: dict[str, Client] = {}


@acm
async def load_aio_clients(

    host: str = '127.0.0.1',
    port: int = None,
    client_id: int = 6116,

    # the API TCP in `ib_insync` connection can be flaky af so instead
    # retry a few times to get the client going..
    connect_retries: int = 3,
    connect_timeout: float = 0.5,

) -> dict[str, Client]:
    '''
    Return an ``ib_insync.IB`` instance wrapped in our client API.

    Client instances are cached for later use.

    TODO: consider doing this with a ctx mngr eventually?

    '''
    global _accounts2clients, _client_cache, _scan_ignore

    conf = get_config()
    ib = None
    client = None

    # attempt to get connection info from config; if no .toml entry
    # exists, we try to load from a default localhost connection.
    localhost = '127.0.0.1'
    host, hosts = conf.get('host'), conf.get('hosts')
    if not (hosts or host):
        host = localhost

    if not hosts:
        hosts = [host]
    elif host and hosts:
        raise ValueError(
            'Specify only one of `host` or `hosts` in `brokers.toml` config')

    try_ports = conf.get(
        'ports',

        # default order is to check for gw first
        [4002, 7497]
    )
    if isinstance(try_ports, dict):
        log.warning(
            '`ib.ports` in `brokers.toml` should be a `list` NOT a `dict`'
        )
        try_ports = list(try_ports.values())

    _err = None
    accounts_def = config.load_accounts(['ib'])
    ports = try_ports if port is None else [port]
    combos = list(itertools.product(hosts, ports))
    accounts_found: dict[str, Client] = {}

    # (re)load any and all clients that can be found
    # from connection details in ``brokers.toml``.
    for host, port in combos:

        sockaddr = (host, port)
        if (
            sockaddr in _client_cache
            or sockaddr in _scan_ignore
        ):
            continue

        ib = NonShittyIB()

        for i in range(connect_retries):
            try:
                await ib.connectAsync(
                    host,
                    port,
                    clientId=client_id,

                    # this timeout is sensative on windows and will
                    # fail without a good "timeout error" so be
                    # careful.
                    timeout=connect_timeout,
                )
                break

            except (
                ConnectionRefusedError,

                # TODO: if trying to scan for remote api clients
                # pretty sure we need to catch this, though it
                # definitely needs a shorter timeout since it hangs
                # for like 5s..
                asyncio.exceptions.TimeoutError,
                OSError,
            ) as ce:
                _err = ce

                if i > 8:
                    # cache logic to avoid rescanning if we already have all
                    # clients loaded.
                    _scan_ignore.add(sockaddr)
                    raise

                log.warning(
                    f'Failed to connect on {port} for {i} time, retrying...')

        # create and cache client
        client = Client(ib)

        # Pre-collect all accounts available for this
        # connection and map account names to this client
        # instance.
        pps = ib.positions()
        if pps:
            for pp in pps:
                accounts_found[
                    accounts_def.inverse[pp.account]
                ] = client

        # if there are accounts without positions we should still
        # register them for this client
        for value in ib.accountValues():
            acct_number = value.account

            entry = accounts_def.inverse.get(acct_number)
            if not entry:
                raise ValueError(
                    'No section in brokers.toml for account:'
                    f' {acct_number}\n'
                    f'Please add entry to continue using this API client'
                )

            # surjection of account names to operating clients.
            if acct_number not in accounts_found:
                accounts_found[entry] = client

        log.info(
            f'Loaded accounts for client @ {host}:{port}\n'
            f'{pformat(accounts_found)}'
        )

        # update all actor-global caches
        log.info(f"Caching client for {sockaddr}")
        _client_cache[sockaddr] = client

        # XXX: why aren't we just updating this directy above
        # instead of using the intermediary `accounts_found`?
        _accounts2clients.update(accounts_found)

    # if we have no clients after the scan loop then error out.
    if not _client_cache:
        raise ConnectionError(
            'No ib APIs could be found scanning @:\n'
            f'{pformat(combos)}\n'
            'Check your `brokers.toml` and/or network'
        ) from _err

    try:
        yield _accounts2clients
    finally:
        # TODO: for re-scans we'll want to not teardown clients which
        # are up and stable right?
        for acct, client in _accounts2clients.items():
            log.info(f'Disconnecting {acct}@{client}')
            client.ib.disconnect()
            _client_cache.pop((host, port))


async def load_clients_for_trio(
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,

) -> None:
    '''
    Pure async mngr proxy to ``load_aio_clients()``.

    This is a bootstrap entrypoing to call from
    a ``tractor.to_asyncio.open_channel_from()``.

    '''
    global _accounts2clients

    if _accounts2clients:
        to_trio.send_nowait(_accounts2clients)
        await asyncio.sleep(float('inf'))

    else:
        async with load_aio_clients() as accts2clients:
            to_trio.send_nowait(accts2clients)

            # TODO: maybe a sync event to wait on instead?
            await asyncio.sleep(float('inf'))


_proxies: dict[str, MethodProxy] = {}


@acm
async def open_client_proxies() -> tuple[
    dict[str, MethodProxy],
    dict[str, Client],
]:
    async with (
        tractor.trionics.maybe_open_context(
            # acm_func=open_client_proxies,
            acm_func=tractor.to_asyncio.open_channel_from,
            kwargs={'target': load_clients_for_trio},

            # lock around current actor task access
            # TODO: maybe this should be the default in tractor?
            key=tractor.current_actor().uid,

        ) as (cache_hit, (clients, from_aio)),

        AsyncExitStack() as stack
    ):
        if cache_hit:
            log.info(f'Re-using cached clients: {clients}')

        for acct_name, client in clients.items():
            proxy = await stack.enter_async_context(
                open_client_proxy(client),
            )
            _proxies[acct_name] = proxy

        yield _proxies, clients


def get_preferred_data_client(
    clients: dict[str, Client],

) -> tuple[str, Client]:
    '''
    Load and return the (first found) `Client` instance that is
    preferred and should be used for data by iterating, in priority
    order, the ``ib.prefer_data_account: list[str]`` account names in
    the users ``brokers.toml`` file.

    '''
    conf = get_config()
    data_accounts = conf['prefer_data_account']

    for name in data_accounts:
        client = clients.get(f'ib.{name}')
        if client:
            return name, client
    else:
        raise ValueError(
            'No preferred data client could be found:\n'
            f'{data_accounts}'
        )


class MethodProxy:

    def __init__(
        self,
        chan: to_asyncio.LinkedTaskChannel,
        event_table: dict[str, trio.Event],
        asyncio_ns: SimpleNamespace,

    ) -> None:
        self.chan = chan
        self.event_table = event_table
        self._aio_ns = asyncio_ns

    async def _run_method(
        self,
        *,
        meth: str = None,
        **kwargs

    ) -> Any:
        '''
        Make a ``Client`` method call by requesting through the
        ``tractor.to_asyncio`` layer.

        '''
        chan = self.chan
        await chan.send((meth, kwargs))

        while not chan.closed():
            # send through method + ``kwargs: dict`` as pair
            msg = await chan.receive()
            # print(f'NEXT MSG: {msg}')

            # TODO: py3.10 ``match:`` syntax B)
            if 'result' in msg:
                res = msg.get('result')
                return res

            elif 'exception' in msg:
                err = msg.get('exception')
                raise err

            elif 'error' in msg:
                etype, emsg = msg
                log.warning(f'IB error relay: {emsg}')
                continue

            else:
                log.warning(f'UNKNOWN IB MSG: {msg}')

    def status_event(
        self,
        pattern: str,

    ) -> Union[dict[str, Any], trio.Event]:

        ev = self.event_table.get(pattern)

        if not ev or ev.is_set():
            # print(f'inserting new data reset event item')
            ev = self.event_table[pattern] = trio.Event()

        return ev

    async def wait_for_data_reset(self) -> None:
        '''
        Send hacker hot keys to ib program and wait
        for the event that declares the data feeds to be
        back up before unblocking.

        '''
        ...


async def open_aio_client_method_relay(
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,
    client: Client,
    event_consumers: dict[str, trio.Event],

) -> None:

    to_trio.send_nowait(client)

    # TODO: separate channel for error handling?
    client.inline_errors(to_trio)

    # relay all method requests to ``asyncio``-side client and deliver
    # back results
    while not to_trio._closed:
        msg = await from_trio.get()
        if msg is None:
            print('asyncio PROXY-RELAY SHUTDOWN')
            break

        meth_name, kwargs = msg
        meth = getattr(client, meth_name)

        try:
            resp = await meth(**kwargs)
            # echo the msg back
            to_trio.send_nowait({'result': resp})

        except (
            RequestError,

            # TODO: relay all errors to trio?
            # BaseException,
        ) as err:
            to_trio.send_nowait({'exception': err})


@acm
async def open_client_proxy(
    client: Client,

) -> MethodProxy:

    event_table = {}

    async with (
        to_asyncio.open_channel_from(
            open_aio_client_method_relay,
            client=client,
            event_consumers=event_table,
        ) as (first, chan),
        trio.open_nursery() as relay_n,
    ):

        assert isinstance(first, Client)
        proxy = MethodProxy(
            chan,
            event_table,
            asyncio_ns=first,
        )

        # mock all remote methods on ib ``Client``.
        for name, method in inspect.getmembers(
            Client, predicate=inspect.isfunction
        ):
            if '_' == name[0]:
                continue
            setattr(proxy, name, partial(proxy._run_method, meth=name))

        async def relay_events():

            async with chan.subscribe() as msg_stream:

                async for msg in msg_stream:
                    if 'event' not in msg:
                        continue

                    # if 'event' in msg:
                    # wake up any system event waiters.
                    etype, status_msg = msg
                    reason = status_msg['reason']

                    ev = proxy.event_table.pop(reason, None)

                    if ev and ev.statistics().tasks_waiting:
                        log.info(f'Relaying ib status message: {msg}')
                        ev.set()

                    continue

        relay_n.start_soon(relay_events)

        yield proxy

        # terminate asyncio side task
        await chan.send(None)


@acm
async def get_client(
    **kwargs,

) -> Client:
    '''
    Init the ``ib_insync`` client in another actor and return
    a method proxy to it.

    '''
    from .feed import open_data_client

    # TODO: the IPC via portal relay layer for when this current
    # actor isn't in aio mode.
    async with open_data_client() as proxy:
        yield proxy


def pack_position(
    pos: Position

) -> dict[str, Any]:
    con = pos.contract

    if isinstance(con, Option):
        # TODO: option symbol parsing and sane display:
        symbol = con.localSymbol.replace(' ', '')

    else:
        # TODO: lookup fqsn even for derivs.
        symbol = con.symbol.lower()

    exch = (con.primaryExchange or con.exchange).lower()
    symkey = '.'.join((symbol, exch))
    if not exch:
        # attempt to lookup the symbol from our
        # hacked set..
        for sym in _adhoc_futes_set:
            if symbol in sym:
                symkey = sym
                break

    expiry = con.lastTradeDateOrContractMonth
    if expiry:
        symkey += f'.{expiry}'

    # TODO: options contracts into a sane format..

    return BrokerdPosition(
        broker='ib',
        account=pos.account,
        symbol=symkey,
        currency=con.currency,
        size=float(pos.position),
        avg_price=float(pos.avgCost) / float(con.multiplier or 1.0),
    )


async def handle_order_requests(

    ems_order_stream: tractor.MsgStream,
    accounts_def: dict[str, str],

) -> None:

    global _accounts2clients

    request_msg: dict
    async for request_msg in ems_order_stream:
        log.info(f'Received order request {request_msg}')

        action = request_msg['action']
        account = request_msg['account']

        acct_number = accounts_def.get(account)
        if not acct_number:
            log.error(
                f'An IB account number for name {account} is not found?\n'
                'Make sure you have all TWS and GW instances running.'
            )
            await ems_order_stream.send(BrokerdError(
                oid=request_msg['oid'],
                symbol=request_msg['symbol'],
                reason=f'No account found: `{account}` ?',
            ).dict())
            continue

        client = _accounts2clients.get(account)
        if not client:
            log.error(
                f'An IB client for account name {account} is not found.\n'
                'Make sure you have all TWS and GW instances running.'
            )
            await ems_order_stream.send(BrokerdError(
                oid=request_msg['oid'],
                symbol=request_msg['symbol'],
                reason=f'No api client loaded for account: `{account}` ?',
            ).dict())
            continue

        if action in {'buy', 'sell'}:
            # validate
            order = BrokerdOrder(**request_msg)

            # call our client api to submit the order
            reqid = client.submit_limit(
                oid=order.oid,
                symbol=order.symbol,
                price=order.price,
                action=order.action,
                size=order.size,
                account=acct_number,

                # XXX: by default 0 tells ``ib_insync`` methods that
                # there is no existing order so ask the client to create
                # a new one (which it seems to do by allocating an int
                # counter - collision prone..)
                reqid=order.reqid,
            )
            if reqid is None:
                await ems_order_stream.send(BrokerdError(
                    oid=request_msg['oid'],
                    symbol=request_msg['symbol'],
                    reason='Order already active?',
                ).dict())

            # deliver ack that order has been submitted to broker routing
            await ems_order_stream.send(
                BrokerdOrderAck(
                    # ems order request id
                    oid=order.oid,
                    # broker specific request id
                    reqid=reqid,
                    time_ns=time.time_ns(),
                    account=account,
                ).dict()
            )

        elif action == 'cancel':
            msg = BrokerdCancel(**request_msg)
            client.submit_cancel(reqid=msg.reqid)

        else:
            log.error(f'Unknown order command: {request_msg}')


@tractor.context
async def trades_dialogue(

    ctx: tractor.Context,
    loglevel: str = None,

) -> AsyncIterator[dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    accounts_def = config.load_accounts(['ib'])

    global _accounts2clients
    global _client_cache

    # deliver positions to subscriber before anything else
    all_positions = []
    accounts = set()
    clients: list[tuple[Client, trio.MemoryReceiveChannel]] = []

    async with (
        trio.open_nursery() as nurse,
        open_client_proxies() as (proxies, aioclients),
    ):
        # for account, client in _accounts2clients.items():
        for account, proxy in proxies.items():

            client = aioclients[account]

            async def open_stream(
                task_status: TaskStatus[
                    trio.abc.ReceiveChannel
                ] = trio.TASK_STATUS_IGNORED,
            ):
                # each api client has a unique event stream
                async with tractor.to_asyncio.open_channel_from(
                    recv_trade_updates,
                    client=client,
                ) as (first, trade_event_stream):

                    task_status.started(trade_event_stream)
                    await trio.sleep_forever()

            trade_event_stream = await nurse.start(open_stream)

            clients.append((client, trade_event_stream))

            assert account in accounts_def
            accounts.add(account)

        for client in aioclients.values():
            for pos in client.positions():

                msg = pack_position(pos)
                msg.account = accounts_def.inverse[msg.account]

                assert msg.account in accounts, (
                    f'Position for unknown account: {msg.account}')

                all_positions.append(msg.dict())

        trades: list[dict] = []
        for proxy in proxies.values():
            trades.append(await proxy.trades())

        log.info(f'Loaded {len(trades)} from this session')
        # TODO: write trades to local ``trades.toml``
        # - use above per-session trades data and write to local file
        # - get the "flex reports" working and pull historical data and
        # also save locally.

        await ctx.started((
            all_positions,
            tuple(name for name in accounts_def if name in accounts),
        ))

        async with (
            ctx.open_stream() as ems_stream,
            trio.open_nursery() as n,
        ):
            # start order request handler **before** local trades event loop
            n.start_soon(handle_order_requests, ems_stream, accounts_def)

            # allocate event relay tasks for each client connection
            for client, stream in clients:
                n.start_soon(
                    deliver_trade_events,
                    stream,
                    ems_stream,
                    accounts_def
                )

            # block until cancelled
            await trio.sleep_forever()


async def deliver_trade_events(

    trade_event_stream: trio.MemoryReceiveChannel,
    ems_stream: tractor.MsgStream,
    accounts_def: dict[str, str],

) -> None:
    '''Format and relay all trade events for a given client to the EMS.

    '''
    action_map = {'BOT': 'buy', 'SLD': 'sell'}

    # TODO: for some reason we can receive a ``None`` here when the
    # ib-gw goes down? Not sure exactly how that's happening looking
    # at the eventkit code above but we should probably handle it...
    async for event_name, item in trade_event_stream:

        log.info(f'ib sending {event_name}:\n{pformat(item)}')

        # TODO: templating the ib statuses in comparison with other
        # brokers is likely the way to go:
        # https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#a17f2a02d6449710b6394d0266a353313
        # short list:
        # - PendingSubmit
        # - PendingCancel
        # - PreSubmitted (simulated orders)
        # - ApiCancelled (cancelled by client before submission
        #                 to routing)
        # - Cancelled
        # - Filled
        # - Inactive (reject or cancelled but not by trader)

        # XXX: here's some other sucky cases from the api
        # - short-sale but securities haven't been located, in this
        #   case we should probably keep the order in some kind of
        #   weird state or cancel it outright?

        # status='PendingSubmit', message=''),
        # status='Cancelled', message='Error 404,
        #   reqId 1550: Order held while securities are located.'),
        # status='PreSubmitted', message='')],

        if event_name == 'status':

            # XXX: begin normalization of nonsense ib_insync internal
            # object-state tracking representations...

            # unwrap needed data from ib_insync internal types
            trade: Trade = item
            status: OrderStatus = trade.orderStatus

            # skip duplicate filled updates - we get the deats
            # from the execution details event
            msg = BrokerdStatus(

                reqid=trade.order.orderId,
                time_ns=time.time_ns(),  # cuz why not
                account=accounts_def.inverse[trade.order.account],

                # everyone doin camel case..
                status=status.status.lower(),  # force lower case

                filled=status.filled,
                reason=status.whyHeld,

                # this seems to not be necessarily up to date in the
                # execDetails event.. so we have to send it here I guess?
                remaining=status.remaining,

                broker_details={'name': 'ib'},
            )

        elif event_name == 'fill':

            # for wtv reason this is a separate event type
            # from IB, not sure why it's needed other then for extra
            # complexity and over-engineering :eyeroll:.
            # we may just end up dropping these events (or
            # translating them to ``Status`` msgs) if we can
            # show the equivalent status events are no more latent.

            # unpack ib_insync types
            # pep-0526 style:
            # https://www.python.org/dev/peps/pep-0526/#global-and-local-variable-annotations
            trade: Trade
            fill: Fill
            trade, fill = item
            execu: Execution = fill.execution

            # TODO: normalize out commissions details?
            details = {
                'contract': asdict(fill.contract),
                'execution': asdict(fill.execution),
                'commissions': asdict(fill.commissionReport),
                'broker_time': execu.time,   # supposedly server fill time
                'name': 'ib',
            }

            msg = BrokerdFill(
                # should match the value returned from `.submit_limit()`
                reqid=execu.orderId,
                time_ns=time.time_ns(),  # cuz why not

                action=action_map[execu.side],
                size=execu.shares,
                price=execu.price,

                broker_details=details,
                # XXX: required by order mode currently
                broker_time=details['broker_time'],

            )

        elif event_name == 'error':

            err: dict = item

            # f$#$% gawd dammit insync..
            con = err['contract']
            if isinstance(con, Contract):
                err['contract'] = asdict(con)

            if err['reqid'] == -1:
                log.error(f'TWS external order error:\n{pformat(err)}')

            # TODO: what schema for this msg if we're going to make it
            # portable across all backends?
            # msg = BrokerdError(**err)
            continue

        elif event_name == 'position':
            msg = pack_position(item)
            msg.account = accounts_def.inverse[msg.account]

        elif event_name == 'event':

            # it's either a general system status event or an external
            # trade event?
            log.info(f"TWS system status: \n{pformat(item)}")

            # TODO: support this again but needs parsing at the callback
            # level...
            # reqid = item.get('reqid', 0)
            # if getattr(msg, 'reqid', 0) < -1:
            # log.info(f"TWS triggered trade\n{pformat(msg.dict())}")

            continue

            # msg.reqid = 'tws-' + str(-1 * reqid)

            # mark msg as from "external system"
            # TODO: probably something better then this.. and start
            # considering multiplayer/group trades tracking
            # msg.broker_details['external_src'] = 'tws'

        # XXX: we always serialize to a dict for msgpack
        # translations, ideally we can move to an msgspec (or other)
        # encoder # that can be enabled in ``tractor`` ahead of
        # time so we can pass through the message types directly.
        await ems_stream.send(msg.dict())


def load_flex_trades(
    path: Optional[str] = None,

) -> dict[str, str]:

    from pprint import pprint
    from ib_insync import flexreport, util

    conf = get_config()

    if not path:
        # load ``brokers.toml`` and try to get the flex
        # token and query id that must be previously defined
        # by the user.
        token = conf.get('flex_token')
        if not token:
            raise ValueError(
               'You must specify a ``flex_token`` field in your'
               '`brokers.toml` in order load your trade log, see our'
               'intructions for how to set this up here:\n'
               'PUT LINK HERE!'
            )

        qid = conf['flex_trades_query_id']

        # TODO: hack this into our logging
        # system like we do with the API client..
        util.logToConsole()

        # TODO: rewrite the query part of this with async..httpx?
        report = flexreport.FlexReport(
            token=token,
            queryId=qid,
        )

    else:
        # XXX: another project we could potentially look at,
        # https://pypi.org/project/ibflex/
        report = flexreport.FlexReport(path=path)

    trade_entries = report.extract('Trade')
    trades = {
        # XXX: LOL apparently ``toml`` has a bug
        # where a section key error will show up in the write
        # if you leave this as an ``int``?
        str(t.__dict__['tradeID']): t.__dict__
        for t in trade_entries
    }

    ln = len(trades)
    log.info(f'Loaded {ln} trades from flex query')

    trades_by_account = {}
    for tid, trade in trades.items():
        trades_by_account.setdefault(
            # oddly for some so-called "BookTrade" entries
            # this field seems to be blank, no cuckin clue.
            # trade['ibExecID']
            str(trade['accountId']), {}
        )[tid] = trade

    section = {'ib': trades_by_account}
    pprint(section)

    # TODO: load the config first and append in
    # the new trades loaded here..
    try:
        config.write(section, 'trades')
    except KeyError:
        import pdbpp; pdbpp.set_trace()  # noqa


if __name__ == '__main__':
    load_flex_trades()
