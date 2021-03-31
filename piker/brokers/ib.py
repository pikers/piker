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
Interactive Brokers API backend.

Note the client runs under an ``asyncio`` loop (since ``ib_insync`` is
built on it) and thus actor aware API calls must be spawned with
``infected_aio==True``.
"""
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from functools import partial
from typing import List, Dict, Any, Tuple, Optional, AsyncIterator, Callable
import asyncio
from pprint import pformat
import inspect
import itertools
import logging
import time

import trio
from trio_typing import TaskStatus
import tractor
from async_generator import aclosing
from ib_insync.wrapper import RequestError
from ib_insync.contract import Contract, ContractDetails, Option
from ib_insync.order import Order
from ib_insync.ticker import Ticker
from ib_insync.objects import Position
import ib_insync as ibis
from ib_insync.wrapper import Wrapper
from ib_insync.client import Client as ib_Client

from ..log import get_logger, get_console_log
from ..data import maybe_spawn_brokerd
from ..data._source import from_df
from ..data._sharedmem import ShmArray
from ._util import SymbolNotFound


log = get_logger(__name__)

# passed to ``tractor.ActorNursery.start_actor()``
_spawn_kwargs = {
    'infect_asyncio': True,
}
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

_show_wap_in_history = False


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
        self._createEvents()
        # XXX: just to override this wrapper
        self.wrapper = NonShittyWrapper(self)
        self.client = ib_Client(self.wrapper)
        # self.errorEvent += self._onError
        self.client.apiEnd += self.disconnectedEvent
        self._logger = logging.getLogger('ib_insync.ib')


# map of symbols to contract ids
_adhoc_cmdty_data_map = {
    # https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Conid%20Info&wlId=IB&conid=69067924
    # NOTE: cmdtys don't have trade data:
    # https://groups.io/g/twsapi/message/44174
    'XAUUSD': ({'conId': 69067924}, {'whatToShow': 'MIDPOINT'}),
}

_enters = 0


class Client:
    """IB wrapped for our broker backend API.

    Note: this client requires running inside an ``asyncio`` loop.

    """
    def __init__(
        self,
        ib: ibis.IB,
    ) -> None:
        self.ib = ib
        self.ib.RaiseRequestErrors = True

        # contract cache
        self._contracts: Dict[str, Contract] = {}
        self._feeds: Dict[str, trio.abc.SendChannel] = {}

        # NOTE: the ib.client here is "throttled" to 45 rps by default

    async def bars(
        self,
        symbol: str,
        # EST in ISO 8601 format is required... below is EPOCH
        start_dt: str = "1970-01-01T00:00:00.000000-05:00",
        end_dt: str = "",

        sample_period_s: str = 1,  # ohlc sample period
        period_count: int = int(2e3),  # <- max per 1s sample query

        is_paid_feed: bool = False,  # placeholder
    ) -> List[Dict[str, Any]]:
        """Retreive OHLCV bars for a symbol over a range to the present.
        """
        bars_kwargs = {'whatToShow': 'TRADES'}

        global _enters
        print(f'ENTER BARS {_enters}')
        _enters += 1

        contract = await self.find_contract(symbol)
        bars_kwargs.update(getattr(contract, 'bars_kwargs', {}))

        # _min = min(2000*100, count)
        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_dt,

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
            raise ValueError(f"No bars retreived for {symbol}?")

        # TODO: rewrite this faster with ``numba``
        # convert to pandas dataframe:
        df = ibis.util.df(bars)
        return bars, from_df(df)

    async def search_stocks(
        self,
        pattern: str,
        # how many contracts to search "up to"
        upto: int = 3,
        asdicts: bool = True,
    ) -> Dict[str, ContractDetails]:
        """Search for stocks matching provided ``str`` pattern.

        Return a dictionary of ``upto`` entries worth of contract details.
        """
        descriptions = await self.ib.reqMatchingSymbolsAsync(pattern)

        futs = []
        for d in descriptions:
            con = d.contract
            futs.append(self.ib.reqContractDetailsAsync(con))

        # batch request all details
        results = await asyncio.gather(*futs)

        # XXX: if there is more then one entry in the details list
        details = {}
        for details_set in results:
            # then the contract is so called "ambiguous".
            for d in details_set:
                con = d.contract
                unique_sym = f'{con.symbol}.{con.primaryExchange}'
                details[unique_sym] = asdict(d) if asdicts else d

                if len(details) == upto:
                    return details

        return details

    async def search_futes(
        self,
        pattern: str,
        # how many contracts to search "up to"
        upto: int = 3,
        asdicts: bool = True,
    ) -> Dict[str, ContractDetails]:
        raise NotImplementedError

    async def get_cont_fute(
        self,
        symbol: str,
        exchange: str,
    ) -> Contract:
        """Get an unqualifed contract for the current "continous" future.
        """
        contcon = ibis.ContFuture(symbol, exchange=exchange)

        # it's the "front" contract returned here
        frontcon = (await self.ib.qualifyContractsAsync(contcon))[0]
        return ibis.Future(conId=frontcon.conId)

    async def find_contract(
        self,
        symbol,
        currency: str = 'USD',
        **kwargs,
    ) -> Contract:

        # TODO: we can't use this currently because
        # ``wrapper.starTicker()`` currently cashes ticker instances
        # which means getting a singel quote will potentially look up
        # a quote for a ticker that it already streaming and thus run
        # into state clobbering (eg. List: Ticker.ticks). It probably
        # makes sense to try this once we get the pub-sub working on
        # individual symbols...

        # XXX UPDATE: we can probably do the tick/trades scraping
        # inside our eventkit handler instead to bypass this entirely?

        # try:
        #     # give the cache a go
        #     return self._contracts[symbol]
        # except KeyError:
        #     log.debug(f'Looking up contract for {symbol}')

        # use heuristics to figure out contract "type"
        try:
            sym, exch = symbol.upper().rsplit('.', maxsplit=1)
        except ValueError:
            # likely there's an embedded `.` for a forex pair
            await tractor.breakpoint()

        # futes
        if exch in ('GLOBEX', 'NYMEX', 'CME', 'CMECRYPTO'):
            con = await self.get_cont_fute(symbol=sym, exchange=exch)

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
                if exch in ('PURE', 'TSE'):
                    # stupid ib...
                    primaryExchange = exch
                    exch = 'SMART'

            con = ibis.Stock(
                symbol=sym,
                exchange=exch,
                primaryExchange=primaryExchange,
                currency=currency,
            )
        try:
            exch = 'SMART' if not exch else exch
            contract = (await self.ib.qualifyContractsAsync(con))[0]

        except IndexError:
            raise ValueError(f"No contract could be found {con}")

        self._contracts[symbol] = contract
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

    async def get_quote(
        self,
        symbol: str,
    ) -> Ticker:
        """Return a single quote for symbol.

        """
        contract = await self.find_contract(symbol)

        details_fute = self.ib.reqContractDetailsAsync(contract)
        ticker: Ticker = self.ib.reqMktData(
            contract,
            snapshot=True,
        )
        ticker = await ticker.updateEvent
        details = (await details_fute)[0]
        return contract, ticker, details

    # async to be consistent for the client proxy, and cuz why not.
    async def submit_limit(
        self,
        # ignored since ib doesn't support defining your
        # own order id
        oid: str,
        symbol: str,
        price: float,
        action: str,
        size: int,

        # XXX: by default 0 tells ``ib_insync`` methods that there is no
        # existing order so ask the client to create a new one (which it
        # seems to do by allocating an int counter - collision prone..)
        brid: int = None,
    ) -> int:
        """Place an order and return integer request id provided by client.

        """
        try:
            contract = self._contracts[symbol]
        except KeyError:
            # require that the symbol has been previously cached by
            # a data feed request - ensure we aren't making orders
            # against non-known prices.
            raise RuntimeError("Can not order {symbol}, no live feed?")

        trade = self.ib.placeOrder(
            contract,
            Order(
                orderId=brid or 0,  # stupid api devs..
                action=action.upper(),  # BUY/SELL
                orderType='LMT',
                lmtPrice=price,
                totalQuantity=size,
                outsideRth=True,

                optOutSmartRouting=True,
                routeMarketableToBbo=True,
                designatedLocation='SMART',
            ),
        )

        # ib doesn't support setting your own id outside
        # their own weird client int counting ids..
        return trade.order.orderId

    async def submit_cancel(
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

    async def recv_trade_updates(
        self,
        to_trio: trio.abc.SendChannel,
    ) -> None:
        """Stream a ticker using the std L1 api.
        """
        self.inline_errors(to_trio)

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

            log.info(f'eventkit event -> {eventkit_obj}: {item}')

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
            eventkit_obj = getattr(self.ib, ev_name)
            handler = partial(push_tradesies, eventkit_obj)
            eventkit_obj.connect(handler)

        # let the engine run and stream
        await self.ib.disconnectedEvent

    def inline_errors(
        self,
        to_trio: trio.abc.SendChannel,
    ) -> None:
        # connect error msgs
        def push_err(
            reqId: int,
            errorCode: int,
            errorString: str,
            contract: Contract,
        ) -> None:
            log.error(errorString)
            try:
                to_trio.send_nowait((
                    'error',
                    # error "object"
                    {'reqid': reqId,
                     'message': errorString,
                     'contract': contract}
                ))
            except trio.BrokenResourceError:
                # XXX: eventkit's ``Event.emit()`` for whatever redic
                # reason will catch and ignore regular exceptions
                # resulting in tracebacks spammed to console..
                # Manually do the dereg ourselves.
                log.exception('Disconnected from errorEvent updates')
                self.ib.errorEvent.disconnect(push_err)

        self.ib.errorEvent.connect(push_err)

    async def positions(
        self,
        account: str = '',
    ) -> List[Position]:
        """
        Retrieve position info for ``account``.
        """
        return self.ib.positions(account=account)


# default config ports
_tws_port: int = 7497
_gw_port: int = 4002
_try_ports = [_tws_port, _gw_port]
_client_ids = itertools.count()
_client_cache = {}


@asynccontextmanager
async def _aio_get_client(
    host: str = '127.0.0.1',
    port: int = None,
    client_id: Optional[int] = None,
) -> Client:
    """Return an ``ib_insync.IB`` instance wrapped in our client API.

    Client instances are cached for later use.
    """
    # first check cache for existing client

    try:
        if port:
            client = _client_cache[(host, port)]
        else:
            # grab first cached client
            client = list(_client_cache.values())[0]

        yield client

    except (KeyError, IndexError):
        # TODO: in case the arbiter has no record
        # of existing brokerd we need to broadcast for one.

        if client_id is None:
            # if this is a persistent brokerd, try to allocate a new id for
            # each client
            client_id = next(_client_ids)

        ib = NonShittyIB()
        ports = _try_ports if port is None else [port]

        _err = None
        for port in ports:
            try:
                log.info(f"Connecting to the EYEBEE on port {port}!")
                await ib.connectAsync(host, port, clientId=client_id)
                break
            except ConnectionRefusedError as ce:
                _err = ce
                log.warning(f'Failed to connect on {port}')
        else:
            raise ConnectionRefusedError(_err)

        # create and cache
        try:
            client = Client(ib)

            _client_cache[(host, port)] = client
            log.debug(f"Caching client for {(host, port)}")

            yield client

        except BaseException:
            ib.disconnect()
            raise


async def _aio_run_client_method(
    meth: str,
    to_trio=None,
    from_trio=None,
    **kwargs,
) -> None:
    async with _aio_get_client() as client:

        async_meth = getattr(client, meth)

        # handle streaming methods
        args = tuple(inspect.getfullargspec(async_meth).args)
        if to_trio and 'to_trio' in args:
            kwargs['to_trio'] = to_trio

        return await async_meth(**kwargs)


async def _trio_run_client_method(
    method: str,
    **kwargs,
) -> None:
    """Asyncio entry point to run tasks against the ``ib_insync`` api.

    """
    ca = tractor.current_actor()
    assert ca.is_infected_aio()

    # if the method is an *async gen* stream for it
    meth = getattr(Client, method)

    args = tuple(inspect.getfullargspec(meth).args)

    if inspect.isasyncgenfunction(meth) or (
        # if the method is an *async func* but manually
        # streams back results, make sure to also stream it
        'to_trio' in args
    ):
        kwargs['_treat_as_stream'] = True

    result = await tractor.to_asyncio.run_task(
        _aio_run_client_method,
        meth=method,
        **kwargs
    )
    return result


class _MethodProxy:
    def __init__(
        self,
        portal: tractor._portal.Portal
    ) -> None:
        self._portal = portal

    async def _run_method(
        self,
        *,
        meth: str = None,
        **kwargs
    ) -> Any:
        return await self._portal.run(
            _trio_run_client_method,
            method=meth,
            **kwargs
        )


def get_client_proxy(portal, target=Client) -> _MethodProxy:

    proxy = _MethodProxy(portal)

    # mock all remote methods
    for name, method in inspect.getmembers(
        target, predicate=inspect.isfunction
    ):
        if '_' == name[0]:
            continue
        setattr(proxy, name, partial(proxy._run_method, meth=name))

    return proxy


@asynccontextmanager
async def get_client(
    **kwargs,
) -> Client:
    """Init the ``ib_insync`` client in another actor and return
    a method proxy to it.
    """
    async with maybe_spawn_brokerd(
        brokername='ib',
        infect_asyncio=True,
        **kwargs
    ) as portal:
        proxy_client = get_client_proxy(portal)
        yield proxy_client


# https://interactivebrokers.github.io/tws-api/tick_types.html
tick_types = {
    77: 'trade',
    48: 'utrade',
    0: 'bsize',
    1: 'bid',
    2: 'ask',
    3: 'asize',
    4: 'last',
    5: 'size',
    8: 'volume',
}


def normalize(
    ticker: Ticker,
    calc_price: bool = False
) -> dict:
    # convert named tuples to dicts so we send usable keys
    new_ticks = []
    for tick in ticker.ticks:
        if tick and not isinstance(tick, dict):
            td = tick._asdict()
            td['type'] = tick_types.get(td['tickType'], 'n/a')

            new_ticks.append(td)

    ticker.ticks = new_ticks

    # some contracts don't have volume so we may want to calculate
    # a midpoint price based on data we can acquire (such as bid / ask)
    if calc_price:
        ticker.ticks.append(
            {'type': 'trade', 'price': ticker.marketPrice()}
        )

    # serialize for transport
    data = asdict(ticker)

    # add time stamps for downstream latency measurements
    data['brokerd_ts'] = time.time()

    # stupid stupid shit...don't even care any more..
    # leave it until we do a proper latency study
    # if ticker.rtTime is not None:
    #     data['broker_ts'] = data['rtTime_s'] = float(
    #         ticker.rtTime.timestamp) / 1000.
    data.pop('rtTime')

    return data


async def backfill_bars(
    sym: str,
    shm: ShmArray,  # type: ignore # noqa
    # count: int = 20,  # NOTE: any more and we'll overrun underlying buffer
    count: int = 10,  # NOTE: any more and we'll overrun the underlying buffer

    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Fill historical bars into shared mem / storage afap.

    TODO: avoid pacing constraints:
    https://github.com/pikers/piker/issues/128

    """
    first_bars, bars_array = await _trio_run_client_method(
        method='bars',
        symbol=sym,
    )

    # write historical data to buffer
    shm.push(bars_array)

    with trio.CancelScope() as cs:

        task_status.started(cs)

        next_dt = first_bars[0].date

        i = 0
        while i < count:

            try:
                bars, bars_array = await _trio_run_client_method(
                    method='bars',
                    symbol=sym,
                    end_dt=next_dt,
                )

                if bars_array is None:
                    raise SymbolNotFound(sym)

                shm.push(bars_array, prepend=True)
                i += 1
                next_dt = bars[0].date

            except RequestError as err:
                # TODO: retreive underlying ``ib_insync`` error?

                if err.code == 162:

                    if 'HMDS query returned no data' in err.message:
                        # means we hit some kind of historical "dead zone"
                        # and further requests seem to always cause
                        # throttling despite the rps being low
                        break

                    else:
                        log.exception(
                            "Data query rate reached: Press `ctrl-alt-f`"
                            "in TWS"
                        )

                        # TODO: should probably create some alert on screen
                        # and then somehow get that to trigger an event here
                        # that restarts/resumes this task?
                        await tractor.breakpoint()


asset_type_map = {
    'STK': 'stock',
    'OPT': 'option',
    'FUT': 'future',
    'CONTFUT': 'continuous_future',
    'CASH': 'forex',
    'IND': 'index',
    'CFD': 'cfd',
    'BOND': 'bond',
    'CMDTY': 'commodity',
    'FOP': 'futures_option',
    'FUND': 'mutual_fund',
    'WAR': 'warrant',
    'IOPT': 'warran',
    'BAG': 'bag',
    # 'NEWS': 'news',
}


_quote_streams: Dict[str, trio.abc.ReceiveStream] = {}


async def _setup_quote_stream(
    symbol: str,
    opts: Tuple[int] = ('375', '233', '236'),
    contract: Optional[Contract] = None,
) -> None:
    """Stream a ticker using the std L1 api.
    """
    global _quote_streams

    async with _aio_get_client() as client:

        contract = contract or (await client.find_contract(symbol))
        ticker: Ticker = client.ib.reqMktData(contract, ','.join(opts))

        # define a simple queue push routine that streams quote packets
        # to trio over the ``to_trio`` memory channel.
        to_trio, from_aio = trio.open_memory_channel(2**8)  # type: ignore

        def push(t):
            """Push quotes to trio task.

            """
            # log.debug(t)
            try:
                to_trio.send_nowait(t)

            except trio.BrokenResourceError:
                # XXX: eventkit's ``Event.emit()`` for whatever redic
                # reason will catch and ignore regular exceptions
                # resulting in tracebacks spammed to console..
                # Manually do the dereg ourselves.
                ticker.updateEvent.disconnect(push)
                log.error(f"Disconnected stream for `{symbol}`")
                client.ib.cancelMktData(contract)

                # decouple broadcast mem chan
                _quote_streams.pop(symbol, None)

        ticker.updateEvent.connect(push)

        return from_aio


async def start_aio_quote_stream(
    symbol: str,
    contract: Optional[Contract] = None,
) -> trio.abc.ReceiveStream:

    global _quote_streams

    from_aio = _quote_streams.get(symbol)
    if from_aio:

        # if we already have a cached feed deliver a rx side clone to consumer
        return from_aio.clone()

    else:

        from_aio = await tractor.to_asyncio.run_task(
            _setup_quote_stream,
            symbol=symbol,
            contract=contract,
        )

        # cache feed for later consumers
        _quote_streams[symbol] = from_aio

        return from_aio


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: List[str],
    shm: ShmArray,
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[Tuple[Dict, Dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    """Stream symbol quotes.

    This is a ``trio`` callable routine meant to be invoked
    once the brokerd is up.
    """

    # TODO: support multiple subscriptions
    sym = symbols[0]

    contract, first_ticker, details = await _trio_run_client_method(
        method='get_quote',
        symbol=sym,
    )

    stream = await start_aio_quote_stream(symbol=sym, contract=contract)

    # pass back some symbol info like min_tick, trading_hours, etc.
    syminfo = asdict(details)
    syminfo.update(syminfo['contract'])

    # TODO: more consistent field translation
    atype = syminfo['asset_type'] = asset_type_map[syminfo['secType']]

    # for stocks it seems TWS reports too small a tick size
    # such that you can't submit orders with that granularity?
    min_tick = 0.01 if atype == 'stock' else 0

    syminfo['price_tick_size'] = max(syminfo['minTick'], min_tick)

    # for "traditional" assets, volume is normally discreet, not a float
    syminfo['lot_tick_size'] = 0.0

    # TODO: for loop through all symbols passed in
    init_msgs = {
        # pass back token, and bool, signalling if we're the writer
        # and that history has been written
        sym: {
            'symbol_info': syminfo,
        }
    }

    # check for special contract types
    if type(first_ticker.contract) not in (ibis.Commodity, ibis.Forex):
        suffix = 'exchange'
        # should be real volume for this contract
        calc_price = False
    else:
        # commodities and forex don't have an exchange name and
        # no real volume so we have to calculate the price
        suffix = 'secType'
        calc_price = True

    # pass first quote asap
    quote = normalize(first_ticker, calc_price=calc_price)
    con = quote['contract']
    topic = '.'.join((con['symbol'], con[suffix])).lower()
    quote['symbol'] = topic

    first_quote = {topic: quote}

    # ugh, clear ticks since we've consumed them
    # (ahem, ib_insync is stateful trash)
    first_ticker.ticks = []

    log.debug(f"First ticker received {quote}")

    task_status.started((init_msgs,  first_quote))

    if type(first_ticker.contract) not in (ibis.Commodity, ibis.Forex):
        suffix = 'exchange'
        calc_price = False  # should be real volume for contract

        # wait for real volume on feed (trading might be closed)
        async with aclosing(stream):

            async for ticker in stream:

                # for a real volume contract we rait for the first
                # "real" trade to take place
                if not calc_price and not ticker.rtTime:
                    # spin consuming tickers until we get a real market datum
                    log.debug(f"New unsent ticker: {ticker}")
                    continue
                else:
                    log.debug("Received first real volume tick")
                    # ugh, clear ticks since we've consumed them
                    # (ahem, ib_insync is truly stateful trash)
                    ticker.ticks = []

                    # XXX: this works because we don't use
                    # ``aclosing()`` above?
                    break

            # tell caller quotes are now coming in live
            feed_is_live.set()

            async for ticker in stream:

                # print(ticker.vwap)
                quote = normalize(
                    ticker,
                    calc_price=calc_price
                )

                con = quote['contract']
                topic = '.'.join((con['symbol'], con[suffix])).lower()
                quote['symbol'] = topic

                await send_chan.send({topic: quote})

                # ugh, clear ticks since we've consumed them
                ticker.ticks = []


def pack_position(pos: Position) -> Dict[str, Any]:
    con = pos.contract

    if isinstance(con, Option):
        # TODO: option symbol parsing and sane display:
        symbol = con.localSymbol.replace(' ', '')

    else:
        symbol = con.symbol

    return {
        'broker': 'ib',
        'account': pos.account,
        'symbol': symbol,
        'currency': con.currency,
        'size': float(pos.position),
        'avg_price': float(pos.avgCost) / float(con.multiplier or 1.0),
    }


@tractor.msg.pub(
    send_on_connect={'local_trades': 'start'}
)
async def stream_trades(
    loglevel: str = None,
    get_topics: Callable = None,
) -> AsyncIterator[Dict[str, Any]]:

    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    stream = await _trio_run_client_method(
        method='recv_trade_updates',
    )

    # deliver positions to subscriber before anything else
    positions = await _trio_run_client_method(method='positions')
    for pos in positions:
        yield {'local_trades': ('position', pack_position(pos))}

    action_map = {'BOT': 'buy', 'SLD': 'sell'}

    async for event_name, item in stream:

        # XXX: begin normalization of nonsense ib_insync internal
        # object-state tracking representations...

        if event_name == 'status':

            # unwrap needed data from ib_insync internal objects
            trade = item
            status = trade.orderStatus

            # skip duplicate filled updates - we get the deats
            # from the execution details event
            msg = {
                'reqid': trade.order.orderId,
                'status': status.status,
                'filled': status.filled,
                'reason': status.whyHeld,

                # this seems to not be necessarily up to date in the
                # execDetails event.. so we have to send it here I guess?
                'remaining': status.remaining,
            }

        elif event_name == 'fill':

            trade, fill = item
            execu = fill.execution

            msg = {
                'reqid': execu.orderId,
                'execid': execu.execId,

                # supposedly IB server fill time
                'broker_time': execu.time,  # converted to float by us
                # ns from main TCP handler by us inside ``ib_insync`` override
                'time': fill.time,
                'time_ns': time.time_ns(),  # cuz why not
                'action': action_map[execu.side],
                'size': execu.shares,
                'price': execu.price,
            }

        elif event_name == 'error':
            msg = item

            # f$#$% gawd dammit insync..
            con = msg['contract']
            if isinstance(con, Contract):
                msg['contract'] = asdict(con)

            if msg['reqid'] == -1:
                log.error(pformat(msg))

            # don't forward, it's pointless..
            continue

        elif event_name == 'position':
            msg = pack_position(item)

        if msg.get('reqid', 0) < -1:
            # it's a trade event generated by TWS usage.
            log.warning(f"TWS triggered trade:\n{pformat(msg)}")

            msg['reqid'] = 'tws-' + str(-1 * msg['reqid'])

            # mark msg as from "external system"
            # TODO: probably something better then this..
            msg['external'] = True

            yield {'remote_trades': (event_name, msg)}
            continue

        yield {'local_trades': (event_name, msg)}
