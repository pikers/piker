"""
Interactive Brokers API backend.

Note the client runs under an ``asyncio`` loop (since ``ib_insync`` is
built on it) and thus actor aware API calls must be spawned with
``infected_aio==True``.
"""
from contextlib import asynccontextmanager
from dataclasses import asdict
from functools import partial
from typing import List, Dict, Any, Tuple, Optional, AsyncGenerator, Callable
import asyncio
import logging
import inspect
import itertools
import time

from async_generator import aclosing
from ib_insync.contract import Contract, ContractDetails
from ib_insync.ticker import Ticker
import ib_insync as ibis
from ib_insync.wrapper import Wrapper
from ib_insync.client import Client as ib_Client
import trio
import tractor

from ..log import get_logger, get_console_log
from ..data import maybe_spawn_brokerd
from ..ui._source import from_df


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


# overrides to sidestep pretty questionable design decisions in
# ``ib_insync``:

class NonShittyWrapper(Wrapper):
    def tcpDataArrived(self):
        """Override time stamps to be floats for now.
        """
        # use a float to store epoch time instead of datetime
        self.lastTime = time.time()
        for ticker in self.pendingTickers:
            ticker.rtTime = None
            ticker.ticks = []
            ticker.tickByTicks = []
            ticker.domTicks = []
        self.pendingTickers = set()


class NonShittyIB(ibis.IB):
    """The beginning of overriding quite a few quetionable decisions
    in this lib.

    - Don't use datetimes
    - Don't use named tuples
    """
    def __init__(self):
        self._createEvents()
        # XXX: just to override this wrapper
        self.wrapper = NonShittyWrapper(self)
        self.client = ib_Client(self.wrapper)
        self.errorEvent += self._onError
        self.client.apiEnd += self.disconnectedEvent
        self._logger = logging.getLogger('ib_insync.ib')


# map of symbols to contract ids
_adhoc_cmdty_data_map = {
    # https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Conid%20Info&wlId=IB&conid=69067924
    # NOTE: cmdtys don't have trade data:
    # https://groups.io/g/twsapi/message/44174
    'XAUUSD': ({'conId': 69067924}, {'whatToShow': 'MIDPOINT'}),
}


class Client:
    """IB wrapped for our broker backend API.

    Note: this client requires running inside an ``asyncio`` loop.
    """
    def __init__(
        self,
        ib: ibis.IB,
    ) -> None:
        self.ib = ib

    async def bars(
        self,
        symbol: str,
        # EST in ISO 8601 format is required... below is EPOCH
        start_date: str = "1970-01-01T00:00:00.000000-05:00",
        time_frame: str = '1m',
        count: int = int(2e3),  # <- max allowed per query
        is_paid_feed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Retreive OHLCV bars for a symbol over a range to the present.
        """
        bars_kwargs = {'whatToShow': 'TRADES'}

        contract = await self.find_contract(symbol)
        bars_kwargs.update(getattr(contract, 'bars_kwargs', {}))

        # _min = min(2000*100, count)
        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            # durationStr='60 S',
            # durationStr='1 D',

            # time length calcs
            durationStr='{count} S'.format(count=1000 * 5),
            barSizeSetting='5 secs',

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

        # convert to pandas dataframe:
        df = ibis.util.df(bars)
        return from_df(df)

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
        frontcon = (await self.ib.qualifyContractsAsync(contcon))[0]
        return ibis.Future(conId=frontcon.conId)

    async def find_contract(
        self,
        symbol,
        currency: str = 'USD',
        **kwargs,
    ) -> Contract:
        # use heuristics to figure out contract "type"
        sym, exch = symbol.upper().split('.')

        # futes
        if exch in ('GLOBEX', 'NYMEX', 'CME', 'CMECRYPTO'):
            con = await self.get_cont_fute(symbol=sym, exchange=exch)

        # commodities
        elif exch == 'CMDTY':  # eg. XAUUSD.CMDTY
            con_kwargs, bars_kwargs = _adhoc_cmdty_data_map[sym]
            con = ibis.Commodity(**con_kwargs)
            con.bars_kwargs = bars_kwargs

        # stonks
        else:
            # TODO: metadata system for all these exchange rules..
            if exch in ('PURE', 'TSE'):  # non-yankee
                currency = 'CAD'

            con = ibis.Stock(symbol=sym, exchange=exch, currency=currency)

        try:
            exch = 'SMART' if not exch else exch
            contract = (await self.ib.qualifyContractsAsync(con))[0]
        except IndexError:
            raise ValueError(f"No contract could be found {con}")
        return contract

    async def stream_ticker(
        self,
        symbol: str,
        to_trio,
        opts: Tuple[int] = ('233', '375'),
        # opts: Tuple[int] = ('459',),
    ) -> None:
        """Stream a ticker using the std L1 api.
        """
        contract = await self.find_contract(symbol)
        ticker: Ticker = self.ib.reqMktData(contract, ','.join(opts))

        def push(t):
            log.debug(t)
            try:
                to_trio.send_nowait(t)
            except trio.BrokenResourceError:
                # XXX: eventkit's ``Event.emit()`` for whatever redic
                # reason will catch and ignore regular exceptions
                # resulting in tracebacks spammed to console..
                # Manually do the dereg ourselves.
                ticker.updateEvent.disconnect(push)
                log.error(f"Disconnected stream for `{symbol}`")
                self.ib.cancelMktData(contract)

        ticker.updateEvent.connect(push)

        # let the engine run and stream
        await self.ib.disconnectedEvent


# default config ports
_tws_port: int = 7497
_gw_port: int = 4002
_try_ports = [_tws_port, _gw_port]


@asynccontextmanager
async def _aio_get_client(
    host: str = '127.0.0.1',
    port: int = None,
    client_id: Optional[int] = None,
) -> Client:
    """Return an ``ib_insync.IB`` instance wrapped in our client API.
    """
    if client_id is None:
        # if this is a persistent brokerd, try to allocate a new id for
        # each client
        try:
            ss = tractor.current_actor().statespace
            client_id = next(ss.setdefault('client_ids', itertools.count()))
            # TODO: in case the arbiter has no record
            # of existing brokerd we need to broadcase for one.
        except RuntimeError:
            # tractor likely isn't running
            client_id = 1

    ib = NonShittyIB()
    ports = _try_ports if port is None else [port]
    _err = None
    for port in ports:
        try:
            await ib.connectAsync(host, port, clientId=client_id)
            break
        except ConnectionRefusedError as ce:
            _err = ce
            log.warning(f'Failed to connect on {port}')
    else:
        raise ConnectionRefusedError(_err)

    try:
        yield Client(ib)
    except BaseException:
        ib.disconnect()
        raise


async def _aio_run_client_method(
    meth: str,
    to_trio=None,
    from_trio=None,
    **kwargs,
) -> None:
    log.info("Connecting to the EYEEEEBEEEEE GATEWAYYYYYYY!")
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
    ca = tractor.current_actor()
    assert ca.is_infected_aio()

    # if the method is an *async gen* stream for it
    meth = getattr(Client, method)
    if inspect.isasyncgenfunction(meth):
        kwargs['_treat_as_stream'] = True

    # if the method is an *async func* but manually
    # streams back results, make sure to also stream it
    args = tuple(inspect.getfullargspec(meth).args)
    if 'to_trio' in args:
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
            __name__,
            '_trio_run_client_method',
            method=meth,
            **kwargs
        )


def get_method_proxy(portal, target) -> _MethodProxy:

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
        expose_mods=[__name__],
        infect_asyncio=True,
        **kwargs
    ) as portal:
        yield get_method_proxy(portal, Client)


def normalize(
    ticker: Ticker,
    calc_price: bool = False
) -> dict:
    # convert named tuples to dicts so we send usable keys
    new_ticks = []
    for tick in ticker.ticks:
        td = tick._asdict()

        if td['tickType'] in (48, 77):
            td['type'] = 'trade'

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
    if ticker.rtTime:
        data['broker_ts'] = data['rtTime_s'] = float(ticker.rtTime) / 1000.

    return data


# TODO: figure out how to share quote feeds sanely despite
# the wacky ``ib_insync`` api.
# @tractor.msg.pub
async def stream_quotes(
    symbols: List[str],
    loglevel: str = None,
    # compat for @tractor.msg.pub
    topics: Any = None,
    get_topics: Callable = None,
) -> AsyncGenerator[str, Dict[str, Any]]:
    """Stream symbol quotes.

    This is a ``trio`` callable routine meant to be invoked
    once the brokerd is up.
    """
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    # TODO: support multiple subscriptions
    sym = symbols[0]

    stream = await tractor.to_asyncio.run_task(
        _trio_run_client_method,
        method='stream_ticker',
        symbol=sym,
    )
    async with aclosing(stream):
        # first quote can be ignored as a 2nd with newer data is sent?
        first_ticker = await stream.__anext__()

        if type(first_ticker.contract) not in (ibis.Commodity,):
            suffix = 'exchange'

            calc_price = False  # should be real volume for contract

            quote = normalize(first_ticker)
            log.debug(f"First ticker received {quote}")

            con = quote['contract']
            topic = '.'.join((con['symbol'], con[suffix])).lower()
            yield {topic: quote}

            # ugh, clear ticks since we've consumed them
            # (ahem, ib_insync is stateful trash)
            first_ticker.ticks = []

            async for ticker in stream:
                # spin consuming tickers until we get a real market datum
                if not ticker.rtTime:
                    log.debug(f"New unsent ticker: {ticker}")
                    continue
                else:
                    log.debug("Received first real volume tick")
                    quote = normalize(ticker)
                    topic = '.'.join((con['symbol'], con[suffix])).lower()
                    yield {topic: quote}

                    # ugh, clear ticks since we've consumed them
                    # (ahem, ib_insync is stateful trash)
                    ticker.ticks = []

                    # XXX: this works because we don't use
                    # ``aclosing()`` above?
                    break
        else:
            # commodities don't have an exchange name for some reason?
            suffix = 'secType'
            calc_price = True

        async for ticker in stream:
            quote = normalize(
                ticker,
                calc_price=calc_price
            )
            con = quote['contract']
            topic = '.'.join((con['symbol'], con[suffix])).lower()
            yield {topic: quote}

            # ugh, clear ticks since we've consumed them
            ticker.ticks = []


if __name__ == '__main__':
    import sys
    sym = sys.argv[1]

    contract = asyncio.run(
        _aio_run_client_method(
            'find_contract',
            symbol=sym,
        )
    )
    print(contract)
