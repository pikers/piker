# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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
from functools import partial
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, AsyncIterator, Callable
import asyncio
import logging
import inspect
import itertools
import time

from async_generator import aclosing
from ib_insync.wrapper import RequestError
from ib_insync.contract import Contract, ContractDetails
from ib_insync.ticker import Ticker
import ib_insync as ibis
from ib_insync.wrapper import Wrapper
from ib_insync.client import Client as ib_Client
import trio
import tractor

from ..log import get_logger, get_console_log
from ..data import (
    maybe_spawn_brokerd,
    iterticks,
    attach_shm_array,
    # get_shm_token,
    subscribe_ohlc_for_increment,
)
from ..data._source import from_df
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
        # use a float to store epoch time instead of datetime
        self.lastTime = time.time()
        for ticker in self.pendingTickers:
            ticker.rtTime = None
            ticker.ticks = []
            ticker.tickByTicks = []
            ticker.domTicks = []
        self.pendingTickers = set()


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

    def onError(self, reqId, errorCode, errorString, contract) -> None:
        breakpoint()

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

            # head = await self.get_head_time(contract)
            # print(head)

        except IndexError:
            raise ValueError(f"No contract could be found {con}")
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

    async def stream_ticker(
        self,
        symbol: str,
        to_trio,
        opts: Tuple[int] = ('375', '233',),
        # opts: Tuple[int] = ('459',),
    ) -> None:
        """Stream a ticker using the std L1 api.
        """
        contract = await self.find_contract(symbol)
        ticker: Ticker = self.ib.reqMktData(contract, ','.join(opts))

        # define a simple queue push routine that streams quote packets
        # to trio over the ``to_trio`` memory channel.

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
                self.ib.cancelMktData(contract)

        ticker.updateEvent.connect(push)

        # let the engine run and stream
        await self.ib.disconnectedEvent


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
    """
    # first check cache for existing client

    # breakpoint()
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


_local_buffer_writers = {}


@asynccontextmanager
async def activate_writer(key: str) -> (bool, trio.Nursery):
    try:
        writer_already_exists = _local_buffer_writers.get(key, False)

        if not writer_already_exists:
            _local_buffer_writers[key] = True

            async with trio.open_nursery() as n:
                yield writer_already_exists, n
        else:
            yield writer_already_exists, None
    finally:
        _local_buffer_writers.pop(key, None)


async def fill_bars(
    sym: str,
    first_bars: list,
    shm: 'ShmArray',  # type: ignore # noqa
    # count: int = 20,  # NOTE: any more and we'll overrun the underlying buffer
    count: int = 2,  # NOTE: any more and we'll overrun the underlying buffer
) -> None:
    """Fill historical bars into shared mem / storage afap.

    TODO: avoid pacing constraints:
    https://github.com/pikers/piker/issues/128

    """
    next_dt = first_bars[0].date

    i = 0
    while i < count:

        try:
            bars, bars_array = await _trio_run_client_method(
                method='bars',
                symbol=sym,
                end_dt=next_dt,

            )
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
                        "Data query rate reached: Press `ctrl-alt-f` in TWS")

                    # TODO: should probably create some alert on screen
                    # and then somehow get that to trigger an event here
                    # that restarts/resumes this task?
                    await tractor.breakpoint()


# TODO: figure out how to share quote feeds sanely despite
# the wacky ``ib_insync`` api.
# @tractor.msg.pub
@tractor.stream
async def stream_quotes(
    ctx: tractor.Context,
    symbols: List[str],
    shm_token: Tuple[str, str, List[tuple]],
    loglevel: str = None,
    # compat for @tractor.msg.pub
    topics: Any = None,
    get_topics: Callable = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Stream symbol quotes.

    This is a ``trio`` callable routine meant to be invoked
    once the brokerd is up.
    """
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    # TODO: support multiple subscriptions
    sym = symbols[0]

    stream = await _trio_run_client_method(
        method='stream_ticker',
        symbol=sym,
    )

    async with aclosing(stream):

        # check if a writer already is alive in a streaming task,
        # otherwise start one and mark it as now existing
        async with activate_writer(
            shm_token['shm_name']
        ) as (writer_already_exists, ln):

            # maybe load historical ohlcv in to shared mem
            # check if shm has already been created by previous
            # feed initialization
            if not writer_already_exists:

                shm = attach_shm_array(
                    token=shm_token,

                    # we are the buffer writer
                    readonly=False,
                )

                # async def retrieve_and_push():
                start = time.time()

                bars, bars_array = await _trio_run_client_method(
                    method='bars',
                    symbol=sym,

                )

                log.info(f"bars_array request: {time.time() - start}")

                if bars_array is None:
                    raise SymbolNotFound(sym)

                # write historical data to buffer
                shm.push(bars_array)
                shm_token = shm.token

                # TODO: generalize this for other brokers
                # start bar filler task in bg
                ln.start_soon(fill_bars, sym, bars, shm)

                times = shm.array['time']
                delay_s = times[-1] - times[times != times[-1]][-1]
                subscribe_ohlc_for_increment(shm, delay_s)

            # pass back token, and bool, signalling if we're the writer
            await ctx.send_yield((shm_token, not writer_already_exists))

            # first quote can be ignored as a 2nd with newer data is sent?
            first_ticker = await stream.__anext__()

            quote = normalize(first_ticker)

            # ugh, clear ticks since we've consumed them
            # (ahem, ib_insync is stateful trash)
            first_ticker.ticks = []

            log.debug(f"First ticker received {quote}")

            if type(first_ticker.contract) not in (ibis.Commodity, ibis.Forex):
                suffix = 'exchange'

                calc_price = False  # should be real volume for contract

                async for ticker in stream:
                    # spin consuming tickers until we get a real market datum
                    if not ticker.rtTime:
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
            else:
                # commodities don't have an exchange name for some reason?
                suffix = 'secType'
                calc_price = True
                ticker = first_ticker

            quote = normalize(ticker, calc_price=calc_price)
            con = quote['contract']
            topic = '.'.join((con['symbol'], con[suffix])).lower()
            quote['symbol'] = topic

            first_quote = {topic: quote}
            ticker.ticks = []

            # yield first quote asap
            await ctx.send_yield(first_quote)

            # real-time stream
            async for ticker in stream:
                # print(ticker.vwap)
                quote = normalize(
                    ticker,
                    calc_price=calc_price
                )
                quote['symbol'] = topic
                # TODO: in theory you can send the IPC msg *before*
                # writing to the sharedmem array to decrease latency,
                # however, that will require `tractor.msg.pub` support
                # here or at least some way to prevent task switching
                # at the yield such that the array write isn't delayed
                # while another consumer is serviced..

                # if we are the lone tick writer start writing
                # the buffer with appropriate trade data
                if not writer_already_exists:
                    for tick in iterticks(quote, types=('trade', 'utrade',)):
                        last = tick['price']

                        # print(f"{quote['symbol']}: {tick}")

                        # update last entry
                        # benchmarked in the 4-5 us range
                        o, high, low, v = shm.array[-1][
                            ['open', 'high', 'low', 'volume']
                        ]

                        new_v = tick.get('size', 0)

                        if v == 0 and new_v:
                            # no trades for this bar yet so the open
                            # is also the close/last trade price
                            o = last

                        shm.array[[
                            'open',
                            'high',
                            'low',
                            'close',
                            'volume',
                        ]][-1] = (
                            o,
                            max(high, last),
                            min(low, last),
                            last,
                            v + new_v,
                        )

                con = quote['contract']
                topic = '.'.join((con['symbol'], con[suffix])).lower()
                quote['symbol'] = topic

                await ctx.send_yield({topic: quote})

                # ugh, clear ticks since we've consumed them
                ticker.ticks = []
