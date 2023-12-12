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
Core API client machinery; mostly sane/useful wrapping around `ib_insync`..

'''
from __future__ import annotations
from contextlib import (
    asynccontextmanager as acm,
    contextmanager as cm,
)
from contextlib import AsyncExitStack
from dataclasses import (
    asdict,
    astuple,
)
from datetime import datetime
from functools import (
    partial,
)
import itertools
from math import isnan
import asyncio
from pprint import pformat
import inspect
import time
from typing import (
    Any,
    Callable,
    Union,
)
from types import SimpleNamespace

from bidict import bidict
import trio
import tractor
from tractor import to_asyncio
from pendulum import (
    from_timestamp,
    DateTime,
    Duration,
    duration as mk_duration,
)
from eventkit import Event
from ib_insync import (
    client as ib_client,
    IB,
    Contract,
    ContractDetails,
    Crypto,
    Commodity,
    Forex,
    Future,
    ContFuture,
    Stock,
    Order,
    Ticker,
    BarDataList,
    Position,
    Fill,
    # Execution,
    # CommissionReport,
    Wrapper,
    RequestError,
)
import numpy as np

# TODO: in hindsight, probably all imports should be
# non-relative for backends so that non-builting backends
# can be easily modelled after this style B)
from piker import config
from piker.accounting import MktPair
from .symbols import (
    con2fqme,
    parse_patt2fqme,
    _adhoc_symbol_map,
    _exch_skip_list,
    _futes_venues,
)
from ._util import (
    log,
    # only for the ib_sync internal logging
    get_logger,
)

_bar_load_dtype: list[tuple[str, type]] = [
    # NOTE XXX: only part that's diff
    # from our default fields where
    # time is normally an int.
    # TODO: can we just cast to this
    # at np.ndarray load time?
    ('time', float),

    ('open', float),
    ('high', float),
    ('low', float),
    ('close', float),
    ('volume', float),
    ('count', int),
]

# Broker specific ohlc schema which includes a vwap field
_ohlc_dtype: list[tuple[str, type]] = _bar_load_dtype.copy()
_ohlc_dtype.insert(
    0,
    ('index', int),
)


_time_units = {
    's': ' sec',
    'm': ' mins',
    'h': ' hours',
}

_bar_sizes = {
    1: '1 Sec',
    60: '1 min',
    60*60: '1 hour',
    24*60*60: '1 day',
}

_show_wap_in_history: bool = False

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


class NonShittyIB(IB):
    '''
    The beginning of overriding quite a few decisions in this lib.

    - Don't use datetimes
    - Don't use named tuples

    '''
    def __init__(self):

        # override `ib_insync` internal loggers so we can see wtf
        # it's doing..
        self._logger = get_logger(
            'ib_insync.ib',
        )
        self._createEvents()

        # XXX: just to override this wrapper
        self.wrapper = NonShittyWrapper(self)
        self.client = ib_client.Client(self.wrapper)
        self.client._logger = get_logger(
            'ib_insync.client',
        )

        # self.errorEvent += self._onError
        self.client.apiEnd += self.disconnectedEvent

_enters = 0


def bars_to_np(bars: list) -> np.ndarray:
    '''
    Convert a "bars list thing" (``BarDataList`` type from ibis)
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
        dtype=_bar_load_dtype,
    )
    assert nparr['time'][0] == bars[0].date.timestamp()
    assert nparr['time'][-1] == bars[-1].date.timestamp()
    return nparr


# NOTE: pacing violations exist for higher sample rates:
# https://interactivebrokers.github.io/tws-api/historical_limitations.html#pacing_violations
# Also see note on duration limits being lifted on 1m+ periods,
# but they say "use with discretion":
# https://interactivebrokers.github.io/tws-api/historical_limitations.html#non-available_hd
_samplings: dict[int, tuple[str, str]] = {
    1: (
        # ib strs
        '1 secs',
        f'{int(2e3)} S',

        mk_duration(seconds=2e3),
    ),
    # TODO: benchmark >1 D duration on query to see if
    # throughput can be made faster during backfilling.
    60: (
        # ib strs
        '1 min',
        '2 D',

        mk_duration(days=2),
    ),
}


@cm
def remove_handler_on_err(
    event: Event,
    handler: Callable,
) -> None:
    try:
        yield
    except trio.BrokenResourceError:
        # XXX: eventkit's ``Event.emit()`` for whatever redic
        # reason will catch and ignore regular exceptions
        # resulting in tracebacks spammed to console..
        # Manually do the dereg ourselves.
        log.exception(f'Disconnected from {event} updates')
        event.disconnect(handler)


class Client:
    '''
    IB wrapped for our broker backend API.

    Note: this client requires running inside an ``asyncio`` loop.

    '''
    # keyed by fqmes
    _contracts: dict[str, Contract] = {}
    # keyed by conId
    _cons: dict[str, Contract] = {}

    # for going between ib and piker types
    _cons2mkts: bidict[Contract, MktPair] = bidict({})

    def __init__(
        self,

        ib: IB,
        config: dict[str, Any],

    ) -> None:

        # stash `brokers.toml` config on client for user settings
        # as needed throughout this backend (eg. vnc sockaddr).
        self.conf = config

        # NOTE: the ib.client here is "throttled" to 45 rps by default
        self.ib = ib
        self.ib.RaiseRequestErrors: bool = True

    async def get_fills(self) -> list[Fill]:
        '''
        Return list of rents `Fills` from trading session.

        In theory this can be configured for dumping clears from multiple
        days but can't member where to set that..

        '''
        fills: list[Fill] = self.ib.fills()
        return fills

    async def orders(self) -> list[Order]:
        return await self.ib.reqAllOpenOrdersAsync(
            apiOnly=False,
        )

    async def bars(
        self,
        fqme: str,

        # EST in ISO 8601 format is required... below is EPOCH
        start_dt: Union[datetime, str] = "1970-01-01T00:00:00.000000-05:00",
        end_dt: Union[datetime, str] = "",

        # ohlc sample period in seconds
        sample_period_s: int = 1,

        # optional "duration of time" equal to the
        # length of the returned history frame.
        duration: str | None = None,

        **kwargs,

    ) -> tuple[BarDataList, np.ndarray, Duration]:
        '''
        Retreive OHLCV bars for a fqme over a range to the present.

        '''
        # See API docs here:
        # https://interactivebrokers.github.io/tws-api/historical_data.html
        bars_kwargs = {'whatToShow': 'TRADES'}
        bars_kwargs.update(kwargs)
        (
            bar_size,
            ib_duration_str,
            default_dt_duration,
        ) = _samplings[sample_period_s]

        dt_duration: DateTime = (
            duration
            or default_dt_duration
        )

        global _enters
        log.info(
            f"REQUESTING {ib_duration_str}'s worth {bar_size} BARS\n"
            f'{_enters} @ end={end_dt}"'
        )

        if not end_dt:
            end_dt = ''

        _enters += 1

        contract: Contract = (await self.find_contracts(fqme))[0]
        bars_kwargs.update(getattr(contract, 'bars_kwargs', {}))

        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_dt,
            formatDate=2,

            # OHLC sampling values:
            # 1 secs, 5 secs, 10 secs, 15 secs, 30 secs, 1 min, 2 mins,
            # 3 mins, 5 mins, 10 mins, 15 mins, 20 mins, 30 mins,
            # 1 hour, 2 hours, 3 hours, 4 hours, 8 hours, 1 day, 1W, 1M
            barSizeSetting=bar_size,

            # time history length values format:
            # ``durationStr=integer{SPACE}unit (S|D|W|M|Y)``
            durationStr=ib_duration_str,

            # always use extended hours
            useRTH=False,

            # restricted per contract type
            **bars_kwargs,
            # whatToShow='MIDPOINT',
            # whatToShow='TRADES',
        )

        # tail case if no history for range or none prior.
        if not bars:
            # NOTE: there's 2 cases here to handle (and this should be
            # read alongside the implementation of
            # ``.reqHistoricalDataAsync()``):
            # - no data is returned for the period likely due to
            # a weekend, holiday or other non-trading period prior to
            # ``end_dt`` which exceeds the ``duration``,
            # - a timeout occurred in which case insync internals return
            # an empty list thing with bars.clear()...
            return [], np.empty(0), dt_duration
            # TODO: we could maybe raise ``NoData`` instead if we
            # rewrite the method in the first case? right now there's no
            # way to detect a timeout.

        # NOTE XXX: ensure minimum duration in bars B)
        # => we recursively call this method until we get at least
        # as many bars such that they sum in aggregate to the the
        # desired total time (duration) at most.
        if end_dt:
            nparr: np.ndarray = bars_to_np(bars)
            times: np.ndarray = nparr['time']
            first: float = times[0]
            tdiff: float = times[-1] - first

            if (
                # len(bars) * sample_period_s) < dt_duration.in_seconds()
                tdiff < dt_duration.in_seconds()
            ):
                end_dt: DateTime = from_timestamp(first)
                log.warning(
                    f'Frame result was shorter then {dt_duration}!?\n'
                    'Recursing for more bars:\n'
                    f'end_dt: {end_dt}\n'
                    f'dt_duration: {dt_duration}\n'
                )
                (
                    r_bars,
                    r_arr,
                    r_duration,
                ) = await self.bars(
                    fqme,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    sample_period_s=sample_period_s,

                    # TODO: make a table for Duration to
                    # the ib str values in order to use this?
                    # duration=duration,
                )
                r_bars.extend(bars)
                bars = r_bars

        nparr = bars_to_np(bars)

        # timestep should always be at least as large as the
        # period step.
        tdiff: np.ndarray = np.diff(nparr['time'])
        to_short: np.ndarray = tdiff < sample_period_s
        if (to_short).any():
            # raise ValueError(
            log.error(
                f'OHLC frame for {sample_period_s} has {to_short.size} '
                'time steps which are shorter then expected?!"'
            )
            # OOF: this will break teardown?
            # breakpoint()

        return bars, nparr, dt_duration

    async def con_deats(
        self,
        contracts: list[Contract],

    ) -> dict[str, ContractDetails]:

        futs: list[asyncio.Future] = []
        for con in contracts:
            exch: str = con.primaryExchange or con.exchange
            if (
                exch
                and exch not in _exch_skip_list
            ):
                futs.append(self.ib.reqContractDetailsAsync(con))

        # batch request all details
        try:
            results: list[ContractDetails] = await asyncio.gather(*futs)
        except RequestError as err:
            msg: str = err.message
            if (
                'No security definition' in msg
            ):
                log.warning(f'{msg}: {contracts}')
                return {}

            raise

        # one set per future result
        details: dict[str, ContractDetails] = {}
        for details_set in results:

            # XXX: if there is more then one entry in the details list
            # then the contract is so called "ambiguous".
            for d in details_set:

                # nested dataclass we probably don't need and that won't
                # IPC serialize..
                d.secIdList = ''
                key, calc_price = con2fqme(d.contract)
                details[key] = d

        return details

    async def search_contracts(
        self,
        pattern: str,
        upto: int = 3,  # how many contracts to search "up to"

    ) -> dict[str, ContractDetails]:
        '''
        Search for ``Contract``s matching provided ``str`` pattern.

        Return a dictionary of ``upto`` entries worth of ``ContractDetails``.

        '''
        descrs: list[ContractDetails] = (
            await self.ib.reqMatchingSymbolsAsync(pattern)
        )
        if descrs is None:
            return {}

        return await self.con_deats(
            # limit to first ``upto`` entries
            [d.contract for d in descrs[:upto]]
        )

    async def search_symbols(
        self,
        pattern: str,
        # how many contracts to search "up to"
        upto: int = 16,
        asdicts: bool = True,

    ) -> dict[str, ContractDetails]:

        # TODO add search though our adhoc-locally defined symbol set
        # for futes/cmdtys/
        try:
            results = await self.search_contracts(
                pattern,
                upto=upto,
            )
        except ConnectionError:
            return {}

        dict_results: dict[str, dict] = {}
        for key, deats in results.copy().items():

            tract = deats.contract
            sym = tract.symbol
            sectype = tract.secType
            deats_dict = asdict(deats)

            if sectype == 'IND':
                results.pop(key)
                key = f'{sym}.IND'
                results[key] = tract
                # exch = tract.exchange

                # XXX: add back one of these to get the weird deadlock
                # on the debugger from root without the latest
                # maybe_wait_for_debugger() fix in the `open_context()`
                # exit.
                # assert 0
                # if con.exchange not in _exch_skip_list:

                exch = tract.exchange
                if exch not in _exch_skip_list:

                    # try to lookup any contracts from our adhoc set
                    # since often the exchange/venue is named slightly
                    # different (eg. BRR.CMECRYPTO` instead of just
                    # `.CME`).
                    info = _adhoc_symbol_map.get(sym)
                    if info:
                        con_kwargs, bars_kwargs = info
                        exch = con_kwargs['exchange']

                    # try get all possible contracts for symbol as per,
                    # https://interactivebrokers.github.io/tws-api/basic_contracts.html#fut
                    con = Future(
                        symbol=sym,
                        exchange=exch,
                    )
                    # TODO: make this work, think it's something to do
                    # with the qualify flag.
                    # cons = await self.find_contracts(
                    #     contract=con,
                    #     err_on_qualify=False,
                    # )
                    # if cons:
                    all_deats = await self.con_deats([con])
                    results |= all_deats
                    for key in all_deats:
                        dict_results[key] = asdict(all_deats[key])

            # forex pairs
            elif sectype == 'CASH':
                results.pop(key)
                dst, src = tract.localSymbol.split('.')
                pair_key = "/".join([dst, src])
                exch = tract.exchange.lower()
                key = f'{pair_key}.{exch}'
                results[key] = tract

                # XXX: again seems to trigger the weird tractor
                # bug with the debugger..
                # assert 0

            dict_results[key] = deats_dict

        return dict_results

    async def get_fute(
        self,
        symbol: str,
        exchange: str,
        expiry: str = '',
        front: bool = False,

    ) -> Contract:
        '''
        Get an unqualifed contract for the current "continous"
        future.

        '''
        # it's the "front" contract returned here
        if front:
            con = (await self.ib.qualifyContractsAsync(
                ContFuture(symbol, exchange=exchange)
            ))[0]
        else:
            con = (await self.ib.qualifyContractsAsync(
                Future(
                    symbol,
                    exchange=exchange,
                    lastTradeDateOrContractMonth=expiry,
                )
            ))[0]

        return con

    # TODO: is this a better approach?
    # @async_lifo_cache()
    async def get_con(
        self,
        conid: int,
    ) -> Contract:
        try:
            return self._cons[conid]
        except KeyError:
            con: Contract = await self.ib.qualifyContractsAsync(
                Contract(conId=conid)
            )
            self._cons[str(conid)] = con[0]
            return con

    async def find_contracts(
        self,
        pattern: str | None = None,
        contract: Contract | None = None,
        qualify: bool = True,
        err_on_qualify: bool = True,

    ) -> Contract:

        if pattern is not None:
            symbol, currency, exch, expiry = parse_patt2fqme(
                pattern,
            )
            sectype: str = ''
            exch: str = exch.upper()

        else:
            assert contract
            symbol: str = contract.symbol
            sectype: str = contract.secType
            exch: str = contract.exchange or contract.primaryExchange
            expiry: str = contract.lastTradeDateOrContractMonth
            currency: str = contract.currency

        # contract searching stage
        # ------------------------

        # futes, ensure exch/venue is uppercase for matching
        # our adhoc set.
        if exch.upper() in _futes_venues:
            if expiry:
                # get the "front" contract
                con = await self.get_fute(
                    symbol=symbol,
                    exchange=exch,
                    expiry=expiry,
                )

            else:
                # get the "front" contract
                con = await self.get_fute(
                    symbol=symbol,
                    exchange=exch,
                    front=True,
                )

        elif (
            exch in {'IDEALPRO'}
            or sectype == 'CASH'
        ):
            pair: str = symbol
            if '/' in symbol:
                src, dst = symbol.split('/')
                pair: str = ''.join([src, dst])

            con = Forex(
                pair=pair,
                currency='',
            )
            con.bars_kwargs = {'whatToShow': 'MIDPOINT'}

        # commodities
        elif exch == 'CMDTY':  # eg. XAUUSD.CMDTY
            con_kwargs, bars_kwargs = _adhoc_symbol_map[symbol.upper()]
            con = Commodity(**con_kwargs)
            con.bars_kwargs = bars_kwargs

        # crypto$
        elif exch == 'PAXOS':  # btc.paxos
            con = Crypto(
                symbol=symbol,
                currency=currency,
            )

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
                # XXX: order is super important here since
                # a primary == 'SMART' won't ever work.
                primaryExchange = exch
                exch = 'SMART'

            con = Stock(
                symbol=symbol,
                exchange=exch,
                primaryExchange=primaryExchange,
                currency=currency,
            )
            exch = 'SMART' if not exch else exch

        contracts: list[Contract] = [con]
        if qualify:
            try:
                contracts: list[Contract] = (
                    await self.ib.qualifyContractsAsync(con)
                )
            except RequestError as err:
                msg = err.message
                if (
                    'No security definition' in msg
                    and not err_on_qualify
                ):
                    log.warning(
                        f'Could not find def for {con}')
                    return None

                else:
                    raise
            if not contracts:
                raise ValueError(f"No contract could be found {con}")

        # pack all contracts into cache
        for tract in contracts:
            exch: str = (
                tract.primaryExchange
                or tract.exchange
                or exch
            )
            pattern: str = f'{symbol}.{exch.lower()}'
            expiry: str = tract.lastTradeDateOrContractMonth
            # add an entry with expiry suffix if available
            if expiry:
                pattern += f'.{expiry}'

            # since pos update msgs will always have the full fqme
            # with suffix?
            pattern += '.ib'

            # directly cache the input pattern to the output
            # contract match as well as by the IB-internal conId.
            self._contracts[pattern] = tract
            self._cons[str(tract.conId)] = tract

        return contracts

    async def get_head_time(
        self,
        fqme: str,

    ) -> datetime:
        '''
        Return the first datetime stamp for ``contract``.

        '''
        contract = (await self.find_contracts(fqme))[0]
        return await self.ib.reqHeadTimeStampAsync(
            contract,
            whatToShow='TRADES',
            useRTH=False,
            formatDate=2,  # timezone aware UTC datetime
        )

    async def get_sym_details(
        self,
        fqme: str,

    ) -> tuple[
        Contract,
        ContractDetails,
    ]:
        '''
        Return matching contracts for a given ``fqme: str`` including
        ``Contract`` and matching ``ContractDetails``.

        '''
        contract: Contract = (await self.find_contracts(fqme))[0]
        details: ContractDetails = (
            await self.ib.reqContractDetailsAsync(contract)
        )[0]
        return contract, details

    async def get_quote(
        self,
        contract: Contract,
        timeout: float = 1,
        raise_on_timeout: bool = False,

    ) -> Ticker | None:
        '''
        Return a single (snap) quote for symbol.

        '''
        ticker: Ticker = self.ib.reqMktData(
            contract,
            snapshot=True,
        )
        ready: ticker.TickerUpdateEvent = ticker.updateEvent

        # ensure a last price gets filled in before we deliver quote
        warnset: bool = False
        for _ in range(100):
            if isnan(ticker.last):

                # wait for a first update(Event)
                try:
                    tkr = await asyncio.wait_for(
                        ready,
                        timeout=timeout,
                    )
                except TimeoutError:
                    if raise_on_timeout:
                        raise
                    return None

                if tkr:
                    break
                else:
                    if not warnset:
                        log.warning(
                            f'Quote for {contract} timed out: market is closed?'
                        )
                        warnset = True

            else:
                log.info(f'Got first quote for {contract}')
                break
        else:
            if not warnset:
                log.warning(
                    f'Contract {contract} is not returning a quote '
                    'it may be outside trading hours?'
                )
                warnset = True

        return ticker

    # async to be consistent for the client proxy, and cuz why not.
    def submit_limit(
        self,
        oid: str,  # ignored since doesn't support defining your own
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

        Relevant docs:
        - https://interactivebrokers.github.io/tws-api/order_limitations.html

        '''
        try:
            con: Contract = self._contracts[symbol]
        except KeyError:
            # require that the symbol has been previously cached by
            # a data feed request - ensure we aren't making orders
            # against non-known prices.
            raise RuntimeError("Can not order {symbol}, no live feed?")

        try:
            trade = self.ib.placeOrder(
                con,
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
                    # TODO: make all orders GTC?
                    # https://interactivebrokers.github.io/tws-api/classIBApi_1_1Order.html#a95539081751afb9980f4c6bd1655a6ba
                    # goodTillDate=f"yyyyMMdd-HH:mm:ss",
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
            con: Contract,

        ) -> None:

            reason: str = errorString

            if reqId == -1:
                # it's a general event?
                key: str = 'event'
                log.info(errorString)

            else:
                key: str = 'error'
                log.error(errorString)

            try:
                to_trio.send_nowait((
                    key,
                    {
                        'type': key,
                        'reqid': reqId,
                        'reason': reason,
                        'error_code': errorCode,
                        'contract': con,
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

        api_err = self.ib.client.apiError

        def report_api_err(msg: str) -> None:
            with remove_handler_on_err(
                api_err,
                report_api_err,
            ):
                to_trio.send_nowait((
                    'error',
                    msg,
                ))
                api_err.clear()  # drop msg history

        api_err.connect(report_api_err)

    def positions(
        self,
        account: str = '',

    ) -> list[Position]:
        """
        Retrieve position info for ``account``.
        """
        return self.ib.positions(account=account)


# per-actor API ep caching
_client_cache: dict[tuple[str, int], Client] = {}
_scan_ignore: set[tuple[str, int]] = set()


def get_config() -> dict[str, Any]:

    conf, path = config.load(
        conf_name='brokers',
    )
    section = conf.get('ib')

    accounts = section.get('accounts')
    if not accounts:
        raise ValueError(
            'brokers.toml -> `ib.accounts` must be defined\n'
            f'location: {path}'
        )

    names = list(accounts.keys())
    accts = section['accounts'] = bidict(accounts)
    log.info(
        f'brokers.toml defines {len(accts)} accounts: '
        f'{pformat(names)}'
    )

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
    connect_timeout: float = 10,
    disconnect_on_exit: bool = True,

) -> dict[str, Client]:
    '''
    Return an ``ib_insync.IB`` instance wrapped in our client API.

    Client instances are cached for later use.

    TODO: consider doing this with a ctx mngr eventually?

    '''
    global _accounts2clients, _client_cache, _scan_ignore

    conf: dict[str, Any] = get_config()
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

        maybe_client = _client_cache.get(sockaddr)
        if (
            sockaddr in _scan_ignore
            or (
                maybe_client
                and maybe_client.ib.isConnected()
            )
        ):
            continue

        ib: IB = NonShittyIB()

        for i in range(connect_retries):
            try:
                await ib.connectAsync(
                    host,
                    port,
                    clientId=client_id + i,

                    # this timeout is sensitive on windows and will
                    # fail without a good "timeout error" so be
                    # careful.
                    timeout=connect_timeout,
                )
                # create and cache client
                client = Client(ib=ib, config=conf)

                # update all actor-global caches
                log.info(f"Caching client for {sockaddr}")
                _client_cache[sockaddr] = client
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
                log.warning(
                    f'Failed to connect on {host}:{port} for {i} time with,\n'
                    f'{ib.client.apiError.value()}\n'
                    'retrying with a new client id..')

        # Pre-collect all accounts available for this
        # connection and map account names to this client
        # instance.
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
        if disconnect_on_exit:
            for acct, client in _accounts2clients.items():
                log.info(f'Disconnecting {acct}@{client}')
                client.ib.disconnect()
                _client_cache.pop((host, port), None)


async def load_clients_for_trio(
    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,

) -> None:
    '''
    Pure async mngr proxy to ``load_aio_clients()``.

    This is a bootstrap entrypoing to call from
    a ``tractor.to_asyncio.open_channel_from()``.

    '''
    async with load_aio_clients() as accts2clients:

        to_trio.send_nowait(accts2clients)

        # TODO: maybe a sync event to wait on instead?
        await asyncio.sleep(float('inf'))


@acm
async def open_client_proxies() -> tuple[
    dict[str, MethodProxy],
    dict[str, Client],
]:
    async with (
        tractor.trionics.maybe_open_context(
            acm_func=tractor.to_asyncio.open_channel_from,
            kwargs={'target': load_clients_for_trio},

            # lock around current actor task access
            # TODO: maybe this should be the default in tractor?
            key=tractor.current_actor().uid,

        ) as (cache_hit, (clients, _)),

        AsyncExitStack() as stack
    ):
        if cache_hit:
            log.info(f'Re-using cached clients: {clients}')

        proxies = {}
        for acct_name, client in clients.items():
            proxy = await stack.enter_async_context(
                open_client_proxy(client),
            )
            proxies[acct_name] = proxy

        yield proxies, clients


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
    data_accounts: list[str] = conf['prefer_data_account']

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

            # TODO: implement reconnect functionality like
            # in our `.data._web_bs.NoBsWs`
            # try:
            #     msg = await chan.receive()
            # except ConnectionError:
            #     self.reset()

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

    # sync with `open_client_proxy()` caller
    to_trio.send_nowait(client)

    # TODO: separate channel for error handling?
    client.inline_errors(to_trio)

    # relay all method requests to ``asyncio``-side client and deliver
    # back results
    while not to_trio._closed:
        msg: tuple[str, dict] | dict | None = await from_trio.get()
        match msg:
            case None:  # termination sentinel
                print('asyncio PROXY-RELAY SHUTDOWN')
                break

            case (meth_name, kwargs):
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

            case {'error': content}:
                to_trio.send_nowait({'exception': content})

            case _:
                raise ValueError(f'Unhandled msg {msg}')


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
            Client,
            predicate=inspect.isfunction,
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
    is_brokercheck: bool = False,
    **kwargs,

) -> Client:
    '''
    Init the ``ib_insync`` client in another actor and return
    a method proxy to it.

    '''
    # hack for `piker brokercheck ib`..
    if is_brokercheck:
        yield Client
        return

    from .feed import open_data_client

    # TODO: the IPC via portal relay layer for when this current
    # actor isn't in aio mode.
    async with open_data_client() as proxy:
        yield proxy
