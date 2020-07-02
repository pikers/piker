"""
Interactive Brokers API backend.

Note the client runs under an ``asyncio`` loop (since ``ib_insync`` is
built on it) and thus actor aware apis must be spawned with
``infected_aio==True``.
"""
import asyncio
from dataclasses import asdict
from functools import partial
import inspect
from typing import List, Dict, Any, Tuple
from contextlib import asynccontextmanager
import time

import tractor
from async_generator import aclosing
import ib_insync as ibis
from ib_insync.ticker import Ticker
from ib_insync.contract import Contract, ContractDetails

from ..log import get_logger, get_console_log


log = get_logger(__name__)


_time_frames = {
    '1s': '1 Sec',
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
        count: int = int(20e3),  # <- max allowed per query
        is_paid_feed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Retreive OHLCV bars for a symbol over a range to the present.
        """
        contract = ibis.ContFuture('ES', exchange='GLOBEX')
        # contract = ibis.Stock('WEED', 'SMART', 'CAD')
        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            # durationStr='60 S',
            durationStr='2000 S',
            barSizeSetting='1 secs',
            whatToShow='TRADES',
            useRTH=False
        )
        # barSizeSetting='1 min', whatToShow='MIDPOINT', useRTH=True)
        # convert to pandas dataframe:
        df = ibis.util.df(bars)
        print(df[['date', 'open', 'high', 'low', 'close', 'volume']])
        from piker.ui._source import from_df
        a = from_df(df)
        # breakpoint()
        print(a)

    # TODO: reimplement this using async batch requests
    # see https://github.com/erdewit/ib_insync/issues/262
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

    def get_cont_fute(
        self,
        symbol: str,
    ) -> Contract:
        raise NotImplementedError

    async def stream_ticker(
        self,
        symbol: str,
        to_trio,
        opts: Tuple[int] = ('233', '375'),
    ) -> None:
        """Stream a ticker using the std L1 api.
        """
        sym, exch = symbol.split('.')
        contract = ibis.Stock(sym.upper(), exchange=exch.upper())
        ticker: Ticker = self.ib.reqMktData(contract, ','.join(opts))
        ticker.updateEvent.connect(lambda t: to_trio.send_nowait(t))

        # let the engine run and stream
        await self.ib.disconnectedEvent


# default config ports
_tws_port: int = 7497
_gw_port: int = 4002


@asynccontextmanager
async def _aio_get_client(
    host: str = '127.0.0.1',
    port: int = None,
    client_id: int = 1,
) -> Client:
    """Return an ``ib_insync.IB`` instance wrapped in our client API.
    """
    ib = ibis.IB()

    if port is None:
        ports = [_tws_port, _gw_port]
    else:
        ports = [port]

    _err = None
    # try all default ports
    for port in ports:
        try:
            await ib.connectAsync(host, port, clientId=client_id)
            break
        except ConnectionRefusedError as ce:
            _err = ce
            print(f'failed to connect on {port}')
    else:
        raise ConnectionRefusedError(_err)

    try:
        yield Client(ib)
    except BaseException:
        ib.disconnect()
        raise


async def _aio_run_client_method(
    meth: str,
    to_trio,
    from_trio,
    **kwargs,
) -> None:
    log.info("Connecting to the EYEEEEBEEEEE GATEWAYYYYYYY!")
    async with _aio_get_client() as client:

        async_meth = getattr(client, meth)

        # handle streaming methods
        args = tuple(inspect.getfullargspec(async_meth).args)
        if 'to_trio' in args:
            kwargs['to_trio'] = to_trio

        return await async_meth(**kwargs)


async def _trio_run_client_method(
    method: str,
    **kwargs,
) -> None:
    ca = tractor.current_actor()
    assert ca.is_infected_aio()

    # if the method is an async gen stream for it
    meth = getattr(Client, method)
    if inspect.isasyncgenfunction(meth):
        kwargs['_treat_as_stream'] = True

    # if the method is an async func but streams back results
    # make sure to also stream from it
    args = tuple(inspect.getfullargspec(meth).args)
    if 'to_trio' in args:
        kwargs['_treat_as_stream'] = True

    result = await tractor.to_asyncio.run_task(
        _aio_run_client_method,
        meth=method,
        **kwargs
    )
    return result


def get_method_proxy(portal):

    class MethodProxy:
        def __init__(self, portal: tractor._portal.Portal):
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

    proxy = MethodProxy(portal)

    # mock all remote methods
    for name, method in inspect.getmembers(
        Client, predicate=inspect.isfunction
    ):
        if '_' == name[0]:
            continue
        setattr(proxy, name, partial(proxy._run_method, meth=name))

    return proxy


@asynccontextmanager
async def maybe_spawn_brokerd(
    **kwargs,
) -> tractor._portal.Portal:
    async with tractor.find_actor('brokerd_ib') as portal:
        if portal is None:  # no broker daemon created yet

            async with tractor.open_nursery() as n:
                # XXX: this needs to somehow be hidden
                portal = await n.start_actor(
                    'brokerd_ib',
                    rpc_module_paths=[__name__],
                    infect_asyncio=True,
                )
                async with tractor.wait_for_actor(
                    'brokerd_ib'
                ) as portal:
                    yield portal

                # client code may block indefinitely so cancel when
                # teardown is invoked
                await n.cancel()


@asynccontextmanager
async def get_client(
    **kwargs,
) -> Client:
    """Init the ``ib_insync`` client in another actor and return
    a method proxy to it.
    """
    async with maybe_spawn_brokerd(**kwargs) as portal:
        yield get_method_proxy(portal)


async def trio_stream_ticker(sym):
    get_console_log('info')

    # con_es = ibis.ContFuture('ES', exchange='GLOBEX')
    # es = ibis.Future('ES', '20200918', exchange='GLOBEX')

    stream = await tractor.to_asyncio.run_task(
        _trio_run_client_method,
        method='stream_ticker',
        symbol=sym,
    )
    async with aclosing(stream):
        async for ticker in stream:
            lft = ticker.lastFillTime
            for tick_data in ticker.ticks:
                value = tick_data._asdict()
                now = time.time()
                value['time'] = now
                value['last_fill_time'] = lft
                if lft:
                    value['latency'] = now - lft
                yield value


async def stream_from_brokerd(sym):

    async with maybe_spawn_brokerd() as portal:
        stream = await portal.run(
            __name__,
            'trio_stream_ticker',
            sym=sym,
        )
        async for tick in stream:
            print(f"trio got: {tick}")


if __name__ == '__main__':
    import sys

    sym = sys.argv[1]

    tractor.run(
        stream_from_brokerd,
        sym,
        # XXX: must be multiprocessing
        start_method='forkserver',
        loglevel='info'
    )
