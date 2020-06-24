"""
Interactive Brokers API backend.
"""
import asyncio
from dataclasses import asdict
from typing import List, Dict, Any
from contextlib import asynccontextmanager

import trio
import ib_insync as ibis
from ib_insync.ticker import Ticker
from ib_insync.contract import Contract, ContractDetails


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
    """
    def __init__(
        self,
        ib: ibis.IB,
    ) -> None:
        self.ib = ib
        # connect data feed callback...
        self.ib.pendingTickersEvent.connect(self.on_tickers)

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
        bars = self.ib.reqHistoricalData(
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
        descriptions = self.ib.reqMatchingSymbols(pattern)
        details = {}
        for description in descriptions:
            con = description.contract
            deats = self.ib.reqContractDetails(con)
            # XXX: if there is more then one entry in the details list
            # then the contract is so called "ambiguous".
            for d in deats:
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


# default config ports
_tws_port: int = 7497
_gw_port: int = 4002


@asynccontextmanager
async def get_client(
    host: str = '127.0.0.1',
    port: int = None,
    client_id: int = 1,
) -> Client:
    """Return an ``ib_insync.IB`` instance wrapped in our client API.
    """
    ib = ibis.IB()
    # TODO: some detection magic to figure out if tws vs. the
    # gateway is up ad choose the appropriate port
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

    yield Client(ib)
    ib.disconnect()


if __name__ == '__main__':

    con_es = ibis.ContFuture('ES', exchange='GLOBEX')
    es = ibis.Future('ES', '20200918', exchange='GLOBEX')
    spy = ibis.Stock('SPY', exchange='ARCA')

    # ticker = client.ib.reqTickByTickData(
    #     contract,
    #     tickType='Last',
    #     numberOfTicks=1,
    # )
    # client.ib.reqTickByTickData(
    #     contract,
    #     tickType='AllLast',
    #     numberOfTicks=1,
    # )
    # client.ib.reqTickByTickData(
    #     contract,
    #     tickType='BidAsk',
    #     numberOfTicks=1,
    # )

    # ITC (inter task comms)
    from_trio = asyncio.Queue()
    to_trio, from_aio = trio.open_memory_channel(float("inf"))

    async def start_ib(from_trio, to_trio):
        print("starting the EYEEEEBEEEEE GATEWAYYYYYYY!")
        async with get_client() as client:

            # stream ticks to trio task
            def ontick(ticker: Ticker):
                for t in ticker.ticks:
                    # send tick data to trio
                    to_trio.send_nowait(t)

            ticker = client.ib.reqMktData(spy, '588', False, False, None)
            ticker.updateEvent += ontick

            n = await from_trio.get()
            assert n == 0

            # sleep and let the engine run
            await asyncio.sleep(float('inf'))

            # TODO: cmd processing from trio
            # while True:
            #     n = await from_trio.get()
            #     print(f"aio got: {n}")
            #     to_trio.send_nowait(n + 1)

    async def trio_main():
        print("trio_main!")

        asyncio.create_task(
            start_ib(from_trio, to_trio)
        )

        from_trio.put_nowait(0)

        async for tick in from_aio:
            print(f"trio got: {tick}")

            # TODO: send cmds to asyncio
            # from_trio.put_nowait(n + 1)

    async def aio_main():
        loop = asyncio.get_running_loop()

        trio_done_fut = asyncio.Future()

        def trio_done_callback(main_outcome):
            print(f"trio_main finished: {main_outcome!r}")
            trio_done_fut.set_result(main_outcome)

        trio.lowlevel.start_guest_run(
            trio_main,
            run_sync_soon_threadsafe=loop.call_soon_threadsafe,
            done_callback=trio_done_callback,
        )

        (await trio_done_fut).unwrap()

    asyncio.run(aio_main())
