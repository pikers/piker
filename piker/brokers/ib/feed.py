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
Data feed endpoints pre-wrapped and ready for use with ``tractor``/``trio``.

"""
from __future__ import annotations
import asyncio
from contextlib import (
    asynccontextmanager as acm,
    nullcontext,
)
from decimal import Decimal
from dataclasses import asdict
from datetime import datetime
from functools import partial
from math import isnan
import time
from typing import (
    Callable,
    Optional,
    Awaitable,
)

from async_generator import aclosing
from fuzzywuzzy import process as fuzzy
import numpy as np
import pendulum
import tractor
import trio
from trio_typing import TaskStatus

from .._util import (
    NoData,
    DataUnavailable,
    SymbolNotFound,
)
from .api import (
    # _adhoc_futes_set,
    con2fqme,
    log,
    load_aio_clients,
    ibis,
    MethodProxy,
    open_client_proxies,
    get_preferred_data_client,
    Ticker,
    Contract,
    RequestError,
)
from ._util import data_reset_hack
from piker._cacheables import (
    async_lifo_cache,
)
from piker.accounting import (
    Asset,
    MktPair,
)
from piker.data.validate import FeedInit


# https://interactivebrokers.github.io/tws-api/tick_types.html
tick_types = {
    77: 'trade',

    # a "utrade" aka an off exchange "unreportable" (dark) vlm:
    # https://interactivebrokers.github.io/tws-api/tick_types.html#rt_volume
    48: 'dark_trade',

    # standard L1 ticks
    0: 'bsize',
    1: 'bid',
    2: 'ask',
    3: 'asize',
    4: 'last',
    5: 'size',
    8: 'volume',

    # ``ib_insync`` already packs these into
    # quotes under the following fields.
    # 55: 'trades_per_min',  # `'tradeRate'`
    # 56: 'vlm_per_min',  # `'volumeRate'`
    # 89: 'shortable',  # `'shortableShares'`
}


@acm
async def open_data_client() -> MethodProxy:
    '''
    Open the first found preferred "data client" as defined in the
    user's ``brokers.toml`` in the ``ib.prefer_data_account`` variable
    and deliver that client wrapped in a ``MethodProxy``.

    '''
    async with (
        open_client_proxies() as (proxies, clients),
    ):
        account_name, client = get_preferred_data_client(clients)
        proxy = proxies.get(f'ib.{account_name}')
        if not proxy:
            raise ValueError(
                f'No preferred data client could be found for {account_name}!'
            )

        yield proxy


@acm
async def open_history_client(
    mkt: MktPair,

) -> tuple[Callable, int]:
    '''
    History retreival endpoint - delivers a historical frame callble
    that takes in ``pendulum.datetime`` and returns ``numpy`` arrays.

    '''
    # TODO: mostly meta-data processing to drive shm and tsdb storage..
    # - add logic to handle tradable hours and only grab
    #   valid bars in the range?
    # - we want to avoid overrunning the underlying shm array buffer and
    #   we should probably calc the number of calls to make depending on
    #   that until we have the `marketstore` daemon in place in which case
    #   the shm size will be driven by user config and available sys
    #   memory.

    # IB's internal symbology does not expect the "source asset" in
    # the "symbol name", what we call the "market name". This is
    # common in most legacy market brokers since it's presumed that
    # given a certain stock exchange, listed assets are traded
    # "from" a particular source fiat, normally something like USD.
    if (
        mkt.src
        and mkt.src.atype == 'fiat'
    ):
        fqme_kwargs: dict[str, Any] = {}

        if mkt.dst.atype == 'forex':

            # XXX: for now we do need the src token kept in since
            fqme_kwargs  = {
                'without_src': False,  # default is True
                'delim_char': '',  # bc they would normally use a frickin `.` smh
            }

        fqme: str = mkt.get_bs_fqme(**(fqme_kwargs))

    else:
        fqme = mkt.bs_fqme

    async with open_data_client() as proxy:


        max_timeout: float = 2.
        mean: float = 0
        count: int = 0

        head_dt: None | datetime = None
        if (
            # fx cons seem to not provide this endpoint?
            'idealpro' not in fqme
        ):
            try:
                head_dt = await proxy.get_head_time(fqme=fqme)
            except RequestError:
                head_dt = None

        async def get_hist(
            timeframe: float,
            end_dt: Optional[datetime] = None,
            start_dt: Optional[datetime] = None,

        ) -> tuple[np.ndarray, str]:
            nonlocal max_timeout, mean, count

            query_start = time.time()
            out, timedout = await get_bars(
                proxy,
                fqme,
                timeframe,
                end_dt=end_dt,
            )
            latency = time.time() - query_start
            if (
                not timedout
                # and latency <= max_timeout
            ):
                count += 1
                mean += latency / count
                print(
                    f'HISTORY FRAME QUERY LATENCY: {latency}\n'
                    f'mean: {mean}'
                )

            if (
                out is None
            ):
                # could be trying to retreive bars over weekend
                log.error(f"Can't grab bars starting at {end_dt}!?!?")
                raise NoData(
                    f'{end_dt}',
                    # frame_size=2000,
                )

            if (
                end_dt
                and head_dt
                and end_dt <= head_dt
            ):
                raise DataUnavailable(f'First timestamp is {head_dt}')

            bars, bars_array, first_dt, last_dt = out

            # volume cleaning since there's -ve entries,
            # wood luv to know what crookery that is..
            vlm = bars_array['volume']
            vlm[vlm < 0] = 0

            return bars_array, first_dt, last_dt

        # TODO: it seems like we can do async queries for ohlc
        # but getting the order right still isn't working and I'm not
        # quite sure why.. needs some tinkering and probably
        # a lookthrough of the ``ib_insync`` machinery, for eg. maybe
        # we have to do the batch queries on the `asyncio` side?
        yield get_hist, {'erlangs': 1, 'rate': 3}


_pacing: str = (
    'Historical Market Data Service error '
    'message:Historical data request pacing violation'
)


async def wait_on_data_reset(
    proxy: MethodProxy,
    reset_type: str = 'data',
    timeout: float = 16, # float('inf'),

    task_status: TaskStatus[
        tuple[
            trio.CancelScope,
            trio.Event,
        ]
    ] = trio.TASK_STATUS_IGNORED,
) -> bool:

    # TODO: we might have to put a task lock around this
    # method..
    hist_ev = proxy.status_event(
        'HMDS data farm connection is OK:ushmds'
    )

    # TODO: other event messages we might want to try and
    # wait for but i wasn't able to get any of this
    # reliable..
    # reconnect_start = proxy.status_event(
    #     'Market data farm is connecting:usfuture'
    # )
    # live_ev = proxy.status_event(
    #     'Market data farm connection is OK:usfuture'
    # )
    # try to wait on the reset event(s) to arrive, a timeout
    # will trigger a retry up to 6 times (for now).
    client = proxy._aio_ns.ib.client

    done = trio.Event()
    with trio.move_on_after(timeout) as cs:

        task_status.started((cs, done))

        log.warning(
            'Sending DATA RESET request:\n'
            f'{client}'
        )
        res = await data_reset_hack(
            vnc_host=client.host,
            reset_type=reset_type,
        )

        if not res:
            log.warning(
                'NO VNC DETECTED!\n'
                'Manually press ctrl-alt-f on your IB java app'
            )
            done.set()
            return False

        # TODO: not sure if waiting on other events
        # is all that useful here or not.
        # - in theory you could wait on one of the ones above first
        # to verify the reset request was sent?
        # - we need the same for real-time quote feeds which can
        # sometimes flake out and stop delivering..
        for name, ev in [
            ('history', hist_ev),
        ]:
            await ev.wait()
            log.info(f"{name} DATA RESET")
            done.set()
            return True

    if cs.cancel_called:
        log.warning(
            'Data reset task canceled?'
        )

    done.set()
    return False


_data_resetter_task: trio.Task | None = None
_failed_resets: int = 0

async def get_bars(

    proxy: MethodProxy,
    fqme: str,
    timeframe: int,

    # blank to start which tells ib to look up the latest datum
    end_dt: str = '',

    # TODO: make this more dynamic based on measured frame rx latency?
    # how long before we trigger a feed reset (seconds)
    feed_reset_timeout: float = 3,

    # how many days to subtract before giving up on further
    # history queries for instrument, presuming that most don't
    # not trade for a week XD
    max_nodatas: int = 6,
    max_failed_resets: int = 6,

    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,

) -> (dict, np.ndarray):
    '''
    Retrieve historical data from a ``trio``-side task using
    a ``MethoProxy``.

    '''
    global _data_resetter_task, _failed_resets
    nodatas_count: int = 0

    data_cs: trio.CancelScope | None = None
    result: tuple[
        ibis.objects.BarDataList,
        np.ndarray,
        datetime,
        datetime,
    ] | None = None
    result_ready = trio.Event()

    async def query():

        global _failed_resets
        nonlocal result, data_cs, end_dt, nodatas_count

        while _failed_resets < max_failed_resets:
            try:
                out = await proxy.bars(
                    fqme=fqme,
                    end_dt=end_dt,
                    sample_period_s=timeframe,

                    # ideally we cancel the request just before we
                    # cancel on the ``trio``-side and trigger a data
                    # reset hack.. the problem is there's no way (with
                    # current impl) to detect a cancel case.
                    # timeout=timeout,
                )
                if out is None:
                    raise NoData(f'{end_dt}')

                bars, bars_array, dt_duration = out

                if (
                    not bars
                    and end_dt
                ):
                    log.warning(
                        f'History is blank for {dt_duration} from {end_dt}'
                    )
                    end_dt -= dt_duration
                    continue

                if bars_array is None:
                    raise SymbolNotFound(fqme)

                first_dt = pendulum.from_timestamp(
                    bars[0].date.timestamp())

                last_dt = pendulum.from_timestamp(
                    bars[-1].date.timestamp())

                time = bars_array['time']
                assert time[-1] == last_dt.timestamp()
                assert time[0] == first_dt.timestamp()
                log.info(
                    f'{len(bars)} bars retreived {first_dt} -> {last_dt}'
                )

                if data_cs:
                    data_cs.cancel()

                result = (bars, bars_array, first_dt, last_dt)

                # signal data reset loop parent task
                result_ready.set()

                return result

            except RequestError as err:
                msg = err.message

                if 'No market data permissions for' in msg:
                    # TODO: signalling for no permissions searches
                    raise NoData(
                        f'Symbol: {fqme}',
                    )

                elif (
                    'HMDS query returned no data' in msg
                ):
                    # XXX: this is now done in the storage mgmt
                    # layer and we shouldn't implicitly decrement
                    # the frame dt index since the upper layer may
                    # be doing so concurrently and we don't want to
                    # be delivering frames that weren't asked for.
                    # try to decrement start point and look further back
                    # end_dt = end_dt.subtract(seconds=2000)
                    logmsg = "SUBTRACTING DAY from DT index"
                    if end_dt is not None:
                        end_dt = end_dt.subtract(days=1)
                    elif end_dt is None:
                        end_dt = pendulum.now().subtract(days=1)

                    log.warning(
                        f'NO DATA found ending @ {end_dt}\n'
                        + logmsg
                    )

                    if nodatas_count >= max_nodatas:
                        raise DataUnavailable(
                            f'Presuming {fqme} has no further history '
                            f'after {max_nodatas} tries..'
                        )

                    nodatas_count += 1
                    continue

                elif 'API historical data query cancelled' in err.message:
                    log.warning(
                        'Query cancelled by IB (:eyeroll:):\n'
                        f'{err.message}'
                    )
                    continue
                elif (
                    'Trading TWS session is connected from a different IP'
                        in err.message
                ):
                    log.warning("ignoring ip address warning")
                    continue

                # XXX: more or less same as above timeout case
                elif _pacing in msg:
                    log.warning(
                        'History throttle rate reached!\n'
                        'Resetting farms with `ctrl-alt-f` hack\n'
                    )

                    client = proxy._aio_ns.ib.client

                    # cancel any existing reset task
                    if data_cs:
                        log.cancel(f'Cancelling existing reset for {client}')
                        data_cs.cancel()

                    # spawn new data reset task
                    data_cs, reset_done = await nurse.start(
                        partial(
                            wait_on_data_reset,
                            proxy,
                            reset_type='connection'
                        )
                    )
                    if reset_done:
                        _failed_resets = 0
                    else:
                        _failed_resets += 1
                    continue

                else:
                    raise

    # TODO: make this global across all history task/requests
    # such that simultaneous symbol queries don't try data resettingn
    # too fast..
    unset_resetter: bool = False
    async with trio.open_nursery() as nurse:

        # start history request that we allow
        # to run indefinitely until a result is acquired
        nurse.start_soon(query)

        # start history reset loop which waits up to the timeout
        # for a result before triggering a data feed reset.
        while not result_ready.is_set():

            with trio.move_on_after(feed_reset_timeout):
                await result_ready.wait()
                break

            if _data_resetter_task:
                # don't double invoke the reset hack if another
                # requester task already has it covered.
                continue
            else:
                _data_resetter_task = trio.lowlevel.current_task()
                unset_resetter = True

            # spawn new data reset task
            data_cs, reset_done = await nurse.start(
                partial(
                    wait_on_data_reset,
                    proxy,
                    reset_type='data',
                )
            )
            # sync wait on reset to complete
            await reset_done.wait()

    _data_resetter_task = None if unset_resetter else _data_resetter_task
    return result, data_cs is not None


# re-mapping to piker asset type names
# https://github.com/erdewit/ib_insync/blob/master/ib_insync/contract.py#L113
_asset_type_map = {
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
    'CRYPTO': 'crypto',  # bc it's diff then fiat?
    # 'NEWS': 'news',
}


_quote_streams: dict[str, trio.abc.ReceiveStream] = {}


async def _setup_quote_stream(

    from_trio: asyncio.Queue,
    to_trio: trio.abc.SendChannel,

    symbol: str,
    opts: tuple[int] = (
        '375',  # RT trade volume (excludes utrades)
        '233',  # RT trade volume (includes utrades)
        '236',  # Shortable shares

        # these all appear to only be updated every 25s thus
        # making them mostly useless and explains why the scanner
        # is always slow XD
        # '293',  # Trade count for day
        '294',  # Trade rate / minute
        '295',  # Vlm rate / minute
    ),
    contract: Contract | None = None,

) -> trio.abc.ReceiveChannel:
    '''
    Stream a ticker using the std L1 api.

    This task is ``asyncio``-side and must be called from
    ``tractor.to_asyncio.open_channel_from()``.

    '''
    global _quote_streams

    to_trio.send_nowait(None)

    async with load_aio_clients(
        disconnect_on_exit=False,
    ) as accts2clients:
        caccount_name, client = get_preferred_data_client(accts2clients)
        contract = contract or (await client.find_contract(symbol))
        ticker: Ticker = client.ib.reqMktData(contract, ','.join(opts))

        # NOTE: it's batch-wise and slow af but I guess could
        # be good for backchecking? Seems to be every 5s maybe?
        # ticker: Ticker = client.ib.reqTickByTickData(
        #     contract, 'Last',
        # )

        # # define a simple queue push routine that streams quote packets
        # # to trio over the ``to_trio`` memory channel.
        # to_trio, from_aio = trio.open_memory_channel(2**8)  # type: ignore
        def teardown():
            ticker.updateEvent.disconnect(push)
            log.error(f"Disconnected stream for `{symbol}`")
            client.ib.cancelMktData(contract)

            # decouple broadcast mem chan
            _quote_streams.pop(symbol, None)

        def push(t: Ticker) -> None:
            """
            Push quotes to trio task.

            """
            # log.debug(t)
            try:
                to_trio.send_nowait(t)

            except (
                trio.BrokenResourceError,

                # XXX: HACK, not sure why this gets left stale (probably
                # due to our terrible ``tractor.to_asyncio``
                # implementation for streams.. but if the mem chan
                # gets left here and starts blocking just kill the feed?
                # trio.WouldBlock,
            ):
                # XXX: eventkit's ``Event.emit()`` for whatever redic
                # reason will catch and ignore regular exceptions
                # resulting in tracebacks spammed to console..
                # Manually do the dereg ourselves.
                teardown()
            except trio.WouldBlock:
                # log.warning(
                #     f'channel is blocking symbol feed for {symbol}?'
                #     f'\n{to_trio.statistics}'
                # )
                pass

            # except trio.WouldBlock:
            #     # for slow debugging purposes to avoid clobbering prompt
            #     # with log msgs
            #     pass

        ticker.updateEvent.connect(push)
        try:
            await asyncio.sleep(float('inf'))
        finally:
            teardown()

        # return from_aio


@acm
async def open_aio_quote_stream(

    symbol: str,
    contract: Contract | None = None,

) -> trio.abc.ReceiveStream:

    from tractor.trionics import broadcast_receiver
    global _quote_streams

    from_aio = _quote_streams.get(symbol)
    if from_aio:

        # if we already have a cached feed deliver a rx side clone
        # to consumer
        async with broadcast_receiver(
            from_aio,
            2**6,
        ) as from_aio:
            yield from_aio
            return

    async with tractor.to_asyncio.open_channel_from(
        _setup_quote_stream,
        symbol=symbol,
        contract=contract,

    ) as (first, from_aio):

        # cache feed for later consumers
        _quote_streams[symbol] = from_aio

        yield from_aio


# TODO: cython/mypyc/numba this!
# or we can at least cache a majority of the values
# except for the ones we expect to change?..
def normalize(
    ticker: Ticker,
    calc_price: bool = False

) -> dict:

    # check for special contract types
    con = ticker.contract
    fqme, calc_price = con2fqme(con)

    # convert named tuples to dicts so we send usable keys
    new_ticks = []
    for tick in ticker.ticks:
        if tick and not isinstance(tick, dict):
            td = tick._asdict()
            td['type'] = tick_types.get(
                td['tickType'],
                'n/a',
            )

            new_ticks.append(td)

            tbt = ticker.tickByTicks
            if tbt:
                print(f'tickbyticks:\n {ticker.tickByTicks}')

    ticker.ticks = new_ticks

    # some contracts don't have volume so we may want to calculate
    # a midpoint price based on data we can acquire (such as bid / ask)
    if calc_price:
        ticker.ticks.append(
            {'type': 'trade', 'price': ticker.marketPrice()}
        )

    # serialize for transport
    data = asdict(ticker)

    # generate fqme with possible specialized suffix
    # for derivatives, note the lowercase.
    data['symbol'] = data['fqme'] = fqme

    # convert named tuples to dicts for transport
    tbts = data.get('tickByTicks')
    if tbts:
        data['tickByTicks'] = [tbt._asdict() for tbt in tbts]

    # add time stamps for downstream latency measurements
    data['brokerd_ts'] = time.time()

    # stupid stupid shit...don't even care any more..
    # leave it until we do a proper latency study
    # if ticker.rtTime is not None:
    #     data['broker_ts'] = data['rtTime_s'] = float(
    #         ticker.rtTime.timestamp) / 1000.
    data.pop('rtTime')

    return data


@async_lifo_cache()
async def get_mkt_info(
    fqme: str,

    proxy: MethodProxy | None = None,

) -> tuple[MktPair, ibis.ContractDetails]:

    # XXX: we don't need to split off any fqme broker part?
    # bs_fqme, _, broker = fqme.partition('.')

    proxy: MethodProxy
    get_details: bool = False
    if proxy is not None:
        client_ctx = nullcontext(proxy)
    else:
        client_ctx = open_data_client

    async with client_ctx as proxy:
        try:
            (
                con,  # Contract
                details,  # ContractDetails
            ) = await proxy.get_sym_details(symbol=fqme)
        except ConnectionError:
            log.exception(f'Proxy is ded {proxy._aio_ns}')
            raise

    # TODO: more consistent field translation
    init_info: dict = {}
    atype = _asset_type_map[con.secType]

    if atype == 'commodity':
        venue: str = 'cmdty'
    else:
        venue = con.primaryExchange or con.exchange

    price_tick: Decimal = Decimal(str(details.minTick))

    if atype == 'stock':
        # XXX: GRRRR they don't support fractional share sizes for
        # stocks from the API?!
        # if con.secType == 'STK':
        size_tick = Decimal('1')
    else:
        size_tick: Decimal = Decimal(str(details.minSize).rstrip('0'))
        # |-> TODO: there is also the Contract.sizeIncrement, bt wtf is it?

    # NOTE: this is duplicate from the .broker.norm_trade_records()
    # routine, we should factor all this parsing somewhere..
    expiry_str = str(con.lastTradeDateOrContractMonth)
    # if expiry:
    #     expiry_str: str = str(pendulum.parse(
    #         str(expiry).strip(' ')
    #     ))

    # TODO: currently we can't pass the fiat src asset because
    # then we'll get a `MNQUSD` request for history data..
    # we need to figure out how we're going to handle this (later?)
    # but likely we want all backends to eventually handle
    # ``dst/src.venue.`` style !?
    src: str | Asset = ''
    if atype == 'forex':
        src = Asset(
            name=str(con.currency),
            atype='fiat',
            tx_tick=Decimal('0.01'),  # right?
        )

    mkt = MktPair(
        dst=Asset(
            name=con.symbol.lower(),
            atype=atype,
            tx_tick=size_tick,
        ),
        src=src,

        price_tick=price_tick,
        size_tick=size_tick,

        bs_mktid=str(con.conId),
        venue=str(venue),
        expiry=expiry_str,
        broker='ib',

        # TODO: options contract info as str?
        # contract_info=<optionsdetails>
    )

    return mkt, details


async def stream_quotes(

    send_chan: trio.abc.SendChannel,
    symbols: list[str],
    feed_is_live: trio.Event,
    loglevel: str = None,

    # startup sync
    task_status: TaskStatus[tuple[dict, dict]] = trio.TASK_STATUS_IGNORED,

) -> None:
    '''
    Stream symbol quotes.

    This is a ``trio`` callable routine meant to be invoked
    once the brokerd is up.

    '''
    # TODO: support multiple subscriptions
    sym = symbols[0]
    log.info(f'request for real-time quotes: {sym}')

    init_msgs: list[FeedInit] = []

    proxy: MethodProxy
    mkt: MktPair
    details: ibis.ContractDetails
    async with open_data_client() as proxy:
        mkt, details = await get_mkt_info(
            sym,
            proxy=proxy,  # passed to avoid implicit client load
        )

        init_msg = FeedInit(mkt_info=mkt)

        if mkt.dst.atype in {
            'forex',
            'index',
            'commodity',
        }:
            # tell sampler config that it shouldn't do vlm summing.
            init_msg.shm_write_opts['sum_tick_vlm'] = False
            init_msg.shm_write_opts['has_vlm'] = False

        init_msgs.append(init_msg)

        con: Contract = details.contract
        first_ticker: Ticker = await proxy.get_quote(contract=con)
        first_quote: dict = normalize(first_ticker)
        log.runtime(f'FIRST QUOTE: {first_quote}')

        # TODO: we should instead spawn a task that waits on a feed to start
        # and let it wait indefinitely..instead of this hard coded stuff.
        with trio.move_on_after(1):
            first_ticker = await proxy.get_quote(contract=con)

        # it might be outside regular trading hours so see if we can at
        # least grab history.
        if (
            isnan(first_ticker.last)  # last quote price value is nan
            and mkt.dst.atype not in {
                'commodity',
                'forex',
                'crypto',
            }
        ):
            task_status.started((init_msgs, first_quote))

            # it's not really live but this will unblock
            # the brokerd feed task to tell the ui to update?
            feed_is_live.set()

            # block and let data history backfill code run.
            await trio.sleep_forever()
            return  # we never expect feed to come up?

        cs: trio.CancelScope | None = None
        startup: bool = True
        while (
            startup
            or cs.cancel_called
        ):
            with trio.CancelScope() as cs:
                async with (
                    trio.open_nursery() as nurse,
                    open_aio_quote_stream(
                        symbol=sym,
                        contract=con,
                    ) as stream,
                ):
                    # ugh, clear ticks since we've consumed them
                    # (ahem, ib_insync is stateful trash)
                    first_ticker.ticks = []

                    # only on first entry at feed boot up
                    if startup:
                        startup = False
                        task_status.started((init_msgs, first_quote))

                    # start a stream restarter task which monitors the
                    # data feed event.
                    async def reset_on_feed():

                        # TODO: this seems to be surpressed from the
                        # traceback in ``tractor``?
                        # assert 0

                        rt_ev = proxy.status_event(
                            'Market data farm connection is OK:usfarm'
                        )
                        await rt_ev.wait()
                        cs.cancel()  # cancel called should now be set

                    nurse.start_soon(reset_on_feed)

                    async with aclosing(stream):
                        # if syminfo.get('no_vlm', False):
                        if not init_msg.shm_write_opts['has_vlm']:

                            # generally speaking these feeds don't
                            # include vlm data.
                            atype = mkt.dst.atype
                            log.info(
                                f'No-vlm {mkt.fqme}@{atype}, skipping quote poll'
                            )

                        else:
                            # wait for real volume on feed (trading might be
                            # closed)
                            while True:
                                ticker = await stream.receive()

                                # for a real volume contract we rait for
                                # the first "real" trade to take place
                                if (
                                    # not calc_price
                                    # and not ticker.rtTime
                                    not ticker.rtTime
                                ):
                                    # spin consuming tickers until we
                                    # get a real market datum
                                    log.debug(f"New unsent ticker: {ticker}")
                                    continue
                                else:
                                    log.debug("Received first volume tick")
                                    # ugh, clear ticks since we've
                                    # consumed them (ahem, ib_insync is
                                    # truly stateful trash)
                                    ticker.ticks = []

                                    # XXX: this works because we don't use
                                    # ``aclosing()`` above?
                                    break

                            quote = normalize(ticker)
                            log.debug(f"First ticker received {quote}")

                        # tell caller quotes are now coming in live
                        feed_is_live.set()

                        # last = time.time()
                        async for ticker in stream:
                            quote = normalize(ticker)
                            fqme = quote['fqme']
                            # print(f'sending {fqme}:\n{quote}')
                            await send_chan.send({fqme: quote})

                            # ugh, clear ticks since we've consumed them
                            ticker.ticks = []
                            # last = time.time()


@tractor.context
async def open_symbol_search(
    ctx: tractor.Context,

) -> None:

    # TODO: load user defined symbol set locally for fast search?
    await ctx.started({})

    async with (
        open_client_proxies() as (proxies, clients),
        open_data_client() as data_proxy,
    ):
        async with ctx.open_stream() as stream:

            # select a non-history client for symbol search to lighten
            # the load in the main data node.
            proxy = data_proxy
            for name, proxy in proxies.items():
                if proxy is data_proxy:
                    continue
                break

            ib_client = proxy._aio_ns.ib
            log.info(f'Using {ib_client} for symbol search')

            last = time.time()
            async for pattern in stream:
                log.info(f'received {pattern}')
                now = time.time()

                # this causes tractor hang...
                # assert 0

                assert pattern, 'IB can not accept blank search pattern'

                # throttle search requests to no faster then 1Hz
                diff = now - last
                if diff < 1.0:
                    log.debug('throttle sleeping')
                    await trio.sleep(diff)
                    try:
                        pattern = stream.receive_nowait()
                    except trio.WouldBlock:
                        pass

                if (
                    not pattern
                    or pattern.isspace()

                    # XXX: not sure if this is a bad assumption but it
                    # seems to make search snappier?
                    or len(pattern) < 1
                ):
                    log.warning('empty pattern received, skipping..')

                    # TODO: *BUG* if nothing is returned here the client
                    # side will cache a null set result and not showing
                    # anything to the use on re-searches when this query
                    # timed out. We probably need a special "timeout" msg
                    # or something...

                    # XXX: this unblocks the far end search task which may
                    # hold up a multi-search nursery block
                    await stream.send({})

                    continue

                log.info(f'searching for {pattern}')

                last = time.time()

                # async batch search using api stocks endpoint and module
                # defined adhoc symbol set.
                stock_results = []

                async def stash_results(target: Awaitable[list]):
                    try:
                        results = await target
                    except tractor.trionics.Lagged:
                        print("IB SYM-SEARCH OVERRUN?!?")
                        return

                    stock_results.extend(results)

                for i in range(10):
                    with trio.move_on_after(3) as cs:
                        async with trio.open_nursery() as sn:
                            sn.start_soon(
                                stash_results,
                                proxy.search_symbols(
                                    pattern=pattern,
                                    upto=5,
                                ),
                            )

                            # trigger async request
                            await trio.sleep(0)

                    if cs.cancelled_caught:
                        log.warning(
                            f'Search timeout? {proxy._aio_ns.ib.client}'
                        )
                        continue
                    else:
                        break

                    # # match against our ad-hoc set immediately
                    # adhoc_matches = fuzzy.extractBests(
                    #     pattern,
                    #     list(_adhoc_futes_set),
                    #     score_cutoff=90,
                    # )
                    # log.info(f'fuzzy matched adhocs: {adhoc_matches}')
                    # adhoc_match_results = {}
                    # if adhoc_matches:
                    #     # TODO: do we need to pull contract details?
                    #     adhoc_match_results = {i[0]: {} for i in
                    #     adhoc_matches}

                log.debug(f'fuzzy matching stocks {stock_results}')
                stock_matches = fuzzy.extractBests(
                    pattern,
                    stock_results,
                    score_cutoff=50,
                )

                # matches = adhoc_match_results | {
                matches = {
                    item[0]: {} for item in stock_matches
                }
                # TODO: we used to deliver contract details
                # {item[2]: item[0] for item in stock_matches}

                log.debug(f"sending matches: {matches.keys()}")
                await stream.send(matches)
