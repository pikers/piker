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
Symbology search and normalization.

'''
from __future__ import annotations
from contextlib import (
    nullcontext,
)
from decimal import Decimal
import time
from typing import (
    Awaitable,
    TYPE_CHECKING,
)

from rapidfuzz import process as fuzzy
import ib_insync as ibis
import tractor
import trio

from piker.accounting import (
    Asset,
    MktPair,
    unpack_fqme,
)
from piker._cacheables import (
    async_lifo_cache,
)

from ._util import (
    log,
)

if TYPE_CHECKING:
    from .api import (
        MethodProxy,
        Client,
    )

_futes_venues = (
    'GLOBEX',
    'NYMEX',
    'CME',
    'CMECRYPTO',
    'COMEX',
    # 'CMDTY',  # special name case..
    'CBOT',  # (treasury) yield futures
)

_adhoc_cmdty_set = {
    # metals
    # https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Conid%20Info&wlId=IB&conid=69067924
    'xauusd.cmdty',  # london gold spot ^
    'xagusd.cmdty',  # silver spot
}

# NOTE: if you aren't seeing one of these symbol's futues contracts
# show up, it's likely the `.<venue>` part is wrong!
_adhoc_futes_set = {

    # equities
    'nq.cme',
    'mnq.cme',  # micro

    'es.cme',
    'mes.cme',  # micro

    # cypto$
    'brr.cme',
    'mbt.cme',  # micro
    'ethusdrr.cme',

    # agriculture
    'he.comex',  # lean hogs
    'le.comex',  # live cattle (geezers)
    'gf.comex',  # feeder cattle (younguns)

    # raw
    'lb.comex',  # random len lumber

    'gc.comex',
    'mgc.comex',  # micro

    # oil & gas
    'cl.nymex',

    'ni.comex',  # silver futes
    'qi.comex',  # mini-silver futes

    # treasury yields
    # etfs by duration:
    # SHY -> IEI -> IEF -> TLT
    'zt.cbot',  # 2y
    'z3n.cbot',  # 3y
    'zf.cbot',  # 5y
    'zn.cbot',  # 10y
    'zb.cbot',  # 30y

    # (micros of above)
    '2yy.cbot',
    '5yy.cbot',
    '10y.cbot',
    '30y.cbot',
}


# taken from list here:
# https://www.interactivebrokers.com/en/trading/products-spot-currencies.php
_adhoc_fiat_set = set((
    'USD, AED, AUD, CAD,'
    'CHF, CNH, CZK, DKK,'
    'EUR, GBP, HKD, HUF,'
    'ILS, JPY, MXN, NOK,'
    'NZD, PLN, RUB, SAR,'
    'SEK, SGD, TRY, ZAR'
    ).split(' ,')
)

# manually discovered tick discrepancies,
# onl god knows how or why they'd cuck these up..
_adhoc_mkt_infos: dict[int | str, dict] = {
    'vtgn.nasdaq': {'price_tick': Decimal('0.01')},
}


# map of symbols to contract ids
_adhoc_symbol_map = {
    # https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Conid%20Info&wlId=IB&conid=69067924

    # NOTE: some cmdtys/metals don't have trade data like gold/usd:
    # https://groups.io/g/twsapi/message/44174
    'XAUUSD': ({'conId': 69067924}, {'whatToShow': 'MIDPOINT'}),
}
for qsn in _adhoc_futes_set:
    sym, venue = qsn.split('.')
    assert venue.upper() in _futes_venues, f'{venue}'
    _adhoc_symbol_map[sym.upper()] = (
        {'exchange': venue},
        {},
    )


# exchanges we don't support at the moment due to not knowing
# how to do symbol-contract lookup correctly likely due
# to not having the data feeds subscribed.
_exch_skip_list = {

    'ASX',  # aussie stocks
    'MEXI',  # mexican stocks

    # no idea
    'NSE',
    'VALUE',
    'FUNDSERV',
    'SWB2',
    'PSE',
    'PHLX',
}

# optional search config the backend can register for
# it's symbol search handling (in this case we avoid
# accepting patterns before the kb has settled more then
# a quarter second).
_search_conf = {
    'pause_period': 6 / 16,
}


@tractor.context
async def open_symbol_search(ctx: tractor.Context) -> None:
    '''
    Symbology search brokerd-endpoint.

    '''
    from .api import open_client_proxies
    from .feed import open_data_client

    # TODO: load user defined symbol set locally for fast search?
    await ctx.started({})

    async with (
        open_client_proxies() as (proxies, _),
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
                now: float = time.time()

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

                async def extend_results(
                    target: Awaitable[list]
                ) -> None:
                    try:
                        results = await target
                    except tractor.trionics.Lagged:
                        print("IB SYM-SEARCH OVERRUN?!?")
                        return

                    stock_results.extend(results)

                for _ in range(10):
                    with trio.move_on_after(3) as cs:
                        async with trio.open_nursery() as sn:
                            sn.start_soon(
                                extend_results,
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
                    elif stock_results:
                        break
                    # else:
                    await tractor.pause()

                    # # match against our ad-hoc set immediately
                    # adhoc_matches = fuzzy.extract(
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
                stock_matches = fuzzy.extract(
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


# re-mapping to piker asset type names
# https://github.com/erdewit/ib_insync/blob/master/ib_insync/contract.py#L113
_asset_type_map = {
    'STK': 'stock',
    'OPT': 'option',
    'FUT': 'future',
    'CONTFUT': 'continuous_future',
    'CASH': 'fiat',
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


def parse_patt2fqme(
    # client: Client,
    pattern: str,

) -> tuple[str, str, str, str]:

    # TODO: we can't use this currently because
    # ``wrapper.starTicker()`` currently cashes ticker instances
    # which means getting a singel quote will potentially look up
    # a quote for a ticker that it already streaming and thus run
    # into state clobbering (eg. list: Ticker.ticks). It probably
    # makes sense to try this once we get the pub-sub working on
    # individual symbols...

    # XXX UPDATE: we can probably do the tick/trades scraping
    # inside our eventkit handler instead to bypass this entirely?

    currency = ''

    # fqme parsing stage
    # ------------------
    if '.ib' in pattern:
        _, symbol, venue, expiry = unpack_fqme(pattern)

    else:
        symbol = pattern
        expiry = ''

        # # another hack for forex pairs lul.
        # if (
        #     '.idealpro' in symbol
        #     # or '/' in symbol
        # ):
        #     exch: str = 'IDEALPRO'
        #     symbol = symbol.removesuffix('.idealpro')
        #     if '/' in symbol:
        #         symbol, currency = symbol.split('/')

        # else:
        # TODO: yes, a cache..
        # try:
        #     # give the cache a go
        #     return client._contracts[symbol]
        # except KeyError:
        #     log.debug(f'Looking up contract for {symbol}')
        expiry: str = ''
        if symbol.count('.') > 1:
            symbol, _, expiry = symbol.rpartition('.')

        # use heuristics to figure out contract "type"
        symbol, venue = symbol.upper().rsplit('.', maxsplit=1)

    return symbol, currency, venue, expiry


def con2fqme(
    con: ibis.Contract,
    _cache: dict[int, (str, bool)] = {}

) -> tuple[str, bool]:
    '''
    Convert contracts to fqme-style strings to be used both in
    symbol-search matching and as feed tokens passed to the front
    end data deed layer.

    Previously seen contracts are cached by id.

    '''
    # should be real volume for this contract by default
    calc_price: bool = False
    if con.conId:
        try:
            # TODO: LOL so apparently IB just changes the contract
            # ID (int) on a whim.. so we probably need to use an
            # FQME style key after all...
            return _cache[con.conId]
        except KeyError:
            pass

    suffix: str = con.primaryExchange or con.exchange
    symbol: str = con.symbol
    expiry: str = con.lastTradeDateOrContractMonth or ''

    match con:
        case ibis.Option():
            # TODO: option symbol parsing and sane display:
            symbol = con.localSymbol.replace(' ', '')

        case (
            ibis.Commodity()
            # search API endpoint returns std con box..
            | ibis.Contract(secType='CMDTY')
        ):
            # commodities and forex don't have an exchange name and
            # no real volume so we have to calculate the price
            suffix = con.secType

            # no real volume on this tract
            calc_price = True

        case ibis.Forex() | ibis.Contract(secType='CASH'):
            dst, src = con.localSymbol.split('.')
            symbol = ''.join([dst, src])
            suffix = con.exchange or 'idealpro'

            # no real volume on forex feeds..
            calc_price = True

    if not suffix:
        entry = _adhoc_symbol_map.get(
            con.symbol or con.localSymbol
        )
        if entry:
            meta, kwargs = entry
            cid = meta.get('conId')
            if cid:
                assert con.conId == meta['conId']
            suffix = meta['exchange']

    # append a `.<suffix>` to the returned symbol
    # key for derivatives that normally is the expiry
    # date key.
    if expiry:
        suffix += f'.{expiry}'

    fqme_key = symbol.lower()
    if suffix:
        fqme_key = '.'.join((fqme_key, suffix)).lower()

    _cache[con.conId] = fqme_key, calc_price
    return fqme_key, calc_price


@async_lifo_cache()
async def get_mkt_info(
    fqme: str,

    proxy: MethodProxy | None = None,

) -> tuple[MktPair, ibis.ContractDetails]:

    if '.ib' not in fqme:
        fqme += '.ib'
    broker, pair, venue, expiry = unpack_fqme(fqme)

    proxy: MethodProxy
    if proxy is not None:
        client_ctx = nullcontext(proxy)
    else:
        from .feed import (
            open_data_client,
        )
        client_ctx = open_data_client

    async with client_ctx as proxy:
        try:
            (
                con,  # Contract
                details,  # ContractDetails
            ) = await proxy.get_sym_details(fqme=fqme)
        except ConnectionError:
            log.exception(f'Proxy is ded {proxy._aio_ns}')
            raise

    # TODO: more consistent field translation
    atype = _asset_type_map[con.secType]

    if atype == 'commodity':
        venue: str = 'cmdty'
    else:
        venue = con.primaryExchange or con.exchange

    price_tick: Decimal = Decimal(str(details.minTick))
    # price_tick: Decimal = Decimal('0.01')

    if atype == 'stock':
        # XXX: GRRRR they don't support fractional share sizes for
        # stocks from the API?!
        # if con.secType == 'STK':
        size_tick = Decimal('1')
    else:
        size_tick: Decimal = Decimal(
            str(details.minSize).rstrip('0')
        )
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
    src = Asset(
        name=str(con.currency).lower(),
        atype='fiat',
        tx_tick=Decimal('0.01'),  # right?
    )
    dst = Asset(
        name=con.symbol.lower(),
        atype=atype,
        tx_tick=size_tick,
    )

    mkt = MktPair(
        src=src,
        dst=dst,

        price_tick=price_tick,
        size_tick=size_tick,

        bs_mktid=str(con.conId),
        venue=str(venue),
        expiry=expiry_str,
        broker='ib',

        # TODO: options contract info as str?
        # contract_info=<optionsdetails>
        _fqme_without_src=(atype != 'fiat'),
    )

    # just.. wow.
    if entry := _adhoc_mkt_infos.get(mkt.bs_fqme):
        log.warning(f'Frickin {mkt.fqme} has an adhoc {entry}..')
        new = mkt.to_dict()
        new['price_tick'] = entry['price_tick']
        new['src'] = src
        new['dst'] = dst
        mkt = MktPair(**new)

    # if possible register the bs_mktid to the just-built
    # mkt so that it can be retreived by order mode tasks later.
    # TODO NOTE: this is going to be problematic if/when we split
    # out the datatd vs. brokerd actors since the mktmap lookup
    # table will now be inaccessible..
    if proxy is not None:
        client: Client = proxy._aio_ns
        client._contracts[mkt.bs_fqme] = con
        client._cons2mkts[con] = mkt

    return mkt, details
