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
Fake trading: a full forward testing simulation engine.

We can real-time emulate any mkt conditions you want bruddr B)
Just slide us the model que quieres..

'''
from collections import defaultdict
from contextlib import asynccontextmanager as acm
from datetime import datetime
from operator import itemgetter
import itertools
import time
from typing import (
    Callable,
)
from types import ModuleType
import uuid

from bidict import bidict
import pendulum
import trio
import tractor

from piker.brokers import get_brokermod
from piker.accounting import (
    Account,
    MktPair,
    Position,
    Transaction,
    TransactionLedger,
    open_account,
    open_trade_ledger,
    unpack_fqme,
)
from piker.data import (
    Feed,
    SymbologyCache,
    iterticks,
    open_feed,
    open_symcache,
)
from piker.types import Struct
from ._util import (
    log,  # sub-sys logger
    get_console_log,
)
from ._messages import (
    BrokerdCancel,
    BrokerdOrder,
    BrokerdOrderAck,
    BrokerdStatus,
    BrokerdFill,
    BrokerdPosition,
    BrokerdError,
)


class PaperBoi(Struct):
    '''
    Emulates a broker order client providing approximately the same API
    and delivering an order-event response stream but with methods for
    triggering desired events based on forward testing engine
    requirements (eg open, closed, fill msgs).

    '''
    broker: str
    ems_trades_stream: tractor.MsgStream
    acnt: Account
    ledger: TransactionLedger
    fees: Callable

    # map of paper "live" orders which be used
    # to simulate fills based on paper engine settings
    _buys: defaultdict[str, bidict]
    _sells: defaultdict[str, bidict]
    _reqids: bidict
    _mkts: dict[str, MktPair] = {}

    # init edge case L1 spread
    last_ask: tuple[float, float] = (float('inf'), 0)  # price, size
    last_bid: tuple[float, float] = (0, 0)

    async def submit_limit(
        self,
        oid: str,  # XXX: see return value
        symbol: str,
        price: float,
        action: str,
        size: float,
        reqid: str | None,

    ) -> int:
        '''
        Place an order and return integer request id provided by client.

        '''
        if action == 'alert':
            # bypass all fill simulation
            return reqid

        entry = self._reqids.get(reqid)
        if entry:
            # order is already existing, this is a modify
            (oid, symbol, action, old_price) = entry
        else:
            # register order internally
            self._reqids[reqid] = (oid, symbol, action, price)

        # TODO: net latency model
        # we checkpoint here quickly particulalry
        # for dark orders since we want the dark_executed
        # to trigger first thus creating a lookup entry
        # in the broker trades event processing loop
        await trio.sleep(0.01)

        if (
            action == 'sell'
            and size > 0
        ):
            size = -size

        msg = BrokerdStatus(
            status='open',
            # account=f'paper_{self.broker}',
            account='paper',
            reqid=reqid,
            time_ns=time.time_ns(),
            filled=0.0,
            reason='paper_trigger',
            remaining=size,

            broker_details={'name': 'paperboi'},
        )
        await self.ems_trades_stream.send(msg)

        # if we're already a clearing price simulate an immediate fill
        if (
            action == 'buy' and (clear_price := self.last_ask[0]) <= price
            ) or (
            action == 'sell' and (clear_price := self.last_bid[0]) >= price
        ):
            await self.fake_fill(
                symbol,
                clear_price,
                size,
                action,
                reqid,
                oid,
            )

        # register this submissions as a paper live order
        else:
            # set the simulated order in the respective table for lookup
            # and trigger by the simulated clearing task normally
            # running ``simulate_fills()``.
            if action == 'buy':
                orders = self._buys

            elif action == 'sell':
                orders = self._sells

            # {symbol -> bidict[oid, (<price data>)]}
            orders[symbol][oid] = (price, size, reqid, action)

        return reqid

    async def submit_cancel(
        self,
        reqid: str,
    ) -> None:

        # TODO: fake market simulation effects
        oid, symbol, action, price = self._reqids[reqid]

        if action == 'buy':
            self._buys[symbol].pop(oid, None)
        elif action == 'sell':
            self._sells[symbol].pop(oid, None)

        # TODO: net latency model
        await trio.sleep(0.01)

        msg = BrokerdStatus(
            status='canceled',
            account='paper',
            reqid=reqid,
            time_ns=time.time_ns(),
            broker_details={'name': 'paperboi'},
        )
        await self.ems_trades_stream.send(msg)

    async def fake_fill(
        self,

        fqme: str,
        price: float,
        size: float,
        action: str,  # one of {'buy', 'sell'}

        reqid: str,
        oid: str,

        # determine whether to send a filled status that has zero
        # remaining lots to fill
        order_complete: bool = True,
        remaining: float = 0,

    ) -> None:
        '''
        Pretend to fill a broker order @ price and size.

        '''
        # TODO: net latency model
        await trio.sleep(0.01)
        fill_time_ns = time.time_ns()
        fill_time_s = time.time()

        fill_msg = BrokerdFill(
            reqid=reqid,
            time_ns=fill_time_ns,
            action=action,
            size=size,
            price=price,
            broker_time=datetime.now().timestamp(),
            broker_details={
                'paper_info': {
                    'oid': oid,
                },
                # mocking ib
                'name': self.broker + '_paper',
            },
        )
        log.info(f'Fake filling order:\n{fill_msg}')
        await self.ems_trades_stream.send(fill_msg)

        if order_complete:
            msg = BrokerdStatus(
                reqid=reqid,
                time_ns=time.time_ns(),
                # account=f'paper_{self.broker}',
                account='paper',
                status='closed',
                filled=size,
                remaining=0 if order_complete else remaining,
            )
            await self.ems_trades_stream.send(msg)

        # NOTE: for paper we set the "bs_mktid" as just the fqme since
        # we don't actually have any unique backend symbol ourselves
        # other then this thing, our fqme address.
        bs_mktid: str = fqme
        if fees := self.fees:
            cost: float = fees(price, size)
        else:
            cost: float = 0

        t = Transaction(
            fqme=fqme,
            tid=oid,
            size=size,
            price=price,
            cost=cost,
            dt=pendulum.from_timestamp(fill_time_s),
            bs_mktid=bs_mktid,
        )

        # update in-mem ledger and pos table
        self.ledger.update_from_t(t)
        self.acnt.update_from_ledger(
            {oid: t},
            symcache=self.ledger._symcache,

            # XXX when a backend has no symcache support yet we can
            # simply pass in the gmi() retreived table created
            # during init :o
            _mktmap_table=self._mkts,
        )

        # transmit pp msg to ems
        pp: Position = self.acnt.pps[bs_mktid]

        pp_msg = BrokerdPosition(
            broker=self.broker,
            account='paper',
            symbol=fqme,

            size=pp.cumsize,
            avg_price=pp.ppu,

            # TODO: we need to look up the asset currency from
            # broker info. i guess for crypto this can be
            # inferred from the pair?
            # currency=bs_mktid,
        )
        # write all updates to filesys immediately
        # (adds latency but that works for simulation anyway)
        self.ledger.write_config()
        self.acnt.write_config()

        await self.ems_trades_stream.send(pp_msg)


async def simulate_fills(
    quote_stream: tractor.MsgStream,  # noqa
    client: PaperBoi,

) -> None:

    # TODO: more machinery to better simulate real-world market things:

    # - slippage models, check what quantopian has:
    # https://github.com/quantopian/zipline/blob/master/zipline/finance/slippage.py
    #   * this should help with simulating partial fills in a fast moving mkt
    #     afaiu

    # - commisions models, also quantopian has em:
    # https://github.com/quantopian/zipline/blob/master/zipline/finance/commission.py

    # - network latency models ??

    # - position tracking:
    # https://github.com/quantopian/zipline/blob/master/zipline/finance/ledger.py

    # this stream may eventually contain multiple symbols
    async for quotes in quote_stream:
        for sym, quote in quotes.items():
            # print(sym)
            for tick in iterticks(
                quote,
                # dark order price filter(s)
                types=('ask', 'bid', 'trade', 'last')
            ):
                tick_price = tick['price']

                buys: bidict[str, tuple] = client._buys[sym]
                iter_buys = reversed(sorted(
                    buys.values(),
                    key=itemgetter(0),
                ))

                def buy_on_ask(our_price):
                    return tick_price <= our_price

                sells: bidict[str, tuple] = client._sells[sym]
                iter_sells = sorted(
                    sells.values(),
                    key=itemgetter(0)
                )

                def sell_on_bid(our_price):
                    return tick_price >= our_price

                match tick:

                    # on an ask queue tick, only clear buy entries
                    case {
                        'price': tick_price,
                        'type': 'ask',
                    }:
                        client.last_ask = (
                            tick_price,
                            tick.get('size', client.last_ask[1]),
                        )

                        iter_entries = zip(
                            iter_buys,
                            itertools.repeat(buy_on_ask)
                        )

                    # on a bid queue tick, only clear sell entries
                    case {
                        'price': tick_price,
                        'type': 'bid',
                    }:
                        client.last_bid = (
                            tick_price,
                            tick.get('size', client.last_bid[1]),
                        )

                        iter_entries = zip(
                            iter_sells,
                            itertools.repeat(sell_on_bid)
                        )

                    # TODO: fix this block, though it definitely
                    # costs a lot more CPU-wise
                    # - doesn't seem like clears are happening still on
                    #   "resting" limit orders?
                    case {
                        'price': tick_price,
                        'type': ('trade' | 'last'),
                    }:
                        # in the clearing price / last price case we
                        # want to iterate both sides of our book for
                        # clears since we don't know which direction the
                        # price is going to move (especially with HFT)
                        # and thus we simply interleave both sides (buys
                        # and sells) until one side clears and then
                        # break until the next tick?
                        def interleave():
                            for pair in zip(
                                iter_buys,
                                iter_sells,
                            ):
                                for order_info, pred in zip(
                                    pair,
                                    itertools.cycle([buy_on_ask, sell_on_bid]),
                                ):
                                    yield order_info, pred

                        iter_entries = interleave()

                    # NOTE: all other (non-clearable) tick event types
                    # - we don't want to sping the simulated clear loop
                    # below unecessarily and further don't want to pop
                    # simulated live orders prematurely.
                    case _:
                        continue

                # iterate all potentially clearable book prices
                # in FIFO order per side.
                for order_info, pred in iter_entries:
                    (our_price, size, reqid, action) = order_info

                    # print(order_info)
                    clearable = pred(our_price)
                    if clearable:
                        # pop and retreive order info
                        oid = {
                            'buy': buys,
                            'sell': sells
                        }[action].inverse.pop(order_info)

                        # clearing price would have filled entirely
                        await client.fake_fill(
                            fqme=sym,
                            # todo slippage to determine fill price
                            price=tick_price,
                            size=size,
                            action=action,
                            reqid=reqid,
                            oid=oid,
                        )


async def handle_order_requests(

    client: PaperBoi,
    ems_order_stream: tractor.MsgStream,

) -> None:

    request_msg: dict
    async for request_msg in ems_order_stream:
        match request_msg:
            case {'action': ('buy' | 'sell')}:
                order = BrokerdOrder(**request_msg)
                account = order.account

                # error on bad inputs
                reason = None
                if account != 'paper':
                    reason = f'No account found:`{account}` (paper only)?'

                elif order.size == 0:
                    reason = 'Invalid size: 0'

                if reason:
                    log.error(reason)
                    await ems_order_stream.send(BrokerdError(
                        oid=order.oid,
                        symbol=order.symbol,
                        reason=reason,
                    ))
                    continue

                reqid = order.reqid or str(uuid.uuid4())

                # deliver ack that order has been submitted to broker routing
                await ems_order_stream.send(
                    BrokerdOrderAck(
                        oid=order.oid,
                        reqid=reqid,
                        account='paper'
                    )
                )

                # call our client api to submit the order
                reqid = await client.submit_limit(
                    oid=order.oid,
                    symbol=f'{order.symbol}.{client.broker}',
                    price=order.price,
                    action=order.action,
                    size=order.size,
                    # XXX: by default 0 tells ``ib_insync`` methods that
                    # there is no existing order so ask the client to create
                    # a new one (which it seems to do by allocating an int
                    # counter - collision prone..)
                    reqid=reqid,
                )
                log.info(f'Submitted paper LIMIT {reqid}:\n{order}')

            case {'action': 'cancel'}:
                msg = BrokerdCancel(**request_msg)
                await client.submit_cancel(
                    reqid=msg.reqid
                )

            case _:
                log.error(f'Unknown order command: {request_msg}')


_reqids: bidict[str, tuple] = {}
_buys: defaultdict[
    str,  # symbol
    bidict[
        str,  # oid
        tuple[float, float, str, str],  # order info
    ]
] = defaultdict(bidict)
_sells: defaultdict[
    str,  # symbol
    bidict[
        str,  # oid
        tuple[float, float, str, str],  # order info
    ]
] = defaultdict(bidict)


@tractor.context
async def open_trade_dialog(

    ctx: tractor.Context,
    broker: str,
    fqme: str | None = None,  # if empty, we only boot broker mode
    loglevel: str = 'warning',

) -> None:

    # enable piker.clearing console log for *this* subactor
    get_console_log(loglevel)

    symcache: SymbologyCache
    async with open_symcache(get_brokermod(broker)) as symcache:

        acnt: Account
        ledger: TransactionLedger
        with (

            # TODO: probably do the symcache and ledger loading
            # implicitly behind this? Deliver an account, and ledger
            # pair or make the ledger an attr of the account?
            open_account(
                broker,
                'paper',
                write_on_exit=True,
            ) as acnt,

            open_trade_ledger(
                broker,
                'paper',
                symcache=symcache,
            ) as ledger
        ):
            # NOTE: WE MUST retreive market(pair) info from each
            # backend broker since ledger entries (in their
            # provider-native format) often don't contain necessary
            # market info per trade record entry..
            # FURTHER, if no fqme was passed in, we presume we're
            # running in "ledger-sync-only mode" and thus we load
            # mkt info for each symbol found in the ledger to
            # an acnt table manually.

            # TODO: how to process ledger info from backends?
            # - should we be rolling our own actor-cached version of these
            #   client API refs or using portal IPC to send requests to the
            #   existing brokerd daemon?
            # - alternatively we can possibly expect and use
            #   a `.broker.ledger.norm_trade()` ep?
            brokermod: ModuleType = get_brokermod(broker)
            gmi: Callable = getattr(brokermod, 'get_mkt_info', None)

            # update all transactions with mkt info before
            # loading any pps
            mkt_by_fqme: dict[str, MktPair] = {}
            if (
                fqme
                and fqme not in symcache.mktmaps
            ):
                log.warning(
                    f'Symcache for {broker} has no `{fqme}` entry?\n'
                    'Manually requesting mkt map data via `.get_mkt_info()`..'
                )

                bs_fqme, _, broker = fqme.rpartition('.')
                mkt, pair = await gmi(bs_fqme)
                mkt_by_fqme[mkt.fqme] = mkt

            # for each sym in the ledger load its `MktPair` info
            for tid, txdict in ledger.data.items():
                l_fqme: str = txdict.get('fqme') or txdict['fqsn']

                if (
                    gmi
                    and l_fqme not in symcache.mktmaps
                    and l_fqme not in mkt_by_fqme
                ):
                    log.warning(
                        f'Symcache for {broker} has no `{l_fqme}` entry?\n'
                        'Manually requesting mkt map data via `.get_mkt_info()`..'
                    )
                    mkt, pair = await gmi(
                        l_fqme.rstrip(f'.{broker}'),
                    )
                    mkt_by_fqme[l_fqme] = mkt

                # if an ``fqme: str`` input was provided we only
                # need a ``MktPair`` for that one market, since we're
                # running in real simulated-clearing mode, not just ledger
                # syncing.
                if (
                    fqme is not None
                    and fqme in mkt_by_fqme
                ):
                    break

            # update pos table from ledger history and provide a ``MktPair``
            # lookup for internal position accounting calcs.
            acnt.update_from_ledger(
                ledger,

                # NOTE: if the symcache fails on fqme lookup
                # (either sycache not yet supported or not filled
                # in) use manually constructed table from calling
                # the `.get_mkt_info()` provider EP above.
                _mktmap_table=mkt_by_fqme,
            )

            pp_msgs: list[BrokerdPosition] = []
            pos: Position
            token: str  # f'{symbol}.{self.broker}'
            for token, pos in acnt.pps.items():

                pp_msgs.append(BrokerdPosition(
                    broker=broker,
                    account='paper',
                    symbol=pos.mkt.fqme,
                    size=pos.cumsize,
                    avg_price=pos.ppu,
                ))

            await ctx.started((
                pp_msgs,
                ['paper'],
            ))

            # write new positions state in case ledger was
            # newer then that tracked in pps.toml
            acnt.write_config()

            # exit early since no fqme was passed,
            # normally this case is just to load
            # positions "offline".
            if fqme is None:
                log.warning(
                    'Paper engine only running in position delivery mode!\n'
                    'NO SIMULATED CLEARING LOOP IS ACTIVE!'
                )
                await trio.sleep_forever()
                return

            feed: Feed
            async with (
                open_feed(
                    [fqme],
                    loglevel=loglevel,
                ) as feed,
            ):
                # sanity check all the mkt infos
                for fqme, flume in feed.flumes.items():
                    mkt: MktPair = symcache.mktmaps.get(fqme) or mkt_by_fqme[fqme]
                    assert mkt == flume.mkt

                get_cost: Callable = getattr(
                    brokermod,
                    'get_cost',
                    None,
                )

                async with (
                    ctx.open_stream() as ems_stream,
                    trio.open_nursery() as n,
                ):
                    client = PaperBoi(
                        broker=broker,
                        ems_trades_stream=ems_stream,
                        acnt=acnt,
                        ledger=ledger,
                        fees=get_cost,

                        _buys=_buys,
                        _sells=_sells,
                        _reqids=_reqids,

                        _mkts=mkt_by_fqme,

                    )

                    n.start_soon(
                        handle_order_requests,
                        client,
                        ems_stream,
                    )

                    # paper engine simulator clearing task
                    await simulate_fills(feed.streams[broker], client)


@acm
async def open_paperboi(
    fqme: str | None = None,
    broker: str | None = None,
    loglevel: str | None = None,

) -> Callable:
    '''
    Spawn a paper engine actor and yield through access to
    its context.

    '''
    if not fqme:
        assert broker, 'One of `broker` or `fqme` is required siss..!'
    else:
        broker, _, _, _ = unpack_fqme(fqme)

    we_spawned: bool = False
    service_name = f'paperboi.{broker}'

    async with (
        tractor.find_actor(service_name) as portal,
        tractor.open_nursery() as an,
    ):
        # NOTE: only spawn if no paperboi already is up since we likely
        # don't need more then one actor for simulated order clearing
        # per broker-backend.
        if portal is None:
            log.info('Starting new paper-engine actor')
            portal = await an.start_actor(
                service_name,
                enable_modules=[__name__]
            )
            we_spawned = True

        async with portal.open_context(
            open_trade_dialog,
            broker=broker,
            fqme=fqme,
            loglevel=loglevel,

        ) as (ctx, first):
            yield ctx, first

            # tear down connection and any spawned actor on exit
            await ctx.cancel()
            if we_spawned:
                await portal.cancel_actor()


def norm_trade(
    tid: str,
    txdict: dict,
    pairs: dict[str, Struct],
    symcache: SymbologyCache | None = None,

    brokermod: ModuleType | None = None,

) -> Transaction:
    from pendulum import (
        DateTime,
        parse,
    )

    # special field handling for datetimes
    # to ensure pendulum is used!
    dt: DateTime = parse(txdict['dt'])
    expiry: str | None = txdict.get('expiry')
    fqme: str = txdict.get('fqme') or txdict.pop('fqsn')

    price: float = txdict['price']
    size: float =  txdict['size']
    cost: float = txdict.get('cost', 0)
    if (
        brokermod
        and (get_cost := getattr(
            brokermod,
            'get_cost',
            False,
        ))
    ):
        cost = get_cost(
            price,
            size,
            is_taker=True,
        )

    return Transaction(
        fqme=fqme,
        tid=txdict['tid'],
        dt=dt,
        price=price,
        size=size,
        cost=cost,
        bs_mktid=txdict['bs_mktid'],
        expiry=parse(expiry) if expiry else None,
        etype='clear',
    )
