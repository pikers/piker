from pprint import pformat
from functools import partial
from decimal import Decimal
from typing import Callable

import tractor
import trio
from uuid import uuid4

from piker.accounting import dec_digits
from piker.clearing import (
    open_ems,
    OrderClient,
)
# TODO: we should probably expose these top level in this subsys?
from piker.clearing._messages import (
    Order,
    Status,
    BrokerdPosition,
)
from piker.data import (
    Flume,
    open_feed,
    Feed,
    # ShmArray,
)


# TODO: handle other statuses:
# - fills, errors, and 
async def wait_for_order_status(
    trades_stream: tractor.MsgStream,
    oid: str,
    expect_status: str,

) -> tuple[
    list[Status],
    list[BrokerdPosition],
]:
    '''
    Wait for a specific order status for a given dialog, return msg flow
    up to that msg and any position update msgs in a tuple.

    '''
    # Wait for position message before moving on to verify flow(s)
    # for the multi-order position entry/exit.
    status_msgs: list[Status] = []
    pp_msgs: list[BrokerdPosition] = []

    async for msg in trades_stream:
        match msg:
            case {'name': 'position'}:
                ppmsg = BrokerdPosition(**msg)
                pp_msgs.append(ppmsg)

            case {
                'name': 'status',
            }:
                msg = Status(**msg)
                status_msgs.append(msg)

                # if we get the status we expect then return all
                # collected msgs from the brokerd dialog up to the
                # exected msg B)
                if (
                     msg.resp == expect_status
                     and msg.oid == oid
                ):
                    return status_msgs, pp_msgs


async def bot_main():
    '''
    Boot the piker runtime, open an ems connection, submit
    and process orders statuses in real-time.

    '''
    from piker.service import maybe_open_pikerd

    ll: str = 'info'

    # open an order ctl client, live data feed, trio nursery for
    # spawning an order trailer task
    client: OrderClient
    trades_stream: tractor.MsgStream
    feed: Feed
    accounts: list[str]

    fqme: str = 'btcusdt.usdtm.perp.binance'

    async with (

        # TODO: do this implicitly inside `open_ems()` ep below?
        # init and sync actor-service runtime
        maybe_open_pikerd(
            loglevel=ll,
            debug_mode=True,

        ),
        open_ems(
            fqme,
            mode='paper',  # {'live', 'paper'}
            # mode='live',  # for real-brokerd submissions
            loglevel=ll,

        ) as (
            client,  # OrderClient
            trades_stream,  # tractor.MsgStream startup_pps,
            _,  # positions
            accounts,
            _,  # dialogs
        ),

        open_feed(
            fqmes=[fqme],
            loglevel=ll,

            # TODO: if you want to throttle via downsampling
            # how many tick updates your feed received on
            # quote streams B)
            # tick_throttle=10,
        ) as feed,

        trio.open_nursery() as tn,
    ):
        print(f'Loaded binance accounts: {accounts}')

        flume: Flume = feed.flumes[fqme]
        min_tick = Decimal(flume.mkt.price_tick)
        min_tick_digits: int = dec_digits(min_tick)
        price_round: Callable = partial(
            round,
            ndigits=min_tick_digits,
        )

        quote_stream: trio.abc.ReceiveChannel = feed.streams['binance']


        clear_margin: float = 0.9995

        async def trailer(
            order: Order,
        ):
            # ref shm OHLCV array history, if you want
            # s_shm: ShmArray = flume.rt_shm
            # m_shm: ShmArray = flume.hist_shm

            # NOTE: if you wanted to frame ticks by type like the
            # the quote throttler does.
            # from piker.data._sampling import frame_ticks

            async for quotes in quote_stream:
                for fqme, quote in quotes.items():
                    # print(quote['symbol'])
                    for tick in reversed(
                        quote.get('ticks', ())
                    ):
                        # print(
                        #     f'{fqme} ticks:\n{pformat(tick)}\n\n'
                        #     # f'last 1s OHLC:\n{s_shm.array[-1]}\n'
                        #     # f'last 1m OHLC:\n{m_shm.array[-1]}\n'
                        # )

                        # always keep live limit 2% below last
                        # clearing price
                        if tick['type'] == 'trade':
                            await client.update(
                                uuid=order.oid,
                                price=price_round(
                                    clear_margin
                                    *
                                    tick['price']
                                ),
                            )
                            msgs, pps = await wait_for_order_status(
                                trades_stream,
                                oid,
                                'open'
                            )
                            # if multiple clears per quote just
                            # skip to the next quote?
                            break


        # setup order dialog via first msg
        size: float = 0.01
        price: float = price_round(
                clear_margin
                *
                flume.first_quote['last']
        )
        oid: str = str(uuid4())
        order = Order(
            exec_mode='live',  # {'dark', 'live', 'alert'}
            action='buy',  # TODO: remove this from our schema?
            oid=oid,
            account='paper',  # use binance.usdtm for binance futes
            # account='binance.usdtm',  # for live binance futes
            size=size,
            symbol=fqme,
            price=price,
            brokers=['binance'],
        )

        await client.send(order)

        msgs, pps = await wait_for_order_status(
            trades_stream,
            oid,
            'open'
        )

        assert not pps
        assert msgs[-1].oid == oid

        # start "trailer task" which tracks rt quote stream
        tn.start_soon(trailer, order)

        try:
            # wait for ctl-c from user..
            await trio.sleep_forever()
        except KeyboardInterrupt:
            # cancel the open order
            await client.cancel(oid)
            msgs, pps = await wait_for_order_status(
                trades_stream,
                oid,
                'canceled'
            )
            raise


if __name__ == '__main__':
    trio.run(bot_main)
