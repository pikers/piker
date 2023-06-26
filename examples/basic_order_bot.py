import tractor
import trio
from uuid import uuid4

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

    port: int = 6666  # non-default pikerd addr
    ll: str = 'info'
    reg_addr: tuple[str, int] = ('127.0.0.1', port)

    # open an order ctl client
    client: OrderClient
    trades_stream: tractor.MsgStream
    accounts: list[str]

    fqme: str = 'btcusdt.usdtm.perp.binance'

    async with (

        # init and sync actor-service runtime
        maybe_open_pikerd(
            registry_addr=reg_addr,
            loglevel=ll,
            debug_mode=False,

        ),
        tractor.wait_for_actor(
            'pikerd',
            arbiter_sockaddr=reg_addr,
        ),

        open_ems(
            fqme,
            mode='paper',  # {'live', 'paper'}
            loglevel=ll,

        ) as (
            client,  # OrderClient
            trades_stream,  # tractor.MsgStream startup_pps,
            _,  # positions
            accounts,
            _,  # dialogs
        )
    ):
        print(f'Loaded binance accounts: {accounts}')

        price: float = 10e3  # non-clearable
        size: float = 0.01
        oid: str = str(uuid4())

        order = Order(
            exec_mode='live',  # {'dark', 'live', 'alert'}
            action='buy',  # TODO: remove this from our schema?
            oid=oid,
            account='paper',  # use binance.usdtm for binance futes
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

        # cancel the open order
        await client.cancel(oid)
        msgs, pps = await wait_for_order_status(
            trades_stream,
            oid,
            'canceled'
        )



if __name__ == '__main__':
    trio.run(bot_main)
