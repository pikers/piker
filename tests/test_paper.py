'''
Paper-mode testing
'''
from contextlib import (
    contextmanager as cm,
)
from typing import (
    Awaitable,
    Callable,
    AsyncContextManager,
    Literal,
)

import trio
# import pytest_trio
from exceptiongroup import BaseExceptionGroup

import pytest
import tractor
from uuid import uuid4
from functools import partial

from piker.service import Services
from piker.log import get_logger
from piker.clearing._messages import (
    Order,
    Status,
    # Cancel,
    BrokerdPosition,
)
from piker.clearing import (
    open_ems,
    OrderClient,
)
from piker.accounting._mktinfo import (
    unpack_fqme,
)
from piker.accounting import (
    open_pps,
    Position,
)

log = get_logger(__name__)


async def open_pikerd(
    open_test_pikerd: AsyncContextManager,

) -> Services:
    async with (
        open_test_pikerd() as (_, _, _, services),
    ):
        yield services


async def submit_order(
    client: OrderClient,
    trades_stream: tractor.MsgStream,
    fqme: str,
    action: Literal['buy', 'sell'],
    price: float = 30000.,
    executions: int = 1,
    size: float = 0.01,
    exec_mode: str = 'live',
    account: str = 'paper',

) -> list[Status | BrokerdPosition]:
    '''
    Start piker, place a trade and assert data in
    pps stream, ledger and position table.

    '''
    sent: list[Order] = []
    broker, key, suffix = unpack_fqme(fqme)

    for _ in range(executions):

        order = Order(
            exec_mode=exec_mode,
            action=action,
            oid=str(uuid4()),
            account=account,
            size=size,
            symbol=fqme,
            price=price,
            brokers=[broker],
        )
        sent.append(order)
        await client.send(order)

        # TODO: i guess we should still test the old sync-API?
        # client.send_nowait(order)

        # Wait for position message before moving on to verify flow(s)
        # for the multi-order position entry/exit.
        msgs: list[Status | BrokerdPosition] = []
        async for msg in trades_stream:
            match msg:
                case {'name': 'position'}:
                    ppmsg = BrokerdPosition(**msg)
                    msgs.append(ppmsg)
                    break

                case {'name': 'status'}:
                    msgs.append(Status(**msg))

    return sent, msgs


def run_and_catch(
    fn: Callable[..., Awaitable],

    expect_errs: tuple[Exception] = (
        KeyboardInterrupt,
        tractor.ContextCancelled,
    )

):
    '''
    Close position and assert empty position in pps

    '''
    if expect_errs:
        with pytest.raises(BaseExceptionGroup) as exc_info:
            trio.run(fn)

        for err in exc_info.value.exceptions:
            assert type(err) in expect_errs
    else:
        trio.run(fn)


@cm
def load_and_check_pos(
    order: Order,
    ppmsg: BrokerdPosition,

) -> None:

    with open_pps(ppmsg.broker, ppmsg.account) as table:

        # NOTE: a special case is here since the `PpTable.pps` are
        # normally indexed by the particular broker's
        # `Position.bs_mktid: str` (a unique market / symbol id provided
        # by their systems/design) but for the paper engine case, this
        # is the same the fqme.
        pp: Position = table.pps[ppmsg.symbol]

        assert ppmsg.size == pp.size
        assert ppmsg.avg_price == pp.ppu

        yield pp


@pytest.mark.trio
async def test_ems_err_on_bad_broker(
    open_pikerd: Services,
    loglevel: str,
):
    try:
        async with open_ems(
            'doggy.smiles',
            mode='paper',
            loglevel=loglevel,
        ) as _:
            pytest.fail('EMS is working on non-broker!?')
    except ModuleNotFoundError:
        pass


async def atest_buy(
    loglevel: str,
):
    '''
    Enter a trade and assert entries are made in pps and ledger files.

    Shutdown the ems-client and ensure on reconnect we get the expected
    matching ``BrokerdPosition`` and pps.toml entries.

    '''
    broker: str = 'kraken'
    mkt_key: str = 'xbtusdt'
    fqme: str = f'{mkt_key}.{broker}'

    startup_pps: dict[
        tuple[str, str],  # brokername, acctid
        list[BrokerdPosition],
    ]
    async with (
        open_ems(
            fqme,
            mode='paper',
            loglevel=loglevel,
        ) as (
            client,  # OrderClient
            trades_stream,  # tractor.MsgStream
            startup_pps,
            accounts,
            dialogs,
        )
    ):
        # no positions on startup
        assert not startup_pps
        assert 'paper' in accounts

        sent, msgs = await submit_order(
            client,
            trades_stream,
            fqme,
            action='buy',
            size=0.01,
        )

        last_order = sent[-1]

        last_resp = msgs[-1]
        assert isinstance(last_resp, BrokerdPosition)

        # check that pps.toml for account has been updated
        with load_and_check_pos(
            last_order,
            last_resp,
        ) as pos:
            return pos

    # disconnect from EMS, then reconnect and ensure we get our same
    # position relayed to us again.

    # _run_test_and_check(
    #     partial(
    #         _async_main,
    #         open_test_pikerd_and_ems=open_test_pikerd_and_ems,
    #         action='buy',
    #         assert_entries=True,
    #     ),
    # )

    # await _async_main(
    #     open_test_pikerd_and_ems=open_test_pikerd_and_ems,
    #     assert_pps=True,
    # )
    # _run_test_and_check(
    #     partial(
    #         _async_main,
    #         open_test_pikerd_and_ems=open_test_pikerd_and_ems,
    #         assert_pps=True,
    #     ),
    # )


def test_open_long(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,

) -> None:

    async def atest():
        async with (
            open_test_pikerd() as (_, _, _, services),
        ):
            assert await atest_buy(loglevel)

            # Teardown piker like a user would from cli
            # raise KeyboardInterrupt

    run_and_catch(
        atest,
        expect_errs=None,
    )
    # Open ems another time and assert existence of prior
    # pps entries confirming they persisted



# def test_sell(
#     open_test_pikerd_and_ems: AsyncContextManager,
# ):
#     '''
#     Sell position and ensure pps are zeroed.

#     '''
#     _run_test_and_check(
#         partial(
#             _async_main,
#             open_test_pikerd_and_ems=open_test_pikerd_and_ems,
#             action='sell',
#             price=1,
#         ),
#     )

#     _run_test_and_check(
#         partial(
#             _async_main,
#             open_test_pikerd_and_ems=open_test_pikerd_and_ems,
#             assert_zeroed_pps=True,
#         ),
#     )


# def test_multi_sell(
#     open_test_pikerd_and_ems: AsyncContextManager,
# ):
#     '''
#     Make 5 market limit buy orders and
#     then sell 5 slots at the same price.
#     Finally, assert cleared positions.

#     '''
#     _run_test_and_check(
#         partial(
#             _async_main,
#             open_test_pikerd_and_ems=open_test_pikerd_and_ems,
#             action='buy',
#             executions=5,
#         ),
#     )

#     _run_test_and_check(
#         partial(
#             _async_main,
#             open_test_pikerd_and_ems=open_test_pikerd_and_ems,
#             action='sell',
#             executions=5,
#             price=1,
#             assert_zeroed_pps=True,
#         ),
#     )
