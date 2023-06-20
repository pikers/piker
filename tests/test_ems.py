'''
Execution mgmt system (EMS) e2e testing.

Most tests leverage our paper clearing engine found (currently) in
``piker.clearing._paper_engine`.

Ideally in the longer run we are able to support forms of (non-clearing)
live order tests against certain backends that make it possible to do
so..

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
from exceptiongroup import BaseExceptionGroup

import pytest
import tractor
from uuid import uuid4

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
from piker.accounting import (
    unpack_fqme,
)
from piker.accounting import (
    open_pps,
    Position,
)

log = get_logger(__name__)



async def order_and_and_wait_for_ppmsg(
    client: OrderClient,
    trades_stream: tractor.MsgStream,
    fqme: str,

    action: Literal['buy', 'sell'],
    price: float = 100e3,  # just a super high price.
    size: float = 0.01,

    exec_mode: str = 'live',
    account: str = 'paper',

) -> list[Status | BrokerdPosition]:
    '''
    Start piker, place a trade and assert data in
    pps stream, ledger and position table.

    '''
    sent: list[Order] = []
    broker, mktep, venue, suffix = unpack_fqme(fqme)

    order = Order(
        exec_mode=exec_mode,
        action=action,  # TODO: remove this from our schema?
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


def run_and_tollerate_cancels(
    fn: Callable[..., Awaitable],

    expect_errs: tuple[Exception] | None = None,
    tollerate_errs: tuple[Exception] = (tractor.ContextCancelled,),

):
    '''
    Run ``trio``-``piker`` runtime with potential tolerance for
    inter-actor cancellation during teardown (normally just
    `tractor.ContextCancelled`s).

    '''
    if expect_errs:
        with pytest.raises(BaseExceptionGroup) as exc_info:
            trio.run(fn)

        for err in exc_info.value.exceptions:
            assert type(err) in expect_errs
    else:
        try:
            trio.run(fn)
        except tollerate_errs:
            pass


@cm
def load_and_check_pos(
    order: Order,
    ppmsg: BrokerdPosition,

) -> None:

    with open_pps(ppmsg.broker, ppmsg.account) as table:

        if ppmsg.size == 0:
            assert ppmsg.symbol not in table.pps
            yield None
            return

        else:
            # NOTE: a special case is here since the `PpTable.pps` are
            # normally indexed by the particular broker's
            # `Position.bs_mktid: str` (a unique market / symbol id provided
            # by their systems/design) but for the paper engine case, this
            # is the same the fqme.
            pp: Position = table.pps[ppmsg.symbol]

            assert ppmsg.size == pp.size
            assert ppmsg.avg_price == pp.ppu

            yield pp


def test_ems_err_on_bad_broker(
    open_test_pikerd: Services,
    loglevel: str,
):
    async def load_bad_fqme():
        try:
            async with (
                open_test_pikerd(
                    debug_mode=False,
                ) as (_, _, _, _),

                open_ems(
                    'doggycoin.doggy',
                    mode='paper',
                    loglevel=loglevel,
                ) as _
            ):
                pytest.fail('EMS is working on non-broker!?')

        # NOTE: emsd should error on the actor's enabled modules
        # import phase, when looking for a backend named `doggy`.
        except tractor.RemoteActorError as re:
            assert re.type == ModuleNotFoundError

    run_and_tollerate_cancels(load_bad_fqme)


async def match_ppmsgs_on_ems_boot(
    ppmsgs: list[BrokerdPosition],

) -> None:
    '''
    Given a list of input position msgs, verify they match
    what is loaded from the EMS on connect.

    '''
    by_acct: dict[tuple, list[BrokerdPosition]] = {}
    for msg in ppmsgs:
        by_acct.setdefault(
            (msg.broker, msg.account),
            [],
        ).append(msg)

    # TODO: actually support multi-mkts to `open_ems()`
    # but for now just pass the first fqme.
    fqme = msg.symbol

    # disconnect from EMS, reconnect and ensure we get our same
    # position relayed to us again in the startup msg.
    async with (
        open_ems(
            fqme,
            mode='paper',
            loglevel='info',
        ) as (
            _,  # OrderClient
            _,  # tractor.MsgStream
            startup_pps,
            accounts,
            _,  # dialogs,
        )
    ):
        for (broker, account), ppmsgs in by_acct.items():
            assert account in accounts

            # lookup all msgs rx-ed for this account
            rx_msgs = startup_pps[(broker, account)]

            for expect_ppmsg in ppmsgs:
                rx_msg = BrokerdPosition(**rx_msgs[expect_ppmsg.symbol])
                assert rx_msg == expect_ppmsg


async def submit_and_check(
    fills: tuple[dict],
    loglevel: str,

) -> tuple[
    BrokerdPosition,
    Position,
]:
    '''
    Enter a trade and assert entries are made in pps and ledger files.

    Shutdown the ems-client and ensure on reconnect we get the expected
    matching ``BrokerdPosition`` and pps.toml entries.

    '''
    broker: str = 'kraken'
    venue: str = 'spot'
    mkt_key: str = 'xbtusdt'
    fqme: str = f'{mkt_key}.{venue}.{broker}'

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
            _,  # dialogs
        )
    ):
        # no positions on startup
        assert not startup_pps
        assert 'paper' in accounts

        od: dict
        for od in fills:
            print(f'Sending order {od} for fill')
            size = od['size']
            sent, msgs = await order_and_and_wait_for_ppmsg(
                client,
                trades_stream,
                fqme,
                action='buy' if size > 0 else 'sell',
                price=100e3 if size > 0 else 0,
                size=size,
            )

        last_order: Order = sent[-1]
        last_resp = msgs[-1]
        assert isinstance(last_resp, BrokerdPosition)
        ppmsg = last_resp

        # check that pps.toml for account has been updated
        # and all ems position msgs match that state.
        with load_and_check_pos(
            last_order,
            ppmsg,
        ) as pos:
            pass

    return ppmsg, pos


@pytest.mark.parametrize(
    'fills',
    [
        # buy and leave
        ({'size': 0.001},),

        # sell short, then buy back to net-zero in dst
        (
            {'size': -0.001},
            {'size': 0.001},
        ),

        # multi-partial entry and exits from net-zero, to short and back
        # to net-zero.
        (
            # enters
            {'size': 0.001},
            {'size': 0.002},

            # partial exit
            {'size': -0.001},

            # partial enter
            {'size': 0.0015},
            {'size': 0.001},
            {'size': 0.002},

            # nearly back to zero.
            {'size': -0.001},

            # switch to net-short
            {'size': -0.025},
            {'size': -0.0195},

            # another entry
            {'size': 0.001},

            # final cover to net-zero again.
            {'size': 0.038},
        ),
    ],
    ids='fills={}'.format,
)
def test_multi_fill_positions(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,

    fills: tuple[dict],

    check_cross_session: bool = False,

) -> None:

    ppmsg: BrokerdPosition
    pos: Position

    accum_size: float = 0
    for fill in fills:
        accum_size += fill['size']

    async def atest():

        # export to outer scope for audit on second runtime-boot.
        nonlocal ppmsg, pos

        async with (
            open_test_pikerd() as (_, _, _, _),
        ):
            ppmsg, pos = await submit_and_check(
                fills=fills,
                loglevel=loglevel,
            )
            assert ppmsg.size == accum_size

    run_and_tollerate_cancels(atest)

    if (
        check_cross_session
        or accum_size != 0
    ):
        # rerun just to check that position info is persistent for the paper
        # account (i.e. a user can expect to see paper pps persist across
        # runtime sessions.
        async def just_check_pp():
            nonlocal ppmsg

            async with (
                open_test_pikerd() as (_, _, _, _),
            ):
                await match_ppmsgs_on_ems_boot([ppmsg])

        run_and_tollerate_cancels(just_check_pp)


# TODO: still need to implement offline storage of darks/alerts/paper
# lives probably all the same way.. see
# https://github.com/pikers/piker/issues/463
def test_open_orders_reloaded(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,

    # fills: tuple[dict],

    check_cross_session: bool = False,
):
    ...


def test_dark_order_clearing():
    ...
