'''
Paper-mode testing
'''

import trio
from exceptiongroup import BaseExceptionGroup
from typing import (
    AsyncContextManager,
    Literal,
)

import pytest
from tractor._exceptions import ContextCancelled
from uuid import uuid4
from functools import partial

from piker.log import get_logger
from piker.clearing._messages import Order
from piker.pp import (
    open_trade_ledger,
    open_pps,
)

log = get_logger(__name__)


def get_fqsn(broker, symbol):
    fqsn = f'{symbol}.{broker}'
    return (fqsn, symbol, broker)


oid = ''
test_exec_mode = 'live'
(fqsn, symbol, broker) = get_fqsn('kraken', 'xbtusdt')
brokers = [broker]
account = 'paper'


async def _async_main(
    open_test_pikerd_and_ems: AsyncContextManager,
    action: Literal['buy', 'sell'] | None = None,
    price: int = 30000,
    executions: int = 1,
    size: float = 0.01,
    # Assert options
    assert_entries: bool = False,
    assert_pps: bool = False,
    assert_zeroed_pps: bool = False,
    assert_msg: bool = False,
) -> None:
    '''
    Start piker, place a trade and assert data in
    pps stream, ledger and position table.

    '''

    oid: str = ''
    last_msg = {}

    async with open_test_pikerd_and_ems() as (
        services,
        (book, trades_stream, pps, accounts, dialogs),
    ):
        if action:
            for x in range(executions):
                oid = str(uuid4())
                order = Order(
                    exec_mode=test_exec_mode,
                    action=action,
                    oid=oid,
                    account=account,
                    size=size,
                    symbol=fqsn,
                    price=price,
                    brokers=brokers,
                )
                # This is actually a syncronous call to push a message
                book.send(order)

                async for msg in trades_stream:
                    last_msg = msg
                    match msg:
                        # Wait for position message before moving on
                        case {'name': 'position'}:
                            break

        if (
            assert_entries 
            or assert_pps 
            or assert_zeroed_pps 
            or assert_msg
        ):
            _assert(
                assert_entries,
                assert_pps,
                assert_zeroed_pps,
                pps,
                last_msg,
                size,
                executions,
            )

        # Teardown piker like a user would
        raise KeyboardInterrupt


def _assert(
    assert_entries,
    assert_pps,
    assert_zerod_pps,
    pps,
    last_msg,
    size,
    executions,
):
    with (
        open_pps(broker, account) as table,
    ):
        '''
        Assert multiple cases including pps,
        ledger and final position message state

        '''
        if assert_entries:
            for key, val in [
                ('broker', broker),
                ('account', account),
                ('symbol', fqsn),
                ('size', size * executions),
                ('currency', symbol),
                ('avg_price', table.pps[symbol].ppu)
            ]:
                assert last_msg[key] == val

        if assert_pps:
            last_ppu = pps[(broker, account)][-1]
            assert last_ppu['avg_price'] == table.pps[symbol].ppu

        if assert_zerod_pps:
            assert not bool(table.pps)


def _run_test_and_check(fn):
    '''        
    Close position and assert empty position in pps

    '''
    with pytest.raises(BaseExceptionGroup) as exc_info:
        trio.run(fn)

    for exception in exc_info.value.exceptions:
        assert isinstance(exception, KeyboardInterrupt) or isinstance(
            exception, ContextCancelled
        )


def test_buy(
    open_test_pikerd_and_ems: AsyncContextManager, 
    delete_testing_dir
):
    '''
    Enter a trade and assert entries are made in pps and ledger files.

    '''
    _run_test_and_check(
        partial(
            _async_main,
            open_test_pikerd_and_ems=open_test_pikerd_and_ems,
            action='buy',
            assert_entries=True,
        ),
    )

    # Open ems and assert existence of pps entries
    _run_test_and_check(
        partial(
            _async_main,
            open_test_pikerd_and_ems=open_test_pikerd_and_ems,
            assert_pps=True,
        ),
    )


def test_sell(
    open_test_pikerd_and_ems: AsyncContextManager, 
    delete_testing_dir
):
    '''
    Sell position ensure pps are zeroed.

    '''
    _run_test_and_check(
        partial(
            _async_main,
            open_test_pikerd_and_ems=open_test_pikerd_and_ems,
            action='sell',
            price=1,
        ),
    )

    _run_test_and_check(
        partial(
            _async_main,
            open_test_pikerd_and_ems=open_test_pikerd_and_ems,
            assert_zeroed_pps=True,
        ),
    )


@pytest.mark.skip(reason='Due to precision issues, this test will currently fail')
def test_multi_sell(
    open_test_pikerd_and_ems: AsyncContextManager, 
    delete_testing_dir
):
    '''
    Make 5 market limit buy orders and 
    then sell 5 slots at the same price. 
    Finally, assert cleared positions.

    '''
    _run_test_and_check(
        partial(
            _async_main,
            open_test_pikerd_and_ems=open_test_pikerd_and_ems,
            action='buy',
            executions=5,
        ),
    )

    _run_test_and_check(
        partial(
            _async_main,
            open_test_pikerd_and_ems=open_test_pikerd_and_ems,
            action='sell',
            executions=5,
            price=1,
            assert_zeroed_pps=True,
        ),
    )
