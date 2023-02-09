from datetime import datetime
import time
import trio
import pytest
import tractor
import math
from piker.log import get_logger
from piker.clearing._messages import ( 
    Order                                    
)
from typing import (
    AsyncContextManager,
    Any,
)

from functools import partial
from piker.pp import (
    Position,
    Transaction,
    open_trade_ledger,
    open_pps
)

from piker._daemon import (
    find_service,
    check_for_service,
    Services,
)
from piker.data import (
    open_feed,
)
from piker.clearing import (
    open_ems,
)
from piker.clearing._messages import (
    BrokerdPosition,
    Status,
)
from piker.clearing._client import (
    OrderBook,
)

log = get_logger(__name__)

_clearing_price: float


def test_paper_trade(
    open_test_pikerd: AsyncContextManager
):
    _cleared_price: float
    test_exec_mode='live'
    test_action = 'buy'
    test_oid = '560beac8-b1b1-4dee-bd1e-6604a704c9ea'
    test_account = 'paper'
    test_size = 1 
    test_price = 30000
    test_broker = 'kraken'
    test_brokers = [test_broker]
    test_symbol = 'xbtusdt' 
    test_fqsn = f'{test_symbol}.{test_broker}'
    test_pp_account = 'piker-paper'


    async def open(
        open_pikerd: AsyncContextManager,
        send_order: bool = False,
        assert_entries: bool = False,
        teardown: bool = True,
    ) -> Any:
        # type declares
        book: OrderBook
        global _cleared_price

        async with (
            open_pikerd() as (_, _, _, services),

            open_ems(
                'xbtusdt.kraken',
                mode='paper',
            ) as (
                book,
                trades_stream,
                pps,
                accounts,
                dialogs,
            ),
        ):
            

            if send_order: 
                order = Order(
                    exec_mode=test_exec_mode,
                    action=test_action,
                    oid=test_oid,
                    account=test_account,
                    size=test_size,
                    symbol=test_fqsn,
                    price=test_price,
                    brokers=test_brokers
                )

                book.send(order)
                
                await trio.sleep(1)
            
            if assert_entries:

                cleared_ledger_entry = {}
                # check if trades have been updated in in ledge and pp 
                with open_trade_ledger(test_broker, test_account) as ledger:
                    cleared_ledger_entry = ledger[test_oid]
                    _cleared_price = cleared_ledger_entry["price"]
                    assert list(ledger.keys())[0] == test_oid
                    assert cleared_ledger_entry['size'] == test_size
                    assert cleared_ledger_entry['fqsn'] == test_fqsn

                     
                with open_pps(test_broker, test_pp_account) as table: 
                    # save pps in local state
                    assert table.brokername == test_broker 
                    assert table.acctid == test_pp_account 
        #                assert cleared_ledger_entry['price'] == table.conf.clears[0].price
                    pp_price = table.conf[test_broker][test_pp_account][test_fqsn]["ppu"] 
                    assert math.isclose(pp_price, cleared_ledger_entry['size'], rel_tol=1)

            if teardown:
                raise KeyboardInterrupt
            return pps
            
    async def open_and_assert_pps(): 
        pps = await open(open_test_pikerd)
        assert pps(test_broker, test_account)[0] == _cleared_price

    with pytest.raises(
        trio.MultiError
    ) as exc_info:
        # run initial time and send sent and assert trade
        trio.run(partial(open, 
                         open_pikerd=open_test_pikerd,
                         send_order=True, 
                         assert_entries=True, 
                        )
                 )

        # Run again just to boot piker
        trio.run(partial(open, 
                         open_pikerd=open_test_pikerd,
                        )
                ) 
        trio.run(open_and_assert_pps)


