import trio
import pytest
import tractor
import math
from piker.log import get_logger
from piker.clearing._messages import ( 
    Order                                    
)
from uuid import uuid4
from typing import (
    AsyncContextManager,
    Literal,
)

from functools import partial
from piker.pp import (
    open_trade_ledger,
    open_pps,
)

from piker.clearing import (
    open_ems,
)

from piker.clearing._client import (
    OrderBook,
)

from piker.clearing._messages import (
    BrokerdPosition
)

from exceptiongroup import BaseExceptionGroup
log = get_logger(__name__)

def test_paper_trade(
    open_test_pikerd: AsyncContextManager
):

    test_exec_mode='live'
    test_account = 'paper'
    test_size = 1 
    test_broker = 'kraken'
    test_brokers = [test_broker]
    test_symbol = 'xbtusdt' 
    test_fqsn = f'{test_symbol}.{test_broker}'
    test_pp_account = 'piker-paper'
    positions: dict[
        # brokername, acctid
        tuple[str, str],
        list[BrokerdPosition],
    ]
    cleared_price: float


    async def _async_main(
        open_pikerd: AsyncContextManager,
        action: Literal['buy', 'sell'] | None = None,
        price: int = 30000,
        assert_entries: bool = False,
    ) -> None:
        """Spawn a paper piper actor, place a trade and assert entries are present
        in both trade ledger and pps tomls. Then restart piker and ensure 
        that pps from previous trade exists in the ems pps. 
        Finally close the position and ensure that the position in pps.toml is closed. 
        """

        oid: str = str(uuid4())
        book: OrderBook
        global cleared_price
        global positions
        
        # Set up piker and EMS
        async with (
            open_pikerd() as (_, _, _, services),
            open_ems(
                test_fqsn,
                mode=test_account,
            ) as (
                book,
                trades_stream,
                pps,
                accounts,
                dialogs,
            ),
        ):

            # Send order to EMS
            if action: 
                order = Order(
                    exec_mode=test_exec_mode,
                    action=action,
                    oid=oid,
                    account=test_account,
                    size=test_size,
                    symbol=test_fqsn,
                    price=price,
                    brokers=test_brokers
                )

                book.send(order)
                
                await trio.sleep(2)

            # Assert entries are made in both ledger and PPS
            if assert_entries:
                cleared_ledger_entry = {}
                with open_trade_ledger(test_broker, test_account) as ledger:
                    cleared_ledger_entry = ledger[oid]
                    cleared_price = cleared_ledger_entry["price"]

                    assert list(ledger.keys())[-1] == oid
                    assert cleared_ledger_entry['size'] == test_size
                    assert cleared_ledger_entry['fqsn'] == test_fqsn
                     
                with open_pps(test_broker, test_pp_account) as table: 
                    pp_price = table.conf[test_broker][test_pp_account][test_fqsn]["ppu"] 
                    assert math.isclose(pp_price, cleared_ledger_entry['size'], rel_tol=1)
                    assert table.brokername == test_broker 
                    assert table.acctid == test_pp_account 
            
            positions = pps

            # Close piker like a user would
            raise KeyboardInterrupt

    # Open piker and ensure last pps price is the same as ledger entry 
    async def _open_and_assert_pps(): 
        await _async_main(open_test_pikerd)
        assert positions(test_broker, test_account)[-1] == cleared_price
   
    # Close position and assert empty position in pps
    async def _close_pp_and_assert():
        await _async_main(open_test_pikerd, 'sell', 1)
        with open_pps(test_broker, test_pp_account) as table: 
            assert len(table.pps) == 0

    # run initial time and send sent and assert trade
    with pytest.raises(
        BaseExceptionGroup
    ):
        trio.run(partial(_async_main, 
                         open_pikerd=open_test_pikerd,
                         action='buy',
                        )
                 )        

    with pytest.raises(
        BaseExceptionGroup
    ):
        trio.run(_open_and_assert_pps)

    with pytest.raises(
        BaseExceptionGroup 
    ): 
        trio.run(_close_pp_and_assert)

