"""
Paper-mode testing
"""

import trio
import math
from shutil import rmtree
from exceptiongroup import BaseExceptionGroup
from typing import (
    AsyncContextManager,
    Literal,
)
from pathlib import Path

import pytest
import tractor
from tractor._exceptions import ContextCancelled
from uuid import uuid4
from functools import partial

from piker.config import get_app_dir
from piker.log import get_logger
from piker.clearing._messages import Order
from piker.pp import (
    PpTable,
    open_trade_ledger,
    open_pps,
)
from piker.clearing import (
    open_ems,
)
from piker.clearing._client import (
    OrderBook,
)
from piker._cacheables import open_cached_client
from piker.clearing._messages import BrokerdPosition

log = get_logger(__name__)


@pytest.fixture(scope="session")
def delete_testing_dir():
    """This fixture removes the temp directory
    used for storing all config/ledger/pp data
    created during testing sessions
    """
    yield
    app_dir = Path(get_app_dir("piker")).resolve()
    if app_dir.is_dir():
        rmtree(str(app_dir))
        assert not app_dir.is_dir()


def get_fqsn(broker, symbol):
    fqsn = f"{symbol}.{broker}"
    return (fqsn, symbol, broker)


def test_paper_trade(open_test_pikerd: AsyncContextManager, delete_testing_dir):
    oid = ""
    test_exec_mode = "live"
    test_account = "paper"
    test_size = 1
    (fqsn, symbol, broker) = get_fqsn("kraken", "xbtusdt")
    brokers = [broker]
    test_pp_account = "piker-paper"
    positions: dict[
        # brokername, acctid
        tuple[str, str],
        list[BrokerdPosition],
    ]

    async def _async_main(
        action: Literal["buy", "sell"] | None = None,
        price: int = 30000,
        assert_entries: bool = False,
        assert_pps: bool = False,
        assert_zeroed_pps: bool = False,
    ) -> None:
        """Spawn a paper piper actor, place a trade and assert entries are present
        in both trade ledger and pps tomls. Then restart piker and ensure
        that pps from previous trade exists in the ems pps.
        Finally close the position and ensure that the position in pps.toml is closed.
        """
        nonlocal oid
        book: OrderBook
        nonlocal positions

        # Set up piker and EMS
        async with (
            open_test_pikerd() as (_, _, _, services),
            open_ems(fqsn, mode="paper") as (
                book,
                trades_stream,
                pps,
                accounts,
                dialogs,
            ),
        ):
            # Send order to EMS
            if action:
                oid = str(uuid4())
                order = Order(
                    exec_mode=test_exec_mode,
                    action=action,
                    oid=oid,
                    account=test_account,
                    size=test_size,
                    symbol=fqsn,
                    price=price,
                    brokers=brokers,
                )
                # This is actually a syncronous call to push a message
                # to the async ems clue - hence why we call trio.sleep afterwards
                book.send(order)

                await trio.sleep(2)

            # Assert entries are made in both ledger and PPS
            if assert_entries or assert_pps or assert_zeroed_pps:
                _assert(assert_entries, assert_pps, assert_zeroed_pps, pps)

            # Close piker like a user would
            raise KeyboardInterrupt

    def _assert(assert_entries: bool, assert_pps: bool, assert_zerod_pps, pps):
        with (
            open_trade_ledger(broker, test_account) as ledger,
            open_pps(broker, test_pp_account) as table,
        ):
            # assert that entires are have been written
            if assert_entries:
                cleared_ledger_entry = ledger[oid]
                assert list(ledger.keys())[-1] == oid
                assert cleared_ledger_entry["size"] == test_size
                assert cleared_ledger_entry["fqsn"] == fqsn
                pp_price = table.conf[broker][test_pp_account][fqsn]["ppu"]
                # Ensure the price-per-unit (breakeven) price is close to our clearing price
                assert math.isclose(pp_price, cleared_ledger_entry["size"], rel_tol=1)
                assert table.brokername == broker
                assert table.acctid == test_pp_account

            # assert that the last pps price is the same as the ledger price
            if assert_pps:
                assert (
                    pps[(broker, test_account)][-1]["avg_price"] == ledger[oid]["price"]
                )

            if assert_zerod_pps:
                # assert that positions are present
                assert not bool(table)

    # Close position and assert empty position in pps
    def _run_test_and_check(exception, fn):
        with pytest.raises(exception) as exc_info:
            trio.run(fn)

        for exception in exc_info.value.exceptions:
            assert isinstance(exception, KeyboardInterrupt) or isinstance(
                exception, ContextCancelled
            )

    # Setablend and execute a trade and assert trade
    _run_test_and_check(
        BaseExceptionGroup,
        partial(_async_main, action="buy", assert_entries=True),
    )

    _run_test_and_check(
        BaseExceptionGroup,
        partial(_async_main, assert_pps=True),
    )

    _run_test_and_check(
        BaseExceptionGroup,
        partial(
            _async_main, action="sell", price=1
        ),
    )

    _run_test_and_check(
        BaseExceptionGroup,
        partial(
            _async_main, assert_zeroed_pps=True
        ),
    )



# def test_paper_client(open_test_pikerd: AsyncContextManager):
#     async def _async_main(
#         open_pikerd: AsyncContextManager,
#     ):
#         (fqsn, symbol, broker) = get_fqsn("kraken", "xbtusdt")
#         async with (
#             open_pikerd() as (_, _, _, services),
#             open_ems(fqsn, mode="paper") as (
#                 book,
#                 trades_stream,
#                 pps,
#                 accounts,
#                 dialogs,
#             ),
#         ):
#             # async with open_cached_client(broker) as client:
#             #     symbol_info = await client.symbol_info()
#             #     print(f'client: {symbol_info['XBTUSDT']}')
#             with (open_pps(broker, "piker-paper") as table,):
#                 print(f"table: {table}")
#
#     trio.run(
#         partial(
#             _async_main,
#             open_pikerd=open_test_pikerd,
#         ),
#     )
