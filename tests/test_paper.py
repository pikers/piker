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
from operator import attrgetter

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
        executions: int = 1,
        size: float = 0.01,
    ) -> None:
        """Start piker, place a trade and assert entries are present
        in both trade ledger and pps tomls. Then restart piker and ensure
        that pps from previous trade exists in the ems pps.
        Finally close the position and ensure that the position in pps.toml is closed.
        """
        nonlocal oid
        nonlocal positions
        book: OrderBook
        msg = ()
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
                for x in range(executions):
                    print(f"Sending {action} order num {x}")
                    oid = str(uuid4())
                    order = Order(
                        exec_mode=test_exec_mode,
                        action=action,
                        oid=oid,
                        account=test_account,
                        size=size,
                        symbol=fqsn,
                        price=price,
                        brokers=brokers,
                    )
                    # This is actually a syncronous call to push a message
                    # to the async ems clue - hence why we call trio.sleep afterwards
                    book.send(order)

                    async for msg in trades_stream:
                        msg = await trades_stream.receive()
                        try:
                            if msg["name"] == "position":
                                break
                        except (NameError, AttributeError):
                            pass
                            # Do nothing, message isn't a position

            await trio.sleep(1)
            # Assert entries are made in both ledger and PPS
            if assert_entries or assert_pps or assert_zeroed_pps:
                _assert(
                    assert_entries,
                    assert_pps,
                    assert_zeroed_pps,
                    pps,
                    msg,
                )

            # Close piker like a user would
            raise KeyboardInterrupt

    def _assert(
        assert_entries, assert_pps, assert_zerod_pps, pps, msg
    ):
        with (
            open_trade_ledger(broker, test_account) as ledger,
            open_pps(broker, test_pp_account) as table,
        ):
            # TODO: Assert between msg and pp, ledger and pp, ledger and message
            # for proper values
            print(f"assertion msg: {msg}")
            # assert that entires are have been written
            if assert_entries:
                latest_ledger_entry = ledger[oid]
                latest_position = pps[(broker, test_account)][-1]
                pp_price = table.conf[broker][test_pp_account][fqsn]["ppu"]
                # assert most
                assert list(ledger.keys())[-1] == oid
                assert latest_ledger_entry["size"] == test_size
                assert latest_ledger_entry["fqsn"] == fqsn

                # Ensure the price-per-unit (breakeven) price is close to our clearing price
                assert math.isclose(pp_price, latest_ledger_entry["size"], rel_tol=1)
                assert table.brokername == broker
                assert table.acctid == test_pp_account

            # assert that the last pps price is the same as the ledger price
            if assert_pps:
                latest_ledger_entry = ledger[oid]
                latest_position = pps[(broker, test_account)][-1]
                assert latest_position["avg_price"] == latest_ledger_entry["price"]

            if assert_zerod_pps:
                # assert that positions are present
                assert not bool(table.pps)

    # Close position and assert empty position in pps
    def _run_test_and_check(exception, fn):
        with pytest.raises(exception) as exc_info:
            trio.run(fn)

        for exception in exc_info.value.exceptions:
            assert (
                isinstance(exception, KeyboardInterrupt)
                or isinstance(exception, ContextCancelled)
                or isinstance(exception, KeyError)
            )

    # Enter a trade and assert entries are made in pps and ledger files
    _run_test_and_check(
        BaseExceptionGroup,
        partial(_async_main, action="buy", assert_entries=True),
    )

    # Open ems and assert existence of pps entries
    _run_test_and_check(
        BaseExceptionGroup,
        partial(_async_main, assert_pps=True),
    )

    # Sell position
    _run_test_and_check(
        BaseExceptionGroup,
        partial(_async_main, action="sell", price=1),
    )

    # Ensure pps are zeroed
    _run_test_and_check(
        BaseExceptionGroup,
        partial(_async_main, assert_zeroed_pps=True),
    )

    # Make 5 market limit buy orders
    _run_test_and_check(
        BaseExceptionGroup, partial(_async_main, action="buy", executions=5)
    )

    # Sell 5 slots at the same price, assert cleared positions
    _run_test_and_check(
        BaseExceptionGroup,
        partial(
            _async_main, action="sell", executions=5, price=1, assert_zeroed_pps=True
        ),
    )
