"""
Paper-mode testing
"""

import os
import trio
import math
from shutil import rmtree
from exceptiongroup import BaseExceptionGroup
from typing import (
    AsyncContextManager,
    Literal,
)

import pytest
import tractor
from tractor._exceptions import ContextCancelled
from uuid import uuid4
from functools import partial

from piker.config import get_app_dir
from piker.log import get_logger
from piker.clearing._messages import Order
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

from piker.clearing._messages import BrokerdPosition

log = get_logger(__name__)


@pytest.fixture(scope="session")
def paper_cleanup():
    """This fixture removes the temp directory
    used for storing all config/ledger/pp data
    created while testing once an entire test session
    has completed.
    """
    yield
    app_dir = get_app_dir("piker")
    if os.path.isfile(app_dir):
        rmtree(app_dir)
        assert not os.path.isfile(app_dir)


def get_fqsn(broker, symbol):
    fqsn = f"{symbol}.{broker}"
    return (fqsn, symbol, broker)


def test_paper_trade(open_test_pikerd: AsyncContextManager):
    cleared_price: float
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
        open_pikerd: AsyncContextManager,
        action: Literal["buy", "sell"] | None = None,
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
        nonlocal cleared_price
        nonlocal positions

        # Set up piker and EMS
        async with (
            open_pikerd() as (_, _, _, services),
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
            if assert_entries:
                cleared_ledger_entry = {}
                with open_trade_ledger(broker, test_account) as ledger:
                    cleared_ledger_entry = ledger[oid]
                    cleared_price = cleared_ledger_entry["price"]

                    assert list(ledger.keys())[-1] == oid
                    assert cleared_ledger_entry["size"] == test_size
                    assert cleared_ledger_entry["fqsn"] == fqsn

                with open_pps(broker, test_pp_account) as table:
                    pp_price = table.conf[broker][test_pp_account][fqsn]["ppu"]
                    # Because we can never really ensure that the order and cleared price
                    # will be the same, we use 'isclose' here to loosly compare
                    assert math.isclose(
                        pp_price, cleared_ledger_entry["size"], rel_tol=1
                    )
                    assert table.brokername == broker
                    assert table.acctid == test_pp_account

            positions = pps

            # Close piker like a user would
            raise KeyboardInterrupt

    # Open piker load pps locally
    # and ensure last pps price is the same as ledger entry
    async def _open_and_assert_pps():
        await _async_main(open_test_pikerd)
        assert positions(broker, test_account)[-1] == cleared_price

    # Close position and assert empty position in pps
    async def _close_pp_and_assert():
        await _async_main(open_test_pikerd, "sell", 1)
        with open_pps(broker, test_pp_account) as table:
            assert len(table.pps) == 0

    def _run_test_and_check(exception, fn):
        with pytest.raises(exception) as exc_info:
            trio.run(fn)

        for exception in exc_info.value.exceptions:
            assert isinstance(exception, KeyboardInterrupt) or isinstance(
                exception, ContextCancelled
            )

    # Send and execute a trade and assert trade
    _run_test_and_check(
        BaseExceptionGroup,
        partial(
            _async_main,
            open_pikerd=open_test_pikerd,
            action="buy",
        ),
    )
    _run_test_and_check(BaseExceptionGroup, _open_and_assert_pps)
    _run_test_and_check(BaseExceptionGroup, _close_pp_and_assert)
