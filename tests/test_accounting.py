'''
`piker.accounting` mgmt calculations for
- positioning
- ledger updates
- config file IO

'''
from pathlib import Path

from piker import config
from piker.accounting import (
    Account,
    calc,
    Position,
    TransactionLedger,
    open_trade_ledger,
    load_account,
    load_account_from_ledger,
)


def test_root_conf_networking_section(
    root_conf: dict,
):
    conf, path = config.load(
        'conf',
        touch_if_dne=True,
    )
    assert conf['network']['tsdb']


def test_account_file_default_empty(
    tmpconfdir: Path,
):
    conf, path = load_account(
        'kraken',
        'paper',
    )

    # ensure the account file empty but created
    # and in the correct place in the filesystem!
    assert not conf
    assert path.parent.is_dir()
    assert path.parent.name == 'accounting'


def test_paper_ledger_position_calcs():
    broker: str = 'binance'
    acnt_name: str = 'paper'

    accounts_path: Path = config.repodir() / 'tests' / '_inputs'

    ldr: TransactionLedger
    with (
        open_trade_ledger(
            broker,
            acnt_name,
            allow_from_sync_code=True,

            _fp=accounts_path,
        ) as ldr,

        # open `polars` acnt dfs Bo
        calc.open_ledger_dfs(
            broker,
            acnt_name,
            ledger=ldr,

            _fp=accounts_path,

        ) as (dfs, ledger),

    ):
        acnt: Account = load_account_from_ledger(
            broker,
            acnt_name,
            ledger=ldr,
            _fp=accounts_path,
        )

        # do manual checks on expected pos calcs based on input
        # ledger B)

        # xrpusdt should have a net-zero size
        xrp: str = 'xrpusdt.spot.binance'
        pos: Position = acnt.pps[xrp]

        # XXX: turns out our old dict-style-processing
        # get's this wrong i think due to dt-sorting..
        # lcum: float = pos.cumsize

        df = dfs[xrp]
        assert df['cumsize'][-1] == 0
        assert pos.cumsize == 0
