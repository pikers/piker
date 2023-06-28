'''
`piker.accounting` mgmt calculations for
- positioning
- ledger updates
- config file IO

'''
from pathlib import Path

from piker import config
from piker.accounting import load_account


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
