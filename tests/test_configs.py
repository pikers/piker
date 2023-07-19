from pathlib import Path

from piker import config


def test_root_conf_networking_section():
    # load account names from ``brokers.toml``
    accounts_def = config.load_accounts(
        providers=['binance'],
    )