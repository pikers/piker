'''
 testing the creation of brokers.toml if it doesn't exist,
 issue was found when trying to run piker chart on windows.
 
'''
from pathlib import Path

from piker import config


def test_brokers_created_from_template_on_load_accounts():
    # load account names from ``brokers.toml``
    accounts_def = config.load_accounts(
        providers=['binance'],
    )