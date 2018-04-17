"""
Broker clients, daemons and general back end machinery.
"""
from importlib import import_module
from types import ModuleType

__brokers__ = [
    'questrade',
    'robinhood',
]


def get_brokermod(brokername: str) -> ModuleType:
    """Return the imported broker module by name.
    """
    module = import_module('.' + brokername, 'piker.brokers')
    # we only allows monkeys because it's for internal keying
    module.name =  module.__name__.split('.')[-1]
    return module


def iter_brokermods():
    """Iterate all built-in broker modules.
    """
    for name in __brokers__:
        yield get_brokermod(name)
