"""
Broker clients, daemons and general back end machinery.
"""
from importlib import import_module
from types import ModuleType

# TODO: move to urllib3/requests once supported
import asks
asks.init('trio')

__brokers__ = [
    'questrade',
    'robinhood',
    'ib',
]


def get_brokermod(brokername: str) -> ModuleType:
    """Return the imported broker module by name.
    """
    module = import_module('.' + brokername, 'piker.brokers')
    # we only allow monkeying because it's for internal keying
    module.name =  module.__name__.split('.')[-1]
    return module


def iter_brokermods():
    """Iterate all built-in broker modules.
    """
    for name in __brokers__:
        yield get_brokermod(name)
