# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Broker clients, daemons and general back end machinery.
"""
from contextlib import (
    asynccontextmanager as acm,
)
from importlib import import_module
from types import ModuleType

from tractor.trionics import maybe_open_context

from ._util import (
    log,
    BrokerError,
    SymbolNotFound,
    NoData,
    DataUnavailable,
    DataThrottle,
    resproc,
    get_logger,
)

__all__: list[str] = [
    'BrokerError',
    'SymbolNotFound',
    'NoData',
    'DataUnavailable',
    'DataThrottle',
    'resproc',
    'get_logger',
]

__brokers__: list[str] = [
    'binance',
    'ib',
    'kraken',
    'kucoin'

    # broken but used to work
    # 'questrade',
    # 'robinhood',

    # TODO: we should get on these stat!
    # alpaca
    # wstrade
    # iex

    # deribit
    # bitso
]


def get_brokermod(brokername: str) -> ModuleType:
    '''
    Return the imported broker module by name.

    '''
    module = import_module('.' + brokername, 'piker.brokers')
    # we only allow monkeying because it's for internal keying
    module.name = module.__name__.split('.')[-1]
    return module


def iter_brokermods():
    '''
    Iterate all built-in broker modules.

    '''
    for name in __brokers__:
        yield get_brokermod(name)


@acm
async def open_cached_client(
    brokername: str,
    **kwargs,

) -> 'Client':  # noqa
    '''
    Get a cached broker client from the current actor's local vars.

    If one has not been setup do it and cache it.

    '''
    brokermod = get_brokermod(brokername)
    async with maybe_open_context(
        acm_func=brokermod.get_client,
        kwargs=kwargs,

    ) as (cache_hit, client):

        if cache_hit:
            log.runtime(f'Reusing existing {client}')

        yield client
