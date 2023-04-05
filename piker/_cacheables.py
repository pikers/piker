# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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
Cacheing apis and toolz.

"""

from collections import OrderedDict
from contextlib import (
    asynccontextmanager as acm,
)

from tractor.trionics import maybe_open_context

from .brokers import get_brokermod
from .log import get_logger


log = get_logger(__name__)


def async_lifo_cache(maxsize=128):
    """Async ``cache`` with a LIFO policy.

    Implemented my own since no one else seems to have
    a standard. I'll wait for the smarter people to come
    up with one, but until then...
    """
    cache = OrderedDict()

    def decorator(fn):

        async def wrapper(*args):
            key = args
            try:
                return cache[key]
            except KeyError:
                if len(cache) >= maxsize:
                    # discard last added new entry
                    cache.popitem()

                # do it
                cache[key] = await fn(*args)
                return cache[key]

        return wrapper

    return decorator


@acm
async def open_cached_client(
    brokername: str,
) -> 'Client':  # noqa
    '''
    Get a cached broker client from the current actor's local vars.

    If one has not been setup do it and cache it.

    '''
    brokermod = get_brokermod(brokername)
    async with maybe_open_context(
        acm_func=brokermod.get_client,
    ) as (cache_hit, client):
        yield client
