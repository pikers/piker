# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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

'''
Cacheing apis and toolz.

'''

from collections import OrderedDict
from contextlib import (
    asynccontextmanager as acm,
)
from typing import (
    Awaitable,
    Callable,
    ParamSpec,
    TypeVar,
)

from tractor.trionics import maybe_open_context

from .brokers import get_brokermod
from .log import get_logger


log = get_logger(__name__)

T = TypeVar("T")
P = ParamSpec("P")


# TODO: move this to `tractor.trionics`..
# - egs. to replicate for tests: https://github.com/aio-libs/async-lru#usage
# - their suite as well:
#   https://github.com/aio-libs/async-lru/tree/master/tests
# - asked trio_util about it too:
#   https://github.com/groove-x/trio-util/issues/21
def async_lifo_cache(
    maxsize=128,

    # NOTE: typing style was learned from:
    # https://stackoverflow.com/a/71132186
) -> Callable[
    Callable[P, Awaitable[T]],
    Callable[
        Callable[P, Awaitable[T]],
        Callable[P, Awaitable[T]],
    ],
]:
    '''
    Async ``cache`` with a LIFO policy.

    Implemented my own since no one else seems to have
    a standard. I'll wait for the smarter people to come
    up with one, but until then...

    NOTE: when decorating, due to this simple/naive implementation, you
    MUST call the decorator like,

    .. code:: python

        @async_lifo_cache()
        async def cache_target():

    '''
    cache = OrderedDict()

    def decorator(
        fn: Callable[P, Awaitable[T]],
    ) -> Callable[P, Awaitable[T]]:

        async def decorated(
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> T:
            key = args
            try:
                return cache[key]
            except KeyError:
                if len(cache) >= maxsize:
                    # discard last added new entry
                    cache.popitem()

                # call underlying
                cache[key] = await fn(
                    *args,
                    **kwargs,
                )
                return cache[key]

        return decorated

    return decorator


# TODO: move this to `.brokers.utils`..
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
