"""
Async utils no one seems to have built into a core lib (yet).
"""
from collections import OrderedDict


def alifo_cache(maxsize=128):
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
