
"""
Profiling wrappers for internal libs.
"""
import time
from functools import wraps


def timeit(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        t = time.time()
        res = fn(*args, **kwargs)
        print(
            '%s.%s: %.4f sec'
            % (fn.__module__, fn.__qualname__, time.time() - t)
        )
        return res

    return wrapper
