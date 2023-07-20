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
Profiling wrappers for internal libs.

"""
import os
import sys
import time
from time import perf_counter
from functools import wraps

# NOTE: you can pass a flag to enable this:
# ``piker chart <args> --profile``.
_pg_profile: bool = False
ms_slower_then: float = 0


def pg_profile_enabled() -> bool:
    global _pg_profile
    return _pg_profile


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


# Modified version of ``pyqtgraph.debug.Profiler`` that
# core seems hesitant to land in:
# https://github.com/pyqtgraph/pyqtgraph/pull/2281
class Profiler(object):
    '''
    Simple profiler allowing measurement of multiple time intervals.

    By default, profilers are disabled.  To enable profiling, set the
    environment variable `PYQTGRAPHPROFILE` to a comma-separated list of
    fully-qualified names of profiled functions.

    Calling a profiler registers a message (defaulting to an increasing
    counter) that contains the time elapsed since the last call.  When the
    profiler is about to be garbage-collected, the messages are passed to the
    outer profiler if one is running, or printed to stdout otherwise.

    If `delayed` is set to False, messages are immediately printed instead.

    Example:
        def function(...):
            profiler = Profiler()
            ... do stuff ...
            profiler('did stuff')
            ... do other stuff ...
            profiler('did other stuff')
            # profiler is garbage-collected and flushed at function end

    If this function is a method of class C, setting `PYQTGRAPHPROFILE` to
    "C.function" (without the module name) will enable this profiler.

    For regular functions, use the qualified name of the function, stripping
    only the initial "pyqtgraph." prefix from the module.
    '''

    _profilers = os.environ.get("PYQTGRAPHPROFILE", None)
    _profilers = _profilers.split(",") if _profilers is not None else []

    _depth = 0

    # NOTE: without this defined at the class level
    # you won't see apprpriately "nested" sub-profiler
    # instance calls.
    _msgs = []

    # set this flag to disable all or individual profilers at runtime
    disable = False

    class DisabledProfiler(object):
        def __init__(self, *args, **kwds):
            pass

        def __call__(self, *args):
            pass

        def finish(self):
            pass

        def mark(self, msg=None):
            pass

    _disabledProfiler = DisabledProfiler()

    def __new__(
        cls,
        msg=None,
        disabled='env',
        delayed=True,
        ms_threshold: float = 0.0,
    ):
        """Optionally create a new profiler based on caller's qualname.

        ``ms_threshold`` can be set to value in ms for which, if the
        total measured time  of the lifetime of this profiler is **less
        than** this value, then no profiling messages will be printed.
        Setting ``delayed=False`` disables this feature since messages
        are emitted immediately.

        """
        if (
            disabled is True
            or (
                disabled == 'env'
                and len(cls._profilers) == 0
            )
        ):
            return cls._disabledProfiler

        # determine the qualified name of the caller function
        caller_frame = sys._getframe(1)
        try:
            caller_object_type = type(caller_frame.f_locals["self"])

        except KeyError:  # we are in a regular function
            qualifier = caller_frame.f_globals["__name__"].split(".", 1)[-1]

        else:  # we are in a method
            qualifier = caller_object_type.__name__
        func_qualname = qualifier + "." + caller_frame.f_code.co_name

        if disabled == 'env' and func_qualname not in cls._profilers:
            # don't do anything
            return cls._disabledProfiler

        cls._depth += 1
        obj = super(Profiler, cls).__new__(cls)
        obj._msgs = []

        # create an actual profiling object
        if cls._depth < 1:
            cls._msgs = []

        obj._name = msg or func_qualname
        obj._delayed = delayed
        obj._markCount = 0
        obj._finished = False
        obj._firstTime = obj._lastTime = perf_counter()
        obj._mt = ms_threshold
        obj._newMsg("> Entering " + obj._name)
        return obj

    def __call__(self, msg=None):
        """Register or print a new message with timing information.
        """
        if self.disable:
            return
        if msg is None:
            msg = str(self._markCount)

        self._markCount += 1
        newTime = perf_counter()
        tot_ms = (newTime - self._firstTime) * 1000
        ms = (newTime - self._lastTime) * 1000
        self._newMsg(
            f"  {msg}: {ms:0.4f}, tot:{tot_ms:0.4f}"
        )

        self._lastTime = newTime

    def mark(self, msg=None):
        self(msg)

    def _newMsg(self, msg, *args):
        msg = "  " * (self._depth - 1) + msg
        if self._delayed:
            self._msgs.append((msg, args))
        else:
            print(msg % args)

    def __del__(self):
        self.finish()

    def finish(self, msg=None):
        """Add a final message; flush the message list if no parent profiler.
        """
        if self._finished or self.disable:
            return

        self._finished = True
        if msg is not None:
            self(msg)

        tot_ms = (perf_counter() - self._firstTime) * 1000
        self._newMsg(
            "< Exiting %s, total time: %0.4f ms",
            self._name,
            tot_ms,
        )

        if tot_ms < self._mt:
            # print(f'{tot_ms} < {self._mt}, clearing')
            # NOTE: this list **must** be an instance var to avoid
            # deleting common messages during GC I think?
            self._msgs.clear()
        # else:
        #     print(f'{tot_ms} > {self._mt}, not clearing')

        # XXX: why is this needed?
        # don't we **want to show** nested profiler messages?
        if self._msgs:  # and self._depth < 1:

            # if self._msgs:
            print("\n".join([m[0] % m[1] for m in self._msgs]))

            # clear all entries
            self._msgs.clear()
            # type(self)._msgs = []

        type(self)._depth -= 1
