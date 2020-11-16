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
NumPy compatible shared memory buffers for real-time FSP.
"""
from typing import List
from dataclasses import dataclass, asdict
from sys import byteorder
from typing import Tuple, Optional
from multiprocessing import shared_memory
from multiprocessing import resource_tracker as mantracker
from _posixshmem import shm_unlink

import tractor
import numpy as np

from ..log import get_logger
from ._source import base_ohlc_dtype


log = get_logger(__name__)


# Tell the "resource tracker" thing to fuck off.
class ManTracker(mantracker.ResourceTracker):
    def register(self, name, rtype):
        pass

    def unregister(self, name, rtype):
        pass

    def ensure_running(self):
        pass


# "know your land and know your prey"
# https://www.dailymotion.com/video/x6ozzco
mantracker._resource_tracker = ManTracker()
mantracker.register = mantracker._resource_tracker.register
mantracker.ensure_running = mantracker._resource_tracker.ensure_running
ensure_running = mantracker._resource_tracker.ensure_running
mantracker.unregister = mantracker._resource_tracker.unregister
mantracker.getfd = mantracker._resource_tracker.getfd


class SharedInt:
    def __init__(
        self,
        token: str,
        create: bool = False,
    ) -> None:
        # create a single entry array for storing an index counter
        self._shm = shared_memory.SharedMemory(
            name=token,
            create=create,
            size=4,  # std int
        )

    @property
    def value(self) -> int:
        return int.from_bytes(self._shm.buf, byteorder)

    @value.setter
    def value(self, value) -> None:
        self._shm.buf[:] = value.to_bytes(4, byteorder)

    def destroy(self) -> None:
        if shared_memory._USE_POSIX:
            # We manually unlink to bypass all the "resource tracker"
            # nonsense meant for non-SC systems.
            shm_unlink(self._shm.name)


@dataclass
class _Token:
    """Internal represenation of a shared memory "token"
    which can be used to key a system wide post shm entry.
    """
    shm_name: str  # this servers as a "key" value
    shm_counter_name: str
    dtype_descr: List[Tuple[str]]

    def __post_init__(self):
        # np.array requires a list for dtype
        self.dtype_descr = np.dtype(
            list(self.dtype_descr)).descr

    def as_msg(self):
        return asdict(self)

    @classmethod
    def from_msg(self, msg: dict) -> '_Token':
        return msg if isinstance(msg, _Token) else _Token(**msg)


# TODO: this api?
# _known_tokens = tractor.ActorVar('_shm_tokens', {})
# _known_tokens = tractor.ContextStack('_known_tokens', )
# _known_tokens = trio.RunVar('shms', {})

# process-local store of keys to tokens
_known_tokens = {}


def get_shm_token(key: str) -> _Token:
    """Convenience func to check if a token
    for the provided key is known by this process.
    """
    return _known_tokens.get(key)


def _make_token(
    key: str,
    dtype: Optional[np.dtype] = None,
) -> _Token:
    """Create a serializable token that can be used
    to access a shared array.
    """
    dtype = base_ohlc_dtype if dtype is None else dtype
    return _Token(
        key,
        key + "_counter",
        np.dtype(dtype).descr
    )


class ShmArray:
    def __init__(
        self,
        shmarr: np.ndarray,
        counter: SharedInt,
        shm: shared_memory.SharedMemory,
        readonly: bool = True,
    ) -> None:
        self._array = shmarr
        self._i = counter
        self._len = len(shmarr)
        self._shm = shm
        self._readonly = readonly

    # TODO: ringbuf api?

    @property
    def _token(self) -> _Token:
        return _Token(
            self._shm.name,
            self._i._shm.name,
            self._array.dtype.descr,
        )

    @property
    def token(self) -> dict:
        """Shared memory token that can be serialized
        and used by another process to attach to this array.
        """
        return self._token.as_msg()

    @property
    def index(self) -> int:
        return self._i.value % self._len

    @property
    def array(self) -> np.ndarray:
        return self._array[:self._i.value]

    def last(
        self,
        length: int = 1,
    ) -> np.ndarray:
        return self.array[-length:]

    def push(
        self,
        data: np.ndarray,
    ) -> int:
        """Ring buffer like "push" to append data
        into the buffer and return updated index.
        """
        length = len(data)
        # TODO: use .index for actual ring logic?
        index = self._i.value
        end = index + length
        self._array[index:end] = data[:]
        self._i.value = end
        return end

    def close(self) -> None:
        self._i._shm.close()
        self._shm.close()

    def destroy(self) -> None:
        if shared_memory._USE_POSIX:
            # We manually unlink to bypass all the "resource tracker"
            # nonsense meant for non-SC systems.
            shm_unlink(self._shm.name)
        self._i.destroy()

    def flush(self) -> None:
        # TODO: flush to storage backend like markestore?
        ...


_lotsa_5s = int(5*60*60*10/5)


def open_shm_array(
    key: Optional[str] = None,
    # approx number of 5s bars in a "day" x2
    size: int = _lotsa_5s,
    dtype: Optional[np.dtype] = None,
    readonly: bool = False,
) -> ShmArray:
    """Open a memory shared ``numpy`` using the standard library.

    This call unlinks (aka permanently destroys) the buffer on teardown
    and thus should be used from the parent-most accessor (process).
    """
    # create new shared mem segment for which we
    # have write permission
    a = np.zeros(size, dtype=dtype)
    shm = shared_memory.SharedMemory(
        name=key,
        create=True,
        size=a.nbytes
    )
    array = np.ndarray(a.shape, dtype=a.dtype, buffer=shm.buf)
    array[:] = a[:]
    array.setflags(write=int(not readonly))

    token = _make_token(
        key=key,
        dtype=dtype
    )

    counter = SharedInt(
        token=token.shm_counter_name,
        create=True,
    )
    counter.value = 0

    shmarr = ShmArray(
        array,
        counter,
        shm,
        readonly=readonly,
    )

    assert shmarr._token == token
    _known_tokens[key] = shmarr.token

    # "unlink" created shm on process teardown by
    # pushing teardown calls onto actor context stack
    tractor._actor._lifetime_stack.callback(shmarr.close)
    tractor._actor._lifetime_stack.callback(shmarr.destroy)

    return shmarr


def attach_shm_array(
    token: Tuple[str, str, Tuple[str, str]],
    size: int = _lotsa_5s,
    readonly: bool = True,
) -> ShmArray:
    """Load and attach to an existing shared memory array previously
    created by another process using ``open_shared_array``.
    """
    token = _Token.from_msg(token)
    key = token.shm_name
    if key in _known_tokens:
        assert _known_tokens[key] == token, "WTF"

    shm = shared_memory.SharedMemory(name=key)
    shmarr = np.ndarray(
        (size,),
        dtype=token.dtype_descr,
        buffer=shm.buf
    )
    shmarr.setflags(write=int(not readonly))

    counter = SharedInt(token=token.shm_counter_name)
    # make sure we can read
    counter.value

    sha = ShmArray(
        shmarr,
        counter,
        shm,
        readonly=readonly,
    )
    # read test
    sha.array

    # Stash key -> token knowledge for future queries
    # via `maybe_opepn_shm_array()` but only after we know
    # we can attach.
    if key not in _known_tokens:
        _known_tokens[key] = token

    # "close" attached shm on process teardown
    tractor._actor._lifetime_stack.callback(sha.close)

    return sha


def maybe_open_shm_array(
    key: str,
    dtype: Optional[np.dtype] = None,
    **kwargs,
) -> Tuple[ShmArray, bool]:
    """Attempt to attach to a shared memory block by a
    "key" determined by the users overall "system"
    (presumes you don't have the block's explicit token).

    This function is meant to solve the problem of
    discovering whether a shared array token has been
    allocated or discovered by the actor running in
    **this** process. Systems where multiple actors
    may seek to access a common block can use this
    function to attempt to acquire a token as discovered
    by the actors who have previously stored a
    "key" -> ``_Token`` map in an actor local variable.

    If you know the explicit ``_Token`` for your memory
    instead use ``attach_shm_array``.
    """
    try:
        # see if we already know this key
        token = _known_tokens[key]
        return attach_shm_array(token=token, **kwargs), False
    except KeyError:
        log.warning(f"Could not find {key} in shms cache")
        if dtype:
            token = _make_token(key, dtype)
            try:
                return attach_shm_array(token=token, **kwargs), False
            except FileNotFoundError:
                log.warning(f"Could not attach to shm with token {token}")

        # This actor does not know about memory
        # associated with the provided "key".
        # Attempt to open a block and expect
        # to fail if a block has been allocated
        # on the OS by someone else.
        return open_shm_array(key=key, dtype=dtype, **kwargs), True
