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
from dataclasses import dataclass, asdict
from sys import byteorder
from typing import List, Tuple, Optional
from multiprocessing.shared_memory import SharedMemory, _USE_POSIX
from multiprocessing import resource_tracker as mantracker
from _posixshmem import shm_unlink

import tractor
import numpy as np

from ..log import get_logger
from ._source import base_ohlc_dtype, base_iohlc_dtype


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
    """Wrapper around a single entry shared memory array which
    holds an ``int`` value used as an index counter.

    """
    def __init__(
        self,
        shm: SharedMemory,
    ) -> None:
        self._shm = shm

    @property
    def value(self) -> int:
        return int.from_bytes(self._shm.buf, byteorder)

    @value.setter
    def value(self, value) -> None:
        self._shm.buf[:] = value.to_bytes(4, byteorder)

    def destroy(self) -> None:
        if _USE_POSIX:
            # We manually unlink to bypass all the "resource tracker"
            # nonsense meant for non-SC systems.
            shm_unlink(self._shm.name)


@dataclass
class _Token:
    """Internal represenation of a shared memory "token"
    which can be used to key a system wide post shm entry.
    """
    shm_name: str  # this servers as a "key" value
    shm_first_index_name: str
    shm_last_index_name: str
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
    dtype = base_iohlc_dtype if dtype is None else dtype
    return _Token(
        key,
        key + "_first",
        key + "_last",
        np.dtype(dtype).descr
    )


class ShmArray:
    """A shared memory ``numpy`` (compatible) array API.

    An underlying shared memory buffer is allocated based on
    a user specified ``numpy.ndarray``. This fixed size array
    can be read and written to by pushing data both onto the "front"
    or "back" of a set index range. The indexes for the "first" and
    "last" index are themselves stored in shared memory (accessed via
    ``SharedInt`` interfaces) values such that multiple processes can
    interact with the same array using a synchronized-index.

    """
    def __init__(
        self,
        shmarr: np.ndarray,
        first: SharedInt,
        last: SharedInt,
        shm: SharedMemory,
        # readonly: bool = True,
    ) -> None:
        self._array = shmarr

        # indexes for first and last indices corresponding
        # to fille data
        self._first = first
        self._last = last

        self._len = len(shmarr)
        self._shm = shm

        # pushing data does not write the index (aka primary key)
        self._write_fields = list(shmarr.dtype.fields.keys())[1:]

    # TODO: ringbuf api?

    @property
    def _token(self) -> _Token:
        return _Token(
            self._shm.name,
            self._first._shm.name,
            self._last._shm.name,
            self._array.dtype.descr,
        )

    @property
    def token(self) -> dict:
        """Shared memory token that can be serialized and used by
        another process to attach to this array.
        """
        return self._token.as_msg()

    @property
    def index(self) -> int:
        return self._last.value % self._len

    @property
    def array(self) -> np.ndarray:
        return self._array[self._first.value:self._last.value]

    def last(
        self,
        length: int = 1,
    ) -> np.ndarray:
        return self.array[-length:]

    def push(
        self,
        data: np.ndarray,
        prepend: bool = False,
    ) -> int:
        """Ring buffer like "push" to append data
        into the buffer and return updated "last" index.
        """
        length = len(data)

        if prepend:
            index = self._first.value - length
        else:
            index = self._last.value

        end = index + length

        fields = self._write_fields

        try:
            self._array[fields][index:end] = data[fields][:]
            if prepend:
                self._first.value = index
            else:
                self._last.value = end
            return end
        except ValueError as err:
            # shoudl raise if diff detected
            self.diff_err_fields(data)

            raise err

    def diff_err_fields(
        self,
        data: np.ndarray,
    ) -> None:
        # reraise with any field discrepancy
        our_fields, their_fields = (
            set(self._array.dtype.fields),
            set(data.dtype.fields),
        )

        only_in_ours = our_fields - their_fields
        only_in_theirs = their_fields - our_fields

        if only_in_ours:
            raise TypeError(
                f"Input array is missing field(s): {only_in_ours}"
            )
        elif only_in_theirs:
            raise TypeError(
                f"Input array has unknown field(s): {only_in_theirs}"
            )

    def prepend(
        self,
        data: np.ndarray,
    ) -> int:
        end = self.push(data, prepend=True)
        assert end

    def close(self) -> None:
        self._first._shm.close()
        self._last._shm.close()
        self._shm.close()

    def destroy(self) -> None:
        if _USE_POSIX:
            # We manually unlink to bypass all the "resource tracker"
            # nonsense meant for non-SC systems.
            shm_unlink(self._shm.name)

        self._first.destroy()
        self._last.destroy()

    def flush(self) -> None:
        # TODO: flush to storage backend like markestore?
        ...


# how  much is probably dependent on lifestyle
_secs_in_day = int(60 * 60 * 12)
_default_size = 2 * _secs_in_day

def open_shm_array(
    key: Optional[str] = None,
    size: int = _default_size,
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
    a['index'] = np.arange(len(a))

    shm = SharedMemory(
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

    # create single entry arrays for storing an first and last indices
    first = SharedInt(
        shm=SharedMemory(
            name=token.shm_first_index_name,
            create=True,
            size=4,  # std int
        )
    )

    last = SharedInt(
        shm=SharedMemory(
            name=token.shm_last_index_name,
            create=True,
            size=4,  # std int
        )
    )

    last.value = first.value = int(_secs_in_day)

    shmarr = ShmArray(
        array,
        first,
        last,
        shm,
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
    size: int = _default_size,
    readonly: bool = True,
) -> ShmArray:
    """Attach to an existing shared memory array previously
    created by another process using ``open_shared_array``.

    No new shared mem is allocated but wrapper types for read/write
    access are constructed.
    """
    token = _Token.from_msg(token)
    key = token.shm_name

    if key in _known_tokens:
        assert _known_tokens[key] == token, "WTF"

    # attach to array buffer and view as per dtype
    shm = SharedMemory(name=key)
    shmarr = np.ndarray(
        (size,),
        dtype=token.dtype_descr,
        buffer=shm.buf
    )
    shmarr.setflags(write=int(not readonly))

    first = SharedInt(
        shm=SharedMemory(
            name=token.shm_first_index_name,
            create=False,
            size=4,  # std int
        ),
    )
    last = SharedInt(
        shm=SharedMemory(
            name=token.shm_last_index_name,
            create=False,
            size=4,  # std int
        ),
    )

    # make sure we can read
    first.value

    sha = ShmArray(
        shmarr,
        first,
        last,
        shm,
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
    """Attempt to attach to a shared memory block using a "key" lookup
    to registered blocks in the users overall "system" registryt
    (presumes you don't have the block's explicit token).

    This function is meant to solve the problem of discovering whether
    a shared array token has been allocated or discovered by the actor
    running in **this** process. Systems where multiple actors may seek
    to access a common block can use this function to attempt to acquire
    a token as discovered by the actors who have previously stored
    a "key" -> ``_Token`` map in an actor local (aka python global)
    variable.

    If you know the explicit ``_Token`` for your memory segment instead
    use ``attach_shm_array``.
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
