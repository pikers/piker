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
NumPy compatible shared memory buffers for real-time IPC streaming.

"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from sys import byteorder
from typing import Optional
from multiprocessing.shared_memory import SharedMemory, _USE_POSIX
from multiprocessing import resource_tracker as mantracker

if _USE_POSIX:
    from _posixshmem import shm_unlink

import tractor
import numpy as np
from pydantic import BaseModel, validator

from ..log import get_logger
from ._source import base_iohlc_dtype


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
        self._shm.buf[:] = value.to_bytes(self._shm.size, byteorder)

    def destroy(self) -> None:
        if _USE_POSIX:
            # We manually unlink to bypass all the "resource tracker"
            # nonsense meant for non-SC systems.
            shm_unlink(self._shm.name)


class _Token(BaseModel):
    '''
    Internal represenation of a shared memory "token"
    which can be used to key a system wide post shm entry.

    '''
    class Config:
        frozen = True

    shm_name: str  # this servers as a "key" value
    shm_first_index_name: str
    shm_last_index_name: str
    dtype_descr: tuple

    @property
    def dtype(self) -> np.dtype:
        return np.dtype(list(map(tuple, self.dtype_descr))).descr

    def as_msg(self):
        return self.dict()

    @classmethod
    def from_msg(cls, msg: dict) -> _Token:
        if isinstance(msg, _Token):
            return msg

        msg['dtype_descr'] = tuple(map(tuple, msg['dtype_descr']))
        return _Token(**msg)


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
    '''
    Create a serializable token that can be used
    to access a shared array.

    '''
    dtype = base_iohlc_dtype if dtype is None else dtype
    return _Token(
        shm_name=key,
        shm_first_index_name=key + "_first",
        shm_last_index_name=key + "_last",
        dtype_descr=np.dtype(dtype).descr
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
        self._post_init: bool = False

        # pushing data does not write the index (aka primary key)
        self._write_fields = list(shmarr.dtype.fields.keys())[1:]

    # TODO: ringbuf api?

    @property
    def _token(self) -> _Token:
        return _Token(
            shm_name=self._shm.name,
            shm_first_index_name=self._first._shm.name,
            shm_last_index_name=self._last._shm.name,
            dtype_descr=tuple(self._array.dtype.descr),
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
        '''Return an up-to-date ``np.ndarray`` view of the
        so-far-written data to the underlying shm buffer.

        '''
        a = self._array[self._first.value:self._last.value]

        # first, last = self._first.value, self._last.value
        # a = self._array[first:last]

        # TODO: eventually comment this once we've not seen it in the
        # wild in a long time..
        # XXX: race where first/last indexes cause a reader
        # to load an empty array..
        if len(a) == 0 and self._post_init:
            raise RuntimeError('Empty array race condition hit!?')
            # breakpoint()

        return a

    def last(
        self,
        length: int = 1,
    ) -> np.ndarray:
        return self.array[-length:]

    def push(
        self,
        data: np.ndarray,

        prepend: bool = False,
        start: Optional[int] = None,

    ) -> int:
        '''Ring buffer like "push" to append data
        into the buffer and return updated "last" index.

        NB: no actual ring logic yet to give a "loop around" on overflow
        condition, lel.
        '''
        self._post_init = True
        length = len(data)
        index = start or self._last.value

        if prepend:
            index = self._first.value - length

            if index < 0:
                raise ValueError(
                    f'Array size of {self._len} was overrun during prepend.\n'
                    f'You have passed {abs(index)} too many datums.'
                )

        end = index + length

        fields = self._write_fields

        try:
            self._array[fields][index:end] = data[fields][:]

            # NOTE: there was a race here between updating
            # the first and last indices and when the next reader
            # tries to access ``.array`` (which due to the index
            # overlap will be empty). Pretty sure we've fixed it now
            # but leaving this here as a reminder.
            if prepend:
                assert index < self._first.value

            if index < self._first.value:
                self._first.value = index
            else:
                self._last.value = end

            return end

        except ValueError as err:
            # should raise if diff detected
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
_secs_in_day = int(60 * 60 * 24)
# we try for 3 times but only on a run-every-other-day kinda week.
_default_size = 3 * _secs_in_day


def open_shm_array(

    key: Optional[str] = None,
    size: int = _default_size,
    dtype: Optional[np.dtype] = None,
    readonly: bool = False,

) -> ShmArray:
    '''Open a memory shared ``numpy`` using the standard library.

    This call unlinks (aka permanently destroys) the buffer on teardown
    and thus should be used from the parent-most accessor (process).

    '''
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
    token: tuple[str, str, tuple[str, str]],
    size: int = _default_size,
    readonly: bool = True,

) -> ShmArray:
    '''
    Attach to an existing shared memory array previously
    created by another process using ``open_shared_array``.

    No new shared mem is allocated but wrapper types for read/write
    access are constructed.

    '''
    token = _Token.from_msg(token)
    key = token.shm_name

    if key in _known_tokens:
        assert _Token.from_msg(_known_tokens[key]) == token, "WTF"

    # attach to array buffer and view as per dtype
    shm = SharedMemory(name=key)
    shmarr = np.ndarray(
        (size,),
        dtype=token.dtype,
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

) -> tuple[ShmArray, bool]:
    '''
    Attempt to attach to a shared memory block using a "key" lookup
    to registered blocks in the users overall "system" registry
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

    '''
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


def try_read(
    array: np.ndarray

) -> Optional[np.ndarray]:
    '''
    Try to read the last row from a shared mem array or ``None``
    if the array read returns a zero-length array result.

    Can be used to check for backfilling race conditions where an array
    is currently being (re-)written by a writer actor but the reader is
    unaware and reads during the window where the first and last indexes
    are being updated.

    '''
    try:
        return array[-1]
    except IndexError:
        # XXX: race condition with backfilling shm.
        #
        # the underlying issue is that a backfill (aka prepend) and subsequent
        # shm array first/last index update could result in an empty array
        # read here since the indices may be updated in such a way that
        # a read delivers an empty array (though it seems like we
        # *should* be able to prevent that?). also, as and alt and
        # something we need anyway, maybe there should be some kind of
        # signal that a prepend is taking place and this consumer can
        # respond (eg. redrawing graphics) accordingly.

        # the array read was emtpy
        return None
