"""
NumPy compatible shared memory buffers for real-time FSP.
"""
from sys import byteorder
from contextlib import contextmanager
from typing import Tuple, Optional
from multiprocessing import shared_memory
from multiprocessing import resource_tracker as mantracker
from _posixshmem import shm_unlink

import numpy as np


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


base_ohlc_dtype = np.dtype(
    [
        ('index', int),
        ('time', float),
        ('open', float),
        ('high', float),
        ('low', float),
        ('close', float),
        ('volume', int),
    ]
)


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
        self._token = self._shm.name

    @property
    def value(self) -> int:
        return int.from_bytes(self._shm.buf, byteorder)

    @value.setter
    def value(self, value) -> None:
        self._shm.buf[:] = value.to_bytes(4, byteorder)


class SharedArray:
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
    def token(self) -> Tuple[str, str]:
        return (self._shm.name, self._i._token)

    @property
    def name(self) -> str:
        return self._shm.name

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
            shm_unlink(self._i._shm.name)
            shm_unlink(self._shm.name)

    def flush(self) -> None:
        # TODO: flush to storage backend like markestore?
        ...


@contextmanager
def open_shared_array(
    name: Optional[str] = None,
    create: bool = True,
    # approx number of 5s bars in a "day"
    size: int = int(60*60*10/5),
    dtype: np.dtype = base_ohlc_dtype,
    readonly: bool = False,
) -> SharedArray:
    """Open a memory shared ``numpy`` using the standard library.

    This call unlinks (aka permanently destroys) the buffer on teardown
    and thus should be used from the parent-most accessor (process).
    """
    # create new shared mem segment for which we
    # have write permission
    a = np.zeros(size, dtype=dtype)
    shm = shared_memory.SharedMemory(name=name, create=True, size=a.nbytes)
    shmarr = np.ndarray(a.shape, dtype=a.dtype, buffer=shm.buf)
    shmarr[:] = a[:]
    shmarr.setflags(write=int(not readonly))

    counter = SharedInt(
        token=shm.name + "_counter",
        create=True,
    )
    counter.value = 0

    sha = SharedArray(
        shmarr,
        counter,
        shm,
        readonly=readonly,
    )
    try:
        yield sha
    finally:
        sha.close()
        print(f"UNLINKING {sha.token}")
        sha.destroy()


@contextmanager
def attach_shared_array(
    token: Tuple[str, str],
    size: int = int(60*60*10/5),
    dtype: np.dtype = base_ohlc_dtype,
    readonly: bool = True,
) -> SharedArray:
    """Load and attach to an existing shared memory array previously
    created by another process using ``open_shared_array``.
    """
    array_name, counter_name = token

    shm = shared_memory.SharedMemory(name=array_name)
    shmarr = np.ndarray((size,), dtype=dtype, buffer=shm.buf)
    shmarr.setflags(write=int(not readonly))

    counter = SharedInt(token=counter_name)
    # make sure we can read
    counter.value

    sha = SharedArray(
        shmarr,
        counter,
        shm,
        readonly=readonly,
    )
    sha.array
    try:
        yield sha
    finally:
        pass
        sha.close()


@contextmanager
def maybe_open_shared_array(
    name: str,
    **kwargs,
) -> SharedArray:
    try:
        with open_shared_array(
            name=name,
            **kwargs,
        ) as shm:
            yield shm
    except FileExistsError:
        with attach_shared_array(
            token=(name, name + '_counter'),
            **kwargs,
        ) as shm:
            yield shm
