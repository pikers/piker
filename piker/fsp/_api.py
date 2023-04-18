# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship of pikers)

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
FSP (financial signal processing) apis.

'''

# TODO: things to figure the heck out:
# - how to handle non-plottable values (pyqtgraph has facility for this
#   now in `arrayToQPath()`)
# - composition of fsps / implicit chaining syntax (we need an issue)

from __future__ import annotations
from functools import partial
from typing import (
    Any,
    Callable,
    Awaitable,
    Optional,
)

import numpy as np
import tractor
from tractor.msg import NamespacePath

from ..data._sharedmem import (
    ShmArray,
    maybe_open_shm_array,
    attach_shm_array,
    _Token,
)
from ..log import get_logger

log = get_logger(__name__)

# global fsp registry filled out by @fsp decorator below
_fsp_registry = {}


def _load_builtins() -> dict[tuple, Callable]:

    # import to implicity trigger registration via ``@fsp``
    from . import _momo  # noqa
    from . import _volume  # noqa

    return _fsp_registry


class Fsp:
    '''
    "Financial signal processor" decorator wrapped async function.

    '''

    # TODO: checkout the advanced features from ``wrapt``:
    # - dynamic enable toggling,
    # https://wrapt.readthedocs.io/en/latest/decorators.html#dynamically-disabling-decorators
    # - custom object proxies, might be useful for implementing n-compose
    # https://wrapt.readthedocs.io/en/latest/wrappers.html#custom-object-proxies
    # - custom function wrappers,
    # https://wrapt.readthedocs.io/en/latest/wrappers.html#custom-function-wrappers

    # actor-local map of source flow shm tokens
    # + the consuming fsp *to* the consumers output
    # shm flow.
    _flow_registry: dict[
        tuple[_Token, str],
        tuple[_Token, Optional[ShmArray]],
    ] = {}

    def __init__(
        self,
        func: Callable[..., Awaitable],
        *,
        outputs: tuple[str] = (),
        display_name: Optional[str] = None,
        **config,

    ) -> None:

        # TODO (maybe):
        # - type introspection?
        # - should we make this a wrapt object proxy?
        self.func = func
        self.__name__ = func.__name__  # XXX: must have func-object name

        self.ns_path: tuple[str, str] = NamespacePath.from_ref(func)
        self.outputs = outputs
        self.config: dict[str, Any] = config

        # register with declared set.
        _fsp_registry[self.ns_path] = self

    @property
    def name(self) -> str:
        return self.__name__

    def __call__(
        self,

        # TODO: when we settle on py3.10 we should probably use the new
        # type annots from pep 612:
        # https://www.python.org/dev/peps/pep-0612/
        # instance,
        *args,
        **kwargs
    ):
        return self.func(*args, **kwargs)

    def get_shm(
        self,
        src_shm: ShmArray,

    ) -> ShmArray:
        '''
        Provide access to allocated shared mem array
        for this "instance" of a signal processor for
        the given ``key``.

        The destination shm "token" and array are cached if possible to
        minimize multiple stdlib/system calls.

        '''
        dst_token, maybe_array = self._flow_registry[
            (src_shm._token, self.name)
        ]
        if maybe_array is None:
            self._flow_registry[
                (src_shm._token, self.name)
            ] = (
                dst_token,
                # "cache" the ``ShmArray`` such that
                # we call the underlying "attach" code as few
                # times as possible as per:
                # - https://github.com/pikers/piker/issues/359
                # - https://github.com/pikers/piker/issues/332
                maybe_array := attach_shm_array(dst_token)
            )

        return maybe_array


def fsp(
    wrapped=None,
    *,
    outputs: tuple[str] = (),
    display_name: Optional[str] = None,
    **config,

) -> Fsp:

    if wrapped is None:
        return partial(
            Fsp,
            outputs=outputs,
            display_name=display_name,
            **config,
        )

    return Fsp(wrapped, outputs=(wrapped.__name__,))


def mk_fsp_shm_key(
    sym: str,
    target: Fsp

) -> str:
    actor_name, uuid = tractor.current_actor().uid
    uuid_snip: str = uuid[:16]
    return f'piker.{actor_name}[{uuid_snip}].{sym}.{target.name}'


def maybe_mk_fsp_shm(
    sym: str,
    target: Fsp,
    readonly: bool = True,

) -> (str, ShmArray, bool):
    '''
    Allocate a single row shm array for an symbol-fsp pair if none
    exists, otherwise load the shm already existing for that token.

    '''
    assert isinstance(sym, str), '`sym` should be file-name-friendly `str`'

    # TODO: load output types from `Fsp`
    # - should `index` be a required internal field?
    fsp_dtype = np.dtype(
        [('index', int)]
        +
        [('time', float)]
        +
        [(field_name, float) for field_name in target.outputs]
    )

    key = mk_fsp_shm_key(sym, target)

    shm, opened = maybe_open_shm_array(
        key,
        # TODO: create entry for each time frame
        dtype=fsp_dtype,
        readonly=True,
    )
    return key, shm, opened
