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
from tractor._portal import NamespacePath
# import wrapt

from ..data._sharedmem import (
    ShmArray,
    maybe_open_shm_array,
)


# global fsp registry filled out by @fsp decorator below
_fsp_builtins = {}
    # 'rsi': _rsi,
    # 'wma': _wma,
    # 'vwap': _tina_vwap,
    # 'dolla_vlm': dolla_vlm,
# }

def _load_builtins() -> dict[tuple, Callable]:
    from ._momo import _rsi, _wma
    from ._volume import tina_vwap, dolla_vlm

    return _fsp_builtins

# TODO: things to figure the heck out:
# - how to handle non-plottable values (pyqtgraph has facility for this
#   now in `arrayToQPath()`)
# - composition of fsps / implicit chaining syntax (we need an issue)


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

    def __init__(
        self,
        func: Callable[..., Awaitable],
        *,
        outputs: tuple[str] = (),
        display_name: Optional[str] = None,
        **config,

    ) -> None:
        # if wrapped is not None:
        #     self.name = wrapped.__name__
        # TODO: should we make this a wrapt object proxy?
        self.func = func
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.ns_path: tuple[str, str] = NamespacePath.from_ref(func)
        _fsp_builtins[self.ns_path] = func
        self.outputs = outputs
        self.config: dict[str, Any] = config

    # @wrapt.decorator
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
        # return wrapped(*args, **kwargs)


def fsp(
    wrapped=None,
    *,
    outputs: tuple[str] = (),
    display_name: Optional[str] = None,
    **config,

) -> Fsp:
    # @wrapt.decorator
    # def wrapper(wrapped, instance, args, kwargs):
    #     return wrapped(*args, **kwargs)

    if wrapped is None:
        # return functools.partial(with_optional_arguments,
        #         myarg1=myarg1, myarg2=myarg2)
        return partial(
            Fsp,
            outputs=outputs,
            display_name=display_name,
            **config,
        )

    # return wrapper(wrapped)
    return Fsp(wrapped, outputs=(wrapped.__name__,))
        # outputs=outputs,
        # display_name=display_name,
        # **config,
    # )(wrapped)


def maybe_mk_fsp_shm(
    sym: str,
    target: fsp,
    # field_name: str,
    # display_name: Optional[str] = None,
    readonly: bool = True,

) -> (ShmArray, bool):
    '''
    Allocate a single row shm array for an symbol-fsp pair if none
    exists, otherwise load the shm already existing for that token.

    '''
    uid = tractor.current_actor().uid

    # load declared fields from fsp and allocate in
    # shm array.
    # if not display_name:
    #     display_name = field_name

    # TODO: load function here and introspect
    # return stream type(s)
    display_name = target.__name__

    # TODO: should `index` be a required internal field?
    fsp_dtype = np.dtype(
        [('index', int)] +
        [(field_name, float) for field_name in target.outputs]
    )

    key = f'{sym}.fsp.{display_name}.{".".join(uid)}'

    shm, opened = maybe_open_shm_array(
        key,
        # TODO: create entry for each time frame
        dtype=fsp_dtype,
        readonly=True,
    )
    return shm, opened
