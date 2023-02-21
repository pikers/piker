# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet

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
micro-ORM for coupling ``pydantic`` models with Qt input/output widgets.

"""
from __future__ import annotations
from typing import (
    Generic,
    TypeVar,
    Callable,
)

# from pydantic import BaseModel, validator
from pydantic.generics import GenericModel
from PyQt5.QtWidgets import (
    QWidget,
    QComboBox,
)

from ._forms import (
    # FontScaledDelegate,
    Edit,
)
from ..data.types import Struct


DataType = TypeVar('DataType')


class Field(GenericModel, Generic[DataType]):
    widget_factory: Callable[
        [QWidget, 'Field'],
        QWidget
    ] | None = None
    value: DataType | None = None


class Selection(Field[DataType], Generic[DataType]):
    '''Model which maps to a finite set of drop down entries declared as
    a ``dict[str, DataType]``.

    '''
    widget_factory = QComboBox
    options: dict[str, DataType]
    # value: DataType = None

    # @validator('value')  # , always=True)
    def set_value_first(
        cls,

        v: DataType,
        values: dict[str, DataType],

    ) -> DataType:
        '''If no initial value is set, use the first in
        the ``options`` dict.

        '''
        # breakpoint()
        options = values['options']
        if v is None:
            return next(options.values())
        else:
            assert v in options, f'{v} is not in {options}'
            return v


# class SizeUnit(Enum):

#     currency = '$ size'
#     percent_of_port = '% of port'
#     shares = '# shares'


# class Weighter(str, Enum):
#     uniform = 'uniform'


class Edit(Field[DataType], Generic[DataType]):
    '''An edit field which takes a number.
    '''
    widget_factory = Edit


class AllocatorPane(Struct):

    account = Selection[str](
        options=dict.fromkeys(
            ['paper', 'ib.paper', 'ib.margin'],
            'paper',
        ),
    )

    allocate = Selection[str](
        # options=list(Size),
        options={
            '$ size': 'currency',
            '% of port': 'percent_of_port',
            '# shares': 'shares',
        },
        # TODO: save/load from config and/or last session
        # value='currency'
    )
    weight = Selection[str](
        options={
            'uniform': 'uniform',
        },
        # value='uniform',
    )
    size = Edit[float](value=1000)
    slots = Edit[int](value=4)
