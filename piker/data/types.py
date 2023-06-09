# piker: trading gear for hackers
# Copyright (C) Guillermo Rodriguez (in stewardship for piker0)

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
Built-in (extension) types.

"""
import builtins
# import sys
from pprint import pformat

import msgspec


class Struct(
    msgspec.Struct,

    # https://jcristharif.com/msgspec/structs.html#tagged-unions
    # tag='pikerstruct',
    # tag=True,
):
    '''
    A "human friendlier" (aka repl buddy) struct subtype.

    '''
    def to_dict(self) -> dict:
        return {
            f: getattr(self, f)
            for f in self.__struct_fields__
        }

    # Lul, doesn't seem to work that well..
    # def __repr__(self):
    #     # only turn on pprint when we detect a python REPL
    #     # at runtime B)
    #     if (
    #         hasattr(sys, 'ps1')
    #         # TODO: check if we're in pdb
    #     ):
    #         return self.pformat()

    #     return super().__repr__()

    def pformat(self) -> str:
        return f'Struct({pformat(self.to_dict())})'

    def copy(
        self,
        update: dict | None = None,

    ) -> msgspec.Struct:
        '''
        Validate-typecast all self defined fields, return a copy of us
        with all such fields.

        This is kinda like the default behaviour in `pydantic.BaseModel`.

        '''
        if update:
            for k, v in update.items():
                setattr(self, k, v)

        # roundtrip serialize to validate
        return msgspec.msgpack.Decoder(
            type=type(self)
        ).decode(
            msgspec.msgpack.Encoder().encode(self)
        )

    # NOTE XXX: this won't work on frozen types!
    # use ``.copy()`` above in such cases.
    def typecast(
        self,
        # fields: list[str] | None = None,
    ) -> None:
        for fname, ftype_str in self.__annotations__.items():
            ftype = getattr(builtins, ftype_str)
            attr  = getattr(self, fname)
            setattr(
                self,
                fname,
                ftype(attr),
            )
