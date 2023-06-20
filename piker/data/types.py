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

    def pformat(self) -> str:
        return f'Struct({pformat(self.to_dict())})'

    def copy(
        self,
        update: dict | None = None,

    ) -> msgspec.Struct:
        '''
        Validate-typecast all self defined fields, return a copy of
        us with all such fields.

        NOTE: This is kinda like the default behaviour in
        `pydantic.BaseModel` except a copy of the object is
        returned making it compat with `frozen=True`.

        '''
        if update:
            for k, v in update.items():
                setattr(self, k, v)

        # NOTE: roundtrip serialize to validate
        # - enode to msgpack binary format,
        # - decode that back to a struct.
        return msgspec.msgpack.Decoder(
            type=type(self)
        ).decode(
            msgspec.msgpack.Encoder().encode(self)
        )

    def typecast(
        self,
        # fields: list[str] | None = None,

    ) -> None:
        '''
        Cast all fields using their declared type annotations
        (kinda like what `pydantic` does by default).

        NOTE: this of course won't work on frozen types, use
        ``.copy()`` above in such cases.

        '''
        annots: dict = self.__annotations__
        for fname, ftype in annots.items():
            if isinstance(ftype, str):
                print(f'{self} has `str` annotations!?\n{annots}\n')
                ftype = getattr(builtins, ftype)

            attr  = getattr(self, fname)
            setattr(
                self,
                fname,
                ftype(attr),
            )
