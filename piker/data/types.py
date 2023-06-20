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

from msgspec import (
    msgpack,
    Struct,
    structs,
)


class Struct(
    Struct,

    # https://jcristharif.com/msgspec/structs.html#tagged-unions
    # tag='pikerstruct',
    # tag=True,
):
    '''
    A "human friendlier" (aka repl buddy) struct subtype.

    '''
    def to_dict(self) -> dict:
        '''
        Like it sounds.. direct delegation to:
        https://jcristharif.com/msgspec/api.html#msgspec.structs.asdict

        TODO: probably just drop this method since it's now a built-int method?

        '''
        return structs.asdict(self)

    def pformat(self) -> str:
        return f'Struct({pformat(self.to_dict())})'

    def copy(
        self,
        update: dict | None = None,

    ) -> Struct:
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
        return msgpack.Decoder(type=type(self)).decode(
            msgpack.Encoder().encode(self)
        )

    def typecast(
        self,

        # TODO: allow only casting a named subset?
        # fields: set[str] | None = None,

    ) -> None:
        '''
        Cast all fields using their declared type annotations
        (kinda like what `pydantic` does by default).

        NOTE: this of course won't work on frozen types, use
        ``.copy()`` above in such cases.

        '''
        # https://jcristharif.com/msgspec/api.html#msgspec.structs.fields
        fi: structs.FieldInfo
        for fi in structs.fields(self):
            setattr(
                self,
                fi.name,
                fi.type(getattr(self, fi.name)),
            )
