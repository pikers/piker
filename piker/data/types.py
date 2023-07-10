# piker: trading gear for hackers
# Copyright (C) (in stewardship for pikers)
#  - Tyler Goodlet
#  - Guillermo Rodriguez

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
Extensions to built-in or (heavily used but 3rd party) friend-lib
types.

'''
from collections import UserList
from pprint import (
    pformat,
)
from typing import Any

from msgspec import (
    msgpack,
    Struct,
    structs,
)


class DiffDump(UserList):
    '''
    Very simple list delegator that repr() dumps (presumed) tuple
    elements of the form `tuple[str, Any, Any]` in a nice
    multi-line readable form for analyzing `Struct` diffs.

    '''
    def __repr__(self) -> str:
        if not len(self):
            return super().__repr__()

        # format by displaying item pair's ``repr()`` on multiple,
        # indented lines such that they are more easily visually
        # comparable when printed to console when printed to
        # console.
        repstr: str = '[\n'
        for k, left, right in self:
            repstr += (
                f'({k},\n'
                f'\t{repr(left)},\n'
                f'\t{repr(right)},\n'
                ')\n'
            )
        repstr += ']\n'
        return repstr


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

    def __sub__(
        self,
        other: Struct,

    ) -> DiffDump[tuple[str, Any, Any]]:
        '''
        Compare fields/items key-wise and return a ``DiffDump``
        for easy visual REPL comparison B)

        '''
        diffs: DiffDump[tuple[str, Any, Any]] = DiffDump()
        for fi in structs.fields(self):
            attr_name: str = fi.name
            ours: Any = getattr(self, attr_name)
            theirs: Any = getattr(other, attr_name)
            if ours != theirs:
                diffs.append((
                    attr_name,
                    ours,
                    theirs,
                ))

        return diffs
