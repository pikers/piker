# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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
Sub-sys module commons.

"""
from collections import ChainMap
from functools import partial
from typing import Any

from ..log import (
    get_logger,
    get_console_log,
)
from piker.data.types import Struct
subsys: str = 'piker.clearing'

log = get_logger(subsys)

get_console_log = partial(
    get_console_log,
    name=subsys,
)


class OrderDialogs(Struct):
    '''
    Order control dialog (and thus transaction) tracking via
    message recording.

    Allows easily recording messages associated with a given set of
    order control transactions and looking up the latest field
    state using the entire (reverse chronological) msg flow.

    '''
    _flows: dict[str, ChainMap] = {}

    def add_msg(
        self,
        oid: str,
        msg: dict,
    ) -> None:

        # NOTE: manually enter a new map on the first msg add to
        # avoid creating one with an empty dict first entry in
        # `ChainMap.maps` which is the default if none passed at
        # init.
        cm: ChainMap = self._flows.get(oid)
        if cm:
            cm.maps.insert(0, msg)
        else:
            cm = ChainMap(msg)
            self._flows[oid] = cm

    # TODO: wrap all this in the `collections.abc.Mapping` interface?
    def get(
        self,
        oid: str,

    ) -> ChainMap[str, Any]:
        '''
        Return the dialog `ChainMap` for provided id.

        '''
        return self._flows.get(oid, None)

    def pop(
        self,
        oid: str,

    ) -> ChainMap[str, Any]:
        '''
        Pop and thus remove the `ChainMap` containing the msg flow
        for the given order id.

        '''
        if (flow := self._flows.pop(oid, None)) is None:
            log.warning(f'No flow found for oid: {oid}')

        return flow
