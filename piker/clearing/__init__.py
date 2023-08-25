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
Market machinery for order executions, book, management.

"""
from ..log import get_logger
from ._client import (
    open_ems,
    OrderClient,
)
from ._ems import (
    open_brokerd_dialog,
)
from ._util import OrderDialogs
from ._messages import(
    Order,
    Status,
    Cancel,

    # TODO: deprecate these and replace end-2-end with
    # client-side-dialog set above B)
    # https://github.com/pikers/piker/issues/514
    BrokerdPosition
)


__all__ = [
    'FeeModel',
    'open_ems',
    'OrderClient',
    'open_brokerd_dialog',
    'OrderDialogs',
    'Order',
    'Status',
    'Cancel',
    'BrokerdPosition'

]

log = get_logger(__name__)
