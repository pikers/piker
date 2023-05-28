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
Ingestion, for dataz.

Api layer likely in here...

"""
from types import ModuleType
from importlib import import_module

from ._util import get_logger

log = get_logger(__name__)

__ingestors__ = [
    'marketstore',
]


def get_ingestormod(name: str) -> ModuleType:
    """Return the imported ingestor module by name.
    """
    module = import_module('.' + name, 'piker.data')
    # we only allow monkeying because it's for internal keying
    module.name = module.__name__.split('.')[-1]
    return module
