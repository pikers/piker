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
Actor-runtime service orchestration machinery.

"""
from __future__ import annotations

from ._util import log
from ._mngr import Services
from ._registry import (  # noqa
    _tractor_kwargs,
    _default_reg_addr,
    _default_registry_host,
    _default_registry_port,
    open_registry,
    find_service,
    check_for_service,
)
from ._daemon import (  # noqa
    maybe_spawn_daemon,
    spawn_brokerd,
    maybe_spawn_brokerd,
    spawn_emsd,
    maybe_open_emsd,
)
from ._actor_runtime import (
    open_piker_runtime,
    maybe_open_pikerd,
    open_pikerd,
    get_tractor_runtime_kwargs,
)


__all__ = [
    'check_for_service',
    'Services',
    'maybe_spawn_daemon',
    'spawn_brokerd',
    'maybe_spawn_brokerd',
    'spawn_emsd',
    'maybe_open_emsd',
    'open_piker_runtime',
    'maybe_open_pikerd',
    'open_pikerd',
    'get_tractor_runtime_kwargs',
]
