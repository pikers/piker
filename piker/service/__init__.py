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

'''
Actor runtime primtives and (distributed) service APIs for,

- daemon-service mgmt: `_daemon` (i.e. low-level spawn and supervise machinery
  for sub-actors like `brokerd`, `emsd`, datad`, etc.)

- service-actor supervision (via `trio` tasks) API: `._mngr`

- discovery interface (via light wrapping around `tractor`'s built-in
  prot): `._registry`

- `docker` cntr SC supervision for use with `trio`: `_ahab`
  - wrappers for marketstore and elasticsearch dbs
  => TODO: maybe to (re)move elsewhere?

'''
from ._mngr import Services as Services
from ._registry import (
    _tractor_kwargs as _tractor_kwargs,
    _default_reg_addr as _default_reg_addr,
    _default_registry_host as _default_registry_host,
    _default_registry_port as _default_registry_port,

    open_registry as open_registry,
    find_service as find_service,
    check_for_service as check_for_service,
)
from ._daemon import (
    maybe_spawn_daemon as maybe_spawn_daemon,
    spawn_emsd as spawn_emsd,
    maybe_open_emsd as maybe_open_emsd,
)
from ._actor_runtime import (
    open_piker_runtime as open_piker_runtime,
    maybe_open_pikerd as maybe_open_pikerd,
    open_pikerd as open_pikerd,
    get_runtime_vars as get_runtime_vars,
)
from ..brokers._daemon import (
    spawn_brokerd as spawn_brokerd,
    maybe_spawn_brokerd as maybe_spawn_brokerd,
)
