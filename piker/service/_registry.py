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
Inter-actor "discovery" (protocol) layer.

"""
from __future__ import annotations
from contextlib import (
    asynccontextmanager as acm,
)
from typing import (
    Any,
)

import tractor

from ._util import (
    log,  # sub-sys logger
)

_default_registry_host: str = '127.0.0.1'
_default_registry_port: int = 6116
_default_reg_addr: tuple[str, int] = (
    _default_registry_host,
    _default_registry_port,
)


# NOTE: this value is set as an actor-global once the first endpoint
# who is capable, spawns a `pikerd` service tree.
_registry: Registry | None = None


class Registry:
    addr: None | tuple[str, int] = None

    # TODO: table of uids to sockaddrs
    peers: dict[
        tuple[str, str],
        tuple[str, int],
    ] = {}


_tractor_kwargs: dict[str, Any] = {}


@acm
async def open_registry(
    addr: None | tuple[str, int] = None,
    ensure_exists: bool = True,

) -> tuple[str, int]:

    global _tractor_kwargs
    actor = tractor.current_actor()
    uid = actor.uid
    if (
        Registry.addr is not None
        and addr
    ):
        raise RuntimeError(
            f'`{uid}` registry addr already bound @ {_registry.sockaddr}'
        )

    was_set: bool = False

    if (
        not tractor.is_root_process()
        and Registry.addr is None
    ):
        Registry.addr = actor._arb_addr

    if (
        ensure_exists
        and Registry.addr is None
    ):
        raise RuntimeError(
            f"`{uid}` registry should already exist bug doesn't?"
        )

    if (
        Registry.addr is None
    ):
        was_set = True
        Registry.addr = addr or _default_reg_addr

    _tractor_kwargs['arbiter_addr'] = Registry.addr

    try:
        yield Registry.addr
    finally:
        # XXX: always clear the global addr if we set it so that the
        # next (set of) calls will apply whatever new one is passed
        # in.
        if was_set:
            Registry.addr = None


@acm
async def find_service(
    service_name: str,
) -> tractor.Portal | None:

    async with open_registry() as reg_addr:
        log.info(f'Scanning for service `{service_name}`')
        # attach to existing daemon by name if possible
        async with tractor.find_actor(
            service_name,
            arbiter_sockaddr=reg_addr,
        ) as maybe_portal:
            yield maybe_portal


async def check_for_service(
    service_name: str,

) -> None | tuple[str, int]:
    '''
    Service daemon "liveness" predicate.

    '''
    async with open_registry(ensure_exists=False) as reg_addr:
        async with tractor.query_actor(
            service_name,
            arbiter_sockaddr=reg_addr,
        ) as sockaddr:
            return sockaddr
