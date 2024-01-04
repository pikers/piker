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
from tractor import Portal

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
    # TODO: should this be a set or should we complain
    # on duplicates?
    addrs: list[tuple[str, int]] = []

    # TODO: table of uids to sockaddrs
    peers: dict[
        tuple[str, str],
        tuple[str, int],
    ] = {}


_tractor_kwargs: dict[str, Any] = {}


@acm
async def open_registry(
    addrs: list[tuple[str, int]],
    ensure_exists: bool = True,

) -> list[tuple[str, int]]:
    '''
    Open the service-actor-discovery registry by returning a set of
    tranport socket-addrs to registrar actors which may be
    contacted and queried for similar addresses for other
    non-registrar actors.

    '''
    global _tractor_kwargs
    actor = tractor.current_actor()
    uid = actor.uid
    preset_reg_addrs: list[tuple[str, int]] = Registry.addrs
    if (
        preset_reg_addrs
        and addrs
    ):
        if preset_reg_addrs != addrs:
            # if any(addr in preset_reg_addrs for addr in addrs):
            diff: set[tuple[str, int]] = set(preset_reg_addrs) - set(addrs)
            if diff:
                log.warning(
                    f'`{uid}` requested only subset of registrars: {addrs}\n'
                    f'However there are more @{diff}'
                )
            else:
                raise RuntimeError(
                    f'`{uid}` has non-matching registrar addresses?\n'
                    f'request: {addrs}\n'
                    f'already set: {preset_reg_addrs}'
                )

    was_set: bool = False

    if (
        not tractor.is_root_process()
        and not Registry.addrs
    ):
        Registry.addrs.extend(actor.reg_addrs)

    if (
        ensure_exists
        and not Registry.addrs
    ):
        raise RuntimeError(
            f"`{uid}` registry should already exist but doesn't?"
        )

    if (
        not Registry.addrs
    ):
        was_set = True
        Registry.addrs = addrs or [_default_reg_addr]

    # NOTE: only spot this seems currently used is inside
    # `.ui._exec` which is the (eventual qtloops) bootstrapping
    # with guest mode.
    _tractor_kwargs['registry_addrs'] = Registry.addrs

    try:
        yield Registry.addrs
    finally:
        # XXX: always clear the global addr if we set it so that the
        # next (set of) calls will apply whatever new one is passed
        # in.
        if was_set:
            Registry.addrs = None


@acm
async def find_service(
    service_name: str,
    registry_addrs: list[tuple[str, int]] | None = None,

    first_only: bool = True,

) -> (
    Portal
    | list[Portal]
    | None
):

    reg_addrs: list[tuple[str, int]]
    async with open_registry(
        addrs=(
            registry_addrs
            # NOTE: if no addr set is passed assume the registry has
            # already been opened and use the previously applied
            # startup set.
            or Registry.addrs
        ),
    ) as reg_addrs:
        log.info(f'Scanning for service `{service_name}`')

        maybe_portals: list[Portal] | Portal | None

        # attach to existing daemon by name if possible
        async with tractor.find_actor(
            service_name,
            registry_addrs=reg_addrs,
            only_first=first_only,  # if set only returns single ref
        ) as maybe_portals:
            if not maybe_portals:
                yield None
                return

            yield maybe_portals


async def check_for_service(
    service_name: str,

) -> None | tuple[str, int]:
    '''
    Service daemon "liveness" predicate.

    '''
    async with (
        open_registry(ensure_exists=False) as reg_addr,
        tractor.query_actor(
            service_name,
            arbiter_sockaddr=reg_addr,
        ) as sockaddr,
    ):
        return sockaddr
