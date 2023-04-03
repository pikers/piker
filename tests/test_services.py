'''
Actor tree daemon sub-service verifications

'''
from typing import AsyncContextManager
from contextlib import asynccontextmanager as acm

import pytest
import trio
import tractor

from piker.service import (
    find_service,
    Services,
)
from piker.data import (
    open_feed,
)
from piker.clearing import (
    open_ems,
)
from piker.clearing._messages import (
    BrokerdPosition,
    Status,
)
from piker.clearing._client import (
    OrderClient,
)


def test_runtime_boot(
    open_test_pikerd: AsyncContextManager
):
    '''
    Verify we can boot the `pikerd` service stack using the
    `open_test_pikerd` fixture helper and that registry address details
    match up.

    '''
    async def main():
        port = 6666
        daemon_addr = ('127.0.0.1', port)
        services: Services

        async with (
            open_test_pikerd(
                reg_addr=daemon_addr,
            ) as (_, _, pikerd_portal, services),

            tractor.wait_for_actor(
                'pikerd',
                arbiter_sockaddr=daemon_addr,
            ) as portal,
        ):
            assert pikerd_portal.channel.raddr == daemon_addr
            assert pikerd_portal.channel.raddr == portal.channel.raddr

    trio.run(main)


@acm
async def ensure_service(
    name: str,
    sockaddr: tuple[str, int] | None = None,
) -> None:
    async with find_service(name) as portal:
        remote_sockaddr = portal.channel.raddr
        print(f'FOUND `{name}` @ {remote_sockaddr}')

        if sockaddr:
            assert remote_sockaddr == sockaddr

        yield portal


def test_ensure_datafeed_actors(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,

) -> None:
    '''
    Verify that booting a data feed starts a `brokerd`
    actor and a singleton global `samplerd` and opening
    an order mode in paper opens the `paperboi` service.

    '''
    actor_name: str = 'brokerd'
    backend: str = 'kraken'
    brokerd_name: str = f'{actor_name}.{backend}'

    async def main():
        async with (
            open_test_pikerd(),
            open_feed(
                ['xbtusdt.kraken'],
                loglevel=loglevel,
            ) as feed
        ):
            # halt rt quote streams since we aren't testing them
            await feed.pause()

            async with (
                ensure_service(brokerd_name),
                ensure_service('samplerd'),
            ):
                pass

    trio.run(main)


def test_ensure_ems_in_paper_actors(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,

) -> None:

    actor_name: str = 'brokerd'
    backend: str = 'kraken'
    brokerd_name: str = f'{actor_name}.{backend}'

    async def main():

        # type declares
        book: OrderClient
        trades_stream: tractor.MsgStream
        pps: dict[str, list[BrokerdPosition]]
        accounts: list[str]
        dialogs: dict[str, Status]

        # ensure we timeout after is startup is too slow.
        # TODO: something like this should be our start point for
        # benchmarking end-to-end startup B)
        with trio.fail_after(9):
            async with (
                open_test_pikerd() as (_, _, _, services),

                open_ems(
                    'xbtusdt.kraken',
                    mode='paper',
                    loglevel=loglevel,
                ) as (
                    book,
                    trades_stream,
                    pps,
                    accounts,
                    dialogs,
                ),
            ):
                # there should be no on-going positions,
                # TODO: though eventually we'll want to validate against
                # local ledger and `pps.toml` state ;)
                assert not pps
                assert not dialogs

                pikerd_subservices = ['emsd', 'samplerd']

                async with (
                    ensure_service('emsd'),
                    ensure_service(brokerd_name),
                    ensure_service(f'paperboi.{backend}'),
                ):
                    for name in pikerd_subservices:
                        assert name in services.service_tasks

                    # brokerd.kraken actor should have been started
                    # implicitly by the ems.
                    assert brokerd_name in services.service_tasks

                    print('ALL SERVICES STARTED, terminating..')
                    await services.cancel_service('emsd')

    with pytest.raises(
        tractor._exceptions.ContextCancelled,
    ) as exc_info:
        trio.run(main)

    cancel_msg: str = '_emsd_main()` was remotely cancelled by its caller'
    assert cancel_msg in exc_info.value.args[0]
