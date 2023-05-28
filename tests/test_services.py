'''
Actor tree daemon sub-service verifications

'''
from typing import (
    AsyncContextManager,
    Callable,
)
from contextlib import asynccontextmanager as acm

from exceptiongroup import BaseExceptionGroup
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
    `open_test_pikerd()` fixture helper and that contact-registry
    address details match up.

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

            # no service tasks should be started
            assert not services.service_tasks

    trio.run(main)


def test_ensure_datafeed_actors(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,
    # cancel_method: str,

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
                await trio.sleep(0.1)

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


def run_test_w_cancel_method(
    cancel_method: str,
    main: Callable,

) -> None:
    '''
    Run our runtime under trio and expect a certain type of cancel condition
    depending on input.

    '''
    cancelled_msg: str = (
        "was remotely cancelled by remote actor (\'pikerd\'")

    if cancel_method == 'sigint':
        with pytest.raises(
            BaseExceptionGroup,
        ) as exc_info:
            trio.run(main)

        multi = exc_info.value

        for suberr in multi.exceptions:
            match suberr:
                # ensure we receive a remote cancellation error caused
                # by the pikerd root actor since we used the
                # `.cancel_service()` API above B)
                case tractor.ContextCancelled():
                    assert cancelled_msg in suberr.args[0]

                case KeyboardInterrupt():
                    pass

                case _:
                    pytest.fail(f'Unexpected error {suberr}')

    elif cancel_method == 'services':

        # XXX NOTE: oddly, when you pass --pdb to pytest, i think since
        # we also use that to enable the underlying tractor debug mode,
        # it causes this to not raise for some reason? So if you see
        # that while changing this test.. it's prolly that.

        with pytest.raises(
            tractor.ContextCancelled
        ) as exc_info:
            trio.run(main)

        assert cancelled_msg in exc_info.value.args[0]

    else:
        pytest.fail(f'Test is broken due to {cancel_method}')


@pytest.mark.parametrize(
    'cancel_method',
    ['services', 'sigint'],
)
def test_ensure_ems_in_paper_actors(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,

    cancel_method: str,

) -> None:

    actor_name: str = 'brokerd'
    backend: str = 'kraken'
    brokerd_name: str = f'{actor_name}.{backend}'

    async def main():

        # type declares
        client: OrderClient
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
                    client,
                    _,  # trades_stream: tractor.MsgStream
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
                # XXX: should be new client with no state from other tests
                assert not client._sent_orders
                assert accounts

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

                    print('ALL SERVICES STARTED, cancelling runtime with:\n'
                          f'-> {cancel_method}')

                    if cancel_method == 'services':
                        await services.cancel_service('emsd')

                    elif cancel_method == 'sigint':
                        raise KeyboardInterrupt

    run_test_w_cancel_method(cancel_method, main)
