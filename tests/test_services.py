'''
Actor tree daemon sub-service verifications

'''
from typing import AsyncContextManager

import trio
import tractor


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

        async with (
            open_test_pikerd(
                reg_addr=daemon_addr,
            ) as (_, _, pikerd_portal),

            tractor.wait_for_actor(
                'pikerd',
                arbiter_sockaddr=daemon_addr,
            ) as portal,
        ):
            assert pikerd_portal.channel.raddr == daemon_addr
            assert pikerd_portal.channel.raddr == portal.channel.raddr

    trio.run(main)
