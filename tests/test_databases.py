import trio

from typing import AsyncContextManager

from elasticsearch import (
    Elasticsearch,
    ConnectionError,
)
from piker.service import marketstore


def test_marketstore_startup_and_version(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,
):
    '''
    Verify marketstore tsdb starts up and we can
    connect with a client to do basic API reqs.

    '''
    async def main():

        async with (
            open_test_pikerd(
                loglevel=loglevel,
                tsdb=True
            ) as (
                _,  # host
                _,  # port
                pikerd_portal,
                services,
            ),
        ):
            # XXX NOTE: we use a retry-connect loop because it seems
            # that if we connect *too fast* to a booting container
            # instance (i.e. if mkts's IPC machinery isn't up early
            # enough) the client will hang on req-resp submissions. So,
            # instead we actually reconnect the client entirely in
            # a loop until we get a response.
            for _ in range(3):

                # NOTE: default sockaddr is embedded within
                async with marketstore.get_client() as client:

                    with trio.move_on_after(1) as cs:
                        syms = await client.list_symbols()

                    if cs.cancelled_caught:
                        continue

                    # should be an empty db (for now) since we spawn
                    # marketstore in a ephemeral test-harness dir.
                    assert not syms
                    print(f'RX syms resp: {syms}')

                    assert (
                        len(await client.server_version()) ==
                        len('3862e9973da36cfc6004b88172c08f09269aaf01')
                    )
                    print('VERSION CHECKED')

                break  # get out of retry-connect loop

    trio.run(main)


def test_elasticsearch_startup_and_version(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,
):
    '''
    Verify elasticsearch starts correctly

    '''
    async def main():
        port = 19200

        async with (
            open_test_pikerd(
                loglevel=loglevel,
                es=True
            ) as (
                _,  # host
                _,  # port
                pikerd_portal,
                services,
            ),
        ):

            for _ in range(240):
                try:
                    es = Elasticsearch(hosts=[f'http://localhost:{port}'])
                except ConnectionError:
                    await trio.sleep(1)
                    continue

                assert es.info()['version']['number'] == '7.17.4'

    trio.run(main)
