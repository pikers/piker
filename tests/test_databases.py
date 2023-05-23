from typing import AsyncContextManager
import logging

import trio
from elasticsearch import (
    Elasticsearch,
    ConnectionError,
)

from piker.service import marketstore
from piker.service import elastic


def test_marketstore_startup_and_version(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,
    root_conf: dict,
):
    '''
    Verify marketstore tsdb starts up and we can
    connect with a client to do basic API reqs.

    '''
    async def main():
        user_conf: dict = {
            'grpc_listen_port': 5995 + 6,
            'ws_listen_port': 5993 + 6,
        }

        dname: str  # service name
        config: dict  # service name

        async with (
            open_test_pikerd(
                loglevel=loglevel,
                # tsdb=True
            ) as (
                _,  # host
                _,  # port
                pikerd_portal,
                services,
            ),

            marketstore.start_ahab_daemon(
                services,
                user_conf,
                loglevel=loglevel,

            ) as (dname, config)
        ):
            # ensure user config was applied
            for k, v in user_conf.items():
                assert config[k] == v

            # netconf: dict = root_conf['network']
            # tsdbconf = netconf['tsdb']

            # TODO: we should probably make this connection poll
            # loop part of the `get_client()` implementation no?

            # XXX NOTE: we use a retry-connect loop because it seems
            # that if we connect *too fast* to a booting container
            # instance (i.e. if mkts's IPC machinery isn't up early
            # enough) the client will hang on req-resp submissions. So,
            # instead we actually reconnect the client entirely in
            # a loop until we get a response.
            for _ in range(3):

                # NOTE: default sockaddr is embedded within
                async with marketstore.get_client(
                    host='localhost',
                    port=user_conf['grpc_listen_port'],

                ) as client:
                    print(f'Client is up @ {user_conf}!')

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
            else:
                raise RuntimeError('Failed to connect to {conf}!')

            # gracefully teardown docker-daemon-service
            print(f'Cancelling docker service {dname}')

    trio.run(main)


def test_elasticsearch_startup_and_version(
    open_test_pikerd: AsyncContextManager,
    loglevel: str,
    log: logging.Logger,
):
    '''
    Verify elasticsearch starts correctly (like at some point before
    infinity time)..

    '''
    async def main():
        port: int = 19200
        user_conf: dict = {
            'port': port,
        }

        dname: str  # service name
        config: dict  # service name

        async with (
            open_test_pikerd(
                loglevel=loglevel,
            ) as (
                _,  # host
                _,  # port
                pikerd_portal,
                services,
            ),
            elastic.start_ahab_daemon(
                services,
                user_conf,
                loglevel=loglevel,

            ) as (dname, config)
        ):
            # TODO: much like the above connect loop for mkts, we should
            # probably make this sync start part of the
            # ``open_client()`` implementation?
            for i in range(240):
                with Elasticsearch(
                    hosts=[f'http://localhost:{port}']
                ) as es:
                    try:

                        resp = es.info()
                        assert (
                            resp['version']['number']
                            ==
                            elastic._config['version']
                        )
                        print(
                            "OMG ELASTIX FINALLY CUKCING CONNECTED!>!>!\n"
                            f'resp: {resp}'
                        )
                        break

                    except ConnectionError:
                        log.exception(
                            f'RETRYING client connection for {i} time!'
                        )
                        await trio.sleep(1)
                        continue

    trio.run(main)
