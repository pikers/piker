import pytest
import trio

from typing import AsyncContextManager

from piker._daemon import Services
from piker.log import get_logger

from elasticsearch import Elasticsearch
from piker.data import marketstore

def test_marketstore(
    open_test_pikerd: AsyncContextManager,
):

    '''
    Verify marketstore starts correctly

    '''
    log = get_logger(__name__)

    async def main():
        # port = 5995

        async with (
            open_test_pikerd(
                loglevel='info',
                tsdb=True
            ) as (s, i, pikerd_portal, services),
            marketstore.get_client() as client
        ):

            assert (
                len(await client.server_version()) ==
                len('3862e9973da36cfc6004b88172c08f09269aaf01')
            )


    trio.run(main)


def test_elasticsearch(
    open_test_pikerd: AsyncContextManager,
):
    '''
    Verify elasticsearch starts correctly

    '''

    log = get_logger(__name__)

    async def main():
        port = 19200

        async with open_test_pikerd(
            loglevel='info',
            es=True
        ) as (s, i, pikerd_portal, services):

            es = Elasticsearch(hosts=[f'http://localhost:{port}'])
            assert es.info()['version']['number'] == '7.17.4'


    trio.run(main)
