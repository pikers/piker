import pytest
import trio

from typing import AsyncContextManager

from piker._daemon import Services
from piker.log import get_logger

from elasticsearch import Elasticsearch


# def test_marketstore( open_test_pikerd: AsyncContextManager):

'''
Verify marketstore starts and closes correctly

'''


def test_elasticsearch(
    open_test_pikerd: AsyncContextManager,
):
    '''
    Verify elasticsearch starts and closes correctly

    '''

    log = get_logger(__name__)

    # log.info('#################### Starting test ####################')

    async def main():
        port = 19200

        async with open_test_pikerd(
            loglevel='info',
            es=True
        ) as (s, i, pikerd_portal, services):

            es = Elasticsearch(hosts=[f'http://localhost:{port}'])
            assert es.info()['version']['number'] == '7.17.4'


    trio.run(main)
