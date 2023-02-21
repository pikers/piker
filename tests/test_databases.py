import pytest
import trio

from typing import AsyncContextManager

from piker._daemon import Services
from piker.log import get_logger


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

	# log = get_logger(__name__)

	# log.info('#################### Starting test ####################')

	async def main():
		port = 19200
		daemon_addr = ('127.0.0.1', port)

		async with (
			open_test_pikerd(
				tsdb=False,
				es=True,
				reg_addr=daemon_addr,
			) as (s, i, pikerd_portal, services),
			# pikerd(),
		):
			assert pikerd_portal.channel.raddr == daemon_addr


	trio.run(main)