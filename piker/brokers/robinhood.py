"""
Robinhood API backend.
"""
import asks
from async_generator import asynccontextmanager

from ..log import get_logger
from ._util import resproc

log = get_logger('robinhood')

_service_ep = 'https://api.robinhood.com'


class _API:
    """Robinhood API endpoints exposed as methods and wrapped with an
    http session.
    """
    def __init__(self, session: asks.Session):
        self._sess = session

    async def _request(self, path: str, params=None) -> dict:
        resp = await self._sess.get(path=f'/{path}', params=params)
        return resproc(resp, log)

    async def quotes(self, symbols: str) -> dict:
        return await self._request('quotes/', params={'symbols': symbols})

    async def fundamentals(self, symbols: str) -> dict:
        return await self._request('fundamentals/', params={'symbols': symbols})


class Client:
    """API client suitable for use as a long running broker daemon or
    single api requests.
    """
    def __init__(self):
        self._sess = asks.Session()
        self._sess.base_location = _service_ep
        self.api = _API(self._sess)

    async def quote(self, symbols: [str]):
        resp = await self.api.quotes(','.join(symbols))
        results = resp['results']
        return {sym: quote for sym, quote in zip(symbols, results)}


@asynccontextmanager
async def get_client() -> Client:
    """Spawn a RH broker client.
    """
    yield Client()
