"""
Questrade API backend.
"""
# from ..config import load
from ..log import get_logger, get_console_log
from pprint import pformat
import sys
import trio

# TODO: move to urllib3/requests once supported
import asks
asks.init('trio')

log = get_logger('questrade')

_refresh_token_ep = 'https://login.questrade.com/oauth2/token'
_version = 'v1'


class ResponseError(Exception):
    "Non-200 OK response code"


def err_on_status(resp: asks.response_objects.Response) -> None:
    """Raise error on non-200 OK response.
    """
    if not resp.status_code == 200:
        raise ResponseError(resp.body)


class Client:
    """API client suitable for use as a long running broker daemon.
    """
    def __init__(self, refresh_token: str):
        self._sess = asks.Session()
        self.refresh_token = refresh_token
        self.ud = None  # userdata

    @classmethod
    async def from_token(cls, refresh_token: str):
        client = cls(refresh_token)
        await client.refresh_access()
        return client

    async def refresh_access(self) -> None:
        """Acquire new ``refresh_token`` and ``access_token`` if necessary.

        """
        resp = await self._sess.get(
            _refresh_token_ep,
            params={'grant_type': 'refresh_token',
                    'refresh_token': self.refresh_token}
        )
        err_on_status(resp)
        data = resp.json()

        self._sess.base_location = data['api_server'] + _version
        self.access_token = data['access_token']
        self.expires_in = data['expires_in']
        self.refresh_token = data['refresh_token']
        self.token_type = data['token_type']

        # set auth token for the session
        self._sess.headers.update(
            {'Authorization': f'{self.token_type} {self.access_token}'}
        )

    async def get_user_data(self) -> dict:
        """Get and store user data from the ``accounts`` endpoint.
        """
        resp = await self._sess.get(path='/accounts')
        err_on_status(resp)
        self.ud = resp.json()
        return self.ud


async def get_client(refresh_token: str = None) -> Client:
    """Gain api access using a user generated token.

    See the instructions::

        http://www.questrade.com/api/documentation/getting-started
    """
    if refresh_token is None:
        # sanitize?
        refresh_token = input(
            "Questrade access token:")

    log.info("Waiting for initial API access...")
    return await Client.from_token(refresh_token)


async def serve_forever(refresh_token: str = None) -> None:
    """Start up a client and serve until terminated.
    """
    client = await get_client(refresh_token=refresh_token)
    data = await client.get_user_data()
    log.info(pformat(data))
    return client


def main() -> None:
    log = get_console_log('INFO')
    argv = sys.argv[1:]

    refresh_token = None
    if argv:
        refresh_token = argv[0]

    # main loop
    try:
        client = trio.run(serve_forever, refresh_token)
    except Exception as err:
        log.exception(err)
    else:
        log.info(
            f"\nLast refresh_token: {client.refresh_token}\n"
            f"Last access_token: {client.access_token}"
        )
