"""
Questrade API backend.
"""
import trio
from . import config
from ..log import get_logger
from pprint import pformat
import time
import datetime
from async_generator import asynccontextmanager

# TODO: move to urllib3/requests once supported
import asks
asks.init('trio')

log = get_logger('questrade')

_refresh_token_ep = 'https://login.questrade.com/oauth2/'
_version = 'v1'


class QuestradeError(Exception):
    "Non-200 OK response code"


def resproc(
    resp: asks.response_objects.Response,
    return_json: bool = True
) -> asks.response_objects.Response:
    """Process response and return its json content.

    Raise the appropriate error on non-200 OK responses.
    """
    data = resp.json()
    log.debug(f"Received json contents:\n{pformat(data)}\n")

    if not resp.status_code == 200:
        raise QuestradeError(resp.body)

    return data if return_json else resp


class API:
    """Questrade API at its finest.
    """
    def __init__(self, session: asks.Session):
        self._sess = session

    async def _request(self, path: str) -> dict:
        resp = await self._sess.get(path=f'/{path}')
        return resproc(resp)

    async def accounts(self):
        return await self._request('accounts')

    async def time(self):
        return await self._request('time')


class Client:
    """API client suitable for use as a long running broker daemon.
    """
    def __init__(self, config: dict):
        self._sess = asks.Session()
        self.api = API(self._sess)
        self.access_data = config
        self.user_data = {}

    async def _new_auth_token(self) -> dict:
        """Request a new api authorization ``refresh_token``.

        Gain api access using either a user provided or existing token.
        See the instructions::

        http://www.questrade.com/api/documentation/getting-started
        http://www.questrade.com/api/documentation/security
        """
        resp = await self._sess.get(
            _refresh_token_ep + 'token',
            params={'grant_type': 'refresh_token',
                    'refresh_token': self.access_data['refresh_token']}
        )
        data = resproc(resp)
        self.access_data.update(data)

        return data

    def _prep_sess(self) -> None:
        """Fill http session with auth headers and a base url.
        """
        data = self.access_data
        # set access token header for the session
        self._sess.headers.update({
            'Authorization': (f"{data['token_type']} {data['access_token']}")})
        # set base API url (asks shorthand)
        self._sess.base_location = self.access_data['api_server'] + _version

    async def _revoke_auth_token(self) -> None:
        """Revoke api access for the current token.
        """
        token = self.access_data['refresh_token']
        log.debug(f"Revoking token {token}")
        resp = await asks.post(
            _refresh_token_ep + 'revoke',
            headers={'token': token}
        )
        return resp

    async def ensure_access(self, force_refresh: bool = False) -> dict:
        """Acquire new ``access_token`` and/or ``refresh_token`` if necessary.

        Checks if the locally cached (file system) ``access_token`` has expired
        (based on a ``expires_at`` time stamp stored in the brokers.ini config)
        expired (normally has a lifetime of 3 days). If ``false is set then
        and refreshs token if necessary using the ``refresh_token``. If the
        ``refresh_token`` has expired a new one needs to be provided by the user.
        """
        access_token = self.access_data.get('access_token')
        expires = float(self.access_data.get('expires_at', 0))
        expires_stamp = datetime.datetime.fromtimestamp(
            expires).strftime('%Y-%m-%d %H:%M:%S')
        if not access_token or (expires < time.time()) or force_refresh:
            log.info(f"Refreshing access token {access_token} which expired at"
                     f" {expires_stamp}")
            data = await self._new_auth_token()

            # store absolute token expiry time
            self.access_data['expires_at'] = time.time() + float(
                data['expires_in'])
        else:
            log.info(f"\nCurrent access token {access_token} expires at"
                     f" {expires_stamp}\n")

        self._prep_sess()
        return self.access_data


def get_config() -> "configparser.ConfigParser":
    conf, path = config.load()
    if not conf.has_section('questrade') or (
        not conf['questrade'].get('refresh_token')
    ):
        log.warn(
            f"No valid refresh token could be found in {path}")
        # get from user
        refresh_token = input("Please provide your Questrade access token: ")
        conf['questrade'] = {'refresh_token': refresh_token}

    return conf


async def token_refresher(client):
    """Coninually refresh the ``access_token`` near its expiry time.
    """
    while True:
        await trio.sleep(float(client.access_data['expires_at']) - time.time() - .1)
        await client.ensure_access()


@asynccontextmanager
async def get_client() -> Client:
    """Spawn a broker client.
    """
    conf = get_config()
    log.debug(f"Loaded config:\n{pformat(dict(conf['questrade']))}\n")
    client = Client(dict(conf['questrade']))
    await client.ensure_access()

    try:
        log.debug("Check time to ensure access token is valid")
        try:
            await client.api.time()
        except Exception as err:
            # access token is likely no good
            log.warn(f"Access token {client.access_data['access_token']} seems"
                     f" expired, forcing refresh")
            await client.ensure_access(force_refresh=True)
            await client.api.time()

        accounts = await client.api.accounts()
        log.info(f"Available accounts:\n{pformat(accounts)}\n")
        yield client
    finally:
        # save access creds for next run
        conf['questrade'] = client.access_data
        config.write(conf)


async def serve_forever() -> None:
    """Start up a client and serve until terminated.
    """
    async with get_client() as client:
        # pretty sure this doesn't work
        # await client._revoke_auth_token()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(token_refresher, client)
