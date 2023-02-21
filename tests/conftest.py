from contextlib import asynccontextmanager as acm
import os

import pytest
import tractor
from piker import (
    # log,
    config,
)
from piker._daemon import (
    Services,
)


def pytest_addoption(parser):
    parser.addoption("--ll", action="store", dest='loglevel',
                     default=None, help="logging level to set when testing")
    parser.addoption("--confdir", default=None,
                     help="Use a practice API account")


@pytest.fixture(scope='session')
def test_config():
    dirname = os.path.dirname
    dirpath = os.path.abspath(
        os.path.join(
            dirname(os.path.realpath(__file__)),
            'data'
        )
    )
    return dirpath


@pytest.fixture(scope='session', autouse=True)
def confdir(request, test_config):
    '''
    If the `--confdir` flag is not passed use the
    broker config file found in that dir.

    '''
    confdir = request.config.option.confdir
    if confdir is not None:
        config._override_config_dir(confdir)

    return confdir


# @pytest.fixture(scope='session', autouse=True)
# def travis(confdir):
#     is_travis = os.environ.get('TRAVIS', False)
#     if is_travis:
#         # this directory is cached, see .travis.yaml
#         conf_file = config.get_broker_conf_path()
#         refresh_token = os.environ['QT_REFRESH_TOKEN']

#         def write_with_token(token):
#             # XXX don't pass the dir path here since may be
#             # written behind the scenes in the `confdir fixture`
#             if not os.path.isfile(conf_file):
#                 open(conf_file, 'w').close()
#             conf, path = config.load()
#             conf.setdefault('questrade', {}).update(
#                 {'refresh_token': token,
#                  'is_practice': 'True'}
#             )
#             config.write(conf, path)

#         async def ensure_config():
#             # try to refresh current token using cached brokers config
#             # if it fails fail try using the refresh token provided by the
#             # env var and if that fails stop the test run here.
#             try:
#                 async with questrade.get_client(ask_user=False):
#                     pass
#             except (
#                 FileNotFoundError, ValueError,
#                 questrade.BrokerError, questrade.QuestradeError,
#                 trio.MultiError,
#             ):
#                 # 3 cases:
#                 # - config doesn't have a ``refresh_token`` k/v
#                 # - cache dir does not exist yet
#                 # - current token is expired; take it form env var
#                 write_with_token(refresh_token)

#                 async with questrade.get_client(ask_user=False):
#                     pass

#         # XXX ``pytest_trio`` doesn't support scope or autouse
#         trio.run(ensure_config)


_ci_env: bool = os.environ.get('CI', False)


@pytest.fixture(scope='session')
def ci_env() -> bool:
    '''
    Detect CI envoirment.

    '''
    return _ci_env


@pytest.fixture
def us_symbols():
    return ['TSLA', 'AAPL', 'CGC', 'CRON']


@pytest.fixture
def tmx_symbols():
    return ['APHA.TO', 'WEED.TO', 'ACB.TO']


@pytest.fixture
def cse_symbols():
    return ['TRUL.CN', 'CWEB.CN', 'SNN.CN']


@acm
async def _open_test_pikerd(
    tsdb: bool = False,
    es: bool = False,
    reg_addr: tuple[str, int] | None = None,
    **kwargs,

) -> tuple[
    str,
    int,
    tractor.Portal
]:
    '''
    Testing helper to startup the service tree and runtime on
    a different port then the default to allow testing alongside
    a running stack.

    '''
    import random
    from piker._daemon import maybe_open_pikerd

    if reg_addr is None:
        port = random.randint(6e3, 7e3)
        reg_addr = ('127.0.0.1', port)

    # try:
    async with (
        maybe_open_pikerd(
            tsdb=tsdb,
            es=es,
            registry_addr=reg_addr,
            **kwargs,
        ) as service_manager,
    ):
        # this proc/actor is the pikerd
        assert service_manager is Services

        async with tractor.wait_for_actor(
            'pikerd',
            arbiter_sockaddr=reg_addr,
        ) as portal:
            raddr = portal.channel.raddr
            assert raddr == reg_addr
            yield (
                raddr[0],
                raddr[1],
                portal,
                service_manager,
            )


@pytest.fixture
def open_test_pikerd():

    yield _open_test_pikerd

    # TODO: teardown checks such as,
    # - no leaked subprocs or shm buffers
    # - all requested container service are torn down
    # - certain ``tractor`` runtime state?
