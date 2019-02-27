import os

import pytest
import tractor
import trio
from piker import log
from piker.brokers import questrade, config


def pytest_addoption(parser):
    parser.addoption("--ll", action="store", dest='loglevel',
                     default=None, help="logging level to set when testing")
    parser.addoption("--confdir", default=None,
                     help="Use a practice API account")


@pytest.fixture(scope='session', autouse=True)
def loglevel(request):
    orig = tractor.log._default_loglevel
    level = tractor.log._default_loglevel = request.config.option.loglevel
    log.get_console_log(level)
    yield level
    tractor.log._default_loglevel = orig


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


@pytest.fixture(scope='session')
def travis():
    is_travis = os.environ.get('TRAVIS', False)
    if is_travis:
        # this directory is cached, see .travis.yaml
        cache_dir = config.get_broker_conf_path()
        refresh_token = os.environ['QT_REFRESH_TOKEN']

        def write_with_token(token):
            conf, path = config.load(cache_dir)
            conf.setdefault('questrade', {}).update(
                {'refresh_token': token,
                 'is_practice': 'True'}
            )
            config.write(conf, path)

        async def ensure_config():
            # try to refresh current token using cached brokers config
            # if it fails fail try using the refresh token provided by the
            # env var and if that fails stop the test run here.
            try:
                async with questrade.get_client(ask_user=False):
                    pass
            except (
                KeyError, ValueError,
                questrade.BrokerError, questrade.QuestradeError
            ):
                # 3 cases:
                # - config doesn't have a ``refresh_token`` k/v
                # - cache dir does not exist yet
                # - current token is expired; take it form env var
                write_with_token(refresh_token)

                async with questrade.get_client(ask_user=False):
                    pass

        # XXX ``pytest_trio`` doesn't support scope or autouse
        trio.run(ensure_config)


@pytest.fixture(scope='session', autouse=True)
def brokerconf(request, test_config, travis):
    """If the `--confdir` flag is not passed use the
    broker config file found in that dir.
    """
    confdir = request.config.option.confdir
    if confdir is not None:
        config._override_config_dir(confdir)

    return config.load()[0]


@pytest.fixture
def us_symbols():
    return ['TSLA', 'AAPL', 'CGC', 'CRON']


@pytest.fixture
def tmx_symbols():
    return ['APHA.TO', 'WEED.TO', 'ACB.TO']


@pytest.fixture
def cse_symbols():
    return ['TRUL.CN', 'CWEB.CN', 'SNN.CN']
