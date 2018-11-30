import pytest
import tractor
from piker import log


def pytest_addoption(parser):
    parser.addoption("--ll", action="store", dest='loglevel',
                     default=None, help="logging level to set when testing")


@pytest.fixture(scope='session', autouse=True)
def loglevel(request):
    orig = tractor.log._default_loglevel
    level = tractor.log._default_loglevel = request.config.option.loglevel
    log.get_console_log(level)
    yield level
    tractor.log._default_loglevel = orig


@pytest.fixture
def brokerconf():
    from piker.brokers import config
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
