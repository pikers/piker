from contextlib import asynccontextmanager as acm
from functools import partial
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
def loglevel(request) -> str:
    return request.config.option.loglevel


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
def confdir(
    request,
    test_config: str,
):
    '''
    If the `--confdir` flag is not passed use the
    broker config file found in that dir.

    '''
    confdir = request.config.option.confdir
    if confdir is not None:
        config._override_config_dir(confdir)

    return confdir


_ci_env: bool = os.environ.get('CI', False)


@pytest.fixture(scope='session')
def ci_env() -> bool:
    '''
    Detect CI envoirment.

    '''
    return _ci_env


@acm
async def _open_test_pikerd(
    reg_addr: tuple[str, int] | None = None,
    loglevel: str = 'warning',
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

    async with (
        maybe_open_pikerd(
            registry_addr=reg_addr,
            loglevel=loglevel,
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
def open_test_pikerd(
    request,
    loglevel: str,
):

    yield partial(
        _open_test_pikerd,

        # bind in level from fixture, which is itself set by
        # `--ll <value>` cli flag.
        loglevel=loglevel,
    )

    # TODO: teardown checks such as,
    # - no leaked subprocs or shm buffers
    # - all requested container service are torn down
    # - certain ``tractor`` runtime state?
