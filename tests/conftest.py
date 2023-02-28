from contextlib import asynccontextmanager as acm
from functools import partial
import os
from typing import AsyncContextManager
from pathlib import Path
from shutil import rmtree

import pytest
import tractor
from piker import (
    # log,
    config,
)
from piker._daemon import (
    Services,
)
from piker.clearing._client import open_ems


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


@acm
async def _open_test_pikerd_and_ems(
    fqsn,
    mode,
    loglevel,
    open_test_pikerd
):
    async with (
        open_test_pikerd() as (_, _, _, services),
        open_ems(
            fqsn,
            mode=mode,
            loglevel=loglevel,
        ) as ems_services):
            yield (services, ems_services)



@pytest.fixture
def open_test_pikerd_and_ems(
    open_test_pikerd,
    fqsn: str = 'xbtusdt.kraken',
    mode: str = 'paper',
    loglevel: str = 'info',
):
    yield partial(
        _open_test_pikerd_and_ems,
        fqsn,
        mode,
        loglevel,
        open_test_pikerd
    ) 

@pytest.fixture(scope='session')
def delete_testing_dir():
    '''
    This fixture removes the temp directory
    used for storing all config/ledger/pp data
    created during testing sessions. During test runs
    this file can be found in .config/piker/_testing

    '''
    yield
    app_dir = Path(config.get_app_dir('piker')).resolve()
    if app_dir.is_dir():
        rmtree(str(app_dir))
        assert not app_dir.is_dir()
