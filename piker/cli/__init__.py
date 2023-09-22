# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of pikers)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

'''
CLI commons.

'''
import os
from contextlib import AsyncExitStack
from types import ModuleType

import click
import trio
import tractor

from ..log import (
    get_console_log,
    get_logger,
    colorize_json,
)
from ..brokers import get_brokermod
from ..service import (
    _default_registry_host,
    _default_registry_port,
)
from .. import config


log = get_logger('piker.cli')


@click.command()
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--pdb', is_flag=True, help='Enable tractor debug mode')
@click.option('--host', '-h', default=None, help='Host addr to bind')
@click.option('--port', '-p', default=None, help='Port number to bind')
@click.option(
    '--tsdb',
    is_flag=True,
    help='Enable local ``marketstore`` instance'
)
@click.option(
    '--es',
    is_flag=True,
    help='Enable local ``elasticsearch`` instance'
)
def pikerd(
    loglevel: str,
    host: str,
    port: int,
    tl: bool,
    pdb: bool,
    tsdb: bool,
    es: bool,
):
    '''
    Spawn the piker broker-daemon.

    '''
    log = get_console_log(loglevel, name='cli')

    if pdb:
        log.warning((
            "\n"
            "!!! YOU HAVE ENABLED DAEMON DEBUG MODE !!!\n"
            "When a `piker` daemon crashes it will block the "
            "task-thread until resumed from console!\n"
            "\n"
        ))

    reg_addr: None | tuple[str, int] = None
    if host or port:
        reg_addr = (
            host or _default_registry_host,
            int(port) or _default_registry_port,
        )

    from .. import service

    async def main():
        service_mngr: service.Services

        async with (
            service.open_pikerd(
                loglevel=loglevel,
                debug_mode=pdb,
                registry_addr=reg_addr,

            ) as service_mngr,  # normally delivers a ``Services`` handle

            AsyncExitStack() as stack,
        ):
            if tsdb:
                dname, conf = await stack.enter_async_context(
                    service.marketstore.start_ahab_daemon(
                        service_mngr,
                        loglevel=loglevel,
                    )
                )
                log.info(f'TSDB `{dname}` up with conf:\n{conf}')

            if es:
                dname, conf = await stack.enter_async_context(
                    service.elastic.start_ahab_daemon(
                        service_mngr,
                        loglevel=loglevel,
                    )
                )
                log.info(f'DB `{dname}` up with conf:\n{conf}')

            await trio.sleep_forever()

    trio.run(main)


@click.group(context_settings=config._context_defaults)
@click.option(
    '--brokers', '-b',
    default=None,
    multiple=True,
    help='Broker backend to use'
)
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--configdir', '-c', help='Configuration directory')
@click.option('--host', '-h', default=None, help='Host addr to bind')
@click.option('--port', '-p', default=None, help='Port number to bind')
@click.pass_context
def cli(
    ctx: click.Context,
    brokers: list[str],
    loglevel: str,
    tl: bool,
    configdir: str,
    host: str,
    port: int,

) -> None:
    if configdir is not None:
        assert os.path.isdir(configdir), f"`{configdir}` is not a valid path"
        config._override_config_dir(configdir)

    # TODO: for typer see
    # https://typer.tiangolo.com/tutorial/commands/context/
    ctx.ensure_object(dict)

    if not brokers:
        # (try to) load all (supposedly) supported data/broker backends
        from piker.brokers import __brokers__
        brokers = __brokers__

    brokermods: dict[str, ModuleType] = {
        broker: get_brokermod(broker) for broker in brokers
    }
    assert brokermods

    reg_addr: None | tuple[str, int] = None
    if host or port:
        reg_addr = (
            host or _default_registry_host,
            int(port) or _default_registry_port,
        )

    ctx.obj.update({
        'brokers': brokers,
        'brokermods': brokermods,
        'loglevel': loglevel,
        'tractorloglevel': None,
        'log': get_console_log(loglevel),
        'confdir': config._config_dir,
        'wl_path': config._watchlists_data_path,
        'registry_addr': reg_addr,
    })

    # allow enabling same loglevel in ``tractor`` machinery
    if tl:
        ctx.obj.update({'tractorloglevel': loglevel})


@cli.command()
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.argument('ports', nargs=-1, required=False)
@click.pass_obj
def services(config, tl, ports):

    from ..service import (
        open_piker_runtime,
        _default_registry_port,
        _default_registry_host,
    )

    host = _default_registry_host
    if not ports:
        ports = [_default_registry_port]

    async def list_services():
        nonlocal host
        async with (
            open_piker_runtime(
                name='service_query',
                loglevel=config['loglevel'] if tl else None,
            ),
            tractor.get_arbiter(
                host=host,
                port=ports[0]
            ) as portal
        ):
            registry = await portal.run_from_ns('self', 'get_registry')
            json_d = {}
            for key, socket in registry.items():
                host, port = socket
                json_d[key] = f'{host}:{port}'
            click.echo(f"{colorize_json(json_d)}")

    trio.run(list_services)


def _load_clis() -> None:
    from ..service import elastic  # noqa
    from ..brokers import cli  # noqa
    from ..ui import cli  # noqa
    from ..watchlists import cli  # noqa

    # typer implemented
    from ..storage import cli  # noqa
    from ..accounting import cli  # noqa


# load downstream cli modules
_load_clis()
