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
from pprint import pformat
from functools import partial

import click
import trio
import tractor

from ..log import get_console_log, get_logger, colorize_json
from ..brokers import get_brokermod
from .._daemon import (
    _default_registry_host,
    _default_registry_port,
)
from .. import config


log = get_logger('cli')


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
@click.option(
    '--mpd',
    is_flag=True,
    help='Read from deribit and dump data to elastic db'
)
def pikerd(
    loglevel: str,
    host: str,
    port: int,
    tl: bool,
    pdb: bool,
    tsdb: bool,
    es: bool,
    mpd: bool,
):
    '''
    Spawn the piker broker-daemon.

    '''

    from .._daemon import open_pikerd
    log = get_console_log(loglevel)

    if pdb:
        log.warning((
            "\n"
            "!!! You have enabled daemon DEBUG mode !!!\n"
            "If a daemon crashes it will likely block"
            " the service until resumed from console!\n"
            "\n"
        ))

    reg_addr: None | tuple[str, int] = None
    if host or port:
        reg_addr = (
            host or _default_registry_host,
            int(port) or _default_registry_port,
        )

    async def main():
        async with (
            open_pikerd(
                tsdb=tsdb,
                es=es,
                mpd=mpd,
                loglevel=loglevel,
                debug_mode=pdb,
                registry_addr=reg_addr,

            ),  # normally delivers a ``Services`` handle
            trio.open_nursery() as n,
        ):

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

    ctx.ensure_object(dict)

    if not brokers:
        # (try to) load all (supposedly) supported data/broker backends
        from piker.brokers import __brokers__
        brokers = __brokers__

    brokermods = [get_brokermod(broker) for broker in brokers]
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

    from .._daemon import (
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
    from ..data import marketstore  # noqa
    from ..data import elastic
    from ..data import cli  # noqa
    from ..brokers import cli  # noqa
    from ..ui import cli  # noqa
    from ..watchlists import cli  # noqa


# load downstream cli modules
_load_clis()
