# piker: trading gear for hackers
# Copyright (C) 2018-present Tyler Goodlet
# (in stewardship for pikers, everywhere.)

# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public
# License along with this program.  If not, see
# <https://www.gnu.org/licenses/>.

'''
CLI commons.

'''
import os
# from contextlib import AsyncExitStack
from types import ModuleType

import click
import trio
import tractor
from tractor._multiaddr import parse_maddr

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


def load_trans_eps(
    network: dict | None = None,
    maddrs: list[tuple] | None = None,

) -> dict[str, dict[str, dict]]:

    # transport-oriented endpoint multi-addresses
    eps: dict[
        str,  # service name, eg. `pikerd`, `emsd`..

        # libp2p style multi-addresses parsed into prot layers
        list[dict[str, str | int]]
    ] = {}

    if (
        network
        and not maddrs
    ):
        # load network section and (attempt to) connect all endpoints
        # which are reachable B)
        for key, maddrs in network.items():
            match key:

                # TODO: resolve table across multiple discov
                # prots Bo
                case 'resolv':
                    pass

                case 'pikerd':
                    dname: str = key
                    for maddr in maddrs:
                        layers: dict = parse_maddr(maddr)
                        eps.setdefault(
                            dname,
                            [],
                        ).append(layers)

    elif maddrs:
        # presume user is manually specifying the root actor ep.
        eps['pikerd'] = [parse_maddr(maddr)]

    return eps


@click.command()
@click.option(
    '--loglevel',
    '-l',
    default='warning',
    help='Logging level',
)
@click.option(
    '--tl',
    is_flag=True,
    help='Enable tractor-runtime logs',
)
@click.option(
    '--pdb',
    is_flag=True,
    help='Enable tractor debug mode',
)
@click.option(
    '--maddr',
    '-m',
    default=None,
    help='Multiaddrs to bind or contact',
)
# @click.option(
#     '--tsdb',
#     is_flag=True,
#     help='Enable local ``marketstore`` instance'
# )
# @click.option(
#     '--es',
#     is_flag=True,
#     help='Enable local ``elasticsearch`` instance'
# )
def pikerd(
    maddr: list[str] | None,
    loglevel: str,
    tl: bool,
    pdb: bool,
    # tsdb: bool,
    # es: bool,
):
    '''
    Spawn the piker broker-daemon.

    '''
    from tractor.devx import maybe_open_crash_handler
    with maybe_open_crash_handler(pdb=pdb):
        log = get_console_log(loglevel, name='cli')

        if pdb:
            log.warning((
                "\n"
                "!!! YOU HAVE ENABLED DAEMON DEBUG MODE !!!\n"
                "When a `piker` daemon crashes it will block the "
                "task-thread until resumed from console!\n"
                "\n"
            ))

        # service-actor registry endpoint socket-address set
        regaddrs: list[tuple[str, int]] = []

        conf, _ = config.load(
            conf_name='conf',
        )
        network: dict = conf.get('network')
        if (
            network is None
            and not maddr
        ):
            regaddrs = [(
                _default_registry_host,
                _default_registry_port,
            )]

        else:
            eps: dict = load_trans_eps(
                network,
                maddr,
            )
            for layers in eps['pikerd']:
                regaddrs.append((
                    layers['ipv4']['addr'],
                    layers['tcp']['port'],
                ))

        from .. import service

        async def main():
            service_mngr: service.Services

            async with (
                service.open_pikerd(
                    registry_addrs=regaddrs,
                    loglevel=loglevel,
                    debug_mode=pdb,

                ) as service_mngr,  # normally delivers a ``Services`` handle

                # AsyncExitStack() as stack,
            ):
                # TODO: spawn all other sub-actor daemons according to
                # multiaddress endpoint spec defined by user config
                assert service_mngr

                # if tsdb:
                #     dname, conf = await stack.enter_async_context(
                #         service.marketstore.start_ahab_daemon(
                #             service_mngr,
                #             loglevel=loglevel,
                #         )
                #     )
                #     log.info(f'TSDB `{dname}` up with conf:\n{conf}')

                # if es:
                #     dname, conf = await stack.enter_async_context(
                #         service.elastic.start_ahab_daemon(
                #             service_mngr,
                #             loglevel=loglevel,
                #         )
                #     )
                #     log.info(f'DB `{dname}` up with conf:\n{conf}')

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
@click.option(
    '--pdb',
    is_flag=True,
    help='Enable runtime debug mode ',
)
@click.option(
    '--maddr',
    '-m',
    default=None,
    multiple=True,
    help='Multiaddr to bind',
)
@click.option(
    '--regaddr',
    '-r',
    default=None,
    help='Registrar addr to contact',
)
@click.pass_context
def cli(
    ctx: click.Context,
    brokers: list[str],
    loglevel: str,
    tl: bool,
    configdir: str,
    pdb: bool,

    # TODO: make these list[str] with multiple -m maddr0 -m maddr1
    maddr: list[str],
    regaddr: str,

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

    # TODO: load endpoints from `conf::[network].pikerd`
    # - pikerd vs. regd, separate registry daemon?
    # - expose datad vs. brokerd?
    # - bind emsd with certain perms on public iface?
    regaddrs: list[tuple[str, int]] = regaddr or [(
        _default_registry_host,
        _default_registry_port,
    )]

    # TODO: factor [network] section parsing out from pikerd
    # above and call it here as well.
    # if maddr:
    #     for addr in maddr:
    #         layers: dict = parse_maddr(addr)

    ctx.obj.update({
        'brokers': brokers,
        'brokermods': brokermods,
        'loglevel': loglevel,
        'tractorloglevel': None,
        'log': get_console_log(loglevel),
        'confdir': config._config_dir,
        'wl_path': config._watchlists_data_path,
        'registry_addrs': regaddrs,
        'pdb': pdb,  # debug mode flag

        # TODO: endpoint parsing, pinging and binding
        # on no existing server.
        # 'maddrs': maddr,
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
    # from ..service import elastic  # noqa
    from ..brokers import cli  # noqa
    from ..ui import cli  # noqa
    from ..watchlists import cli  # noqa

    # typer implemented
    from ..storage import cli  # noqa
    from ..accounting import cli  # noqa


# load downstream cli modules
_load_clis()
