"""
CLI commons.
"""
import os

import click
import trio
import tractor

from ..log import get_console_log, get_logger, colorize_json
from ..brokers import get_brokermod
from .._daemon import _tractor_kwargs
from .. import config


log = get_logger('cli')
DEFAULT_BROKER = 'questrade'

_config_dir = click.get_app_dir('piker')
_watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')
_context_defaults = dict(
    default_map={
        # Questrade specific quote poll rates
        'monitor': {
            'rate': 3,
        },
        'optschain': {
            'rate': 1,
        },
    }
)


@click.command()
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--pdb', is_flag=True, help='Enable tractor debug mode')
@click.option('--host', '-h', default='127.0.0.1', help='Host address to bind')
def pikerd(loglevel, host, tl, pdb):
    """Spawn the piker broker-daemon.
    """
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

    async def main():
        async with open_pikerd(loglevel=loglevel, debug_mode=pdb):
            await trio.sleep_forever()

    trio.run(main)


@click.group(context_settings=_context_defaults)
@click.option(
    '--brokers', '-b',
    default=[DEFAULT_BROKER],
    multiple=True,
    help='Broker backend to use'
)
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.option('--configdir', '-c', help='Configuration directory')
@click.pass_context
def cli(ctx, brokers, loglevel, tl, configdir):
    if configdir is not None:
        assert os.path.isdir(configdir), f"`{configdir}` is not a valid path"
        config._override_config_dir(configdir)

    ctx.ensure_object(dict)

    if len(brokers) == 1:
        brokermods = [get_brokermod(brokers[0])]
    else:
        brokermods = [get_brokermod(broker) for broker in brokers]

    ctx.obj.update({
        'brokers': brokers,
        'brokermods': brokermods,
        'loglevel': loglevel,
        'tractorloglevel': None,
        'log': get_console_log(loglevel),
        'confdir': _config_dir,
        'wl_path': _watchlists_data_path,
    })

    # allow enabling same loglevel in ``tractor`` machinery
    if tl:
        ctx.obj.update({'tractorloglevel': loglevel})


@cli.command()
@click.option('--tl', is_flag=True, help='Enable tractor logging')
@click.argument('names', nargs=-1, required=False)
@click.pass_obj
def services(config, tl, names):

    async def list_services():

        async with tractor.get_arbiter(
            *_tractor_kwargs['arbiter_addr']
        ) as portal:
            registry = await portal.run('self', 'get_registry')
            json_d = {}
            for uid, socket in registry.items():
                name, uuid = uid
                host, port = socket
                json_d[f'{name}.{uuid}'] = f'{host}:{port}'
            click.echo(
                f"Available `piker` services:\n{colorize_json(json_d)}"
            )

    tractor.run(
        list_services,
        name='service_query',
        loglevel=config['loglevel'] if tl else None,
        arbiter_addr=_tractor_kwargs['arbiter_addr'],
    )


def _load_clis() -> None:
    from ..data import marketstore  # noqa
    from ..data import cli  # noqa
    from ..brokers import cli  # noqa
    from ..ui import cli  # noqa
    from ..watchlists import cli  # noqa


# load downstream cli modules
_load_clis()
