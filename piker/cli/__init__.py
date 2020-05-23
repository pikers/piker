"""
CLI commons.
"""
import os

import click
import tractor

from ..log import get_console_log, get_logger
from ..brokers import get_brokermod, config
from ..brokers.core import _data_mods

log = get_logger('cli')
DEFAULT_BROKER = 'questrade'

# _config_dir = click.get_app_dir('piker')
# _watchlists_data_path = os.path.join(_config_dir, 'watchlists.json')
_context_defaults = dict(
    default_map={
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
@click.option('--host', '-h', default='127.0.0.1', help='Host address to bind')
def pikerd(loglevel, host, tl):
    """Spawn the piker broker-daemon.
    """
    get_console_log(loglevel)
    tractor.run_daemon(
        rpc_module_paths=_data_mods,
        name='brokerd',
        loglevel=loglevel if tl else None,
    )


@click.group(context_settings=_context_defaults)
@click.option('--broker', '-b', default=DEFAULT_BROKER,
              help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
@click.option('--configdir', '-c', help='Configuration directory')
@click.pass_context
def cli(ctx, broker, loglevel, configdir):
    if configdir is not None:
        assert os.path.isdir(configdir), f"`{configdir}` is not a valid path"
        config._override_config_dir(configdir)

    # ensure that ctx.obj exists even though we aren't using it (yet)
    ctx.ensure_object(dict)
    ctx.obj.update({
        'broker': broker,
        'brokermod': get_brokermod(broker),
        'loglevel': loglevel,
        'log': get_console_log(loglevel),
    })


# load downstream cli modules
from ..brokers import cli as _
