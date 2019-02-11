"""
Broker configuration mgmt.
"""
from os import path, makedirs
import configparser
import click
from ..log import get_logger

log = get_logger('broker-config')

_config_dir = click.get_app_dir('piker')
_broker_conf_path = path.join(_config_dir, 'brokers.ini')


def load(path: str = None) -> (configparser.ConfigParser, str):
    """Load broker config.
    """
    path = path or _broker_conf_path
    config = configparser.ConfigParser()
    read = config.read(path)
    log.debug(f"Read config file {path}")
    return config, path


def write(config: configparser.ConfigParser) -> None:
    """Write broker config to disk.

    Create a ``brokers.ini`` file if one does not exist.
    """
    if not path.isdir(_config_dir):
        log.debug(f"Creating config dir {_config_dir}")
        makedirs(_config_dir)

    log.debug(f"Writing config file {_broker_conf_path}")
    with open(_broker_conf_path, 'w') as cf:
        return config.write(cf)
