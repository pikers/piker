"""
Broker configuration mgmt.
"""
import os
import configparser
import click
from ..log import get_logger

log = get_logger('broker-config')

_config_dir = click.get_app_dir('piker')
_file_name = 'brokers.ini'


def _override_config_dir(
    path: str
) -> None:
    global _config_dir
    _config_dir = path


def get_broker_conf_path():
    return os.path.join(_config_dir, _file_name)


def load(
    path: str = None
) -> (configparser.ConfigParser, str):
    """Load broker config.
    """
    path = path or get_broker_conf_path()
    config = configparser.ConfigParser()
    config.read(path)
    log.debug(f"Read config file {path}")
    return config, path


def write(
    config: configparser.ConfigParser,
    path: str = None,
) -> None:
    """Write broker config to disk.

    Create a ``brokers.ini`` file if one does not exist.
    """
    path = path or get_broker_conf_path()
    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        log.debug(f"Creating config dir {_config_dir}")
        os.makedirs(dirname)

    log.debug(f"Writing config file {path}")
    with open(path, 'w') as cf:
        return config.write(cf)
