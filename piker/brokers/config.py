"""
Broker configuration mgmt.
"""
from os import path
import configparser
import click
from ..log import get_logger

log = get_logger('broker-config')

_broker_conf_path = path.join(click.get_app_dir('piker'), 'brokers.ini')


def load() -> (configparser.ConfigParser, str):
    """Load broker config.

    Create a ``broker.ini`` file if one dne.
    """
    config = configparser.ConfigParser()
    read = config.read(_broker_conf_path)
    log.debug(f"Read config file {_broker_conf_path}")
    return config, _broker_conf_path


def write(config: configparser.ConfigParser) -> None:
    """Write broker config to disk.
    """
    log.debug(f"Writing config file {_broker_conf_path}")
    with open(_broker_conf_path, 'w') as cf:
        return config.write(cf)
