# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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

"""
Broker configuration mgmt.
"""
import os
from os.path import dirname
import shutil

import toml
import click

from ..log import get_logger

log = get_logger('broker-config')

_config_dir = click.get_app_dir('piker')
_file_name = 'brokers.toml'


def _override_config_dir(
    path: str
) -> None:
    global _config_dir
    _config_dir = path


def get_broker_conf_path():
    """Return the default config path normally under
    ``~/.config/piker`` on linux.

    Contains files such as:
    - brokers.toml
    - watchlists.toml
    - signals.toml
    - strats.toml

    """
    return os.path.join(_config_dir, _file_name)


def repodir():
    """Return the abspath to the repo directory.
    """
    dirpath = os.path.abspath(
        # we're 3 levels down in **this** module file
        dirname(dirname(dirname(os.path.realpath(__file__))))
    )
    return dirpath


def load(
    path: str = None
) -> (dict, str):
    """Load broker config.
    """
    path = path or get_broker_conf_path()
    if not os.path.isfile(path):
        shutil.copyfile(
            os.path.join(repodir(), 'data/brokers.toml'),
            path,
        )

    config = toml.load(path)
    log.debug(f"Read config file {path}")
    return config, path


def write(
    config: dict,  # toml config as dict
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

    if not config:
        raise ValueError(
            "Watch out you're trying to write a blank config!")

    log.debug(f"Writing config file {path}")
    with open(path, 'w') as cf:
        return toml.dump(config, cf)
