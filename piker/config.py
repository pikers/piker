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

from typing import Optional
from pathlib import Path

import toml
import click

from bidict import bidict

from .log import get_logger

log = get_logger('broker-config')

_config_dir = click.get_app_dir('piker')
_file_name = 'brokers.toml'


class BrokerConfigurationError(BaseException):
    ...


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
    conf_path: str = None,
    raw: bool = False
) -> (dict, str):
    """Load broker config.
    """
    conf_path = conf_path or get_broker_conf_path()
    Path(conf_path).parent.mkdir(exist_ok=True)
    if not os.path.isfile(conf_path):
        shutil.copyfile(
            os.path.join(repodir(), 'data/brokers.toml'),
            conf_path,
        )

    if raw:
        with open(conf_path, 'r') as cf:
            config = cf.read()
    else:
        config = toml.load(conf_path)
    log.debug(f"Read config file {conf_path}")
    return config, conf_path


def write(
    config: Optional[dict] = None,  # toml config as dict
    raw: Optional[str] = None,  # toml config as string
    path: str = None,
) -> None:
    """Write broker config to disk.
    Create a ``brokers.ini`` file if one does not exist.
    """
    path = path or get_broker_conf_path()
    Path(path).parent.mkdir(exist_ok=True)

    if not config and not raw:
        raise ValueError(
            "Watch out you're trying to write a blank config!")

    log.debug(f"Writing config file {path}")
    with open(path, 'w') as cf:
        if config:
            return toml.dump(config, cf)
        elif raw:
            return cf.write(raw)


def load_accounts(

    providers: Optional[list[str]] = None

) -> bidict[str, Optional[str]]:

    conf, path = load()
    accounts = bidict()
    for provider_name, section in conf.items():
        accounts_section = section.get('accounts')
        if (
            providers is None or
            providers and provider_name in providers
        ):
            if accounts_section is None:
                log.warning(f'No accounts named for {provider_name}?')
                continue
            else:
                for label, value in accounts_section.items():
                    accounts[
                        f'{provider_name}.{label}'
                    ] = value

    # our default paper engine entry
    accounts['paper'] = None
    return accounts


# XXX: Recursive getting & setting

def get_value(_dict, _section):
    subs = _section.split('.')
    if len(subs) > 1:
        return get_value(
            _dict[subs[0]],
            '.'.join(subs[1:]),
        )

    else:
        return _dict[_section]


def set_value(_dict, _section, val):
    subs = _section.split('.')
    if len(subs) > 1:
        if subs[0] not in _dict:
            _dict[subs[0]] = {}

        return set_value(
            _dict[subs[0]],
            '.'.join(subs[1:]),
            val
        )

    else:
        _dict[_section] = val


def del_value(_dict, _section):
    subs = _section.split('.')
    if len(subs) > 1:
        if subs[0] not in _dict:
            return

        return del_value(
            _dict[subs[0]],
            '.'.join(subs[1:])
        )

    else:
        if _section not in _dict:
            return

        del _dict[_section]
