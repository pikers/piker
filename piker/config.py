# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship for pikers)

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
import platform
import sys
import os
from os import path
from os.path import dirname
import shutil
from typing import Optional

from bidict import bidict
import toml

from .log import get_logger

log = get_logger('broker-config')

# taken from ``click`` since apparently they have some
# super weirdness with sigint and sudo..no clue
def get_app_dir(app_name, roaming=True, force_posix=False):
    r"""Returns the config folder for the application.  The default behavior
    is to return whatever is most appropriate for the operating system.

    To give you an idea, for an app called ``"Foo Bar"``, something like
    the following folders could be returned:

    Mac OS X:
      ``~/Library/Application Support/Foo Bar``
    Mac OS X (POSIX):
      ``~/.foo-bar``
    Unix:
      ``~/.config/foo-bar``
    Unix (POSIX):
      ``~/.foo-bar``
    Win XP (roaming):
      ``C:\Documents and Settings\<user>\Local Settings\Application Data\Foo``
    Win XP (not roaming):
      ``C:\Documents and Settings\<user>\Application Data\Foo Bar``
    Win 7 (roaming):
      ``C:\Users\<user>\AppData\Roaming\Foo Bar``
    Win 7 (not roaming):
      ``C:\Users\<user>\AppData\Local\Foo Bar``

    .. versionadded:: 2.0

    :param app_name: the application name.  This should be properly capitalized
                     and can contain whitespace.
    :param roaming: controls if the folder should be roaming or not on Windows.
                    Has no affect otherwise.
    :param force_posix: if this is set to `True` then on any POSIX system the
                        folder will be stored in the home folder with a leading
                        dot instead of the XDG config home or darwin's
                        application support folder.
    """
    def _posixify(name):
        return "-".join(name.split()).lower()
    
    # TODO: This is a hacky way to a) determine we're testing
    # and b) creating a test dir. We should aim to set a variable
    # within the tractor runtimes and store testing config data
    # outside of the users filesystem 
    if "pytest" in sys.modules:
        app_name += '/_testing'

    # if WIN:
    if platform.system() == 'Windows':
        key = "APPDATA" if roaming else "LOCALAPPDATA"
        folder = os.environ.get(key)
        if folder is None:
            folder = os.path.expanduser("~")
        return os.path.join(folder, app_name)
    if force_posix:
        return os.path.join(
            os.path.expanduser("~/.{}".format(_posixify(app_name))))
    if sys.platform == "darwin":
        return os.path.join(
            os.path.expanduser("~/Library/Application Support"), app_name
        )
    return os.path.join(
        os.environ.get("XDG_CONFIG_HOME", os.path.expanduser("~/.config")),
        _posixify(app_name),
    )

_config_dir = _click_config_dir = get_app_dir('piker')
_parent_user = os.environ.get('SUDO_USER')

if _parent_user:
    non_root_user_dir = os.path.expanduser(
        f'~{_parent_user}'
    )
    root = 'root'
    _config_dir = (
        non_root_user_dir +
        _click_config_dir[
            _click_config_dir.rfind(root) + len(root):
        ]
    )

_conf_names: set[str] = {
    'brokers',
    'pps',
    'trades',
    'watchlists',
    'paper_trades'
}

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


def _override_config_dir(
    path: str
) -> None:
    global _config_dir
    _config_dir = path


def _conf_fn_w_ext(
    name: str,
) -> str:
    # change this if we ever change the config file format.
    return f'{name}.toml'


def get_conf_path(
    conf_name: str = 'brokers',

) -> str:
    '''
    Return the top-level default config path normally under
    ``~/.config/piker`` on linux for a given ``conf_name``, the config
    name.

    Contains files such as:
    - brokers.toml
    - pp.toml
    - watchlists.toml

    # maybe coming soon ;)
    - signals.toml
    - strats.toml

    '''
    assert conf_name in _conf_names
    fn = _conf_fn_w_ext(conf_name)
    return os.path.join(
        _config_dir,
        fn,
    )


def repodir():
    '''
    Return the abspath to the repo directory.

    '''
    dirpath = path.abspath(
        # we're 3 levels down in **this** module file
        dirname(dirname(os.path.realpath(__file__)))
    )
    return dirpath


def load(
    conf_name: str = 'brokers',
    path: str = None,

    **tomlkws,

) -> (dict, str):
    '''
    Load config file by name.

    '''
    path = path or get_conf_path(conf_name)

    if not os.path.isdir(_config_dir):
        os.mkdir(_config_dir)

    if not os.path.isfile(path):
        fn = _conf_fn_w_ext(conf_name)

        template = os.path.join(
            repodir(),
            'config',
            fn
        )
        # try to copy in a template config to the user's directory
        # if one exists.
        if os.path.isfile(template):
            shutil.copyfile(template, path)
        else:
            # create an empty file
            with open(path, 'x'):
                pass
    else:
        with open(path, 'r'):
            pass  # touch it

    config = toml.load(path, **tomlkws)
    log.debug(f"Read config file {path}")
    return config, path


def write(
    config: dict,  # toml config as dict
    name: str = 'brokers',
    path: str = None,
    **toml_kwargs,

) -> None:
    ''''
    Write broker config to disk.

    Create a ``brokers.ini`` file if one does not exist.

    '''
    path = path or get_conf_path(name)
    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        log.debug(f"Creating config dir {_config_dir}")
        os.makedirs(dirname)

    if not config:
        raise ValueError(
            "Watch out you're trying to write a blank config!")

    log.debug(
        f"Writing config `{name}` file to:\n"
        f"{path}"
    )
    with open(path, 'w') as cf:
        return toml.dump(
            config,
            cf,
            **toml_kwargs,
        )


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
