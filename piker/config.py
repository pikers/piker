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
Platform configuration (files) mgmt.

"""
import platform
import sys
import os
import shutil
import time
from typing import (
    Callable,
    MutableMapping,
)
from pathlib import Path

from bidict import bidict
import tomlkit
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


from .log import get_logger

log = get_logger('broker-config')


# XXX NOTE: taken from ``click`` since apparently they have some
# super weirdness with sigint and sudo..no clue
# we're probably going to slowly just modify it to our own version over
# time..
def get_app_dir(
    app_name: str,
    roaming: bool = True,
    force_posix: bool = False,

) -> str:
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

    # NOTE: for testing with `pytest` we leverage the `tmp_dir`
    # fixture to generate (and clean up) a test-request-specific
    # directory for isolated configuration files such that,
    # - multiple tests can run (possibly in parallel) without data races
    #   on the config state,
    # - we don't need to ever worry about leaking configs into the
    #   system thus avoiding needing to manage config cleaup fixtures or
    #   other bothers (since obviously `tmp_dir` cleans up after itself).
    #
    # In order to "pass down" the test dir path to all (sub-)actors in
    # the actor tree we preload the root actor's runtime vars state (an
    # internal mechanism for inheriting state down an actor tree in
    # `tractor`) with the testing dir and check for it whenever we
    # detect `pytest` is being used (which it isn't under normal
    # operation).
    if "pytest" in sys.modules:
        import tractor
        actor = tractor.current_actor(err_on_no_runtime=False)
        if actor:  # runtime is up
            rvs = tractor._state._runtime_vars
            testdirpath = Path(rvs['piker_vars']['piker_test_dir'])
            assert testdirpath.exists(), 'piker test harness might be borked!?'
            app_name = str(testdirpath)

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


_click_config_dir: Path = Path(get_app_dir('piker'))
_config_dir: Path = _click_config_dir
_parent_user: str = os.environ.get('SUDO_USER')

if _parent_user:
    non_root_user_dir = Path(
        os.path.expanduser(f'~{_parent_user}')
    )
    root: str = 'root'
    _ccds: str = str(_click_config_dir)  # click config dir string
    i_tail: int = int(_ccds.rfind(root) + len(root))
    _config_dir = (
        non_root_user_dir
        /
        Path(_ccds[i_tail+1:])  # +1 to capture trailing '/'
    )


_conf_names: set[str] = {
    'conf',  # god config
    'brokers',  # sec backend deatz
    'watchlists',  # (user defined) market lists
}

# TODO: probably drop all this super legacy, questrade specific,
# config stuff XD ?
_watchlists_data_path: Path = _config_dir / Path('watchlists.json')
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

) -> Path:
    '''
    Return the top-level default config path normally under
    ``~/.config/piker`` on linux for a given ``conf_name``, the config
    name.

    Contains files such as:
    - brokers.toml
    - watchlists.toml

    # maybe coming soon ;)
    - signals.toml
    - strats.toml

    '''
    if 'account.' not in conf_name:
        assert str(conf_name) in _conf_names

    fn = _conf_fn_w_ext(conf_name)
    return _config_dir / Path(fn)


def repodir() -> Path:
    '''
    Return the abspath as ``Path`` to the git repo's root dir.

    '''
    return Path(__file__).absolute().parent.parent


def load(
    conf_name: str = 'brokers',  # appended with .toml suffix
    path: Path | None = None,

    decode: Callable[
        [str | bytes,],
        MutableMapping,
    ] = tomllib.loads,

    touch_if_dne: bool = False,

    **tomlkws,

) -> tuple[dict, Path]:
    '''
    Load config file by name.

    If desired config is not in the top level piker-user config path then
    pass the ``path: Path`` explicitly.

    '''
    path: Path = path or get_conf_path(conf_name)

    # create the $HOME/.config/piker dir if dne
    if not _config_dir.is_dir():
        _config_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    if (
        not path.is_file()
        and touch_if_dne
    ):
        fn: str = _conf_fn_w_ext(conf_name)

        # try to copy in a template config to the user's directory if
        # one exists.
        template: Path = repodir() / 'config' / fn
        if template.is_file():
            shutil.copyfile(template, path)

        else:  # just touch an empty file with same name
            with path.open(mode='x'):
                pass

    with path.open(mode='r') as fp:
        config: dict = decode(
            fp.read(),
            **tomlkws,
        )

    log.debug(f"Read config file {path}")
    return config, path


def load_account(
    brokername: str,
    acctid: str,

) -> tuple[dict, Path]:
    '''
    Load a accounting (with positions) file from
    ~/.config/piker/accounting/account.<brokername>.<acctid>.toml.

    '''
    legacy_fn: str = f'pps.{brokername}.{acctid}.toml'
    fn: str = f'account.{brokername}.{acctid}.toml'

    dirpath: Path = _config_dir / 'accounting'
    config, path = load(
        path=dirpath / fn,
        decode=tomlkit.parse,
    )

    if not config:
        legacypath = dirpath / legacy_fn
        log.warning(
            f'Your account file is using the legacy `pps.` prefix..\n'
            f'Rewriting contents to new name -> {path}\n'
            'Please delete the old file!\n'
            f'|-> {legacypath}\n'
        )
        legacy_config, _ = load(
            path=legacypath,

            # TODO: move to tomlkit:
            # - needs to be fixed to support bidict?
            #   https://github.com/sdispater/tomlkit/issues/289
            # - we need to use or fork's fix to do multiline array
            #   indenting.
            decode=tomlkit.parse,
        )
        config.update(legacy_config)

        # XXX: override the presumably previously non-existant
        # file with legacy's contents.
        write(
            config,
            path=path,
            fail_empty=False,
        )

    return config, path


def load_ledger(
    brokername: str,
    acctid: str,

) -> tuple[dict, Path]:

    ldir: Path = _config_dir / 'accounting' / 'ledgers'
    if not ldir.is_dir():
        ldir.mkdir()

    fname = f'trades_{brokername}_{acctid}.toml'
    fpath: Path = ldir / fname

    if not fpath.is_file():
        log.info(
            f'Creating new local trades ledger: {fpath}'
        )
        fpath.touch()

    with fpath.open(mode='rb') as cf:
        start = time.time()
        ledger_dict = tomllib.load(cf)
        log.debug(f'Ledger load took {time.time() - start}s')

    return ledger_dict, fpath


def write(
    config: dict,  # toml config as dict

    name: str | None = None,
    path: Path | None = None,
    fail_empty: bool = True,

    **toml_kwargs,

) -> None:
    ''''
    Write broker config to disk.

    Create a ``brokers.ini`` file if one does not exist.

    '''
    if name:
        path: Path = path or get_conf_path(name)
        dirname: Path = path.parent
        if not dirname.is_dir():
            log.debug(f"Creating config dir {_config_dir}")
            dirname.mkdir()

    if (
        not config
        and fail_empty
    ):
        raise ValueError(
            "Watch out you're trying to write a blank config!"
        )

    log.debug(
        f"Writing config `{name}` file to:\n"
        f"{path}"
    )
    with path.open(mode='w') as fp:
        return tomlkit.dump(  # preserve style on write B)
            config,
            fp,
            **toml_kwargs,
        )


def load_accounts(
    providers: list[str] | None = None

) -> bidict[str, str | None]:

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
