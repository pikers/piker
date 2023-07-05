# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

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

'''
Mega-simple symbology cache via TOML files.

Allow backend data providers and/or brokers to stash their
symbology sets (aka the meta data we normalize into our
`.accounting.MktPair` type) to the filesystem for faster lookup and
offline usage.

'''
from __future__ import annotations
from contextlib import asynccontextmanager as acm
from pathlib import Path
from typing import Any

import tomli_w  # for fast symbol cache writing
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from msgspec import field

from ..log import get_logger
from .. import config
from ..brokers import open_cached_client
from .types import Struct
from ..accounting import (
    Asset,
    # MktPair,
)

log = get_logger('data.cache')


class AssetsInfo(Struct):
    '''
    Asset meta-data cache which holds lookup tables for 3 sets of
    market-symbology related struct-types required by the
    `.accounting` and `.data` subsystems.

    '''
    provider: str
    fp: Path
    assets: dict[str, Asset] = field(default_factory=dict)

    # backend-system pairs loaded in provider (schema) specific
    # structs.
    pairs: dict[str, Struct] = field(default_factory=dict)

    # TODO: piker-normalized `.accounting.MktPair` table?
    # loaded from the `.pairs` and a normalizer
    # provided by the backend pkg.
    # mkt_pairs: dict[str, MktPair] = field(default_factory=dict)

    def write_config(self) -> None:
        cachedict: dict[str, Any] = {
            'assets': self.assets,
            'pairs': self.pairs,
        }
        try:
            with self.fp.open(mode='wb') as fp:
                tomli_w.dump(cachedict, fp)
        except TypeError:
            self.fp.unlink()
            raise


_caches: dict[str, AssetsInfo] = {}


@acm
async def open_symbology_cache(
    provider: str,
    reload: bool = False,

) -> AssetsInfo:
    global _caches

    if not reload:
        try:
            yield _caches[provider]
        except KeyError:
            log.warning('No asset info cache exists yet for '
                        f'`{provider}`')

    cachedir: Path = config.get_conf_dir() / '_cache'
    if not cachedir.is_dir():
        log.info(f'Creating `nativedb` director: {cachedir}')
        cachedir.mkdir()

    cachefile: Path = cachedir / f'{str(provider)}_symbology.toml'

    cache = AssetsInfo(
        provider=provider,
        fp=cachefile,
    )

    # if no cache exists or an explicit reload is requested, load
    # the provider API and call appropriate endpoints to populate
    # the mkt and asset tables.
    if (
        reload
        or not cachefile.is_file()
    ):
        async with open_cached_client(provider) as client:

            if get_assets := getattr(client, 'get_assets', None):
                assets: dict[str, Asset] = await get_assets()
                for bs_mktid, asset in assets.items():
                    cache.assets[bs_mktid] = asset.to_dict()
            else:
                log.warning(
                    'No symbology cache `Asset` support for `{provider}`..\n'
                    'Implement `Client.get_assets()`!'
                )

            if get_mkt_pairs := getattr(client, 'get_mkt_pairs', None):
                for bs_mktid, pair in (await get_mkt_pairs()).items():
                    cache.pairs[bs_mktid] = pair.to_dict()
            else:
                log.warning(
                    'No symbology cache `Pair` support for `{provider}`..\n'
                    'Implement `Client.get_mkt_pairs()`!'
                )

                # TODO: pack into `MktPair` normalized types as
                # well?

        # only (re-)write if explicit reload or non-existing
        cache.write_config()
    else:
        with cachefile.open('rb') as existing_fp:
            data: dict[str, dict] = tomllib.load(existing_fp)

            for key, table in data.items():
                attr: dict[str, Any] = getattr(cache, key)
                if attr != table:
                    log.info(f'OUT-OF-SYNC symbology cache: {key}')

                setattr(cache, key, table)

    yield cache
    cache.write_config()
