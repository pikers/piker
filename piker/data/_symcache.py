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
from contextlib import (
    # asynccontextmanager as acm,
    contextmanager as cm,
)
from pathlib import Path
from pprint import pformat
from typing import (
    Any,
    TYPE_CHECKING,
)
from types import ModuleType

from fuzzywuzzy import process as fuzzy
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

if TYPE_CHECKING:
    from ..accounting import (
        Asset,
        MktPair,
    )

log = get_logger('data.cache')


class SymbologyCache(Struct):
    '''
    Asset meta-data cache which holds lookup tables for 3 sets of
    market-symbology related struct-types required by the
    `.accounting` and `.data` subsystems.

    '''
    mod: ModuleType
    fp: Path

    # all asset-money-systems descriptions as minimally defined by
    # in `.accounting.Asset`
    assets: dict[str, Asset] = field(default_factory=dict)

    # backend-system pairs loaded in provider (schema) specific
    # structs.
    pairs: dict[str, Struct] = field(default_factory=dict)

    # TODO: piker-normalized `.accounting.MktPair` table?
    # loaded from the `.pairs` and a normalizer
    # provided by the backend pkg.
    mktmaps: dict[str, MktPair] = field(default_factory=dict)

    def write_config(self) -> None:
        cachedict: dict[str, Any] = {}
        for key, attr in {
            'assets': self.assets,
            'pairs': self.pairs,
        }.items():
            if not attr:
                log.warning(
                    f'Asset cache table for `{key}` is empty?'
                )
                continue

            cachedict[key] = attr

        # serialize mkts
        mktmapsdict = cachedict['mktmaps'] = {}
        for fqme, mkt in self.mktmaps.items():
            mktmapsdict[fqme] = mkt.to_dict()

        try:
            with self.fp.open(mode='wb') as fp:
                tomli_w.dump(cachedict, fp)
        except TypeError:
            self.fp.unlink()
            raise

    async def load(self) -> None:
        async with open_cached_client(self.mod.name) as client:

            if get_assets := getattr(client, 'get_assets', None):
                assets: dict[str, Asset] = await get_assets()
                for bs_mktid, asset in assets.items():
                    self.assets[bs_mktid] = asset.to_dict()
            else:
                log.warning(
                    'No symbology cache `Asset` support for `{provider}`..\n'
                    'Implement `Client.get_assets()`!'
                )

            if get_mkt_pairs := getattr(client, 'get_mkt_pairs', None):
                pairs: dict[str, Struct] = await get_mkt_pairs()
                for bs_fqme, pair in pairs.items():

                    entry = await self.mod.get_mkt_info(pair.bs_fqme)
                    if not entry:
                        continue

                    mkt: MktPair
                    pair: Struct
                    mkt, _pair = entry
                    assert _pair is pair
                    self.pairs[pair.bs_fqme] = pair.to_dict()
                    self.mktmaps[mkt.fqme] = mkt

            else:
                log.warning(
                    'No symbology cache `Pair` support for `{provider}`..\n'
                    'Implement `Client.get_mkt_pairs()`!'
                )

        return self

    def search(
        self,
        pattern: str,

    ) -> dict[str, Struct]:

        matches = fuzzy.extractBests(
            pattern,
            self.mktmaps,
            score_cutoff=50,
        )

        # repack in dict[fqme, MktPair] form
        return {
            item[0].fqme: item[0]
            for item in matches
        }


# actor-process-local in-mem-cache of symcaches (by backend).
_caches: dict[str, SymbologyCache] = {}


@cm
def open_symcache(
    mod: ModuleType,
    reload: bool = False,

) -> SymbologyCache:

    provider: str = mod.name

    # actor-level cache-cache XD
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

    cachefile: Path = cachedir / f'{str(provider)}.symcache.toml'

    cache = SymbologyCache(
        mod=mod,
        fp=cachefile,
    )

    # if no cache exists or an explicit reload is requested, load
    # the provider API and call appropriate endpoints to populate
    # the mkt and asset tables.
    if (
        reload
        or not cachefile.is_file()
    ):
        log.info(f'GENERATING symbology cache for `{mod.name}`')

        import tractor
        import trio

        # spawn tractor runtime and generate cache
        # if not existing.
        async def sched_gen_symcache():

            async with (
                # only for runtime
                tractor.open_nursery(debug_mode=True),
            ):
                return await cache.load()

        cache: SymbologyCache = trio.run(sched_gen_symcache)

        # only (re-)write if explicit reload or non-existing
        cache.write_config()

    else:
        log.info(
            f'Loading EXISTING `{mod.name}` symbology cache:\n'
            f'> {cache.fp}'
        )
        import time
        from ..accounting import (
            Asset,
            MktPair,
        )

        now = time.time()
        with cachefile.open('rb') as existing_fp:
            data: dict[str, dict] = tomllib.load(existing_fp)
            log.runtime(f'SYMCACHE TOML LOAD TIME: {time.time() - now}')

            # copy in backend specific pairs table directly without
            # struct loading for now..
            pairtable = data.pop('pairs')
            cache.pairs = pairtable

            # TODO: some kinda way to allow the backend
            # to provide a struct-loader per entry?
            # for key, pairtable in pairtable.items():
            #     pair: Struct = cache.mod.load_pair(pairtable)
            #     cache.pairs[key] = pair

            # load `dict` -> `Asset`
            assettable = data.pop('assets')
            for name, asdict in assettable.items():
                cache.assets[name] = Asset.from_msg(asdict)

            # load `dict` -> `MktPair`
            dne: list[str] = []
            mkttable = data.pop('mktmaps')
            for fqme, mktdict in mkttable.items():

                mkt = MktPair.from_msg(mktdict)
                assert mkt.fqme == fqme

                # sanity check asset refs from those (presumably)
                # loaded asset set above.
                # src_k: str = pairtable.get('bs_src_asset,
                src: Asset = cache.assets[mkt.src.name]
                assert src == mkt.src
                dst: Asset
                if not (dst := cache.assets.get(mkt.dst.name)):
                    dne.append(mkt.dst.name)
                    continue
                else:
                    assert dst.name == mkt.dst.name

                cache.mktmaps[fqme] = mkt

            log.warning(
                f'These `MktPair.dst: Asset`s DNE says `{mod.name}` ?\n'
                f'{pformat(dne)}'
            )

        # TODO: use a real profiling sys..
        # https://github.com/pikers/piker/issues/337
        log.info(f'SYMCACHE LOAD TIME: {time.time() - now}')

    yield cache

    # TODO: write only when changes detected? but that should
    # never happen right except on reload?
    # cache.write_config()


def get_symcache(
    provider: str,
    force_reload: bool = False,

) -> SymbologyCache:
    '''
    Get any available symbology/assets cache from
    sync code by manually running `trio` to do the work.

    '''
    from ..brokers import get_brokermod

    try:
        with open_symcache(
            get_brokermod(provider),
            reload=force_reload,

        ) as symcache:
            return symcache
    except BaseException:
        import pdbp
        pdbp.xpm()
