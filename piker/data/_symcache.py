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
    asynccontextmanager as acm,
)
from pathlib import Path
from pprint import pformat
from typing import (
    Any,
    Sequence,
    Hashable,
    TYPE_CHECKING,
)
from types import ModuleType

from rapidfuzz import process as fuzzy
import tomli_w  # for fast symbol cache writing
import tractor
import trio
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from msgspec import field

from piker.log import get_logger
from piker import config
from piker.types import Struct
from piker.brokers import (
    open_cached_client,
    get_brokermod,
)

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
    # serialized namespace path to the backend's pair-info-`Struct`
    # defn B)
    pair_ns_path: tractor.msg.NamespacePath | None = None

    # TODO: piker-normalized `.accounting.MktPair` table?
    # loaded from the `.pairs` and a normalizer
    # provided by the backend pkg.
    mktmaps: dict[str, MktPair] = field(default_factory=dict)

    def write_config(self) -> None:

        # put the backend's pair-struct type ref at the top
        # of file if possible.
        cachedict: dict[str, Any] = {
            'pair_ns_path': str(self.pair_ns_path) or '',
        }

        # serialize all tables as dicts for TOML.
        for key, table in {
            'assets': self.assets,
            'pairs': self.pairs,
            'mktmaps': self.mktmaps,
        }.items():
            if not table:
                log.warning(
                    f'Asset cache table for `{key}` is empty?'
                )
                continue

            dct = cachedict[key] = {}
            for key, struct in table.items():
                dct[key] = struct.to_dict(include_non_members=False)

        try:
            with self.fp.open(mode='wb') as fp:
                tomli_w.dump(cachedict, fp)
        except TypeError:
            self.fp.unlink()
            raise

    async def load(self) -> None:
        '''
        Explicitly load the "symbology set" for this provider by using
        2 required `Client` methods:

          - `.get_assets()`: returning a table of `Asset`s
          - `.get_mkt_pairs()`: returning a table of pair-`Struct`
            types, custom defined by the particular backend.

        AND, the required `.get_mkt_info()` module-level endpoint
        which maps `fqme: str` -> `MktPair`s.

        These tables are then used to fill out the `.assets`, `.pairs` and
        `.mktmaps` tables on this cache instance, respectively.

        '''
        async with open_cached_client(self.mod.name) as client:

            if get_assets := getattr(client, 'get_assets', None):
                assets: dict[str, Asset] = await get_assets()
                for bs_mktid, asset in assets.items():
                    self.assets[bs_mktid] = asset
            else:
                log.warning(
                    'No symbology cache `Asset` support for `{provider}`..\n'
                    'Implement `Client.get_assets()`!'
                )

            if get_mkt_pairs := getattr(client, 'get_mkt_pairs', None):

                pairs: dict[str, Struct] = await get_mkt_pairs()
                for bs_fqme, pair in pairs.items():

                    # NOTE: every backend defined pair should
                    # declare it's ns path for roundtrip
                    # serialization lookup.
                    if not getattr(pair, 'ns_path', None):
                        raise TypeError(
                            f'Pair-struct for {self.mod.name} MUST define a '
                            '`.ns_path: str`!\n'
                            f'{pair}'
                        )

                    entry = await self.mod.get_mkt_info(pair.bs_fqme)
                    if not entry:
                        continue

                    mkt: MktPair
                    pair: Struct
                    mkt, _pair = entry
                    assert _pair is pair, (
                        f'`{self.mod.name}` backend probably has a '
                        'keying-symmetry problem between the pair-`Struct` '
                        'returned from `Client.get_mkt_pairs()`and the '
                        'module level endpoint: `.get_mkt_info()`\n\n'
                        "Here's the struct diff:\n"
                        f'{_pair - pair}'
                    )
                    # NOTE XXX: this means backends MUST implement
                    # a `Struct.bs_mktid: str` field to provide
                    # a native-keyed map to their own symbol
                    # set(s).
                    self.pairs[pair.bs_mktid] = pair

                    # NOTE: `MktPair`s are keyed here using piker's
                    # internal FQME schema so that search,
                    # accounting and feed init can be accomplished
                    # a sane, uniform, normalized basis.
                    self.mktmaps[mkt.fqme] = mkt

                self.pair_ns_path: str = tractor.msg.NamespacePath.from_ref(
                    pair,
                )

            else:
                log.warning(
                    'No symbology cache `Pair` support for `{provider}`..\n'
                    'Implement `Client.get_mkt_pairs()`!'
                )

        return self

    @classmethod
    def from_dict(
        cls: type,
        data: dict,
        **kwargs,
    ) -> SymbologyCache:

        # normal init inputs
        cache = cls(**kwargs)

        # XXX WARNING: this may break if backend namespacing
        # changes (eg. `Pair` class def is moved to another
        # module) in which case you can manually update the
        # `pair_ns_path` in the symcache file and try again.
        # TODO: probably a verbose error about this?
        Pair: type = tractor.msg.NamespacePath(
            str(data['pair_ns_path'])
        ).load_ref()

        pairtable = data.pop('pairs')
        for key, pairtable in pairtable.items():

            # allow each serialized pair-dict-table to declare its
            # specific struct type's path in cases where a backend
            # supports multiples (normally with different
            # schemas..) and we are storing them in a flat `.pairs`
            # table.
            ThisPair = Pair
            if this_pair_type := pairtable.get('ns_path'):
                ThisPair: type = tractor.msg.NamespacePath(
                    str(this_pair_type)
                ).load_ref()

            pair: Struct = ThisPair(**pairtable)
            cache.pairs[key] = pair

        from ..accounting import (
            Asset,
            MktPair,
        )

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
            f'These `MktPair.dst: Asset`s DNE says `{cache.mod.name}`?\n'
            f'{pformat(dne)}'
        )
        return cache

    @staticmethod
    async def from_scratch(
        mod: ModuleType,
        fp: Path,
        **kwargs,

    ) -> SymbologyCache:
        '''
        Generate (a) new symcache (contents) entirely from scratch
        including all (TOML) serialized data and file.

        '''
        log.info(f'GENERATING symbology cache for `{mod.name}`')
        cache = SymbologyCache(
            mod=mod,
            fp=fp,
            **kwargs,
        )
        await cache.load()
        cache.write_config()
        return cache

    def search(
        self,
        pattern: str,
        table: str = 'mktmaps'

    ) -> dict[str, Struct]:
        '''
        (Fuzzy) search this cache's `.mktmaps` table, which is
        keyed by FQMEs, for `pattern: str` and return the best
        matches in a `dict` including the `MktPair` values.

        '''
        matches = fuzzy.extract(
            pattern,
            getattr(self, table),
            score_cutoff=50,
        )

        # repack in dict[fqme, MktPair] form
        return {
            item[0].fqme: item[0]
            for item in matches
        }


# actor-process-local in-mem-cache of symcaches (by backend).
_caches: dict[str, SymbologyCache] = {}


def mk_cachefile(
    provider: str,
) -> Path:
    cachedir: Path = config.get_conf_dir() / '_cache'
    if not cachedir.is_dir():
        log.info(f'Creating `nativedb` director: {cachedir}')
        cachedir.mkdir()

    cachefile: Path = cachedir / f'{str(provider)}.symcache.toml'
    cachefile.touch()
    return cachefile


@acm
async def open_symcache(
    mod_or_name: ModuleType | str,

    reload: bool = False,
    only_from_memcache: bool = False,  # no API req
    _no_symcache: bool = False,  # no backend support

) -> SymbologyCache:

    if isinstance(mod_or_name, str):
        mod = get_brokermod(mod_or_name)
    else:
        mod: ModuleType = mod_or_name

    provider: str = mod.name
    cachefile: Path = mk_cachefile(provider)

    # NOTE: certain backends might not support a symbology cache
    # (easily) and thus we allow for an empty instance to be loaded
    # and manually filled in at the whim of the caller presuming
    # the backend pkg-module is annotated appropriately.
    if (
        getattr(mod, '_no_symcache', False)
        or _no_symcache
    ):
        yield SymbologyCache(
            mod=mod,
            fp=cachefile,
        )
        # don't do nuttin
        return

    # actor-level cache-cache XD
    global _caches
    if not reload:
        try:
            yield _caches[provider]
        except KeyError:
            msg: str = (
                f'No asset info cache exists yet for `{provider}`'
            )
            if only_from_memcache:
                raise RuntimeError(msg)
            else:
                log.warning(msg)

    # if no cache exists or an explicit reload is requested, load
    # the provider API and call appropriate endpoints to populate
    # the mkt and asset tables.
    if (
        reload
        or not cachefile.is_file()
    ):
        cache = await SymbologyCache.from_scratch(
            mod=mod,
            fp=cachefile,
        )

    else:
        log.info(
            f'Loading EXISTING `{mod.name}` symbology cache:\n'
            f'> {cachefile}'
        )
        import time
        now = time.time()
        with cachefile.open('rb') as existing_fp:
            data: dict[str, dict] = tomllib.load(existing_fp)
            log.runtime(f'SYMCACHE TOML LOAD TIME: {time.time() - now}')

            # if there's an empty file for some reason we need
            # to do a full reload as well!
            if not data:
                cache = await SymbologyCache.from_scratch(
                    mod=mod,
                    fp=cachefile,
                )
            else:
                cache = SymbologyCache.from_dict(
                    data,
                    mod=mod,
                    fp=cachefile,
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
    Get any available symbology/assets cache from sync code by
    (maybe) manually running `trio` to do the work.

    '''
    # spawn tractor runtime and generate cache
    # if not existing.
    async def sched_gen_symcache():
        async with (
            # only for runtime's debug mode
            tractor.open_nursery(debug_mode=True),

            open_symcache(
                get_brokermod(provider),
                reload=force_reload,
            ) as symcache,
        ):
            return symcache

    try:
        symcache: SymbologyCache = trio.run(sched_gen_symcache)
        assert symcache
    except BaseException:
        import pdbp
        pdbp.xpm()

    return symcache


def match_from_pairs(
    pairs: dict[str, Struct],
    query: str,
    score_cutoff: int = 50,
    **extract_kwargs,

) -> dict[str, Struct]:
    '''
    Fuzzy search over a "pairs table" maintained by most backends
    as part of their symbology-info caching internals.

    Scan the native symbol key set and return best ranked
    matches back in a new `dict`.

    '''

    # TODO: somehow cache this list (per call) like we were in
    # `open_symbol_search()`?
    keys: list[str] = list(pairs)
    matches: list[tuple[
        Sequence[Hashable],  # matching input key
        Any,  # scores
        Any,
    ]] = fuzzy.extract(
        # NOTE: most backends provide keys uppercased
        query=query,
        choices=keys,
        score_cutoff=score_cutoff,
        **extract_kwargs,
    )

    # pop and repack pairs in output dict
    matched_pairs: dict[str, Struct] = {}
    for item in matches:
        pair_key: str = item[0]
        matched_pairs[pair_key] = pairs[pair_key]

    return matched_pairs
