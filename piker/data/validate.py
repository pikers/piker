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
Data feed synchronization protocols, init msgs, and general
data-provider-backend-agnostic schema definitions.

'''
from decimal import Decimal
from pprint import pformat
from types import ModuleType
from typing import (
    Any,
    Callable,
)

from msgspec import field

from .types import Struct
from ..accounting import (
    Asset,
    MktPair,
)
from ._util import log


class FeedInitializationError(ValueError):
    '''
    Live data feed setup failed due to API / msg incompatiblity!

    '''


class FeedInit(Struct, frozen=True):
    '''
    A stringent data provider startup msg schema validator.

    The fields defined here are matched with those absolutely required
    from each backend broker/data provider.

    '''
    mkt_info: MktPair

    # NOTE: only field we use rn in ``.data.feed``
    # TODO: maybe make a SamplerConfig(Struct)?
    shm_write_opts: dict[str, Any] = field(
        default_factory=lambda: {
        'has_vlm': True,
        'sum_tick_vlm': True,
    })

# XXX: we group backend endpoints into 3
# groups to determine "degrees" of functionality.
_eps: dict[str, list[str]] = {

    # basic API `Client` layer
    'middleware': [
        'get_client',
    ],

    # (live) data streaming / loading / search
    'datad': [
        'get_mkt_info',
        'open_history_client',
        'open_symbol_search',
        'stream_quotes',
    ],

    # live order control and trading
    'brokerd': [
        'trades_dialogue',
        'open_trade_dialog',  # live order ctl
        'norm_trade', # ledger normalizer for txns
    ],
}


def validate_backend(
    mod: ModuleType,
    syms: list[str],
    init_msgs: list[FeedInit] | dict[str, dict[str, Any]],

    # TODO: do a module method scan and report mismatches.
    check_eps: bool = False,

    api_log_msg_level: str = 'critical'

) -> FeedInit:
    '''
    Fail on malformed live quotes feed config/init or warn on changes
    that haven't been implemented by this backend yet.

    '''
    for daemon_name, eps in _eps.items():
        for name in eps:
            ep: Callable = getattr(
                mod,
                name,
                None,
            )
            if ep is None:
                log.warning(
                    f'Provider backend {mod.name} is missing '
                    f'{daemon_name} support :(\n'
                    f'The following endpoint is missing: {name}'
                )

    inits: list[
        FeedInit | dict[str, Any]
    ] = init_msgs

    # convert to list if from old dict-style
    if isinstance(init_msgs, dict):
        inits = list(init_msgs.values())

    init: FeedInit | dict[str, Any]
    for i, init in enumerate(inits):

        # XXX: eventually this WILL NOT necessarily be true.
        if i > 0:
            assert not len(init_msgs) == 1
            if isinstance(init_msgs, dict):
                keys: set = set(init_msgs.keys()) - set(syms)
                raise FeedInitializationError(
                    'TOO MANY INIT MSGS!\n'
                    f'Unexpected keys: {keys}\n'
                    'ALL MSGS:\n'
                    f'{pformat(init_msgs)}\n'
                )
            else:
                raise FeedInitializationError(
                    'TOO MANY INIT MSGS!\n'
                    f'{pformat(init_msgs)}\n'
                )

    # TODO: once all backends are updated we can remove this branching.
    rx_msg: bool = False
    warn_msg: str = ''
    if not isinstance(init, FeedInit):
        warn_msg += (
            '\n'
            '--------------------------\n'
            ':::DEPRECATED API STYLE:::\n'
            '--------------------------\n'
            f'`{mod.name}.stream_quotes()` should deliver '
            '`.started(FeedInit)`\n'
            f'|-> CURRENTLY it is using DEPRECATED `.started(dict)` style!\n'
            f'|-> SEE `FeedInit` in `piker.data.validate`\n'
            '--------------------------------------------\n'
        )
    else:
        rx_msg = True

    # verify feed init state / schema
    bs_fqme: str  # backend specific fqme
    mkt: MktPair

    match init:

        # backend is using old dict msg delivery
        case {
            'symbol_info': dict(symbol_info),
            'fqsn': bs_fqme,
        } | {
            'mkt_info': dict(symbol_info),
            'fqsn': bs_fqme,
        }:
            symbol_info: dict
            warn_msg += (
                'It may also be still using the legacy `Symbol` style API\n'
                'IT SHOULD BE PORTED TO THE NEW '
                '`.accounting._mktinfo.MktPair`\n'
                'STATTTTT!!!\n'
            )

            # XXX use default legacy (aka discrete precision) mkt
            # price/size_ticks if none delivered.
            price_tick = symbol_info.get(
                'price_tick_size',
                Decimal('0.01'),
            )
            size_tick = symbol_info.get(
                'lot_tick_size',
                Decimal('1'),
            )
            bs_mktid = init.get('bs_mktid') or bs_fqme
            mkt = MktPair.from_fqme(
                fqme=f'{bs_fqme}.{mod.name}',

                price_tick=price_tick,
                size_tick=size_tick,

                bs_mktid=str(bs_mktid),
                _atype=symbol_info['asset_type']
            )

        # backend is using new `MktPair` but not entirely
        case {
            'mkt_info': MktPair(
                dst=Asset(),
            ) as mkt,
            'fqsn': bs_fqme,
        }:
            warn_msg += (
                f'{mod.name} in API compat transition?\n'
                "It's half dict, half man..\n"
                '-------------------------------------\n'
            )

        case FeedInit(
            mkt_info=MktPair(dst=Asset()) as mkt,
            shm_write_opts=dict(shm_opts),
        ) as init:
            name: str = mod.name
            log.info(
                f"{name}'s `MktPair` info:\n"
                f'{pformat(mkt.to_dict())}\n'
                f'shm conf: {pformat(shm_opts)}\n'
            )

        case _:
            raise FeedInitializationError(init)

    # build a msg if we received a dict for input.
    if not rx_msg:
        assert bs_fqme in mkt.fqme
        init = FeedInit(
            mkt_info=mkt,
            shm_write_opts=init.get('shm_write_opts'),
        )

    # `MktPair` value audits
    mkt = init.mkt_info
    assert mkt.type_key

    # backend is using new `MktPair` but not embedded `Asset` types
    # for the .src/.dst..
    if not isinstance(mkt.src, Asset):
        warn_msg += (
            f'ALSO, {mod.name.upper()} should try to deliver\n'
            'the new `MktPair.src: Asset` field!\n'
            '-----------------------------------------------\n'
        )

    # complain about any non-idealities
    if warn_msg:
        # TODO: would be nice to register an API_COMPAT or something in
        # maybe cyan for this in general throughput piker no?
        logmeth = getattr(log, api_log_msg_level)
        logmeth(warn_msg)

    return init.copy()
