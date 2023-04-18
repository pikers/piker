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
)

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
    # backend specific, market endpoint id
    bs_mktid: str
    mkt_info: MktPair
    shm_write_opts: dict[str, Any] | None = None


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
    if isinstance(init_msgs, dict):
        for i, (sym_str, msg) in enumerate(init_msgs.items()):
            init: FeedInit | dict[str, Any] = msg

            # XXX: eventually this WILL NOT necessarily be true.
            if i > 0:
                assert not len(init_msgs) == 1
                keys: set = set(init_msgs.keys()) - set(syms)
                raise FeedInitializationError(
                    'TOO MANY INIT MSGS!\n'
                    f'Unexpected keys: {keys}\n'
                    'ALL MSGS:\n'
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
    bs_mktid: str  # backend specific (unique) market id
    bs_fqme: str  # backend specific fqme
    mkt: MktPair

    match init:
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
            mkt = MktPair.from_fqme(
                fqme=f'{bs_fqme}.{mod.name}',

                price_tick=price_tick,
                size_tick=size_tick,

                bs_mktid=str(init['bs_mktid']),
                _atype=symbol_info['asset_type']
            )

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
            # bs_mktid=bs_mktid,
            mkt_info=MktPair(dst=Asset()) as mkt,
            shm_write_opts=dict(),
        ) as init:
            log.info(
                f'NICE JOB {mod.name} BACKEND!\n'
                'You are fully up to API spec B):\n'
                f'{init.to_dict()}'
            )

        case _:
            raise FeedInitializationError(init)

    # build a msg if we received a dict for input.
    if not rx_msg:
        init = FeedInit(
            bs_mktid=mkt.bs_mktid,
            mkt_info=mkt,
            shm_write_opts=init.get('shm_write_opts'),
        )

    # `MktPair` value audits
    mkt = init.mkt_info
    assert bs_fqme in mkt.fqme
    assert mkt.type_key

    # `MktPair` wish list
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
