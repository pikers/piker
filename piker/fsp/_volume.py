# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship of pikers)

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

from typing import AsyncIterator, Optional, Union

import numpy as np
from tractor.trionics._broadcast import AsyncReceiver

from ._api import fsp
from ..data import iterticks
from ..data._sharedmem import ShmArray
from ._momo import _wma
from ..log import get_logger

log = get_logger(__name__)


# NOTE: is the same as our `wma` fsp, and if so which one is faster?
# Ohhh, this is an IIR style i think? So it has an anchor point
# effectively instead of a moving window/FIR style?
def wap(

    signal: np.ndarray,
    weights: np.ndarray,

) -> np.ndarray:
    '''
    Weighted average price from signal and weights.

    '''
    cum_weights = np.cumsum(weights)
    cum_weighted_input = np.cumsum(signal * weights)

    # cum_weighted_input / cum_weights
    # but, avoid divide by zero errors
    avg = np.divide(
        cum_weighted_input,
        cum_weights,
        where=cum_weights != 0
    )

    return (
        avg,
        cum_weighted_input,
        cum_weights,
    )


@fsp
async def tina_vwap(

    source: AsyncReceiver[dict],
    ohlcv: ShmArray,  # OHLC sampled history

    # TODO: anchor logic (eg. to session start)
    anchors: Optional[np.ndarray] = None,

) -> Union[
    AsyncIterator[np.ndarray],
    float
]:
    '''
    Streaming volume weighted moving average.

    Calling this "tina" for now since we're using HLC3 instead of tick.

    '''
    if anchors is None:
        # TODO:
        # anchor to session start of data if possible
        pass

    a = ohlcv.array
    chl3 = (a['close'] + a['high'] + a['low']) / 3
    v = a['volume']

    h_vwap, cum_wp, cum_v = wap(chl3, v)

    # deliver historical output as "first yield"
    yield h_vwap

    w_tot = cum_wp[-1]
    v_tot = cum_v[-1]
    # vwap_tot = h_vwap[-1]

    async for quote in source:
        for tick in iterticks(
            quote,
            types=['trade'],
        ):

            # c, h, l, v = ohlcv.array[-1][
            #     ['closes', 'high', 'low', 'volume']
            # ]

            # this computes tick-by-tick weightings from here forward
            size = tick['size']
            price = tick['price']

            v_tot += size
            w_tot += price * size

            # yield ((((o + h + l) / 3) * v) weights_tot) / v_tot
            yield 'tina_vwap', w_tot / v_tot


@fsp(
    outputs=(
        'dolla_vlm',
        'dark_vlm',
        'trade_count',
        'dark_trade_count',
    ),
    curve_style='step',
)
async def dolla_vlm(
    source: AsyncReceiver[dict],
    ohlcv: ShmArray,  # OHLC sampled history

) -> AsyncIterator[
    tuple[str, Union[np.ndarray, float]],
]:
    '''
    "Dollar Volume", aka the volume in asset-currency-units (usually
    a fiat) computed from some price function for the sample step
    *multiplied* (*) by the asset unit volume.

    Useful for comparing cross asset "money flow" in #s that are
    asset-currency-independent.

    '''
    a = ohlcv.array
    chl3 = (a['close'] + a['high'] + a['low']) / 3
    v = a['volume']

    # on first iteration yield history
    yield {
        'dolla_vlm': chl3 * v,
        'dark_vlm': None,
    }

    i = ohlcv.index
    dvlm = vlm = 0
    dark_trade_count = trade_count = 0

    async for quote in source:
        for tick in iterticks(
            quote,
            types=(
                'trade',
                'dark_trade',
            ),
            deduplicate_darks=True,
        ):

            # this computes tick-by-tick weightings from here forward
            size = tick['size']
            price = tick['price']

            li = ohlcv.index
            if li > i:
                i = li
                trade_count = dark_trade_count = dvlm = vlm = 0

            # TODO: for marginned instruments (futes, etfs?) we need to
            # show the margin $vlm by multiplying by whatever multiplier
            # is reported in the sym info.

            ttype = tick.get('type')

            if ttype == 'dark_trade':
                dvlm += price * size
                yield 'dark_vlm', dvlm

                dark_trade_count += 1
                yield 'dark_trade_count', dark_trade_count

                # print(f'{dark_trade_count}th dark_trade: {tick}')

            else:
                # print(f'vlm: {tick}')
                vlm += price * size
                yield 'dolla_vlm', vlm

                trade_count += 1
                yield 'trade_count', trade_count

            # TODO: plot both to compare?
            # c, h, l, v = ohlcv.last()[
            #     ['close', 'high', 'low', 'volume']
            # ][0]
            # tina_lvlm = c+h+l/3 * v
            # print(f' tinal vlm: {tina_lvlm}')


@fsp(
    # TODO: eventually I guess we should support some kinda declarative
    # graphics config syntax per output yah? That seems like a clean way
    # to let users configure things? Not sure how exactly to offer that
    # api as well as how to expose such a thing *inside* the body?
    outputs=(
        # pulled verbatim from `ib` for now
        '1m_trade_rate',
        '1m_vlm_rate',

        # our own instantaneous rate calcs which are all
        # parameterized by a samples count (bars) period
        'trade_rate',
        'dark_trade_rate',

        'dvlm_rate',
        'dark_dvlm_rate',
    ),
    curve_style='line',
)
async def flow_rates(
    source: AsyncReceiver[dict],
    ohlcv: ShmArray,  # OHLC sampled history

    # TODO (idea): a dynamic generic / boxing type that can be updated by other
    # FSPs, user input, and possibly any general event stream in
    # real-time. Hint: ideally implemented with caching until mutated
    # ;)
    period: 'Param[int]' = 1,  # noqa

    # TODO: support other means by providing a map
    # to weights `partial()`-ed with `wma()`?
    mean_type: str = 'arithmetic',

    # TODO (idea): a generic for declaring boxed fsps much like ``pytest``
    # fixtures? This probably needs a lot of thought if we want to offer
    # a higher level composition syntax eventually (oh right gotta make
    # an issue for that).
    # ideas for how to allow composition / intercalling:
    # - offer a `Fsp.get_history()` to do the first yield output?
    #  * err wait can we just have shm access directly?
    # - how would it work if some consumer fsp wanted to dynamically
    # change params which are input to the callee fsp? i guess we could
    # lazy copy in that case?
    # dvlm: 'Fsp[dolla_vlm]'

) -> AsyncIterator[
    tuple[str, Union[np.ndarray, float]],
]:
    # generally no history available prior to real-time calcs
    yield {
        # from ib
        '1m_trade_rate': None,
        '1m_vlm_rate': None,

        'trade_rate': None,
        'dark_trade_rate': None,

        'dvlm_rate': None,
        'dark_dvlm_rate': None,
    }

    quote = await anext(source)

    # ltr = 0
    # lvr = 0
    tr = quote.get('tradeRate')
    yield '1m_trade_rate', tr or 0
    vr = quote.get('volumeRate')
    yield '1m_vlm_rate', vr or 0

    yield 'trade_rate', 0
    yield 'dark_trade_rate', 0
    yield 'dvlm_rate', 0
    yield 'dark_dvlm_rate', 0

    # NOTE: in theory we could dynamically allocate a cascade based on
    # this call but not sure if that's too "dynamic" in terms of
    # validating cascade flows from message typing perspective.

    # attach to ``dolla_vlm`` fsp running
    # on this same source flow.
    dvlm_shm = dolla_vlm.get_shm(ohlcv)

    # precompute arithmetic mean weights (all ones)
    seq = np.full((period,), 1)
    weights = seq / seq.sum()

    async for quote in source:
        if not quote:
            log.error("OH WTF NO QUOTE IN FSP")
            continue

        # dvlm_wma = _wma(
        #     dvlm_shm.array['dolla_vlm'],
        #     period,
        #     weights=weights,
        # )
        # yield 'dvlm_rate', dvlm_wma[-1]

        if period > 1:
            trade_rate_wma = _wma(
                dvlm_shm.array['trade_count'][-period:],
                period,
                weights=weights,
            )
            trade_rate = trade_rate_wma[-1]
            # print(trade_rate)
            yield 'trade_rate', trade_rate
        else:
            # instantaneous rate per sample step
            count = dvlm_shm.array['trade_count'][-1]
            yield 'trade_rate', count

        # TODO: skip this if no dark vlm is declared
        # by symbol info (eg. in crypto$)
        # dark_dvlm_wma = _wma(
        #     dvlm_shm.array['dark_vlm'],
        #     period,
        #     weights=weights,
        # )
        # yield 'dark_dvlm_rate', dark_dvlm_wma[-1]

        if period > 1:
            dark_trade_rate_wma = _wma(
                dvlm_shm.array['dark_trade_count'][-period:],
                period,
                weights=weights,
            )
            yield 'dark_trade_rate', dark_trade_rate_wma[-1]
        else:
            # instantaneous rate per sample step
            dark_count = dvlm_shm.array['dark_trade_count'][-1]
            yield 'dark_trade_rate', dark_count

        # XXX: ib specific schema we should
        # probably pre-pack ourselves.

        # tr = quote.get('tradeRate')
        # if tr is not None and tr != ltr:
        #     # print(f'trade rate: {tr}')
        #     yield '1m_trade_rate', tr
        #     ltr = tr

        # vr = quote.get('volumeRate')
        # if vr is not None and vr != lvr:
        #     # print(f'vlm rate: {vr}')
        #     yield '1m_vlm_rate', vr
        #     lvr = vr
