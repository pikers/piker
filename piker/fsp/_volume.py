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
from ..data._normalize import iterticks
from ..data._sharedmem import ShmArray


def wap(

    signal: np.ndarray,
    weights: np.ndarray,

) -> np.ndarray:
    """Weighted average price from signal and weights.

    """
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
        for tick in iterticks(quote, types=['trade']):

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
    outputs=('dolla_vlm', 'dark_vlm'),
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
    output = vlm = 0
    dvlm = 0

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
                vlm = 0
                dvlm = 0

            # TODO: for marginned instruments (futes, etfs?) we need to
            # show the margin $vlm by multiplying by whatever multiplier
            # is reported in the sym info.

            ttype = tick.get('type')

            if ttype == 'dark_trade':
                # print(f'dark_trade: {tick}')
                key = 'dark_vlm'
                dvlm += price * size
                output = dvlm

            else:
                # print(f'vlm: {tick}')
                key = 'dolla_vlm'
                vlm += price * size
                output = vlm

            # TODO: plot both to compare?
            # c, h, l, v = ohlcv.last()[
            #     ['close', 'high', 'low', 'volume']
            # ][0]
            # tina_lvlm = c+h+l/3 * v
            # print(f' tinal vlm: {tina_lvlm}')

            yield key, output


@fsp(
    outputs=(
        '1m_trade_rate',
        '1m_vlm_rate',
    ),
    curve_style='line',
)
async def flow_rates(
    source: AsyncReceiver[dict],
    ohlcv: ShmArray,  # OHLC sampled history

) -> AsyncIterator[
    tuple[str, Union[np.ndarray, float]],
]:
    # generally no history available prior to real-time calcs
    yield {
        '1m_trade_rate': None,
        '1m_vlm_rate': None,
    }

    ltr = 0
    lvr = 0
    async for quote in source:
        if quote:

            tr = quote['tradeRate']
            if tr != ltr:
                print(f'trade rate: {tr}')
                yield '1m_trade_rate', tr
                ltr = tr

            vr = quote['volumeRate']
            if vr != lvr:
                print(f'vlm rate: {vr}')
                yield '1m_vlm_rate', vr
                lvr = vr
