"""
Momentum bby.
"""
from typing import AsyncIterator

import numpy as np
from numba import jit, float64, optional


# TODO: things to figure the fuck out:
# - how to handle non-plottable values
# - composition of fsps / implicit chaining

@jit(
    float64[:](
        float64[:],
        optional(float64),
        optional(float64)
    ),
    nopython=True,
    nogil=True
)
def ema(
    y: 'np.ndarray[float64]',
    alpha: optional(float64) = None,
    ylast: optional(float64) = None,
) -> 'np.ndarray[float64]':
    r"""Exponential weighted moving average owka 'Exponential smoothing'.

    - https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
    - https://en.wikipedia.org/wiki/Exponential_smoothing

    Fun facts:
        A geometric progression is the discrete version of an
        exponential function, that is where the name for this
        smoothing method originated according to statistics lore. In
        signal processing parlance, an EMA is a first order IIR filter.

    .. math::

        .tex
        {S_{t}={\begin{cases}Y_{1},&t=1
         \\\alpha Y_{t}+(1-\alpha )\cdot S_{t-1},&t>1\end{cases}}}

        .nerd
        (2) s = {
                s[0] = y[0]; t = 0
                s[t] = a*y[t] + (1-a)*s[t-1], t > 0.
            }
    """
    n = y.shape[0]

    if alpha is None:
        # https://en.wikipedia.org/wiki/Moving_average#Relationship_between_SMA_and_EMA
        # use the "center of mass" convention making an ema compare
        # directly to the com of a SMA or WMA:
        alpha = 2 / float(n + 1)

    s = np.empty(n, dtype=float64)

    if n == 1:
        s[0] = y[0] * alpha + ylast * (1 - alpha)

    else:
        if ylast is None:
            s[0] = y[0]
        else:
            s[0] = ylast

        for i in range(1, n):
            s[i] = y[i] * alpha + s[i-1] * (1 - alpha)

    return s


# @jit(
#     float64[:](
#         float64[:],
#         int64,
#     ),
#     # nopython=True,
#     nogil=True
# )
def rsi(
    signal: 'np.ndarray[float64]',
    period: int = 14,
    up_ema_last: float64 = None,
    down_ema_last: float64 = None,
) -> 'np.ndarray[float64]':
    alpha = 1/period
    # print(signal)

    df = np.diff(signal)
    up, down = np.where(df > 0, df, 0), np.where(df < 0, -df, 0)
    up_ema = ema(up, alpha, up_ema_last)
    down_ema = ema(down, alpha, down_ema_last)
    rs = np.divide(
        up_ema,
        down_ema,
        out=np.zeros_like(up_ema),
        where=down_ema!=0
    )
    # print(f'up_ema: {up_ema}\ndown_ema: {down_ema}')
    # print(f'rs: {rs}')
    # map rs through sigmoid (with range [0, 100])
    rsi = 100 - 100 / (1 + rs)
    # rsi = 100 * (up_ema / (up_ema + down_ema))
    # also return the last ema state for next iteration
    return rsi, up_ema[-1], down_ema[-1]


# @piker.fsp(
#     aggregates=['30s', '1m', '5m', '1H', '4H', '1D'],
# )
async def _rsi(
    source: 'QuoteStream[Dict[str, Any]]',  # noqa
    ohlcv: np.ndarray,
    period: int = 14,
) -> AsyncIterator[np.ndarray]:
    """Multi-timeframe streaming RSI.

    https://en.wikipedia.org/wiki/Relative_strength_index
    """
    sig = ohlcv['close']

    # TODO: the emas here should be seeded with a period SMA as per
    # wilder's original formula..
    rsi_h, up_ema_last, down_ema_last = rsi(sig, period, None, None)

    # deliver history
    yield rsi_h

    _last = sig[-1]

    async for quote in source:
        # tick based updates
        for tick in quote.get('ticks', ()):
            if tick.get('type') == 'trade':
                curr = tick['price']
                last = np.array([_last, curr])
                # await tractor.breakpoint()
                rsi_out, up_ema_last, down_ema_last = rsi(
                    last,
                    period=period,
                    up_ema_last=up_ema_last,
                    down_ema_last=down_ema_last,
                )
                _last = curr
                # print(f'last: {last}\n rsi: {rsi_out}')
                yield rsi_out[-1]


async def wma(
    source,  #: AsyncStream[np.ndarray],
    ohlcv: np.ndarray,  # price time-frame "aware"
    lookback: np.ndarray,  # price time-frame "aware"
    weights: np.ndarray,
) -> AsyncIterator[np.ndarray]:  # i like FinSigStream
    """Weighted moving average.

    ``weights`` is a sequence of already scaled values. As an example
    for the WMA often found in "techincal analysis":
    ``weights = np.arange(1, N) * N*(N-1)/2``.
    """
    length = len(weights)
    _lookback = np.zeros(length - 1)

    ohlcv.from_tf('5m')

    # async for frame_len, frame in source:
    async for frame in source:
        wma = np.convolve(
            ohlcv[-length:]['close'],
            # np.concatenate((_lookback, frame)),
            weights,
            'valid'
        )
        # todo: handle case where frame_len < length - 1
        _lookback = frame[-(length-1):]  # noqa
        yield wma
