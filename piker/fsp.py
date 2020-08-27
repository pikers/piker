"""
Financial signal processing for the peeps.
"""
from typing import AsyncIterator, List

import numpy as np

from .log import get_logger
from . import data

log = get_logger(__name__)


def rec2array(
    rec: np.ndarray,
    fields: List[str] = None
) -> np.ndarray:
    """Convert record array to std array.

    Taken from:
    https://github.com/scikit-hep/root_numpy/blob/master/root_numpy/_utils.py#L20
    """
    simplify = False

    if fields is None:
        fields = rec.dtype.names
    elif isinstance(fields, str):
        fields = [fields]
        simplify = True

    # Creates a copy and casts all data to the same type
    arr = np.dstack([rec[field] for field in fields])

    # Check for array-type fields. If none, then remove outer dimension.
    # Only need to check first field since np.dstack will anyway raise an
    # exception if the shapes don't match
    # np.dstack will also fail if fields is an empty list
    if not rec.dtype[fields[0]].shape:
        arr = arr[0]

    if simplify:
        # remove last dimension (will be of size 1)
        arr = arr.reshape(arr.shape[:-1])

    return arr


async def pull_and_process(
    bars: np.ndarray,
    brokername: str,
    # symbols: List[str],
    symbol: str,
    fsp_func_name: str,
) -> AsyncIterator[dict]:

    # async def _yield_bars():
    #     yield bars

    # hist_out: np.ndarray = None

    func = latency

    # Conduct a single iteration of fsp with historical bars input
    # async for hist_out in func(_yield_bars(), bars):
    #     yield {symbol: hist_out}

    # open a data feed stream with requested broker
    async with data.open_feed(
        brokername,
        [symbol],
    ) as (fquote, stream):

        # TODO: load appropriate fsp with input args

        async def filter_by_sym(sym, stream):
            async for quotes in stream:
                for symbol, quotes in quotes.items():
                    if symbol == sym:
                        yield quotes

        async for processed in func(
            filter_by_sym(symbol, stream),
            bars,
        ):
            print(f"{fsp_func_name}: {processed}")
            yield processed


# TODO: things to figure the fuck out:
# - how to handle non-plottable values
# - composition of fsps / implicit chaining

async def latency(
    source: 'TickStream[Dict[str, float]]',
    ohlcv: np.ndarray
) -> AsyncIterator[np.ndarray]:
    """Compute High-Low midpoint value.
    """
    # TODO: do we want to offer yielding this async
    # before the rt data connection comes up?

    # deliver zeros for all prior history
    yield np.zeros(len(ohlcv))

    async for quote in source:
        ts = quote.get('broker_ts')
        if ts:
            print(
                f"broker time: {quote['broker_ts']}"
                f"brokerd time: {quote['brokerd_ts']}"
            )
            value = quote['brokerd_ts'] - quote['broker_ts']
            yield value


async def last(
    source: 'TickStream[Dict[str, float]]',
    ohlcv: np.ndarray
) -> AsyncIterator[np.ndarray]:
    """Compute High-Low midpoint value.
    """
    # deliver historical processed data first
    yield ohlcv['close']

    async for quote in source:
        yield quote['close']


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
        _lookback = frame[-(length-1):]
        yield wma
