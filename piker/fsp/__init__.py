"""
Financial signal processing for the peeps.
"""
from typing import AsyncIterator, Callable

import numpy as np

from ..log import get_logger
from .. import data
from ._momo import _rsi

log = get_logger(__name__)


_fsps = {'rsi': _rsi}


async def latency(
    source: 'TickStream[Dict[str, float]]',  # noqa
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
            # This is codified in the per-broker normalization layer
            # TODO: Add more measure points and diffs for full system
            # stack tracing.
            value = quote['brokerd_ts'] - quote['broker_ts']
            yield value


async def stream_and_process(
    bars: np.ndarray,
    brokername: str,
    # symbols: List[str],
    symbol: str,
    fsp_func_name: str,
) -> AsyncIterator[dict]:

    # remember, msgpack-numpy's ``from_buffer` returns read-only array
    # bars = np.array(bars[list(ohlc_dtype.names)])

    # async def _yield_bars():
    #     yield bars

    # hist_out: np.ndarray = None

    # Conduct a single iteration of fsp with historical bars input
    # async for hist_out in func(_yield_bars(), bars):
    #     yield {symbol: hist_out}
    func: Callable = _fsps[fsp_func_name]

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
            log.info(f"{fsp_func_name}: {processed}")
            yield processed
