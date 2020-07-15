"""
``marketstore`` integration.

- client management routines
- ticK data ingest routines
- websocket client for subscribing to write triggers
- todo: tick sequence stream-cloning for testing
- todo: docker container management automation
"""
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Callable, Tuple
import time
from math import isnan

import msgpack
import numpy as np
import pandas as pd
import pymarketstore as pymkts
import tractor
from trio_websocket import open_websocket_url

from ..log import get_logger, get_console_log
from ..data import open_feed


log = get_logger(__name__)

_tick_tbk_ids: Tuple[str, str] = ('1Sec', 'TICK')
_tick_tbk: str = '{}/' + '/'.join(_tick_tbk_ids)
_url: str = 'http://localhost:5993/rpc'
_quote_dt = [
    # these two are required for as a "primary key"
    ('Epoch', 'i8'),
    ('Nanoseconds', 'i4'),

    ('Tick', 'i4'),  # (-1, 0, 1) = (on bid, same, on ask)
    # ('fill_time', 'f4'),
    ('Last', 'f4'),
    ('Bid', 'f4'),
    ('Bsize', 'i8'),
    ('Asize', 'i8'),
    ('Ask', 'f4'),
    ('Size', 'i8'),
    ('Volume', 'i8'),
    # ('brokerd_ts', 'i64'),
    # ('VWAP', 'f4')
]
_quote_tmp = {}.fromkeys(dict(_quote_dt).keys(), np.nan)
_tick_map = {
    'Up': 1,
    'Equal': 0,
    'Down': -1,
    None: np.nan,
}


class MarketStoreError(Exception):
    "Generic marketstore client error"


def err_on_resp(response: dict) -> None:
    """Raise any errors found in responses from client request.
    """
    responses = response['responses']
    if responses is not None:
        for r in responses:
            err = r['error']
            if err:
                raise MarketStoreError(err)


def quote_to_marketstore_structarray(
    quote: Dict[str, Any],
    last_fill: str,
) -> np.array:
    """Return marketstore writeable structarray from quote ``dict``.
    """
    if last_fill:
        # new fill bby
        now = timestamp(last_fill)
    else:
        # this should get inserted upstream by the broker-client to
        # subtract from IPC latency
        now = time.time_ns()

    secs, ns = now / 10**9, now % 10**9

    # pack into List[Tuple[str, Any]]
    array_input = []

    # insert 'Epoch' entry first and then 'Nanoseconds'.
    array_input.append(int(secs))
    array_input.append(int(ns))

    # append remaining fields
    for name, dt in _quote_dt[2:]:
        if 'f' in dt:
            none = np.nan
        else:
            # for ``np.int`` we use 0 as a null value
            none = 0

        # casefold? see https://github.com/alpacahq/marketstore/issues/324
        val = quote.get(name.casefold(), none)
        array_input.append(val)

    return np.array([tuple(array_input)], dtype=_quote_dt)


def timestamp(datestr: str) -> int:
    """Return marketstore compatible 'Epoch' integer in nanoseconds
    from a date formatted str.
    """
    return int(pd.Timestamp(datestr).value)


def mk_tbk(keys: Tuple[str, str, str]) -> str:
    """Generate a marketstore table key from a tuple.

    Converts,
        ``('SPY', '1Sec', 'TICK')`` -> ``"SPY/1Sec/TICK"```
    """
    return '{}/' + '/'.join(keys)


class Client:
    """Async wrapper around the alpaca ``pymarketstore`` sync client.

    This will server as the shell for building out a proper async client
    that isn't horribly documented and un-tested..
    """
    def __init__(self, url: str):
        self._client = pymkts.Client(url)

    async def _invoke(
        self,
        meth: Callable,
        *args,
        **kwargs,
    ) -> Any:
        return err_on_resp(meth(*args, **kwargs))

    async def destroy(
        self,
        tbk: Tuple[str, str, str],
    ) -> None:
        return await self._invoke(self._client.destroy, mk_tbk(tbk))

    async def list_symbols(
        self,
        tbk: str,
    ) -> List[str]:
        return await self._invoke(self._client.list_symbols, mk_tbk(tbk))

    async def write(
        self,
        symbol: str,
        array: np.ndarray,
    ) -> None:
        start = time.time()
        await self._invoke(
            self._client.write,
            array,
            _tick_tbk.format(symbol),
            isvariablelength=True
        )
        log.debug(f"{symbol} write time (s): {time.time() - start}")

    def query(
        self,
        symbol,
        tbk: Tuple[str, str] = _tick_tbk_ids,
    ) -> pd.DataFrame:
        # XXX: causes crash
        # client.query(pymkts.Params(symbol, '*', 'OHCLV'
        result = self._client.query(
            pymkts.Params(symbol, *tbk),
        )
        return result.first().df()


@asynccontextmanager
async def get_client(
    url: str = _url,
) -> Client:
    yield Client(url)


async def ingest_quote_stream(
    symbols: List[str],
    brokername: str,
    tries: int = 1,
    loglevel: str = None,
) -> None:
    """Ingest a broker quote stream into marketstore in (sampled) tick format.
    """
    async with open_feed(
        brokername,
        symbols,
        loglevel=loglevel,
    ) as (first_quotes, qstream):

        quote_cache = first_quotes.copy()

        async with get_client() as ms_client:

            # start ingest to marketstore
            async for quotes in qstream:
                log.info(quotes)
                for symbol, quote in quotes.items():

                    # remap tick strs to ints
                    quote['tick'] = _tick_map[quote.get('tick', 'Equal')]

                    # check for volume update (i.e. did trades happen
                    # since last quote)
                    new_vol = quote.get('volume', None)
                    if new_vol is None:
                        log.debug(f"No fills for {symbol}")
                        if new_vol == quote_cache.get('volume'):
                            # should never happen due to field diffing
                            # on sender side
                            log.error(
                                f"{symbol}: got same volume as last quote?")

                    quote_cache.update(quote)

                    a = quote_to_marketstore_structarray(
                        quote,
                        # TODO: check this closer to the broker query api
                        last_fill=quote.get('fill_time', '')
                    )
                    await ms_client.write(symbol, a)


async def stream_quotes(
    symbols: List[str],
    host: str = 'localhost',
    port: int = 5993,
    diff_cached: bool = True,
    loglevel: str = None,
) -> None:
    """Open a symbol stream from a running instance of marketstore and
    log to console.
    """
    # XXX: required to propagate ``tractor`` loglevel to piker logging
    get_console_log(loglevel or tractor.current_actor().loglevel)

    tbks: Dict[str, str] = {sym: f"{sym}/*/*" for sym in symbols}

    async with open_websocket_url(f'ws://{host}:{port}/ws') as ws:
        # send subs topics to server
        resp = await ws.send_message(
            msgpack.dumps({'streams': list(tbks.values())})
        )
        log.info(resp)

        async def recv() -> Dict[str, Any]:
            return msgpack.loads((await ws.get_message()), encoding='utf-8')

        streams = (await recv())['streams']
        log.info(f"Subscribed to {streams}")

        _cache = {}

        while True:
            msg = await recv()

            # unpack symbol and quote data
            # key is in format ``<SYMBOL>/<TIMEFRAME>/<ID>``
            symbol = msg['key'].split('/')[0]
            data = msg['data']

            # calc time stamp(s)
            s, ns = data.pop('Epoch'), data.pop('Nanoseconds')
            ts = s * 10**9 + ns
            data['broker_fill_time_ns'] = ts

            quote = {}
            for k, v in data.items():
                if isnan(v):
                    continue

                quote[k.lower()] = v

            quote['symbol'] = symbol

            quotes = {}

            if diff_cached:
                last = _cache.setdefault(symbol, {})
                new = set(quote.items()) - set(last.items())
                if new:
                    log.info(f"New quote {quote['symbol']}:\n{new}")

                    # only ship diff updates and other required fields
                    payload = {k: quote[k] for k, v in new}
                    payload['symbol'] = symbol

                    # if there was volume likely the last size of
                    # shares traded is useful info and it's possible
                    # that the set difference from above will disregard
                    # a "size" value since the same # of shares were traded
                    size = quote.get('size')
                    volume = quote.get('volume')
                    if size and volume:
                        new_volume_since_last = max(
                            volume - last.get('volume', 0), 0)
                        log.warning(
                            f"NEW VOLUME {symbol}:{new_volume_since_last}")
                        payload['size'] = size
                        payload['last'] = quote.get('last')

                    # XXX: we append to a list for the options case where the
                    # subscription topic (key) is the same for all
                    # expiries even though this is uncessary for the
                    # stock case (different topic [i.e. symbol] for each
                    # quote).
                    quotes.setdefault(symbol, []).append(payload)

                    # update cache
                    _cache[symbol].update(quote)
            else:
                quotes = {
                    symbol: [{key.lower(): val for key, val in quote.items()}]}

            if quotes:
                yield quotes
