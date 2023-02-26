provider "spec" (aka backends)
==============================
``piker`` abstracts and encapsulates real-time data feeds across a slew
of providers covering many (pretty much any) instrument class.

This doc is shoddy attempt as specifying what a backend must provide as
a basic api, per functionality-feature set, in order to be supported for
bny of real-time and historical data feeds and order control via the
``emsd`` clearing system.

"providers" must offer a top plevel namespace (normally exposed as
a python module) which offers to a certain set of (async) functions
to deliver info through a real-time, normalized data layer.

Generally speaking we break each ``piker.brokers.<backend_name>`` into
a python package containing 3 sub-modules:
- ``.api`` containing lowest level client code used to interact
  specifically with the APIs of the exchange, broker or data provider.
- ``.feed`` which provides historical and real-time quote stream data
  provider endpoints called by piker's data layer in
  ``piker.data.feed``.
- ``.broker`` which defines endpoints expected by
  ``pikerd.clearing._ems`` and which are expected to adhere to the msg
  protocol defined in ``piker.clrearing._messages``.


Our current set of "production" grade backends includes:
- ``kraken``
- ``ib``


data feeds
----------

real-time quotes and tick streaming:

.. code:: python

    async def stream_quotes(

        send_chan: trio.abc.SendChannel,
        symbols: List[str],
        shm: ShmArray,
        feed_is_live: trio.Event,
        loglevel: str = None,  # log level passed in from user config

        # startup sync via ``trio``
        task_status: TaskStatus[Tuple[Dict, Dict]] = trio.TASK_STATUS_IGNORED,

    ) -> None:


this routine must eventually deliver realt-time quote messages by sending them on
the passed in ``send_chan``; these messages must have specific format.
there is a very simple but required startup sequence:

message starup sequence:
************************
at a minimum, and asap, a first quote message should be returned for
each requested symbol in ``symbols``. the message should have a minimum
format:

.. code:: python

    quote_msg: dict[str, Any] = {
        'symbol': 'xbtusd',  # or wtv symbol was requested

        # this field is required in the initial first quote only (though
        # is recommended in all follow up quotes) but can be 
        'last': <last clearing price>,  # float

        # tick stream fields (see below for schema/format)
        'ticks': list[dict[str, Any]],
    }

further streamed quote messages should be in this same format.
``ticks`` is an optional sequence


historical OHLCV sampling
-------------------------
Example endpoint copyed from the ``binance`` backend:

.. code:: python

   @acm
    async def open_history_client(
        symbol: str,

    ) -> tuple[Callable, int]:

        # TODO implement history getter for the new storage layer.
        async with open_cached_client('binance') as client:

            async def get_ohlc(
                timeframe: float,
                end_dt: datetime | None = None,
                start_dt: datetime | None = None,

            ) -> tuple[
                np.ndarray,
                datetime,  # start
                datetime,  # end
            ]:
                if timeframe != 60:
                    raise DataUnavailable('Only 1m bars are supported')

                array = await client.bars(
                    symbol,
                    start_dt=start_dt,
                    end_dt=end_dt,
                )
                times = array['time']
                if (
                    end_dt is None
                ):
                    inow = round(time.time())
                    if (inow - times[-1]) > 60:
                        await tractor.breakpoint()

                start_dt = pendulum.from_timestamp(times[0])
                end_dt = pendulum.from_timestamp(times[-1])

                return array, start_dt, end_dt

            yield get_ohlc, {'erlangs': 3, 'rate': 3}


This `@acm` routine is responsible for setting up an async historical
data query routine for both charting and any local storage requirements.

The returned async func should retreive, normalize and deliver
a ``tuple[np.ndarray, pendulum.dateime, pendulum.dateime]`` of the the
``numpy``-ified data, the start and stop datetimes for the delivered
history "frame". The history backloading routines inside
``piker.data.feed`` expect this interface for both loading history into
``ShmArrayt`` real-time buffers as well as any configured
time-series-database (tsdb) and  normally the format of this data is
OHLCV sampled price and volume data but in theory can be high
reslolution tick/trades/book times series in the future.

Currently sampling routines for charting and fsp processing expects
a max resolution of 1s (second) OHLCV sampled data.


OHLCV minmal schema
********************
ohlcv at a minimum is normally pushed to local shared memory (shm)
numpy compatible arrays which are read by both UI components for display
as well auto-strats and algorithmic trading engines. shm is obviously
used for speed. we also intend to eventually support pure shm tick
streams for ultra low latency processing by external processes/services.

the provider module at a minimum must define a ``numpy`` structured
array dtype ``ohlc_dtype = np.dtype(_ohlc_dtype)`` where the
``_ohlc_dtype`` is normally defined  in standard list-tuple synatx as:

.. code:: python

    # Broker specific ohlc schema which includes a vwap field
    _ohlc_dtype = [
        ('index', int),
        ('time', int),
        ('open', float),
        ('high', float),
        ('low', float),
        ('close', float),
        ('volume', float),
        ('count', int),
        ('bar_wap', float),
    ]
