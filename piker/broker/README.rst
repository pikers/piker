provider "spec" (aka backends)
==============================
``piker`` abstracts and encapsulates real-time data feeds across a slew
of providers covering many (pretty much any) instrument class.


this is shoddy attempt as specifying what a backend must provide
as a basic api, per functionality-feature set, in order to be
supported in zee sytem.

"providers" must offer a top plevel namespace (normally exposed as
a python module) which offers to a certain set of (async) functions
to deliver info through a real-time, normalized data layer.


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

.. code:: python

    async def backfill_bars(
        sym: str,
        shm: ShmArray,  # type: ignore # noqa

        count: int = 10,  # NOTE: any more and we'll overrun the underlying buffer

        # startup sync via ``trio``
        task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,

    ) -> None:

this routine is responsible for setting up historical data for both
charting and any local storage requirements. it should retreive, normalize and
push the data to shared memory. normally the format of this data is
OHLCV sampled price and volume data but can be high reslolution
tick/trades/book times series as well. currently charting can expects
a max resolution of 1s (second) OHLCV sampled data (any more then this
and you probably can't do much with it as a manual trader; this is
obviously not true of automatic strats realized as code).

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
