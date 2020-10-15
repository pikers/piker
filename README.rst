piker
-----
trading gear for hackers.

|travis|

``piker`` is an attempt at a pro-grade, broker agnostic, next-gen FOSS
toolset for real-time trading and financial analysis targetted at
hardcore Linux users.

it tries to use as much bleeding edge tech as possible including (but not limited to):

- latest Python for glue_ and business logic
- trio_ for `structured concurrency`_
- tractor_ for distributed, multi-core, real-time streaming
- marketstore_ for historical and real-time tick data persistence and sharing
- techtonicdb_ for L2 book storage
- Qt_ for pristine high performance UIs
- pyqtgraph_ for real-time charting
- ``numpy`` and ``numba`` for `fast numerics`_

.. |travis| image:: https://img.shields.io/travis/pikers/piker/master.svg
    :target: https://travis-ci.org/pikers/piker
.. _trio: https://github.com/python-trio/trio
.. _tractor: https://github.com/goodboy/tractor
.. _structured concurrency: https://trio.discourse.group/
.. _marketstore: https://github.com/alpacahq/marketstore
.. _techtonicdb: https://github.com/0b01/tectonicdb
.. _Qt: https://www.qt.io/
.. _pyqtgraph: https://github.com/pyqtgraph/pyqtgraph
.. _glue: https://numpy.org/doc/stable/user/c-info.python-as-glue.html#using-python-as-glue
.. _fast numerics: https://zerowithdot.com/python-numpy-and-pandas-performance/


Focus and Features:
*******************
- zero web
- zero pump
- zero "backtesting" (aka yabf)
- zero "cloud"
- 100% federated: your code, your hardware, your broker's data feeds
- privacy
- broker/exchange agnostic
- built on a structured concurrent actor model
- production grade, highly attractive native UIs
- expected to be used from tiling wms
- sophisticated rt charting
- emphasis on collaboration through UI and data sharing
- zero interest in adoption by suits
- not built for *sale*; built for *people*
- no corporate friendly license, ever.

fitting with these tenets, we're always open to new framework
suggestions and ideas.

building the best looking, most reliable, keyboard friendly trading
platform is the dream.  feel free to pipe in with your ideas and quiffs.


Install
*******
``piker`` is currently under heavy pre-alpha development and as such should
be cloned from this repo and hacked on directly.

A couple bleeding edge components are being used atm pertaining to
new components within `trio`_.

For a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    pip install -e .


Broker Support
**************
For live data feeds the in-progress set of supported brokers is:

- Questrade_ which comes with effectively free L1
- IB_ via ``ib_insync``
- Webull_ via the reverse engineered public API
- Kraken_ for crypto over their public websocket API

If you want your broker supported and they have an API let us know.

.. _Questrade: https://www.questrade.com/api/documentation
.. _IB: https://interactivebrokers.github.io/tws-api/index.html
.. _Webull: https://www.kraken.com/features/api#public-market-data
.. _Kraken: https://www.kraken.com/features/api#public-market-data


Check out our charts
********************
bet you weren't expecting this from the foss bby::

    piker -b kraken chart XBTUSD


If anyone asks you what this project is about
*********************************************
tell them *it's a broken crypto trading platform that doesn't scale*.

How do i get involved?
**********************
coming soon.
