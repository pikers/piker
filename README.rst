piker
-----
trading gear for hackers.

|gh_actions|

.. |gh_actions| image:: https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fpikers%2Fpiker%2Fbadge&style=popout-square
    :target: https://actions-badge.atrox.dev/piker/pikers/goto

``piker`` is a broker agnostic, next-gen FOSS toolset for real-time
trading targeted at hardcore Linux users.

we use as much bleeding edge tech as possible including (but not limited to):

- latest python for glue_
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


focus and features:
*******************
- zero web, cloud or "backtesting frameworks" (aka yabf)
- zero self promotion (aka pump); expected throughout the community
- 100% federated: your code, your hardware, your data feeds, your broker fills
- broker/exchange/asset-class agnostic
- privacy
- real-time financial signal processing from the ground up
- high quality, attractive, native UX with expected use in tiling wms
- sophisticated rt charting and data sharing facilities
- geared for collaboration within trader communities
- zero interest in adoption by suits; no corporate friendly license, ever.
- not built for *sale*; built for *people*

fitting with these tenets, we're always open to new framework
suggestions and ideas.

building the best looking, most reliable, keyboard friendly trading
platform is the dream.  feel free to pipe in with your ideas and quiffs.


install
*******
``piker`` is currently under heavy pre-alpha development and as such
should be cloned from this repo and hacked on directly.

a couple bleeding edge components are being used atm pertaining to
new components within `trio`_.

for a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    virtualenv env
    source ./env/bin/activate
    pip install -e .


broker Support
**************
for live data feeds the in-progress set of supported brokers is:

- IB_ via ``ib_insync``
- questrade_ which comes with effectively free L1
- kraken_ for crypto over their public websocket API

coming soon...

- webull_ via the reverse engineered public API
- yahoo via yliveticker_
- coinbase_ through websocket feed

if you want your broker supported and they have an API let us know.

.. _IB: https://interactivebrokers.github.io/tws-api/index.html
.. _questrade: https://www.questrade.com/api/documentation
.. _kraken: https://www.kraken.com/features/api#public-market-data
.. _webull: https://github.com/tedchou12/webull
.. _yliveticker: https://github.com/yahoofinancelive/yliveticker
.. _coinbase: https://docs.pro.coinbase.com/#websocket-feed

check out our charts
********************
bet you weren't expecting this from the foss bby::

    piker -b kraken chart XBTUSD


if anyone asks you what this project is about
*********************************************
you don't talk about it.

how do i get involved?
**********************
enter the matrix.

learning the code is to your benefit and acts as a filter for desired
users; many alpha nuggets within.
