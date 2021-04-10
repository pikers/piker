piker
-----
trading gear for hackers.

|gh_actions|

.. |gh_actions| image:: https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fpikers%2Fpiker%2Fbadge&style=popout-square
    :target: https://actions-badge.atrox.dev/piker/pikers/goto

``piker`` is a broker agnostic, next-gen FOSS toolset for real-time
computational trading targeted at `hardcore Linux users <comp_trader>`_ .

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
.. _comp_trader: https://jfaleiro.wordpress.com/2019/10/09/computational-trader/


focus and features:
*******************
- 100% federated: your code, your hardware, your data feeds, your broker fills.
- zero web: low latency, native software that doesn't try to re-invent the OS
- maximal **privacy**: prevent brokers and mms from knowing your
  planz; smack their spreads with dark volume.
- zero clutter: modal, context oriented UIs that echew minimalism, reduce
  thought noise and encourage un-emotion.
- first class parallelism: built from the ground up on next-gen structured concurrency
  primitives.
- traders first: broker/exchange/asset-class agnostic
- systems grounded: real-time financial signal processing that will
  make any queuing or DSP eng juice their shorts.
- non-tina UX: sleek, powerful keyboard driven interaction with expected use in tiling wms
- data collaboration: every process and protocol is multi-host scalable.
- fight club ready: zero interest in adoption by suits; no corporate friendly license, ever.

fitting with these tenets, we're always open to new framework suggestions and ideas.

building the best looking, most reliable, keyboard friendly trading
platform is the dream; join the cause.


install
*******
``piker`` is currently under heavy pre-alpha development and as such
should be cloned from this repo and hacked on directly.

for a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    virtualenv env
    source ./env/bin/activate
    pip install -r requirements.txt -e .


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


run in distributed mode
***********************
start the service daemon::

    pikerd -l info


connect yourt chart::

    piker -b kraken chart XMRXBT


enjoy persistent real-time data feeds tied to daemon lifetime.


if anyone asks you what this project is about
*********************************************
you don't talk about it.


how do i get involved?
**********************
enter the matrix.


how come there ain't that many docs
***********************************
suck it up, learn the code; no one is trying to sell you on anything.


who is `piker0`?
****************
who do you think?
