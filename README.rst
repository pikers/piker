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
- trio_ & tractor_ for our distributed, multi-core, real-time streaming
  `structured concurrency`_ runtime B)
- Qt_ for pristine high performance UIs
- pyqtgraph_ for real-time charting
- ``polars`` ``numpy`` and ``numba`` for `fast numerics`_
- `apache arrow and parquet`_ for time series history management
  persistence and sharing
- (prototyped) techtonicdb_ for L2 book storage

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
.. _apache arrow and parquet: https://arrow.apache.org/faq/
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


sane install with `poetry`
**************************
TODO!


rigorous install on ``nixos`` using ``poetry2nix``
**************************************************
TODO!


hacky install on nixos
**********************
`NixOS` is our core devs' distro of choice for which we offer
a stringently defined development shell envoirment that can be loaded with::

    nix-shell develop.nix

this will setup the required python environment to run piker, make sure to
run::

    pip install -r requirements.txt -e .

once after loading the shell


install wild-west style via `pip`
*********************************
``piker`` is currently under heavy pre-alpha development and as such
should be cloned from this repo and hacked on directly.

for a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    virtualenv env
    source ./env/bin/activate
    pip install -r requirements.txt -e .


check out our charts
********************
bet you weren't expecting this from the foss::

    piker -l info -b kraken -b binance chart btcusdt.binance --pdb


this runs the main chart (currently with 1m sampled OHLC) in in debug
mode and you can practice paper trading using the following
micro-manual:

``order_mode`` (
    edge triggered activation by any of the following keys,
    ``mouse-click`` on y-level to submit at that price
    ):

    - ``f``/ ``ctl-f`` to stage buy
    - ``d``/ ``ctl-d`` to stage sell
    - ``a`` to stage alert


``search_mode`` (
    ``ctl-l`` or ``ctl-space`` to open,
    ``ctl-c`` or ``ctl-space`` to close
    ) :

    - begin typing to have symbol search automatically lookup
      symbols from all loaded backend (broker) providers
    - arrow keys and mouse click to navigate selection
    - vi-like ``ctl-[hjkl]`` for navigation


you can also configure your position allocation limits from the
sidepane.


run in distributed mode
***********************
start the service manager and data feed daemon in the background and
connect to it::

    pikerd -l info --pdb


connect your chart::

    piker -l info -b kraken -b binance chart xmrusdt.binance --pdb


enjoy persistent real-time data feeds tied to daemon lifetime. the next
time you spawn a chart it will load much faster since the data feed has
been cached and is now always running live in the background until you
kill ``pikerd``.


if anyone asks you what this project is about
*********************************************
you don't talk about it.


how do i get involved?
**********************
enter the matrix.


how come there ain't that many docs
***********************************
suck it up, learn the code; no one is trying to sell you on anything.
also, we need lotsa help so if you want to start somewhere and can't
necessarily write serious code, this might be the place for you!
