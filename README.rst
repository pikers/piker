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


install for tinas
*****************
for windows peeps you can start by installing all the prerequisite software:
install git with all default settings - https://git-scm.com/download/win
install visual studio code default settings - select default profile for terminal as "cmd" (command prompt) - https://code.visualstudio.com/download
install anaconda all default settings - https://www.anaconda.com/products/individual
install microsoft build tools (check the box for Desktop development for C++)  - https://visualstudio.microsoft.com/visual-cpp-build-tools/

then, `crack a conda shell`_ and run the following commands::

    mkdir code # create code directory
    cd code # change directory to code
    pwd # shows present working directory, should show code inside default user folder
    git clone https://github.com/pikers/piker.git # downloads piker installation package from github
    cd piker # change directory to piker
    
    conda create -n py39 # creates conda environment named py39
    conda activate py39 # activates py39
    conda install -c conda-forge python-levenshtein # in case it is not already installed
    conda install pip # may already be installed
    pip # will show if pip is installed
    
    pip install -e . -r requirements.txt # install piker in editable mode
    piker -b kraken -b binance chart btcusdt.binance # command to load chart for test

potential errors:
- FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\user\\AppData\\Roaming\\piker\\brokers.toml'
- navigate to file directory above (may be different on your machine, location should be listed in the error)
- create folder and copy and paste file or create a blank file using notepad --> 'C:\\Users\\user\\code\\data/brokers.toml' 
    
now that piker is installed we can set up vscode as the default terminal for running piker and editing the code.
within vscode you are going to want cmd.exe as your default terminal.

in order to look coolio in front of all ur tina friends (and maybe
want to help us with testin, hackzing or configgin), install
`vscode`_ and `setup a coolio tiled wm console`_ so you can start
living the life of the tech literate..

.. _conda installed: https://
.. _C++ build toolz: https://
.. _crack a conda shell: https://
.. _vscode: https://

.. link to the tina guide
.. _setup a coolio tiled wm console: https://

provider support
****************
for live data feeds the in-progress set of supported brokers is:

- IB_ via ``ib_insync``
- binance_ and kraken_ for crypto over their public websocket API
- questrade_ (ish) which comes with effectively free L1

coming soon...

- webull_ via the reverse engineered public API
- yahoo via yliveticker_

if you want your broker supported and they have an API let us know.

.. _IB: https://interactivebrokers.github.io/tws-api/index.html
.. _questrade: https://www.questrade.com/api/documentation
.. _kraken: https://www.kraken.com/features/api#public-market-data
.. _binance: https://github.com/pikers/piker/pull/182
.. _webull: https://github.com/tedchou12/webull
.. _yliveticker: https://github.com/yahoofinancelive/yliveticker
.. _coinbase: https://docs.pro.coinbase.com/#websocket-feed


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
