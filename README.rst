piker
-----
Trading gear for hackers.

|travis|

``piker`` is an attempt at a pro-grade, broker agnostic, next-gen FOSS toolset for real-time
trading and financial analysis targetted at hardcore Linux users.

It tries to use as much bleeding edge tech as possible including (but not limited to):

- Python 3.7+ for glue_ and business logic
- trio_ for async
- tractor_ as the underlying actor model
- marketstore_ for historical and real-time tick data persistence and sharing
- techtonicdb_ for L2 book storage
- Qt_ for pristine high performance UIs

.. |travis| image:: https://img.shields.io/travis/pikers/piker/master.svg
    :target: https://travis-ci.org/pikers/piker
.. _trio: https://github.com/python-trio/trio
.. _tractor: https://github.com/goodboy/tractor
.. _marketstore: https://github.com/alpacahq/marketstore
.. _techtonicdb: https://github.com/0b01/tectonicdb
.. _Qt: https://www.qt.io/
.. _glue: https://numpy.org/doc/stable/user/c-info.python-as-glue.html#using-python-as-glue


Focus and Features:
*******************
- 100% federated: running your code on your hardware with your
  broker's data feeds, privately, **is the point** (this is not a web-based *I
  don't know how to run my own system* project).
- Asset class, broker, exchange agnostic.
- Built on a highly reliable `structured concurrent actor model
  <tractor>`_ with built in async streaming and scalability protocols
  allowing for a distributed architecture from the ground up.
- Privacy: your orders, indicators, algos are all run client side and
  are shared only with the (groups of) traders you specify.
- Production grade, highly attractive native UIs that feel and fit like
  a proper pair of skinny jeans; only meant to be used with a proper
  tiling window manager (no, we are not ignorant enough to roll our own).
- Sophisticated charting capable of processing large data sets in real-time
  while sanely displaying complex models and strategy systems.
- Built-in support for *hipstery* indicators and studies that you
  probably haven't heard of but that the authors **know** generate alpha
  when paired with the right strategies.
- Emphasis on collaboration through sharing of data, ideas, and processing
  power. We will not host your code in the cloud nor ask you to
  participate in any lame "alpha competitions".
- Adoption is very low priority, especially if you're not an experienced
  trader; the system is not built for sale it is built for *people*.
- No, we will never have a "corporation friendly license"; if you intend to use
  this code base we must know about it.

Fitting with these tenets, we're always open to new framework suggestions and ideas.

Building the best looking, most reliable, keyboard friendly trading platform is the dream.
Feel free to pipe in with your ideas and quiffs.


Install
*******
``piker`` is currently under heavy pre-alpha development and as such should
be cloned from this repo and hacked on directly.

A couple bleeding edge components are being used atm pertaining to
async ports of libraries for use with `trio`_.

Before installing make sure you have `pipenv`_ and have installed
``python3.7`` as well as `kivy source build`_ dependencies
since currently there's reliance on an async development branch.

`kivy` dependencies
===================
On Archlinux you need the following dependencies::

   pacman -S python-docutils gstreamer sdl2_ttf sdl2_mixer sdl2_image xclip

To manually install the async branch of ``kivy`` from github do (though
this should be done as part of the ``pipenv install`` below)::

    pipenv install -e 'git+git://github.com/matham/kivy.git@async-loop#egg=kivy'


.. _kivy source build:
    https://kivy.org/docs/installation/installation-linux.html#installation-in-a-virtual-environment


For a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    pipenv install --pre -e .
    pipenv shell


Broker Support
**************
For live data feeds the only fully functional broker at the moment is Questrade_.
Eventual support is in the works for `IB`, `TD Ameritrade` and `IEX`.
If you want your broker supported and they have an API let us know.

.. _Questrade: https://www.questrade.com/api/documentation


Play with some UIs
******************

To start the real-time index monitor with the `questrade` backend::

    piker -l info monitor indexes


If you want to see super granular price changes, increase the
broker quote query ``rate`` with ``-r``::

    piker monitor indexes -r 10


It is also possible to run the broker data feed micro service as a daemon::

    pikerd -l info

Then start the client app as normal::

    piker monitor indexes


.. _pipenv: https://docs.pipenv.org/
