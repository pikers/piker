piker
-----
Anti-fragile_ trading gear for hackers, scientists, stay-at-home quants and underpants warriors.

|pypi| |travis| |versions| |license| |docs|

.. |travis| image:: https://img.shields.io/travis/pikers/piker/master.svg
    :target: https://travis-ci.org/pikers/piker

.. _Anti-fragile: https://www.sciencedirect.com/science/article/pii/S1877050916302290

Install
*******
``piker`` is currently under heavy alpha development and as such should
be cloned from this repo and hacked on directly.

A couple bleeding edge components are being used atm pertaining to
async ports of libraries for use with `trio`_.

Before installing make sure you have `pipenv`_.
For a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    pipenv install --dev -e .
    pipenv shell

To start the real-time index ETF watchlist::

    piker watch indexes -l info


If you want to see super granular price changes, increase the
broker quote query ``rate`` with ``-r``::

    piker watch indexes -l info -r 10


It is also possible to run the broker-client micro service as a daemon::

    pikerd -l info

Then start the client app as normal::

    piker watch indexes -l info


.. _trio: https://github.com/python-trio/trio
.. _pipenv: https://docs.pipenv.org/

Laggy distros
=============
For those running pop-culture distros that don't yet ship ``python3.6``
you'll need to install it as well as `kivy source build`_ dependencies
since currently there's reliance on an async development branch.

.. _kivy source build:
    https://kivy.org/docs/installation/installation-linux.html#installation-in-a-virtual-environment

Tech
****
``piker`` is an attempt at a pro-grade, next-gen open source toolset
for trading and financial analysis. As such, it tries to use as much
cutting edge tech as possible including Python 3.6+ and ``trio``.
