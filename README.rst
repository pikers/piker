piker
-----
Trading gear for hackers.

|pypi| |travis| |versions| |license| |docs|

``piker`` is an attempt at a pro-grade, broker agnostic, next-gen FOSS toolset for real-time
trading and financial analysis.

It tries to use as much cutting edge tech as possible including (but not limited to):

- Python 3.7+
- ``trio``
- ``tractor``

.. |travis| image:: https://img.shields.io/travis/pikers/piker/master.svg
    :target: https://travis-ci.org/pikers/piker


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

To start the real-time index ETF watchlist with the `robinhood` backend::

    piker -l info monitor indexes


If you want to see super granular price changes, increase the
broker quote query ``rate`` with ``-r``::

    piker monitor indexes -r 10


It is also possible to run the broker data feed micro service as a daemon::

    pikerd -l info

Then start the client app as normal::

    piker monitor indexes


.. _trio: https://github.com/python-trio/trio
.. _pipenv: https://docs.pipenv.org/


Finicky dependencies
====================
For those running pop-culture distros that don't yet ship ``python3.7``
you'll need to install it as well as `kivy source build`_ dependencies
since currently there's reliance on an async development branch.

.. _kivy source build:
    https://kivy.org/docs/installation/installation-linux.html#installation-in-a-virtual-environment
