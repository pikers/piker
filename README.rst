piker
------
Anti-fragile trading gear for hackers, scientists, quants and underpants warriors.


Install
*******
``piker`` is currently under heavy alpha development and as such should
be cloned from this repo and hacked on directly.

A couple very alpha components are being used atm pertaining to
async ports of libraries for use with ``trio``.

Before installing make sure you have ``pip`` and ``virtualenv``.

Then for a development install::

    $ git clone git@github.com:pikers/piker.git
    $ cd piker
    $ virtualenv env
    $ source ./env/bin/activate
    (env) $ pip install cython
    (env) $ pip install -e ./ -r requirements.txt

To start the real-time watchlist::

    (env) $ piker watch cannabis

If you insist on trying to install it (which should work) please do it
from this GitHub repository::

    pip install git+git://github.com/pikers/piker.git


Tech
****
``piker`` is an attempt at a pro-grade, next-gen open source toolset
for trading and financial analysis. As such, it tries to use as much
cutting edge tech as possible including Python 3.6+ and ``trio``.
