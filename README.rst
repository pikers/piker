piker
-----
Anti-fragile trading gear for hackers, scientists, stay-at-home quants and underpants warriors.


Install
*******
``piker`` is currently under heavy alpha development and as such should
be cloned from this repo and hacked on directly.

A couple bleeding edge components are being used atm pertaining to
async ports of libraries for use with `trio`_.

Before installing make sure you have ``pip`` and ``virtualenv``.
For a development install::

    git clone git@github.com:pikers/piker.git
    cd piker
    virtualenv env
    source ./env/bin/activate
    pip install cython
    pip install -e ./ -r requirements.txt

To start the real-time index ETF watchlist::

    piker watch indexes -l info


If you want to see super granular price changes, increase the
broker quote query ``rate`` with ``-r``::

    piker watch indexes -l info -r 10


.. _trio: https://github.com/python-trio/trio

Laggy distros
=============
For those running pop-culture distros that don't yet ship ``python3.6``
you'll need to install it as well as `kivy source build`_ dependencies
since currently there's reliance on an async development branch.

For `ubuntu` this looks like::

    sudo add-apt-repository ppa:jonathonf/python-3.6
    sudo apt-get update
    sudo apt-get install -y \
        build-essential \
        python3.6 \
        python3.6-dev \
        ffmpeg \
        libsdl2-dev \
        libsdl2-image-dev \
        libsdl2-mixer-dev \
        libsdl2-ttf-dev \
        libportmidi-dev \
        libswscale-dev \
        libavformat-dev \
        libavcodec-dev \
        zlib1g-dev

    # then to create your virtualenv with py3.6
    virtualenv -p $(which python3.6) env


.. _kivy source build:
    https://kivy.org/docs/installation/installation-linux.html#installation-in-a-virtual-environment

Tech
****
``piker`` is an attempt at a pro-grade, next-gen open source toolset
for trading and financial analysis. As such, it tries to use as much
cutting edge tech as possible including Python 3.6+ and ``trio``.
