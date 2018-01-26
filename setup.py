#!/usr/bin/env python
#
# Copyright 2018 Tyler Goodlet
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup

with open('README.rst', encoding='utf-8') as f:
    readme = f.read()


setup(
    name="piker",
    version='0.1.0.alpha0',
    description='Trading gear for hackers, scientists, quants and underpants warriors.',
    long_description=readme,
    license='Mozilla',
    author='Tyler Goodlet',
    maintainer='Tyler Goodlet',
    maintainer_email='tgoodlet@gmail.com',
    url='https://github.com/pikers/piker',
    platforms=['linux'],
    packages=[
        'piker',
        'piker.brokers',
    ],
    entry_points={
        'console_scripts': [
            'pikerd = piker.brokers.cli:pikerd',
            'piker = piker.brokers.cli:cli',
        ]
    },
    install_requires=[
        'click', 'colorlog', 'trio', 'attrs', 'async_generator',
        'pygments',
    ],
    extras_require={
        'questrade': ['asks'],
    },
    tests_require=['pytest'],
    python_requires=">=3.6",
    keywords=["async", "trading", "finance", "quant", "charting"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Operating System :: POSIX :: Linux',
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
    ],
)
