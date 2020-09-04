#!/usr/bin/env python

# piker: trading gear for hackers
# Copyright 2018 Tyler Goodlet

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from setuptools import setup

with open('README.rst', encoding='utf-8') as f:
    readme = f.read()


setup(
    name="piker",
    version='0.1.0.alpha0.dev0',
    description='trading gear for hackers.',
    long_description=readme,
    license='AGPLv3',
    author='Tyler Goodlet',
    maintainer='Tyler Goodlet',
    url='https://github.com/pikers/piker',
    platforms=['linux'],
    packages=[
        'piker',
        'piker.brokers',
        'piker.ui',
        'piker.testing',
    ],
    entry_points={
        'console_scripts': [
            'piker = piker.cli:cli',
            'pikerd = piker.cli:pikerd',
        ]
    },
    install_requires=[
        'click',
        'colorlog',
        'trio',
        'attrs',
        'async_generator',
        'pygments',

        # brokers
        'asks',
        'ib_insync',

        # numerics
        'arrow',  # better datetimes
        'cython',
        'numpy',
        'pandas',

        # tsdbs
        'pymarketstore',
        #'kivy',  see requirement.txt; using a custom branch atm
    ],
    tests_require=['pytest'],
    python_requires=">=3.7",  # literally for ``datetime.datetime.fromisoformat``...
    keywords=["async", "trading", "finance", "quant", "charting"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Operating System :: POSIX :: Linux',
        "Programming Language :: Python :: Implementation :: CPython",
        # "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
    ],
)
