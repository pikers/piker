#!/usr/bin/env python

# piker: trading gear for hackers
# Copyright (C) 2018-present  Tyler Goodlet (in stewardship of piker0)

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

from setuptools import setup, find_packages

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
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'piker = piker.cli:cli',
            'pikerd = piker.cli:pikerd',
        ]
    },
    install_requires=[
        'toml',
        'click',
        'colorlog',
        'attrs',
        'pygments',
        'colorama',  # numba traceback coloring
        'pydantic',  # structured data

        # async
        'trio',
        'trio-websocket',
        # 'tractor',  # from github currently
        'msgspec',  # performant IPC messaging
        'async_generator',

        # brokers
        'asks==2.4.8',
        'ib_insync',

        # numerics
        'arrow',  # better datetimes
        'bidict',  # 2 way map
        'cython',
        'numpy',
        'numba',
        'pandas',

        # UI
        'PyQt5',
        'pyqtgraph',
        'qdarkstyle >= 3.0.2',
        # fuzzy search
        'fuzzywuzzy[speedup]',

        # tsdbs
        'pymarketstore',
    ],
    tests_require=['pytest'],
    python_requires=">=3.9",  # literally for ``datetime.datetime.fromisoformat``...
    keywords=["async", "trading", "finance", "quant", "charting"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: ',
        'Operating System :: POSIX :: Linux',
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
    ],
)
