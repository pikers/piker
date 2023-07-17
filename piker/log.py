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

"""
Log like a forester!
"""
import logging
import json

import tractor
from pygments import (
    highlight,
    lexers,
    formatters,
)

# Makes it so we only see the full module name when using ``__name__``
# without the extra "piker." prefix.
_proj_name: str = 'piker'


def get_logger(
    name: str = None,

) -> logging.Logger:
    '''
    Return the package log or a sub-log for `name` if provided.

    '''
    return tractor.log.get_logger(
        name=name,
        _root_name=_proj_name,
    )


def get_console_log(
    level: str | None = None,
    name: str | None = None,

) -> logging.Logger:
    '''
    Get the package logger and enable a handler which writes to stderr.

    Yeah yeah, i know we can use ``DictConfig``. You do it...

    '''
    return tractor.log.get_console_log(
        level,
        name=name,
        _root_name=_proj_name,
    )  # our root logger


def colorize_json(
    data: dict,
    style='algol_nu',
):
    '''
    Colorize json output using ``pygments``.

    '''
    formatted_json = json.dumps(
        data,
        sort_keys=True,
        indent=4,
    )
    return highlight(
        formatted_json,
        lexers.JsonLexer(),

        # likeable styles: algol_nu, tango, monokai
        formatters.TerminalTrueColorFormatter(style=style)
    )
