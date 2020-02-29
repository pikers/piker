"""
Log like a forester!
"""
import logging
import json

import tractor
from pygments import highlight, lexers, formatters

_proj_name = 'piker'


def get_logger(name: str = None) -> logging.Logger:
    '''Return the package log or a sub-log for `name` if provided.
    '''
    return tractor.log.get_logger(name=name, _root_name=_proj_name)


def get_console_log(level: str = None, name: str = None) -> logging.Logger:
    '''Get the package logger and enable a handler which writes to stderr.

    Yeah yeah, i know we can use ``DictConfig``. You do it...
    '''
    return tractor.log.get_console_log(
        level, name=name, _root_name=_proj_name)  # our root logger


def colorize_json(data, style='algol_nu'):
    """Colorize json output using ``pygments``.
    """
    formatted_json = json.dumps(data, sort_keys=True, indent=4)
    return highlight(
        formatted_json, lexers.JsonLexer(),
        # likeable styles: algol_nu, tango, monokai
        formatters.TerminalTrueColorFormatter(style=style)
    )
