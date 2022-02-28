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
Handy utils.
"""
import json
import asks
import logging

from ..log import colorize_json


class BrokerError(Exception):
    "Generic broker issue"


class SymbolNotFound(BrokerError):
    "Symbol not found by broker search"


class NoData(BrokerError):
    "Symbol data not permitted"


def resproc(
    resp: asks.response_objects.Response,
    log: logging.Logger,
    return_json: bool = True,
    log_resp: bool = False,

) -> asks.response_objects.Response:
    """Process response and return its json content.

    Raise the appropriate error on non-200 OK responses.
    """
    if not resp.status_code == 200:
        raise BrokerError(resp.body)
    try:
        json = resp.json()
    except json.decoder.JSONDecodeError:
        log.exception(f"Failed to process {resp}:\n{resp.text}")
        raise BrokerError(resp.text)

    if log_resp:
        log.debug(f"Received json contents:\n{colorize_json(json)}")

    return json if return_json else resp
