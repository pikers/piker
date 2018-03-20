"""
Handy utils.
"""
import json
import asks
import logging

from ..log import colorize_json


class BrokerError(Exception):
    "Generic broker issue"


def resproc(
    resp: asks.response_objects.Response,
    log: logging.Logger,
    return_json: bool = True
) -> asks.response_objects.Response:
    """Process response and return its json content.

    Raise the appropriate error on non-200 OK responses.
    """
    if not resp.status_code == 200:
        raise BrokerError(resp.body)
    try:
        data = resp.json()
    except json.decoder.JSONDecodeError:
        log.exception(f"Failed to process {resp}:\n{resp.text}")
        raise BrokerError(resp.text)
    else:
        log.trace(f"Received json contents:\n{colorize_json(data)}")

    return data if return_json else resp
