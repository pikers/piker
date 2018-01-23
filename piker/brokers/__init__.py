"""
Broker clients, daemons and general back end machinery.
"""
import sys
import trio
from pprint import pformat
from .questrade import serve_forever
from ..log import get_console_log


def main() -> None:
    log = get_console_log('info', name='questrade')
    argv = sys.argv[1:]

    refresh_token = None
    if argv:
        refresh_token = argv[0]

    # main loop
    try:
        client = trio.run(serve_forever, refresh_token)
    except Exception as err:
        log.exception(err)
    else:
        log.debug(f"Exiting with last access info:\n{pformat(client.access_data)}\n")
