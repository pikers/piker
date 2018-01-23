"""
Broker client-daemons and general back end machinery.
"""
import sys
import trio
from .questrade import serve_forever
from ..log import get_console_log


def main() -> None:
    log = get_console_log('INFO', name='questrade')
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
        log.info(
            f"\nLast refresh_token: {client.access_data['refresh_token']}\n"
            f"Last access_token: {client.access_data['access_token']}\n"
        )
