"""
Console interface to broker client/daemons.
"""
from pprint import pformat
import click
import trio
from ..log import get_console_log


def run(loglevel, main):
    log = get_console_log(loglevel)

    # main loop
    try:
        client = trio.run(main)
    except Exception as err:
        log.exception(err)
    else:
        log.debug(
            f"Exiting with last access info:\n{pformat(client.access_data)}\n")


@click.command()
@click.option('--broker', default='questrade', help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
def pikerd(broker, loglevel):
    # import broker module daemon entry point
    from .questrade import serve_forever
    run(loglevel, serve_forever)
