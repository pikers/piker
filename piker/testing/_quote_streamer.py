"""
Mock a broker module.
"""
from itertools import cycle
import json
from os import path
import trio
from async_generator import asynccontextmanager
from ..brokers import questrade
from ..calc import percent_change


get_client = questrade.get_client

# @asynccontextmanager
# async def get_client() -> None:
#     """Shim client factory.
#     """
#     yield None


async def poll_tickers(
    client, tickers: [str], q: trio.Queue) -> None:
    """Stream quotes from a local json store.
    """
    with open(path.join(path.dirname(__file__), 'quotes.json'), 'r') as quotes_file:
        content = quotes_file.read()

    pkts = content.split('--')  # simulate 2 separate quote packets
    payloads = [json.loads(pkt)['quotes'] for pkt in pkts]

    for payload in cycle(payloads):
        q.put_nowait(payload)
        await trio.sleep(1/2.)
