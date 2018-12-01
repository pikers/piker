"""
Broker high level API layer.
"""
import inspect
from types import ModuleType
from typing import List, Dict, Any, Optional

from ..log import get_logger
from .data import DataFeed


log = get_logger('broker.core')


async def api(brokermod: ModuleType, methname: str, **kwargs) -> dict:
    """Make (proxy through) a broker API call by name and return its result.
    """
    async with brokermod.get_client() as client:

        meth = getattr(client.api, methname, None)
        if meth is None:
            log.warning(
                f"Couldn't find API method {methname} looking up on client")
            meth = getattr(client, methname, None)

        if meth is None:
            log.error(f"No api method `{methname}` could be found?")
            return

        if not kwargs:
            # verify kwargs requirements are met
            sig = inspect.signature(meth)
            if sig.parameters:
                log.error(
                    f"Argument(s) are required by the `{methname}` method: "
                    f"{tuple(sig.parameters.keys())}")
                return

        return await meth(**kwargs)


async def stocks_quote(
    brokermod: ModuleType,
    tickers: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Return quotes dict for ``tickers``.
    """
    async with brokermod.get_client() as client:
        return await client.quote(tickers)


# TODO: these need tests
async def option_chain(
    brokermod: ModuleType,
    symbol: str,
    date: Optional[str] = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Return option chain for ``symbol`` for ``date``.

    By default all expiries are returned. If ``date`` is provided
    then contract quotes for that single expiry are returned.
    """
    async with brokermod.get_client() as client:
        if date:
            id = int((await client.tickers2ids([symbol]))[symbol])
            # build contracts dict for single expiry
            return await client.option_chains(
                {(symbol, id, date): {}})
        else:
            # get all contract expiries
            # (takes a long-ass time on QT fwiw)
            contracts = await client.get_all_contracts([symbol])
            # return chains for all dates
            return await client.option_chains(contracts)


async def contracts(
    brokermod: ModuleType,
    symbol: str,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Return option contracts (all expiries) for ``symbol``.
    """
    async with brokermod.get_client() as client:
        # return await client.get_all_contracts([symbol])
        return await client.get_all_contracts([symbol])
