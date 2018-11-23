"""
Actor model API testing
"""
import pytest
import tractor


async def rx_price_quotes_from_brokerd(us_symbols):
    """Verify we can spawn a daemon actor and retrieve streamed price data.
    """
    async with tractor.find_actor('brokerd') as portals:
        if not portals:
            # only one per host address, spawns an actor if None
            async with tractor.open_nursery() as nursery:
                # no brokerd actor found
                portal = await nursery.start_actor(
                    'brokerd',
                    rpc_module_paths=['piker.brokers.data'],
                    statespace={
                        'broker2tickersubs': {},
                        'clients': {},
                        'dtasks': set()
                    },
                )

                # gotta expose in a broker agnostic way...
                # retrieve initial symbol data
                # sd = await portal.run(
                #     'piker.brokers.data', 'symbol_data', symbols=us_symbols)
                # assert list(sd.keys()) == us_symbols

                gen = await portal.run(
                    'piker.brokers.data',
                    'start_quote_stream',
                    broker='robinhood',
                    tickers=us_symbols,
                )
                # it'd sure be nice to have an asyncitertools here...
                async for quotes in gen:
                    assert quotes
                    for key in quotes:
                        assert key in us_symbols
                    break
                    # terminate far-end async-gen
                    # await gen.asend(None)
                    # break

                # stop all spawned subactors
                await nursery.cancel()

    # arbitter is cancelled here due to `find_actors()` internals
    # (which internally uses `get_arbiter` which kills its channel
    # server scope on exit)


def test_rx_price_quotes_from_brokerd(us_symbols):
    tractor.run(
        rx_price_quotes_from_brokerd,
        us_symbols,
        name='arbiter',
    )
