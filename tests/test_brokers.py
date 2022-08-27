from tractor.testing import tractor_test

from piker.brokers import __brokers__, get_brokermod


async def run_method(client, meth_name: str, **kwargs):
    result = await getattr(client, meth_name)(**kwargs)
    return result


async def run_test(broker_name: str):
    brokermod = get_brokermod(broker_name)

    assert hasattr(brokermod, 'get_client')

    async with brokermod.get_client(is_brokercheck=True) as client:

        # check for methods present on brokermod
        method_list = [
            'get_client',
            'trades_dialogue',
            'open_history_client',
            'open_symbol_search',
            'stream_quotes',
        ]

        for method in method_list:
            assert hasattr(brokermod, method)

        # check for methods present con brokermod.Client and their
        # results

        # for private methods only check is present
        method_list = [
            'get_balances',
            'get_assets',
            'get_trades',
            'get_xfers',
            'submit_limit',
            'submit_cancel',
            'search_symbols',
        ]

        for method_name in method_list:
            assert hasattr(client, method_name)


        # check for methods present con brokermod.Client and their
        # results

        syms = await run_method(client, 'symbol_info')

        if len(syms) == 0:
            raise BaseException('Empty Symbol list?')

        first_sym = tuple(syms.keys())[0]

        method_list = [
            ('cache_symbols', {}),
            ('search_symbols', {'pattern': first_sym[:-1]}),
            ('bars', {'symbol': first_sym})
        ]

        for method_name, method_kwargs in method_list:
             await run_method(client, method_name, **method_kwargs)


@tractor_test
async def test_brokercheck_all():
    for broker in __brokers__:
        await run_test(broker)

