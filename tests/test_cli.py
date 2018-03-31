"""
CLI testing, dawg.
"""
import json
import subprocess
import pytest
import os.path
import logging


def run(cmd):
    """Run cmd and check for zero return code.
    """
    cp = subprocess.run(cmd.split())
    cp.check_returncode()
    return cp


def verify_keys(tickers, quotes_dict):
    """Verify all ticker names are keys in ``quotes_dict``.
    """
    for key, quote in quotes_dict.items():
        assert key in tickers


@pytest.fixture
def nyse_tickers():
    """List of well known NYSE ticker symbols.
    """
    return ('TD', 'CRON', 'TSLA', 'AAPL')


def test_known_quotes(capfd, nyse_tickers):
    """Verify quotes are dumped to the console as json.
    """
    run(f"piker quote {' '.join(nyse_tickers)}")

    # verify output can be parsed as json
    out, err = capfd.readouterr()
    quotes_dict = json.loads(out)
    verify_keys(nyse_tickers, quotes_dict)


@pytest.mark.parametrize(
    'multiple_tickers',
    [True, False]
)
def test_quotes_ticker_not_found(
    capfd, caplog, nyse_tickers, multiple_tickers
):
    """Verify that if a ticker can't be found it's quote value is
    ``None`` and a warning log message is emitted to the console.
    """
    bad_ticker = ('doggy',)
    tickers = bad_ticker + nyse_tickers if multiple_tickers else bad_ticker

    run(f"piker quote {' '.join(tickers)}")

    out, err = capfd.readouterr()
    if out:
        # verify output can be parsed as json
        quotes_dict = json.loads(out)
        verify_keys(tickers, quotes_dict)
        # check for warning log message when some quotes are found
        warnmsg = f'Could not find symbol {bad_ticker[0]}'
        assert warnmsg in err
    else:
        # when no quotes are found we should get an error message
        errmsg = f'No quotes could be found for {bad_ticker}'
        assert errmsg in err


def test_api_method(nyse_tickers, capfd):
    """Ensure a low level api method can be called via CLI.
    """
    run(f"piker api quotes symbols={','.join(nyse_tickers)}")
    out, err = capfd.readouterr()
    quotes_dict = json.loads(out)
    assert isinstance(quotes_dict, dict)


def test_api_method_not_found(nyse_tickers, capfd):
    """Ensure an error messages is printed when an API method isn't found.
    """
    bad_meth = 'doggy'
    run(f"piker api {bad_meth} names={' '.join(nyse_tickers)}")
    out, err = capfd.readouterr()
    assert 'null' in out
    assert f'No api method `{bad_meth}` could be found?' in err
