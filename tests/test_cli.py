"""
CLI testing, dawg.
"""
import json
import subprocess
import pytest
import tempfile
import os.path
import logging

import piker.watchlists as wl
import piker.cli as cli
from piker.log import colorize_json


def run(cmd, *args):
    """Run cmd and check for zero return code.
    """
    cp = subprocess.run(cmd.split() + list(args))
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


@pytest.fixture
def temp_dir():
    """Creates a path to a pretend config dir in a temporary directory for
    testing.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        yield os.path.join(tempdir, 'piker')


@pytest.fixture
def piker_dir(temp_dir):
    wl.make_config_dir(temp_dir)
    json_file_path = os.path.join(temp_dir, 'watchlists.json')
    watchlist = {
        'dad': ['GM', 'TSLA', 'DOL.TO', 'CIM', 'SPY', 'SHOP.TO'],
        'pharma': ['ATE.VN'],
        'indexes': ['SPY', 'DAX', 'QQQ', 'DIA'],
    }
    wl.write_sorted_json(watchlist, json_file_path)
    yield json_file_path


def test_show_watchlists(capfd, piker_dir):
    """Ensure a watchlist is printed.
    """
    expected_out = json.dumps({
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'QQQ', 'SPY'],
        'pharma': ['ATE.VN'],
    }, indent=4)
    run(f"piker watchlists -d {piker_dir} show")
    out, err = capfd.readouterr()
    assert out.strip() == expected_out


def test_dump_watchlists(capfd, piker_dir):
    """Ensure watchlist is dumped.
    """
    expected_out = json.dumps({
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'QQQ', 'SPY'],
        'pharma': ['ATE.VN'],
    })
    run(f"piker watchlists -d {piker_dir} dump")
    out, err = capfd.readouterr()
    assert out.strip() == expected_out


def test_ticker_added_to_watchlists(capfd, piker_dir):
    expected_out = {
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'QQQ', 'SPY'],
        'pharma': ['ATE.VN', 'CRACK'],
    }
    run(f"piker watchlists -d {piker_dir} add pharma CRACK")
    out = wl.ensure_watchlists(piker_dir)
    assert out == expected_out


def test_ticker_removed_from_watchlists(capfd, piker_dir):
    expected_out = {
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'SPY'],
        'pharma': ['ATE.VN'],
    }
    run(f"piker watchlists -d {piker_dir} remove indexes QQQ")
    out = wl.ensure_watchlists(piker_dir)
    assert out == expected_out


def test_group_deleted_from_watchlists(capfd, piker_dir):
    expected_out = {
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'QQQ', 'SPY'],
    }
    run(f"piker watchlists -d {piker_dir} delete pharma")
    out = wl.ensure_watchlists(piker_dir)
    assert out == expected_out


def test_watchlists_loaded(capfd, piker_dir):
    expected_out_text = json.dumps({
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'pharma': ['ATE.VN'],
    })
    expected_out = {
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'pharma': ['ATE.VN'],
    }
    run(f"piker watchlists -d {piker_dir} load", expected_out_text)
    out = wl.ensure_watchlists(piker_dir)
    assert out == expected_out


def test_watchlists_is_merge(capfd, piker_dir):
    orig_watchlist = {
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'QQQ', 'SPY'],
        'pharma': ['ATE.VN'],
    }
    list_to_merge = json.dumps({
        'drugs': ['CRACK']
    })
    expected_out = {
        'dad': ['CIM', 'DOL.TO', 'GM', 'SHOP.TO', 'SPY', 'TSLA'],
        'indexes': ['DAX', 'DIA', 'QQQ', 'SPY'],
        'pharma': ['ATE.VN'],
        'drugs': ['CRACK']
    }
    wl.write_sorted_json(orig_watchlist, piker_dir)
    run(f"piker watchlists -d {piker_dir} merge", list_to_merge)
    out = wl.ensure_watchlists(piker_dir)
    assert out == expected_out
