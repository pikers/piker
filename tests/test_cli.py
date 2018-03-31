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


@pytest.fixture
def temp_dir():
    #Create temporary directory
    with tempfile.TemporaryDirectory() as tempdir:
        config_dir = os.path.join(tempdir, 'piker')
        yield config_dir


@pytest.fixture
def piker_dir(temp_dir):
    wl.make_config_dir(temp_dir)
    path = os.path.join(temp_dir, 'watchlists.json')
    yield path


def test_watchlist_is_sorted_and_saved_to_file(piker_dir):
    """Ensure that watchlist is sorted and saved to file
    """
    wl_temp = {'test': ['TEST.CN', 'AAA'], 'AA': ['TEST.CN']}
    wl_sort = {'AA': ['TEST.CN'], 'test': ['AAA', 'TEST.CN']}
    wl.write_sorted_json(wl_temp, piker_dir)
    temp_sorted = wl.ensure_watchlists(piker_dir)
    assert temp_sorted == wl_sort


def test_watchlists_config_dir_created(caplog, temp_dir):
    """Ensure that a config directory is created
    """
    with caplog.at_level(logging.DEBUG):
        wl.make_config_dir(temp_dir)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelname == 'DEBUG'
    assert record.message == f"Creating config dir {temp_dir}"
    assert os.path.isdir(temp_dir)
    #Test that there is no error and that a log message is not generatd
    #when trying to create a directory that already exists
    with caplog.at_level(logging.DEBUG):
        wl.make_config_dir(temp_dir)
    assert len(caplog.records) == 1


def test_watchlist_is_read_from_file(piker_dir):
    """Ensure json info is read from file or an empty dict is generated
    """
    wl_temp = wl.ensure_watchlists(piker_dir)
    assert wl_temp == {}
    wl_temp2 = '{"AA": ["TEST.CN"]}'
    wl.load_watchlists(wl_temp2, piker_dir)
    assert json.loads(wl_temp2) == wl.ensure_watchlists(piker_dir)


def test_watchlist_loaded(piker_dir):
    """Ensure that text respresentation of a watchlist is loaded to file
    """
    wl_temp = '{"test": ["TEST.CN"]}'
    wl.load_watchlists(wl_temp, piker_dir)
    wl_temp2 = wl.ensure_watchlists(piker_dir)
    assert wl_temp == json.dumps(wl_temp2)


def test_new_watchlist_group_added(piker_dir):
    """Ensure that a new watchlist key is added to the watchlists dictionary
    """
    wl_temp = {}
    wl.new_group('test', wl_temp, piker_dir)
    wl_temp = wl.ensure_watchlists(piker_dir)
    assert len(wl_temp.keys()) == 1


def test_new_ticker_added(piker_dir):
    """Ensure that a new ticker is added to a watchlist
    """
    wl_temp = {'test': []}
    wl.add_ticker('test', 'TEST.CN', wl_temp, piker_dir)
    wl_temp = wl.ensure_watchlists(piker_dir)
    assert len(wl_temp['test']) == 1


def test_ticker_is_removed(piker_dir):
    """Verify that passed in ticker is removed
    """
    wl_temp = {'test': ['TEST.CN']}
    wl.remove_ticker('test', 'TEST.CN', wl_temp, piker_dir)
    wl_temp = wl.ensure_watchlists(piker_dir)
    assert wl_temp == {'test': []}

def test_group_is_deleted(piker_dir):
    """Check that watchlist group is removed
    """
    wl_temp = {'test': ['TEST.CN']}
    wl.delete_group('test', wl_temp, piker_dir)
    wl_temp = wl.ensure_watchlists(piker_dir)
    assert wl_temp.get('test') == None


def test_watchlist_is_merged(piker_dir):
    """Ensure that watchlist is merged
    """
    wl_temp = {'test': ['TEST.CN']}
    wl_temp2 = '{"test2": ["TEST2.CN"]}'
    wl.merge_watchlist(wl_temp2, wl_temp, piker_dir)
    wl_temp3 = wl.ensure_watchlists(piker_dir)
    assert wl_temp3 == {'test': ['TEST.CN'], 'test2': ['TEST2.CN']}
