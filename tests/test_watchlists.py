"""
Watchlists testing.
"""
import pytest
import tempfile
import os.path
import logging

import piker.watchlists as wl


@pytest.fixture
def temp_dir():
    """Creates a path to a pretend config dir in a temporary directory for
    testing.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        config_dir = os.path.join(tempdir, 'piker')
        yield config_dir


@pytest.fixture
def piker_dir(temp_dir):
    wl.make_config_dir(temp_dir)
    yield os.path.join(temp_dir, 'watchlists.json')


def test_watchlist_is_sorted_no_dups_and_saved_to_file(piker_dir):
    wl_temp = {'test': ['TEST.CN', 'AAA'], 'AA': ['TEST.CN', 'TEST.CN'],
               'AA': ['TEST.CN']}
    wl_sort = {'AA': ['TEST.CN'], 'test': ['AAA', 'TEST.CN']}
    wl.write_to_file(wl_temp, piker_dir)
    temp_sorted = wl.ensure_watchlists(piker_dir)
    assert temp_sorted == wl_sort


def test_watchlists_config_dir_created(caplog, temp_dir):
    """Ensure that a config directory is created.
    """
    with caplog.at_level(logging.DEBUG):
        wl.make_config_dir(temp_dir)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelname == 'DEBUG'
    assert record.message == f"Creating config dir {temp_dir}"
    assert os.path.isdir(temp_dir)
    # Test that there is no error and that a log message is not generatd
    # when trying to create a directory that already exists
    with caplog.at_level(logging.DEBUG):
        wl.make_config_dir(temp_dir)
    # There should be no additional log message.
    assert len(caplog.records) == 1


def test_watchlist_is_read_from_file(piker_dir):
    """Ensure json info is read from file or an empty dict is generated
    and that text respresentation of a watchlist is saved to file.
    """
    wl_temp = wl.ensure_watchlists(piker_dir)
    assert wl_temp == {}
    wl_temp2 = {"AA": ["TEST.CN"]}
    wl.write_to_file(wl_temp2, piker_dir)
    assert wl_temp2 == wl.ensure_watchlists(piker_dir)


def test_new_ticker_added():
    """Ensure that a new ticker is added to a watchlist for both cases.
    """
    wl_temp = wl.add_ticker('test', 'TEST.CN', {'test': ['TEST2.CN']})
    assert len(wl_temp['test']) == 2
    wl_temp = wl.add_ticker('test2', 'TEST.CN', wl_temp)
    assert wl_temp['test2']


def test_ticker_is_removed():
    """Verify that passed in ticker is removed and that a group is removed
    if no tickers left.
    """
    wl_temp = {'test': ['TEST.CN', 'TEST2.CN'], 'test2': ['TEST.CN']}
    wl_temp = wl.remove_ticker('test', 'TEST.CN', wl_temp)
    wl_temp = wl.remove_ticker('test2', 'TEST.CN', wl_temp)
    assert wl_temp == {'test': ['TEST2.CN']}
    assert not wl_temp.get('test2')

    # verify trying to remove from nonexistant list
    with pytest.raises(KeyError):
        wl.remove_ticker('doggy', 'TEST.CN', wl_temp)

    # verify trying to remove non-existing ticker
    with pytest.raises(ValueError):
        wl.remove_ticker('test', 'TEST.CN', wl_temp)


def test_group_is_deleted():
    """Check that watchlist group is removed.
    """
    wl_temp = {'test': ['TEST.CN']}
    wl_temp = wl.delete_group('test', wl_temp)
    assert not wl_temp.get('test')


def test_watchlist_is_merged():
    """Ensure that watchlist is merged.
    """
    wl_temp = {'test': ['TEST.CN']}
    wl_temp2 = {'test': ['TOAST'], "test2": ["TEST2.CN"]}
    wl_temp3 = wl.merge_watchlist(wl_temp2, wl_temp)
    assert wl_temp3 == {'test': ['TEST.CN', 'TOAST'], 'test2': ['TEST2.CN']}
