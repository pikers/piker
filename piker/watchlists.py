import os
import json
from collections import defaultdict

from .log import get_logger

log = get_logger(__name__)


def write_sorted_json(watchlist, path):
    for key in watchlist:
        watchlist[key] = sorted(list(set(watchlist[key])))
    with open(path, 'w') as f:
        json.dump(watchlist, f, sort_keys=True)


def make_config_dir(dir_path):
    if not os.path.isdir(dir_path):
        log.debug(f"Creating config dir {dir_path}")
        os.makedirs(dir_path)


def ensure_watchlists(file_path):
    mode = 'r' if os.path.isfile(file_path) else 'w'
    with open(file_path, mode) as f:
        return json.load(f) if not os.stat(file_path).st_size == 0 else {}


def write_watchlists(watchlist, path):
    write_sorted_json(json.loads(watchlist), path)


def add_ticker(name, ticker_name, watchlist, path):
    watchlist.setdefault(name, []).append(str(ticker_name).upper())
    write_sorted_json(watchlist, path)


def remove_ticker(name, ticker_name, watchlist, path):
    if name in watchlist:
        watchlist[name].remove(str(ticker_name).upper())
        if watchlist[name] == []:
            del watchlist[name]
    write_sorted_json(watchlist, path)


def delete_group(name, watchlist, path):
    if name in watchlist:
        del watchlist[name]
    write_sorted_json(watchlist, path)


def merge_watchlist(watchlist_to_merge, watchlist, path):
    merged_watchlist = defaultdict(list)
    watchlist_to_merge = json.loads(watchlist_to_merge)
    for d in (watchlist, watchlist_to_merge):
        for key, value in d.items():
            merged_watchlist[key].extend(value)
    write_sorted_json(merged_watchlist, path)
