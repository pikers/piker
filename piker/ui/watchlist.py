"""
A real-time, sorted watchlist
"""
import os
from importlib import import_module

import click
import trio

# use the trio async loop
os.environ['KIVY_EVENTLOOP'] = 'trio'

from kivy.uix.widget import Widget
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.core.window import Window
from kivy.properties import DictProperty
from kivy.lang import Builder
from kivy import utils
from kivy.app import async_runTouchApp

from ..log import get_logger, get_console_log

log = get_logger('watchlist')


def same_rgb(val):
    return ', '.join(map(str, [val]*3))


def colorcode(name):
    if not name:
        name = 'darkgray'
    _names2hexs = {
        'darkgray': 'a9a9a9',
        'green': '008000',
        'red': 'ff3333',
        'red2': 'ff0000',
        'dark_red': '8b0000',
        'firebrick': 'b22222',
    }
    return utils.rgba(_names2hexs[name])


_kv = (f'''
#:kivy 1.10.0

<HeaderCell>
    # font_size: '15'
    size: self.texture_size
    # size_hint_y: None
    # height: '100dp'
    outline_color: {same_rgb(0.01)}
    width: '100dp'
    valign: 'middle'
    halign: 'center'
    canvas.before:
        Color:
            rgb: {same_rgb(0.13)}
        Rectangle:
            pos: self.pos
            size: self.size

<Cell>
    text_size: self.size
    size: self.texture_size
    # font_size: '15'
    font_color: {colorcode('darkgray')}
    # font_name: 'sans serif'
    valign: 'middle'
    halign: 'center'
    # outline_color: {same_rgb(0.01)}
    canvas.before:
        Color:
            rgb: {same_rgb(0.05)}
        Rectangle:
            pos: self.pos
            size: self.size

<TickerTable>
    spacing: '5dp'
    row_force_default: True
    row_default_height: 75
    # size_hint_y: None
    size_hint: 1, None
    cols: 1

<Row>
    spacing: '4dp'
    minimum_height: 200  # should be pulled from Cell text size
    minimum_width: 200
    row_force_default: True
    row_default_height: 75
    outline_color: {same_rgb(2)}
    size_hint: 1, None
''')


_qt_keys = {
    # 'symbol': 'symbol',  # done manually in remap_keys
    'lastTradePrice': 'last',
    'lastTradeSize': 'last size',
    'askPrice': 'ask',
    'askSize': 'ask price',
    'bidPrice': 'bid',
    'bidSize': 'bid size',
    'volume': 'vol',
    'VWAP': 'vwap',
    'high52w': 'high52w',
    'highPrice': 'high',
    # "lastTradePriceTrHrs": 7.99,
    # "lastTradeTick": "Equal",
    # "lastTradeTime": "2018-01-30T18:28:23.434000-05:00",
    'low52w': 'low52w',
    'lowPrice': 'low day',
    'openPrice': 'open',
    # "symbolId": 3575753,
    # "tier": "",
    'isHalted': 'halted',
    'delay': 'delay',  # as subscript 'p'
}


def remap_keys(quote, keymap=_qt_keys):
    """Remap a list of quote dicts ``quotes`` using
    the mapping of old keys -> new keys ``keymap``.
    """
    open_price = quote['openPrice']
    new = {
        'symbol': quote['symbol'],
        '%': f"{(quote['lastTradePrice'] - open_price) / open_price:10.2f}"
    }
    for key, new_key in keymap.items():
        value = quote[key]
        new[new_key] = value

    return new


class HeaderCell(Label):
    """Column header cell label.
    """


class Cell(Label):
    """Data header cell label.
    """


class Row(GridLayout):
    """A grid for displaying a row of ticker quote data.

    The row fields can be updated using the ``fields`` property which will in
    turn adjust the text color of the values based on content changes.
    """
    def __init__(self, record, headers=(), cell_type=Cell, **kwargs):
        super(Row, self).__init__(cols=len(record), **kwargs)
        self._cell_widgets = {}
        self._last_record = record

        # build out row using Cell labels
        for key, val in record.items():
            header = key in headers
            cell = self._append_cell(val, header=header)
            self._cell_widgets[key] = cell

    def _append_cell(self, text, colorname=None, header=False):
        if not len(self._cell_widgets) < self.cols:
            raise ValueError(f"Can not append more then {self.cols} cells")

        # header cells just have a different colour
        celltype = HeaderCell if header else Cell
        cell = celltype(text=str(text), color=colorcode(colorname))
        self.add_widget(cell)
        return cell


class TickerTable(GridLayout):
    """A grid for displaying ticker quote records as a table.
    """
    def __init__(self, **kwargs):
        super(TickerTable, self).__init__(**kwargs)
        self.symbols2rows = {}

    def append_row(self, record, colorname='firebrick'):
        row = Row(record, headers=('symbol',))
        # store ref to each row
        self.symbols2rows[row._last_record['symbol']] = row
        self.add_widget(row)
        return row


def header_row(headers):
    """Create a single "header" row from a sequence of keys.
    """
    # process headers via first quote record
    headers_dict = {key: key for key in headers}
    row = Row(headers_dict, headers=headers)
    return row


def ticker_table(quotes, **kwargs):
    """Create a new ticker table from a list of quote dicts.
    """
    table = TickerTable(cols=1)

    for ticker_record in quotes:
        table.append_row(ticker_record)

    return table


async def update_quotes(widgets, queue):
    """Process live quotes by updating ticker rows.
    """
    grid = widgets['grid']

    while True:
        log.debug("Waiting on quotes")
        quotes = await queue.get()
        rows = []
        for quote in quotes:
            data = remap_keys(quote)
            row = grid.symbols2rows[data['symbol']]
            rows.append((data, row))
            new = set(data.items()) - set(row._last_record.items())
            if new:
                for key, val in filter(lambda item: item[0] != '%', new):
                    # logic for value coloring: up-green, down-red
                    if row._last_record[key] < val:
                        color = colorcode('green')
                    elif row._last_record[key] > val:
                        color = colorcode('red2')

                    cell = row._cell_widgets[key]
                    cell.text = str(val)
                    cell.color = color

                row._last_record = data

            hdrcell = row._cell_widgets['symbol']
            chngcell = row._cell_widgets['%']
            daychange = float(data['%'])
            if daychange < 0.:
                color = colorcode('red2')
                chngcell.color = hdrcell.color = color
            elif daychange > 0.:
                color = colorcode('green')
                chngcell.color = hdrcell.color = color

        # sort rows by % change
        for i, pair in enumerate(
            sorted(rows, key=lambda item: float(item[0]['%']))
        ):
            data, row = pair
            if grid.children[i] != row:
                grid.remove_widget(row)
                grid.add_widget(row, index=i)


async def run_kivy(root, nursery):
    '''Trio-kivy entry point.
    '''
    # run kivy
    await async_runTouchApp(root)
    # now cancel all the other tasks that may be running
    nursery.cancel_scope.cancel()


async def _async_main(tickers, brokermod):
    '''Launch kivy app + all other related tasks.
    '''
    queue = trio.Queue(1000)

    async with brokermod.get_client() as client:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(brokermod.poll_tickers, client, tickers, queue)

            # get first quotes response
            quotes = []
            pkts = await queue.get()
            for quote in pkts:
                quotes.append(remap_keys(quote))

            # build out UI
            Builder.load_string(_kv)
            root = BoxLayout(orientation='vertical')
            header = header_row(quotes[0].keys())
            root.add_widget(header)
            grid = ticker_table(quotes)
            grid.bind(minimum_height=grid.setter('height'))
            scroll = ScrollView(
                size=(Window.width, Window.height), bar_margin=10)
            scroll.add_widget(grid)
            root.add_widget(scroll)

            widgets = {
                'grid': grid,
                'root': root,
                'header': header,
                'scroll': scroll,
            }

            nursery.start_soon(run_kivy, widgets['root'], nursery)
            nursery.start_soon(update_quotes, widgets, queue)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--broker', default='questrade', help='Broker backend to use')
@click.option('--loglevel', '-l', default='warning', help='Logging level')
def run(loglevel, broker):
    """Spawn a watchlist.
    """
    get_console_log(loglevel)  # activate console logging
    brokermod = import_module('.' + broker, 'piker.brokers')

    watchlists = {
        'cannabis': [
            'EMH.VN', 'LEAF.TO', 'HVT.VN', 'HMMJ.TO', 'APH.TO',
            'CBW.VN', 'TRST.CN', 'VFF.TO', 'ACB.TO', 'ABCN.VN'
            'APH.TO', 'MARI.CN', 'WMD.VN', 'LEAF.TO', 'THCX.VN'
        ],
    }
    # broker_conf_path = os.path.join(click.get_app_dir('piker'), 'watchlists.json')
    # from piker.testing import _quote_streamer as brokermod
    trio.run(_async_main, watchlists['cannabis'], brokermod)
