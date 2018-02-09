"""
A real-time, sorted watchlist
"""
from importlib import import_module

import click
import trio

from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.lang import Builder
from kivy import utils
from kivy.app import async_runTouchApp


from ..log import get_logger, get_console_log
log = get_logger('watchlist')


def same_rgb(val):
    return ', '.join(map(str, [val]*3))


_colors2hexs = {
    'darkgray': 'a9a9a9',
    'gray': '808080',
    'green': '008000',
    'forestgreen': '228b22',
    'seagreen': '2e8b57',
    'red2': 'ff3333',
    'red': 'ff0000',
    'tomato': 'ff6347',
    'darkred': '8b0000',
    'firebrick': 'b22222',
    'maroon': '800000',
    'gainsboro': 'dcdcdc',
}

_colors = {key: utils.rgba(val) for key, val in _colors2hexs.items()}


def colorcode(name):
    return _colors[name if name else 'gray']


_kv = (f'''
#:kivy 1.10.0

<Cell>
    text_size: self.size
    size: self.texture_size
    # font_size: '15'
    # size_hint_y: None
    font_color: {colorcode('gray')}
    # font_name: 'sans serif'
    # height: 50
    # width: 50
    valign: 'middle'
    halign: 'center'
    outline_color: {same_rgb(0.001)}
    canvas.before:
        Color:
            rgb: {same_rgb(0.03)}
        RoundedRectangle:
            pos: self.pos
            size: self.size
            radius: [7,]

<HeaderCell@Cell>
    canvas.before:
        Color:
            rgb: {same_rgb(0.12)}
        RoundedRectangle:
            pos: self.pos
            size: self.size
            radius: [7,]

<TickerTable>
    spacing: '5dp'
    row_force_default: True
    row_default_height: 75
    cols: 1

<Row>
    spacing: '6dp'
    minimum_height: 200  # should be pulled from Cell text size
    minimum_width: 200
    row_force_default: True
    row_default_height: 75
    outline_color: {same_rgb(.7)}
''')


_qt_keys = {
    # 'symbol': 'symbol',  # done manually in remap_keys
    'lastTradePrice': 'last',
    'lastTradeSize': 'last size',
    'askPrice': 'ask',
    'askSize': 'ask size',
    'bidPrice': 'bid',
    'bidSize': 'bid size',
    'volume': 'vol',
    'VWAP': 'VWAP',
    'high52w': 'high52w',
    'highPrice': 'high',
    # "lastTradePriceTrHrs": 7.99,
    # "lastTradeTick": "Equal",
    # "lastTradeTime": "2018-01-30T18:28:23.434000-05:00",
    # 'low52w': 'low52w',
    'lowPrice': 'low day',
    'openPrice': 'open',
    # "symbolId": 3575753,
    # "tier": "",
    # 'isHalted': 'halted',
    # 'delay': 'delay',  # as subscript 'p'
}


def remap_keys(quote: dict, keymap: dict = _qt_keys, symbol_data: dict = None):
    """Remap a list of quote dicts ``quotes`` using
    the mapping of old keys -> new keys ``keymap``.
    """
    if symbol_data:  # we can only compute % change from symbols data
        previous = symbol_data[quote['symbol']]['prevDayClosePrice']
        change = (quote['lastTradePrice'] - previous) / previous * 100
    else:
        change = 0
    new = {
        'symbol': quote['symbol'],
        '%': f"{change:.2f}"
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

    def append_row(self, record):
        """Append a `Row` of `Cell` objects to this table.
        """
        row = Row(record, headers=('symbol',))
        # store ref to each row
        self.symbols2rows[row._last_record['symbol']] = row
        self.add_widget(row)
        return row


def header_row(headers, **kwargs):
    """Create a single "header" row from a sequence of keys.
    """
    headers_dict = {key: key for key in headers}
    row = Row(headers_dict, headers=headers, **kwargs)
    return row


def ticker_table(quotes, **kwargs):
    """Create a new ticker table from a list of quote dicts.
    """
    table = TickerTable(cols=1, **kwargs)
    for ticker_record in quotes:
        table.append_row(ticker_record)
    return table


async def update_quotes(
    widgets: dict,
    queue: trio.Queue,
    symbol_data: dict,
    first_quotes: dict
):
    """Process live quotes by updating ticker rows.
    """
    grid = widgets['grid']

    def color_row(row, data):
        hdrcell = row._cell_widgets['symbol']
        chngcell = row._cell_widgets['%']
        daychange = float(data['%'])
        if daychange < 0.:
            color = colorcode('red2')
        elif daychange > 0.:
            color = colorcode('forestgreen')
        else:
            color = colorcode('gray')

        chngcell.color = hdrcell.color = color

    # initial coloring
    all_rows = []
    for quote in first_quotes:
        row = grid.symbols2rows[quote['symbol']]
        all_rows.append((quote, row))
        color_row(row, quote)

    while True:
        log.debug("Waiting on quotes")
        quotes = await queue.get()
        for quote in quotes:
            data = remap_keys(quote, symbol_data=symbol_data)
            row = grid.symbols2rows[data['symbol']]

            # color changed field values
            for key, val in data.items():
                # logic for cell text coloring: up-green, down-red
                if row._last_record[key] < val:
                    color = colorcode('green')
                elif row._last_record[key] > val:
                    color = colorcode('red')
                else:
                    color = colorcode('gray')

                cell = row._cell_widgets[key]
                cell.text = str(val)
                cell.color = color

            color_row(row, data)
            row._last_record = data

        # sort rows by daily % change since open
        grid.clear_widgets()
        for i, (data, row) in enumerate(
            sorted(all_rows, key=lambda item: float(item[0]['%']))
        ):
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
            # get long term data including last days close price
            sd = await client.symbols(tickers)

            nursery.start_soon(brokermod.poll_tickers, client, tickers, queue)

            # get first quotes response
            pkts = await queue.get()

            if pkts[0]['lastTradePrice'] is None:
                log.error("Questrade API is down temporarily")
                nursery.cancel_scope.cancel()
                return

            first_quotes = [
                remap_keys(quote, symbol_data=sd) for quote in pkts]

            # build out UI
            Builder.load_string(_kv)
            root = BoxLayout(orientation='vertical', padding=5, spacing=-20)
            header = header_row(
                first_quotes[0].keys(),
                size_hint=(1, None),
            )
            root.add_widget(header)
            grid = ticker_table(
                first_quotes,
                size_hint=(1, None),
            )
            grid.bind(minimum_height=grid.setter('height'))
            scroll = ScrollView()
            scroll.add_widget(grid)
            root.add_widget(scroll)

            widgets = {
                'grid': grid,
                'root': root,
                'header': header,
                'scroll': scroll,
            }

            nursery.start_soon(run_kivy, widgets['root'], nursery)
            nursery.start_soon(update_quotes, widgets, queue, sd, first_quotes)


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
            'CBW.VN', 'TRST.CN', 'VFF.TO', 'ACB.TO', 'ABCN.VN',
            'APH.TO', 'MARI.CN', 'WMD.VN', 'LEAF.TO', 'THCX.VN',
            'WEED.TO', 'NINE.VN', 'RTI.VN', 'SNN.CN', 'ACB.TO',
            'OGI.VN', 'IMH.VN', 'FIRE.VN', 'EAT.CN', 'NUU.VN',
            'WMD.VN', 'HEMP.VN', 'CALI.CN', 'RBQ.CN',
        ],
    }
    # broker_conf_path = os.path.join(
    #     click.get_app_dir('piker'), 'watchlists.json')
    # from piker.testing import _quote_streamer as brokermod
    trio.run(_async_main, watchlists['cannabis'], brokermod)
