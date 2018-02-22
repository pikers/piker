"""
A real-time, sorted watchlist.

Launch with ``piker watch <watchlist name>``.

(Currently there's a bunch of QT specific stuff in here)
"""
from itertools import chain
from functools import partial

import trio
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.stacklayout import StackLayout
from kivy.uix.button import Button
from kivy.lang import Builder
from kivy import utils
from kivy.app import async_runTouchApp
from kivy.core.window import Window


from ..calc import humanize, percent_change
from ..log import get_logger
from .kb import PagerView

log = get_logger('watchlist')


_colors2hexs = {
    'darkgray': 'a9a9a9',
    'gray': '808080',
    'green': '008000',
    'forestgreen': '228b22',
    'red2': 'ff3333',
    'red': 'ff0000',
    'firebrick': 'b22222',
}

_colors = {key: utils.rgba(val) for key, val in _colors2hexs.items()}


def colorcode(name):
    return _colors[name if name else 'gray']


_bs = 3  # border size
_color = [0.13]*3  # nice shade of gray
_kv = (f'''
#:kivy 1.10.0

<Cell>
    font_size: 18
    text_size: self.size
    size: self.texture_size
    color: {colorcode('gray')}
    font_color: {colorcode('gray')}
    font_name: 'Roboto-Regular'
    background_color: [0.13]*3 + [1]
    background_normal: ''
    valign: 'middle'
    halign: 'center'
    outline_color: [0.1]*4

<HeaderCell>
    font_size: 20
    background_color: [0]*4
    canvas.before:
        Color:
            rgb: {_color}
        BorderImage:  # use a fixed size border
            pos: self.pos
            size: [self.size[0] - {_bs}, self.size[1]]
            # 0s are because the containing TickerTable already has spacing
            border: [0, {_bs} , 0, {_bs}]

<TickerTable>
    spacing: '{_bs}dp'
    row_force_default: True
    row_default_height: 75
    cols: 1

<BidAskLayout>
    spacing: [{_bs}, 0]

<Row>
    # minimum_height: 200  # should be pulled from Cell text size
    # minimum_width: 200
    # row_force_default: True
    # row_default_height: 75
    outline_color: [.7]*4

<SearchBar>
    size_hint: 1, 0.03
    font_size: 25
    background_color: [0.13]*3 + [1]
''')


# Questrade key conversion / column order
_qt_keys = {
    'symbol': 'symbol',  # done manually in qtconvert
    '%': '%',
    'lastTradePrice': 'last',
    'askPrice': 'ask',
    'bidPrice': 'bid',
    'lastTradeSize': 'size',
    'bidSize': 'bsize',
    'askSize': 'asize',
    'VWAP': ('VWAP', partial(round, ndigits=3)),
    'volume': ('vol', humanize),
    'mktcap': ('mktcap', humanize),
    'openPrice': 'open',
    'lowPrice': 'low',
    'highPrice': 'high',
    'low52w': 'low52w',
    'high52w': 'high52w',
    # "lastTradePriceTrHrs": 7.99,
    # "lastTradeTick": "Equal",
    # "lastTradeTime": "2018-01-30T18:28:23.434000-05:00",
    # "symbolId": 3575753,
    # "tier": "",
    # 'isHalted': 'halted',
    # 'delay': 'delay',  # as subscript 'p'
}


def qtconvert(
    quote: dict, keymap: dict = _qt_keys, symbol_data: dict = None
) -> (dict, dict):
    """Remap a list of quote dicts ``quotes`` using the mapping of old keys
    -> new keys ``keymap``.

    Returns 2 dicts: first is the original values mapped by new keys,
    and the second is the same but with all values converted to a
    "display-friendly" string format.
    """
    last = quote['lastTradePrice']
    symbol = quote['symbol']
    if symbol_data:  # we can only compute % change from symbols data
        previous = symbol_data[symbol]['prevDayClosePrice']
        change = percent_change(previous, last)
        share_count = symbol_data[symbol].get('outstandingShares', None)
        mktcap = share_count * last if share_count else 'NA'
    else:
        change = 0
    computed = {
        'symbol': quote['symbol'],
        '%': round(change, 3),
        'mktcap': mktcap,
    }
    new = {}
    displayable = {}

    for key, new_key in keymap.items():
        display_value = value = quote.get(key) or computed.get(key)

        # API servers can return `None` vals when markets are closed (weekend)
        value = 0 if value is None else value

        # convert values to a displayble format
        if isinstance(new_key, tuple):
            new_key, func = new_key
            display_value = func(value)

        new[new_key] = value
        displayable[new_key] = display_value

    return new, displayable


class HeaderCell(Button):
    """Column header cell label.
    """
    def on_press(self, value=None):
        # clicking on a col header indicates to rows by this column
        # in `update_quotes()`
        table = self.row.table
        if self.row.is_header:
            table.sort_key = self.key

            last = table.last_clicked_col_cell
            if last and last is not self:
                last.underline = False
                last.bold = False

            # outline the header text to indicate it's been the last clicked
            self.underline = True
            self.bold = True
            # mark this cell as the last
            table.last_clicked_col_cell = self
            # sort and render the rows immediately
            self.row.table.render_rows(table.quote_cache)

        # allow highlighting of row headers for tracking
        elif self.is_header:
            if self.background_color == self.color:
                self.background_color = [0]*4
            else:
                self.background_color = self.color


class Cell(Button):
    """Data cell.
    """


class BidAskLayout(StackLayout):
    """Cell which houses three buttons containing a last, bid, and ask in a
    single unit oriented with the last 2 under the first.
    """
    def __init__(self, values, header=False, **kwargs):
        super(BidAskLayout, self).__init__(orientation='lr-tb', **kwargs)
        assert len(values) == 3, "You can only provide 3 values: last,bid,ask"
        self._keys2cells = {}
        cell_type = HeaderCell if header else Cell
        top_size = cell_type().font_size
        small_size = top_size - 5
        top_prop = 0.7
        bottom_prop = 1 - top_prop
        for (key, size_hint, font_size), value in zip(
            [('last', (1, top_prop), top_size),
             ('bid', (0.5, bottom_prop), small_size),
             ('ask', (0.5, bottom_prop), small_size)],
            values
        ):
            cell = cell_type(
                text=str(value),
                size_hint=size_hint,
                width=self.width/2 - 3,
                font_size=font_size
            )
            self._keys2cells[key] = cell
            cell.key = value
            cell.is_header = header
            setattr(self, key, cell)
            self.add_widget(cell)

        # should be assigned by referrer
        self.row = None

    def get_cell(self, key):
        return self._keys2cells[key]

    @property
    def row(self):
        return self.row

    @row.setter
    def row(self, row):
        for cell in self.cells:
            cell.row = row

    @property
    def cells(self):
        return [self.last, self.bid, self.ask]


class Row(GridLayout):
    """A grid for displaying a row of ticker quote data.

    The row fields can be updated using the ``fields`` property which will in
    turn adjust the text color of the values based on content changes.
    """
    def __init__(
        self, record, headers=(), table=None, is_header_row=False,
        **kwargs
    ):
        super(Row, self).__init__(cols=len(record), **kwargs)
        self._cell_widgets = {}
        self._last_record = record
        self.table = table
        self.is_header = is_header_row

        # create `BidAskCells` first
        bidasks = {
            'last': ['bid', 'ask'],
            'size': ['bsize', 'asize']
        }
        ba_cells = {}
        layouts = {}
        for key, children in bidasks.items():
            layout = BidAskLayout(
                [record[key]] + [record[child] for child in children],
                header=is_header_row
            )
            layout.row = self
            layouts[key] = layout
            for i, child in enumerate([key] + children):
                ba_cells[child] = layout.cells[i]

        children_flat = list(chain.from_iterable(bidasks.values()))
        self._cell_widgets.update(ba_cells)

        # build out row using Cell labels
        for (key, val) in record.items():
            header = key in headers

            # handle bidask cells
            if key in layouts:
                self.add_widget(layouts[key])
            elif key in children_flat:
                # these cells have already been added to the `BidAskLayout`
                continue
            else:
                cell = self._append_cell(val, header=header)
                cell.key = key
                self._cell_widgets[key] = cell

    def get_cell(self, key):
        return self._cell_widgets[key]

    def _append_cell(self, text, header=False):
        if not len(self._cell_widgets) < self.cols:
            raise ValueError(f"Can not append more then {self.cols} cells")

        # header cells just have a different colour
        celltype = HeaderCell if header else Cell
        cell = celltype(text=str(text))
        cell.is_header = header
        cell.row = self
        self.add_widget(cell)
        return cell

    def update(self, record, displayable):
        # color changed field values
        for key, val in record.items():
            # logic for cell text coloring: up-green, down-red
            if self._last_record[key] < val:
                color = colorcode('forestgreen')
            elif self._last_record[key] > val:
                color = colorcode('red2')
            else:
                color = colorcode('gray')

            cell = self.get_cell(key)
            cell.text = str(displayable[key])
            cell.color = color

        self._last_record = record


class TickerTable(GridLayout):
    """A grid for displaying ticker quote records as a table.
    """
    def __init__(self, sort_key='%', quote_cache={}, **kwargs):
        super(TickerTable, self).__init__(**kwargs)
        self.symbols2rows = {}
        self.sort_key = sort_key
        self.quote_cache = quote_cache
        self.row_filter = lambda item: item
        # for tracking last clicked column header cell
        self.last_clicked_col_cell = None

    def append_row(self, record):
        """Append a `Row` of `Cell` objects to this table.
        """
        row = Row(record, headers=('symbol',), table=self)
        # store ref to each row
        self.symbols2rows[row._last_record['symbol']] = row
        self.add_widget(row)
        return row

    def render_rows(
            self, pairs: (dict, Row), sort_key: str = None, row_filter=None,
    ):
        """Sort and render all rows on the ticker grid from ``pairs``.
        """
        self.clear_widgets()
        sort_key = sort_key or self.sort_key
        for data, row in filter(
            row_filter or self.row_filter,
                reversed(
                    sorted(pairs.values(), key=lambda item: item[0][sort_key])
                )
        ):
            self.add_widget(row)  # row append


def header_row(headers, **kwargs):
    """Create a single "header" row from a sequence of keys.
    """
    headers_dict = {key: key for key in headers}
    row = Row(headers_dict, headers=headers, is_header_row=True, **kwargs)
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
        hdrcell = row.get_cell('symbol')
        chngcell = row.get_cell('%')
        daychange = float(data['%'])
        if daychange < 0.:
            color = colorcode('red2')
        elif daychange > 0.:
            color = colorcode('forestgreen')
        else:
            color = colorcode('gray')

        chngcell.color = hdrcell.color = color

        # if the cell has been "highlighted" make sure to change its color
        if hdrcell.background_color != [0]*4:
            hdrcell.background_color = color

    cache = {}
    grid.quote_cache = cache

    # initial coloring
    for quote in first_quotes:
        sym = quote['symbol']
        row = grid.symbols2rows[sym]
        record, displayable = qtconvert(quote, symbol_data=symbol_data)
        row.update(record, displayable)
        color_row(row, record)
        cache[sym] = (record, row)

    grid.render_rows(cache)

    # core cell update loop
    while True:
        log.debug("Waiting on quotes")
        quotes = await queue.get()  # new quotes data only
        for quote in quotes:
            record, displayable = qtconvert(quote, symbol_data=symbol_data)
            row = grid.symbols2rows[record['symbol']]
            cache[record['symbol']] = (record, row)
            row.update(record, displayable)
            color_row(row, record)

        grid.render_rows(cache)


async def run_kivy(root, nursery):
    '''Trio-kivy entry point.
    '''
    await async_runTouchApp(root)  # run kivy
    nursery.cancel_scope.cancel()  # cancel all other tasks that may be running


async def _async_main(name, watchlists, brokermod):
    '''Launch kivy app + all other related tasks.

    This is started with cli command `piker watch`.
    '''
    tickers = watchlists[name]
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
                qtconvert(quote, symbol_data=sd)[0] for quote in pkts]

            # build out UI
            Window.set_title(f"watchlist: {name}\t(press ? for help)")
            Builder.load_string(_kv)
            # anchor = AnchorLayout(anchor_x='right', anchor_y='bottom')
            box = BoxLayout(orientation='vertical', padding=5, spacing=5)
            # anchor.add_widget(box)
            header = header_row(
                first_quotes[0].keys(),
                size_hint=(1, None),
            )
            box.add_widget(header)
            grid = ticker_table(
                first_quotes,
                size_hint=(1, None),
            )
            # associate the col headers row with the ticker table even though
            # they're technically wrapped separately in containing BoxLayout
            header.table = grid
            # mark the initial sorted column header as bold and underlined
            sort_cell = header.get_cell(grid.sort_key)
            sort_cell.bold = sort_cell.underline = True
            grid.last_clicked_col_cell = sort_cell

            # set up a pager view for large ticker lists
            grid.bind(minimum_height=grid.setter('height'))
            pager = PagerView(box, nursery)
            pager.add_widget(grid)
            box.add_widget(pager)

            widgets = {
                # 'anchor': anchor,
                'root': box,
                'grid': grid,
                'box': box,
                'header': header,
                'pager': pager,
            }
            nursery.start_soon(run_kivy, widgets['root'], nursery)
            nursery.start_soon(update_quotes, widgets, queue, sd, pkts)
