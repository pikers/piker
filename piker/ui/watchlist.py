"""
A real-time, sorted watchlist.

Launch with ``piker watch <watchlist name>``.

(Currently there's a bunch of questrade specific stuff in here)
"""
from itertools import chain
from types import ModuleType, AsyncGeneratorType

import trio
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.stacklayout import StackLayout
from kivy.uix.button import Button
from kivy.lang import Builder
from kivy import utils
from kivy.app import async_runTouchApp
from kivy.core.window import Window

from ..log import get_logger
from .pager import PagerView

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
    font_size: 20
    # text_size: self.size
    size: self.texture_size
    color: {colorcode('gray')}
    font_color: {colorcode('gray')}
    font_name: 'Roboto-Regular'
    background_color: [0.13]*3 + [1]
    background_normal: ''
    valign: 'middle'
    halign: 'center'
    # outline_color: [0.1]*4

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
    # outline_color: [.7]*4

<SearchBar>
    # part of the `PagerView`
    size_hint: 1, 0.03
    font_size: 25
    background_color: [0.13]*3 + [1]
''')


class HeaderCell(Button):
    """Column header cell label.
    """
    def on_press(self, value=None):
        """Clicking on a col header indicates to sort rows by this column
        in `update_quotes()`.
        """
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
        small_size = top_size - 2
        top_prop = 0.7  # proportion of size used by top cell
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
        self, record, headers=(), bidasks=None, table=None,
        is_header_row=False,
        **kwargs
    ):
        super(Row, self).__init__(cols=len(record), **kwargs)
        self._cell_widgets = {}
        self._last_record = record
        self.table = table
        self.is_header = is_header_row

        # create `BidAskCells` first
        layouts = {}
        bidasks = bidasks or {}
        ba_cells = {}
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

    def append_row(self, record, bidasks=None):
        """Append a `Row` of `Cell` objects to this table.
        """
        row = Row(record, headers=('symbol',), bidasks=bidasks, table=self)
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

    def ticker_search(self, patt):
        """Return sequence of matches when pattern ``patt`` is in a
        symbol name. Most naive algo possible for the moment.
        """
        for symbol, row in self.symbols2rows.items():
            if patt in symbol:
                yield symbol, row

    def search(self, patt):
        """Search bar api compat.
        """
        return dict(self.ticker_search(patt)) or {}


async def update_quotes(
    nursery: 'Nursery',
    brokermod: ModuleType,
    widgets: dict,
    agen: AsyncGeneratorType,
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
    for sym, quote in first_quotes.items():
        row = grid.symbols2rows[sym]
        record, displayable = brokermod.format_quote(
            quote, symbol_data=symbol_data)
        row.update(record, displayable)
        color_row(row, record)
        cache[sym] = (record, row)

    grid.render_rows(cache)

    # core cell update loop
    async for quotes in agen:  # new quotes data only
        for symbol, quote in quotes.items():
            record, displayable = brokermod.format_quote(
                quote, symbol_data=symbol_data)
            row = grid.symbols2rows[symbol]
            cache[symbol] = (record, row)
            row.update(record, displayable)
            color_row(row, record)

        grid.render_rows(cache)
        log.debug("Waiting on quotes")

    log.warn("Server connection dropped")
    nursery.cancel_scope.cancel()


async def _async_main(name, portal, tickers, brokermod, rate):
    '''Launch kivy app + all other related tasks.

    This is started with cli command `piker watch`.
    '''
    # subscribe for tickers (this performs a possible filtering
    # where invalid symbols are discarded)
    sd = await portal.run(
        "piker.brokers.core", 'symbol_data',
        broker=brokermod.name, tickers=tickers)

    # an async generator instance
    agen = await portal.run(
        "piker.brokers.core", 'start_quote_stream',
        broker=brokermod.name, tickers=tickers)

    async with trio.open_nursery() as nursery:
        # get first quotes response
        log.debug("Waiting on first quote...")
        quotes = await agen.__anext__()
        first_quotes = [
            brokermod.format_quote(quote, symbol_data=sd)[0]
            for quote in quotes.values()]

        if first_quotes[0].get('last') is None:
            log.error("Broker API is down temporarily")
            nursery.cancel_scope.cancel()
            return

        # build out UI
        Window.set_title(f"watchlist: {name}\t(press ? for help)")
        Builder.load_string(_kv)
        box = BoxLayout(orientation='vertical', padding=5, spacing=5)

        # define bid-ask "stacked" cells
        # (TODO: needs some rethinking and renaming for sure)
        bidasks = brokermod._bidasks

        # add header row
        headers = first_quotes[0].keys()
        header = Row(
            {key: key for key in headers},
            headers=headers,
            bidasks=bidasks,
            is_header_row=True,
            size_hint=(1, None),
        )
        box.add_widget(header)

        # build grid
        grid = TickerTable(
            cols=1,
            size_hint=(1, None),
        )
        for ticker_record in first_quotes:
            grid.append_row(ticker_record, bidasks=bidasks)
        # associate the col headers row with the ticker table even though
        # they're technically wrapped separately in containing BoxLayout
        header.table = grid

        # mark the initial sorted column header as bold and underlined
        sort_cell = header.get_cell(grid.sort_key)
        sort_cell.bold = sort_cell.underline = True
        grid.last_clicked_col_cell = sort_cell

        # set up a pager view for large ticker lists
        grid.bind(minimum_height=grid.setter('height'))
        pager = PagerView(box, grid, nursery)
        box.add_widget(pager)

        widgets = {
            # 'anchor': anchor,
            'root': box,
            'grid': grid,
            'box': box,
            'header': header,
            'pager': pager,
        }
        # nursery.start_soon(run_kivy, widgets['root'], nursery)
        nursery.start_soon(
            update_quotes, nursery, brokermod, widgets, agen, sd, quotes)

        # Trio-kivy entry point.
        await async_runTouchApp(widgets['root'])  # run kivy
        await agen.aclose()  # cancel aysnc gen call
        await portal.run(
            "piker.brokers.core", 'modify_quote_stream',
            broker=brokermod.name, tickers=[])
