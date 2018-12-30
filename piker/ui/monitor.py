"""
monitor: a real-time, sorted watchlist.

Launch with ``piker monitor <watchlist name>``.

(Currently there's a bunch of questrade specific stuff in here)
"""
from itertools import chain
from types import ModuleType, AsyncGeneratorType
from typing import List, Callable
from bisect import bisect

import trio
import tractor
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.stacklayout import StackLayout
from kivy.uix.button import Button
from kivy.lang import Builder
from kivy import utils
from kivy.app import async_runTouchApp
from kivy.core.window import Window
from kivy.properties import BooleanProperty

from ..log import get_logger
from .pager import PagerView
from .kivy.mouse_over import new_mouse_over_group


HoverBehavior = new_mouse_over_group()
log = get_logger('monitor')


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


_bs = 0.75  # border size

# medium shade of gray that seems to match the
# default i3 window borders
_i3_rgba = [0.14]*3 + [1]

# slightly off black like the jellybean bg from
# vim colorscheme
_cell_rgba = [0.07]*3 + [1]
_black_rgba = [0]*4

_kv = (f'''
#:kivy 1.10.0

<Cell>
    font_size: 21

    # make text wrap to botom
    text_size: self.size
    halign: 'center'
    valign: 'middle'
    size: self.texture_size

    # don't set these as the update loop already does it
    # color: {colorcode('gray')}
    # font_color: {colorcode('gray')}
    # font_name: 'Hack-Regular'

    # if `highlight` is set use i3 color by default transparent; use row color
    # this is currently used for expiry cells on the options chain
    background_color: {_i3_rgba} if self.click_toggle else {_black_rgba}
    # must be set to allow 'plain bg colors' since default texture is grey
    # but right now is only set for option chain expiry buttons
    # background_normal: ''
    # spacing: 0, 0
    # padding: 3, 3


<HeaderCell>
    font_size: 21
    # canvas.before:
    #     Color:
    #         rgba: [0.13]*4
    #     BorderImage:  # use a fixed size border
    #         pos: self.pos
    #         size: [self.size[0] - {_bs}, self.size[1]]
    #         # 0s are because the containing TickerTable already has spacing
    #         # border: [0, {_bs} , 0, {_bs}]
    #         border: [0, {_bs} , 0, 0]


<TickerTable>
    spacing: [{_bs}]
    # row_force_default: True
    row_default_height: 62
    cols: 1
    canvas.before:
        Color:
            # i3 style gray as background
            rgba: {_i3_rgba}
        Rectangle:
            # scale with container self here refers to the widget i.e BoxLayout
            pos: self.pos
            size: self.size


<BidAskLayout>
    spacing: [{_bs}, 0]


<Row>
    # minimum_height: 200  # should be pulled from Cell text size
    # minimum_width: 200
    # row_force_default: True
    # row_default_height: 61  # determines the header row size
    padding: [0]*4
    spacing: [0]
    canvas.before:
        Color:
            rgba: {_cell_rgba}
        Rectangle:
            # self here refers to the widget i.e Row(GridLayout)
            pos: self.pos
            size: self.size
        # row higlighting on mouse over
        Color:
            rgba: {_i3_rgba}
        # RoundedRectangle:
        Rectangle:
            size: self.width, self.height if self.hovered else 1
            pos: self.pos
            # radius: (0,)


# part of the `PagerView`
<SearchBar>
    size_hint: 1, None
    # static size of 51 px
    height: 51
    font_size: 25
    background_color: {_i3_rgba}
''')


class Cell(Button):
    """Data cell: the fundemental widget.

    ``key`` is the column name index value.
    """
    click_toggle = BooleanProperty(False)

    def __init__(self, key=None, is_header=False, **kwargs):
        super(Cell, self).__init__(**kwargs)
        self.key = key
        self.row = None
        self.is_header = is_header

    def on_press(self, value=None):
        self.row.on_press()


class HeaderCell(Cell):
    """Column header cell label.
    """
    def on_press(self, value=None):
        """Clicking on a col header indicates to sort rows by this column
        in `update_quotes()`.
        """
        table = self.row.table
        # if this is a row header cell then sort by the clicked field
        if self.row.is_header:
            table.sort_key = self.key

            last = table.last_clicked_col_cell
            if last and last is not self:
                last.underline = False
                last.bold = False

            # outline the header text to indicate it's been the last clicked
            self.underline = True
            self.bold = True
            # mark this cell as the last selected
            table.last_clicked_col_cell = self
            # sort and render the rows immediately
            self.row.table.render_rows(table.symbols2rows.values())

        # TODO: make this some kind of small geometry instead
        # (maybe like how trading view does it).
        # allow highlighting of row headers for tracking
        elif self.is_header:
            if self.background_color == self.color:
                self.background_color = _black_rgba
            else:
                self.background_color = self.color


class BidAskLayout(StackLayout):
    """Cell which houses three buttons containing a last, bid, and ask in a
    single unit oriented with the last 2 under the first.
    """
    def __init__(self, values, header=False, **kwargs):
        # uncomment to get vertical stacked bid-ask
        # super(BidAskLayout, self).__init__(orientation='bt-lr', **kwargs)
        super(BidAskLayout, self).__init__(orientation='lr-tb', **kwargs)
        assert len(values) == 3, "You can only provide 3 values: last,bid,ask"
        self._keys2cells = {}
        cell_type = HeaderCell if header else Cell
        top_size = cell_type().font_size
        small_size = top_size - 4
        top_prop = 0.5  # proportion of size used by top cell
        bottom_prop = 1 - top_prop
        for (key, size_hint, font_size), value in zip(
            [('last', (1, top_prop), top_size),
             ('bid', (0.5, bottom_prop), small_size),
             ('ask', (0.5, bottom_prop), small_size)],
            # uncomment to get vertical stacked bid-ask
            # [('last', (top_prop, 1), top_size),
            #  ('bid', (bottom_prop, 0.5), small_size),
            #  ('ask', (bottom_prop, 0.5), small_size)],
            values
        ):
            cell = cell_type(
                text=str(value),
                size_hint=size_hint,
                # width=self.width/2 - 3,
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
        return self._keys2cells.get(key)

    @property
    def row(self):
        return self.row

    @row.setter
    def row(self, row):
        # so hideous
        for cell in self.cells:
            cell.row = row

    @property
    def cells(self):
        return [self.last, self.bid, self.ask]


class Row(HoverBehavior, GridLayout):
    """A grid for displaying a row of ticker quote data.
    """
    def __init__(
        self,
        record,
        headers=(),
        no_cell=(),
        bidasks=None,
        table=None,
        is_header=False,
        cell_type=None,
        **kwargs
    ):
        super().__init__(cols=len(record), **kwargs)
        self._cell_widgets = {}
        self._last_record = record
        self.table = table
        self.is_header = is_header
        self._cell_type = cell_type
        self.widget = self

        # Create `BidAskCells` first.
        # bid/ask cells are just 3 cells grouped in a
        # ``BidAskLayout`` which just stacks the parent cell
        # on top of 2 children.
        layouts = {}
        bidasks = bidasks or {}
        ba_cells = {}
        for key, children in bidasks.items():
            layout = BidAskLayout(
                [record[key]] + [record[child] for child in children],
                header=is_header
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
            elif key not in no_cell:
                cell = self._append_cell(val, key, header=header)
                cell.key = key
                self._cell_widgets[key] = cell

    def iter_cells(self):
        return self._cell_widgets.items()

    def get_cell(self, key):
        return self._cell_widgets.get(key)

    def get_field(self, key):
        return self._last_record[key]

    def _append_cell(self, text, key, header=False):
        if not len(self._cell_widgets) < self.cols:
            raise ValueError(f"Can not append more then {self.cols} cells")

        # header cells just have a different colour
        celltype = self._cell_type or (HeaderCell if header else Cell)
        cell = celltype(text=str(text), key=key)
        cell.is_header = header
        cell.row = self
        self.add_widget(cell)
        return cell

    def update(self, record, displayable):
        """Update this row's cells with new values from a quote
        ``record``.

        Return all cells that changed in a ``dict``.
        """
        # color changed field values
        cells = {}
        gray = colorcode('gray')
        fgreen = colorcode('forestgreen')
        red = colorcode('red2')
        for key, val in record.items():
            last = self.get_field(key)
            color = gray
            try:
                # logic for cell text coloring: up-green, down-red
                if last < val:
                    color = fgreen
                elif last > val:
                    color = red
            except TypeError:
                log.warn(f"wtf QT {val} is not regular?")

            cell = self.get_cell(key)
            # some displayable fields might have specifically
            # not had cells created as set in the `no_cell` attr
            if cell is not None:
                cell.text = str(displayable[key])
                cell.color = color
                if color != gray:
                    cells[key] = cell

        self._last_record = record
        return cells

    # mouse over handlers
    def on_enter(self):
        """Highlight layout on enter.
        """
        log.debug(
            f"Entered row {self} through {self.border_point}")
        # don't highlight header row
        if self.is_header:
            self.hovered = False

    def on_leave(self):
        """Un-highlight layout on exit.
        """
        log.debug(
            f"Left row {self} through {self.border_point}")

    def on_press(self, value=None):
        log.info(f"Pressed row for {self._last_record['symbol']}")
        if self.table and not self.is_header:
            for q in self.table._click_queues:
                q.put_nowait(self._last_record['symbol'])


class TickerTable(GridLayout):
    """A grid for displaying ticker quote records as a table.
    """
    def __init__(self, sort_key='%', auto_sort=True, **kwargs):
        super(TickerTable, self).__init__(**kwargs)
        self.symbols2rows = {}
        self.sort_key = sort_key
        # for tracking last clicked column header cell
        self.last_clicked_col_cell = None
        self._auto_sort = auto_sort
        self._symbols2index = {}
        self._sorted = []
        self._click_queues: List[trio.Queue] = []

    def append_row(self, key, row):
        """Append a `Row` of `Cell` objects to this table.
        """
        # store ref to each row
        self.symbols2rows[key] = row
        self.add_widget(row)
        self._sorted.append(row)
        return row

    def clear(self):
        self.clear_widgets()
        self._sorted.clear()

    def render_rows(
        self,
        changed: set,
        sort_key: str = None,
    ):
        """Sort and render all rows on the ticker grid from ``syms2rows``.
        """
        sort_key = sort_key or self.sort_key
        key_row_pairs = list(sorted(
                [(row.get_field(sort_key), row) for row in self._sorted],
                key=lambda item: item[0],
        ))
        if key_row_pairs:
            sorted_keys, sorted_rows = zip(*key_row_pairs)
            sorted_keys, sorted_rows = list(sorted_keys), list(sorted_rows)
        else:
            sorted_keys, sorted_rows = [], []

        # now remove and re-insert any rows that need to be shuffled
        # due to new a new field change
        for row in changed:
            try:
                old_index = sorted_rows.index(row)
            except ValueError:
                # row is not yet added so nothing to remove
                pass
            else:
                del sorted_rows[old_index]
                del sorted_keys[old_index]
                self._sorted.remove(row)
                self.remove_widget(row)

        for row in changed:
            key = row.get_field(sort_key)
            index = bisect(sorted_keys, key)
            sorted_keys.insert(index, key)
            self._sorted.insert(index, row)
            self.add_widget(row, index=index)

    def ticker_search(self, patt):
        """Return sequence of matches when pattern ``patt`` is in a
        symbol name. Most naive algo possible for the moment.
        """
        for symbol, row in self.symbols2rows.items():
            if patt in symbol:
                yield symbol, row

    def get_row(self, symbol: str) -> Row:
        return self.symbols2rows[symbol]

    def search(self, patt):
        """Search bar api compat.
        """
        return dict(self.ticker_search(patt)) or {}


async def update_quotes(
    nursery: trio._core._run.Nursery,
    formatter: Callable,
    widgets: dict,
    agen: AsyncGeneratorType,
    symbol_data: dict,
    first_quotes: dict,
    task_status: trio._core._run._TaskStatus = trio.TASK_STATUS_IGNORED,
):
    """Process live quotes by updating ticker rows.
    """
    log.debug("Initializing UI update loop")
    table = widgets['table']
    flash_keys = {'low', 'high'}

    async def revert_cells_color(cells):
        await trio.sleep(0.3)
        for cell in cells:
            cell.background_color = _black_rgba

    def color_row(row, record, cells):
        hdrcell = row.get_cell('symbol')
        chngcell = row.get_cell('%')

        # determine daily change color
        color = colorcode('gray')
        percent_change = record.get('%')
        if percent_change:
            daychange = float(record['%'])
            if daychange < 0.:
                color = colorcode('red2')
            elif daychange > 0.:
                color = colorcode('forestgreen')

        # update row header and '%' cell text color
        if chngcell:
            chngcell.color = color
            hdrcell.color = color
            # if the cell has been "highlighted" make sure to change its color
            if hdrcell.background_color != [0]*4:
                hdrcell.background_color = color

        # briefly highlight bg of certain cells on each trade execution
        unflash = set()
        tick_color = None
        last = cells.get('last')
        if not last:
            vol = cells.get('vol')
            if not vol:
                return  # no trade exec took place

            # flash gray on volume tick
            # (means trade exec @ current price)
            last = row.get_cell('last')
            tick_color = colorcode('gray')
        else:
            tick_color = last.color

        last.background_color = tick_color
        unflash.add(last)
        # flash the size cell
        size = row.get_cell('size')
        size.background_color = tick_color
        unflash.add(size)

        # flash all other cells
        for key in flash_keys:
            cell = cells.get(key)
            if cell:
                cell.background_color = cell.color
                unflash.add(cell)

        # revert flash state momentarily
        nursery.start_soon(revert_cells_color, unflash)

    # initial coloring
    to_sort = set()
    for sym, quote in first_quotes.items():
        row = table.get_row(sym)
        record, displayable = formatter(
            quote, symbol_data=symbol_data)
        row.update(record, displayable)
        color_row(row, record, {})
        to_sort.add(row.widget)

    table.render_rows(to_sort)

    log.debug("Finished initializing update loop")
    task_status.started()
    # real-time cell update loop
    async for quotes in agen:  # new quotes data only
        to_sort = set()
        for symbol, quote in quotes.items():
            row = table.get_row(symbol)
            record, displayable = formatter(
                quote, symbol_data=symbol_data)

            # determine if sorting should happen
            sort_key = table.sort_key
            new = record[sort_key]
            last = row.get_field(sort_key)
            if new != last:
                to_sort.add(row.widget)

            # update and color
            cells = row.update(record, displayable)
            color_row(row, record, cells)

        if to_sort:
            table.render_rows(to_sort)

        log.debug("Waiting on quotes")

    log.warn("Data feed connection dropped")
    # XXX: if we're cancelled this should never get called
    # nursery.cancel_scope.cancel()


async def stream_symbol_selection():
    """An RPC async gen for streaming the symbol corresponding
    value corresponding to the last clicked row.
    """
    widgets = tractor.current_actor().statespace['widgets']
    table = widgets['table']
    q = trio.Queue(1)
    table._click_queues.append(q)
    try:
        async for symbol in q:
            yield symbol
    finally:
        table._click_queues.remove(q)


async def _async_main(
    name: str,
    portal: tractor._portal.Portal,
    tickers: List[str],
    brokermod: ModuleType,
    rate: int = 3,
    test: bool = False
) -> None:
    '''Launch kivy app + all other related tasks.

    This is started with cli cmd `piker monitor`.
    '''
    if test:
        # stream from a local test file
        quote_gen = await portal.run(
            "piker.brokers.data", 'stream_from_file',
            filename=test
        )
        # TODO: need a set of test packets to make this work
        # seriously fu QT
        # sd = {}
    else:
        # start live streaming from broker daemon
        quote_gen = await portal.run(
            "piker.brokers.data",
            'start_quote_stream',
            broker=brokermod.name,
            symbols=tickers,
            rate=3,
        )

    # subscribe for tickers (this performs a possible filtering
    # where invalid symbols are discarded)
    sd = await portal.run(
        "piker.brokers.data", 'symbol_data',
        broker=brokermod.name, tickers=tickers)

    # get first quotes response
    log.debug("Waiting on first quote...")
    quotes = await quote_gen.__anext__()
    first_quotes = [
        brokermod.format_stock_quote(quote, symbol_data=sd)[0]
        for quote in quotes.values()]

    if first_quotes[0].get('last') is None:
        log.error("Broker API is down temporarily")
        return

    # build out UI
    Window.set_title(f"monitor: {name}\t(press ? for help)")
    Builder.load_string(_kv)
    box = BoxLayout(orientation='vertical', spacing=0)

    # define bid-ask "stacked" cells
    # (TODO: needs some rethinking and renaming for sure)
    bidasks = brokermod._stock_bidasks

    # add header row
    headers = first_quotes[0].keys()
    header = Row(
        {key: key for key in headers},
        headers=headers,
        bidasks=bidasks,
        is_header=True,
        size_hint=(1, None),
    )
    box.add_widget(header)

    # build table
    table = TickerTable(
        cols=1,
        size_hint=(1, None),
    )
    for ticker_record in first_quotes:
        table.append_row(
            ticker_record['symbol'],
            Row(ticker_record, headers=('symbol',),
                bidasks=bidasks, table=table)
        )

    # associate the col headers row with the ticker table even though
    # they're technically wrapped separately in containing BoxLayout
    header.table = table

    # mark the initial sorted column header as bold and underlined
    sort_cell = header.get_cell(table.sort_key)
    sort_cell.bold = sort_cell.underline = True
    table.last_clicked_col_cell = sort_cell

    # set up a pager view for large ticker lists
    table.bind(minimum_height=table.setter('height'))

    try:
        async with trio.open_nursery() as nursery:
            pager = PagerView(
                container=box,
                contained=table,
                nursery=nursery
            )
            box.add_widget(pager)

            widgets = {
                'root': box,
                'table': table,
                'box': box,
                'header': header,
                'pager': pager,
            }
            nursery.start_soon(
                update_quotes,
                nursery,
                brokermod.format_stock_quote,
                widgets,
                quote_gen,
                sd,
                quotes
            )

            # Trio-kivy entry point.
            await async_runTouchApp(widgets['root'])  # run kivy
            # cancel GUI update task
            nursery.cancel_scope.cancel()
    finally:
        with trio.open_cancel_scope(shield=True):
            # cancel aysnc gen call
            await quote_gen.aclose()
