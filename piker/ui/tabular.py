"""
Real-time table components
"""
from itertools import chain
from typing import List
from bisect import bisect

import trio
from kivy.uix.gridlayout import GridLayout
from kivy.uix.stacklayout import StackLayout
from kivy.uix.button import Button
from kivy import utils
from kivy.properties import BooleanProperty

from ..log import get_logger
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
_fs = 20  # font size

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
    font_size: {_fs}

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
    font_size: {_fs}
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
    row_default_height: 56
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

        self._last_record.update(record)
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
            self.table.last_clicked_row = self
            symbol = self._last_record['symbol']
            for sendchan in self.table._click_queues:
                try:
                    sendchan.send_nowait(symbol)
                except trio.BrokenResourceError:
                    # indicates streaming client actor has terminated
                    log.warning("Symbol selection stream was already closed")


class TickerTable(GridLayout):
    """A grid for displaying ticker quote records as a table.
    """
    def __init__(self, sort_key='%', auto_sort=True, **kwargs):
        super(TickerTable, self).__init__(**kwargs)
        self.symbols2rows = {}
        self.sort_key = sort_key
        # for tracking last clicked column header cell
        self.last_clicked_col_cell = None
        self.last_clicked_row = None
        self._auto_sort = auto_sort
        self._symbols2index = {}
        self._sorted = []
        self._click_queues: List[trio.abc.SendChannel[str]] = []

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
