"""
options: a real-time option chain.

Launch with ``piker options <symbol>``.
"""
import types
from functools import partial
from typing import Dict, List

import trio
from async_generator import asynccontextmanager
import tractor
from kivy.uix.boxlayout import BoxLayout
from kivy.lang import Builder
from kivy.app import async_runTouchApp
from kivy.core.window import Window

from ..log import get_logger
from ..brokers.core import contracts
from .pager import PagerView

from .monitor import Row, HeaderCell, Cell, TickerTable, update_quotes


log = get_logger('option_chain')


async def modify_symbol(symbol):
    pass


class StrikeCell(HeaderCell):
    """Strike cell"""


_no_display = ['symbol', 'contract_type', 'strike', 'time', 'open']
_strike_row_cache = {}
_strike_cell_cache = {}


class StrikeRow(BoxLayout):
    """A 'row' composed of two ``Row``s sandwiching a
    ``StrikeCell`.
    """
    _row_cache = {}

    def __init__(self, strike, **kwargs):
        super().__init__(orientation='horizontal', **kwargs)
        self.strike = strike
        # store 2 rows: 1 for call, 1 for put
        self._sub_rows = {}
        self._widgets_added = False

    def append_sub_row(
        self,
        record: dict,
        displayable: dict,
        bidasks=None,
        headers=(),
        table=None,
        **kwargs,
    ) -> None:
        # the 'contract_type' determines whether this
        # is a put or call row
        contract_type = record['contract_type']

        # We want to only create a few ``Row`` widgets as possible to
        # speed up rendering; we cache sub rows after creation.
        row = self._row_cache.get((self.strike, contract_type))
        if not row:
            # reverse order of call side cells
            if contract_type == 'call':
                record = dict(list(reversed(list(record.items()))))

            row = Row(
                record,
                bidasks=bidasks,
                headers=headers,
                table=table,
                no_cell=_no_display,
                **kwargs
            )
            self._row_cache[(self.strike, contract_type)] = row
        else:
            # must update the internal cells
            row.update(record, displayable)

        # reassign widget for when rendered in the update loop
        row.widget = self
        self._sub_rows[contract_type] = row

        if self.is_populated() and not self._widgets_added:
            # calls on the left
            self.add_widget(self._sub_rows['call'])
            strike_cell = _strike_cell_cache.setdefault(
                self.strike, StrikeCell(
                    key=self.strike,
                    text=str(self.strike),
                    # is_header=True,
                    # make centre strike cell nice and small
                    size_hint=(1/10., 1),
                )
            )
            # strikes in the middle
            self.add_widget(strike_cell)
            # puts on the right
            self.add_widget(self._sub_rows['put'])
            self._widgets_added = True

    def is_populated(self):
        """Bool determing if both a put and call subrow have beed appended.
        """
        return len(self._sub_rows) == 2

    def has_widgets(self):
        return self._widgets_added

    def update(self, record, displayable):
        self._sub_rows[record['contract_type']].update(
            record, displayable)

    def get_field(self, key):
        """Always sort on the lone field, the strike price.
        """
        return int(self.strike)


class ExpiryButton(Cell):
    # must be set to allow 'plain bg colors' since default texture is grey
    background_normal = ''

    def on_press(self, value=None):
        # import pdb; pdb.set_trace()
        last = self.chain._last_expiry
        if last:
            last.click_toggle = False
        self.chain._last_expiry = self

        log.info(f"Clicked {self}")
        self.click_toggle = True
        if self.chain.sub[1] == self.key:
            log.info(f"Clicked {self} is already selected")
            return
        log.info(f"Subscribing for {self.chain.sub}")
        self.chain.start_displaying(self.chain.sub[0], self.key)


class DataFeed(object):
    """Data feed client for streaming symbol data from a remote
    broker data source.
    """
    def __init__(self, portal, brokermod):
        self.portal = portal
        self.brokermod = brokermod
        self.sub = None
        self.quote_gen = None
        self._mutex = trio.StrictFIFOLock()

    async def open_stream(self, symbols, rate=3, test=None):
        async with self._mutex:
            try:
                if self.quote_gen is not None and symbols != self.sub:
                    log.info(
                        f"Stopping pre-existing subscription for {self.sub}")
                    await self.quote_gen.aclose()
                    self.sub = symbols

                if test:
                    # stream from a local test file
                    quote_gen = await self.portal.run(
                        "piker.brokers.data", 'stream_from_file',
                        filename=test
                    )
                else:
                    log.info(f"Starting new stream for {self.sub}")
                    # start live streaming from broker daemon
                    quote_gen = await self.portal.run(
                        "piker.brokers.data",
                        'start_quote_stream',
                        broker=self.brokermod.name,
                        symbols=symbols,
                        feed_type='option',
                        rate=rate,
                    )

                # get first quotes response
                log.debug(f"Waiting on first quote for {symbols}...")
                quotes = {}
                with trio.move_on_after(5):
                    quotes = await quote_gen.__anext__()

                self.quote_gen = quote_gen
                self.first_quotes = quotes
                # self.records = records
                # self.displayables = displayables
                return quote_gen, quotes
            except Exception:
                if self.quote_gen:
                    await self.quote_gen.aclose()
                raise


class OptionChain(object):
    """A real-time options chain UI.
    """
    def __init__(
        self,
        symbol: str,
        expiry: str,
        widgets: dict,
        bidasks: Dict[str, List[str]],
        feed: DataFeed,
        rate: int = 1,
    ):
        self.sub = (symbol, expiry)
        self.widgets = widgets
        self.bidasks = bidasks
        self._strikes2rows = {}
        self._nursery = None
        self._update_nursery = None
        self.feed = feed
        self._update_cs = None
        # TODO: this should be moved down to the data feed layer
        # right now it's only needed for the UI uupdate loop to cancel itself
        self._first_quotes = None
        self._last_expiry = None

    @asynccontextmanager
    async def open_scope(self):
        """Open an internal resource and update task scope required
        to allow for dynamic real-time operation.
        """
        # assign us to each expiry button
        for key, button in (
            self.widgets['expiry_buttons']._cell_widgets.items()
        ):
            button.chain = self

        async with trio.open_nursery() as n:
            self._nursery = n
            n.start_soon(self.start_updating)
            yield self
            n.cancel_scope.cancel()

        self._nursery = None
        await self.feed.quote_gen.aclose()

    def clear(self):
        """Clear the strike rows from the internal table.
        """
        table = self.widgets['table']
        table.clear()
        self._strikes2rows.clear()

    def render_rows(self, records, displayables):
        """Render all strike rows in the internal table.
        """
        log.debug("Rendering rows")
        table = self.widgets['table']
        for record, display in zip(
            sorted(records, key=lambda q: q['strike']),
            displayables
        ):
            strike = record['strike']
            strike_row = _strike_row_cache.setdefault(
                strike, StrikeRow(strike))
            strike_row.append_sub_row(
                record,
                display,
                bidasks=self.bidasks,
                table=table,
            )
            if strike_row.is_populated():
                # We must fill out the the table's symbol2rows manually
                # using each contracts "symbol" so that the quote updater
                # task can look up the right row to update easily
                # See update_quotes() and ``Row`` internals for details.
                for contract_type, row in strike_row._sub_rows.items():
                    symbol = row._last_record['symbol']
                    table.symbols2rows[symbol] = row

                if strike not in self._strikes2rows:
                    # re-adding widgets is an error
                    self._strikes2rows[strike] = strike_row

        log.debug("Finished rendering rows!")

    async def start_feed(
        self,
        symbol: str,
        expiry: str,
        # max QT rate per API customer is approx 4 rps
        # and usually 3 rps is allocated to the stock monitor
        rate: int = 1,
        test: str = None
    ):
        if self.feed.sub != self.sub:
            return await self.feed.open_stream([(symbol, expiry)], rate=rate)
        else:
            feed = self.feed
            return feed.quote_gen, feed.first_quotes

    async def start_updating(self):
        if self._update_cs:
            log.warn("Cancelling existing update task")
            self._update_cs.cancel()
            await trio.sleep(0)

        # drop all current rows
        self.clear()

        if self._nursery is None:
            raise RuntimeError(
                "You must call await `start()` first!")

        n = self._nursery
        log.debug(f"Waiting on first_quotes for {self.sub}")
        quote_gen, first_quotes = await self.start_feed(*self.sub)
        log.debug(f"Got first_quotes for {self.sub}")

        # redraw the UI
        records, displayables = zip(*[
            self.feed.brokermod.format_option_quote(quote, {})
            for quote in first_quotes.values()
        ])
        self.render_rows(records, displayables)

        with trio.open_cancel_scope() as cs:
            self._update_cs = cs
            await n.start(
                partial(
                    update_quotes,
                    n,
                    self.feed.brokermod.format_option_quote,
                    self.widgets,
                    quote_gen,
                    symbol_data={},
                    first_quotes=first_quotes,
                )
            )

    def start_displaying(self, symbol, expiry):
        self.sub = (symbol, expiry)
        self._nursery.start_soon(self.start_updating)


async def new_chain_ui(
    portal: tractor._portal.Portal,
    symbol: str,
    expiry: str,
    contracts,
    brokermod: types.ModuleType,
    nursery: trio._core._run.Nursery,
    rate: int = 1,
) -> None:
    """Create and return a new option chain UI.
    """
    widgets = {}
    # define bid-ask "stacked" cells
    # (TODO: needs some rethinking and renaming for sure)
    bidasks = brokermod._option_bidasks

    feed = DataFeed(portal, brokermod)
    chain = OptionChain(
        symbol,
        expiry,
        widgets,
        bidasks,
        feed,
        rate=rate,
    )

    quote_gen, first_quotes = await chain.start_feed(symbol, expiry)
    records, displayables = zip(*[
        brokermod.format_option_quote(quote, {})
        for quote in first_quotes.values()
    ])

    # build out root UI
    title = f"option chain: {symbol}\t(press ? for help)"
    Window.set_title(title)

    # use `monitor` styling for now
    from .monitor import _kv
    Builder.load_string(_kv)

    # the master container
    container = BoxLayout(orientation='vertical', spacing=0)

    # TODO: figure out how to compact these buttons
    expiries = {
        key.expiry: key.expiry[:key.expiry.find('T')]
        for key in contracts
    }
    expiry_buttons = Row(
        record=expiries,
        headers=expiries,
        is_header=True,
        size_hint=(1, None),
        cell_type=ExpiryButton,
    )
    # top row of expiry buttons
    container.add_widget(expiry_buttons)

    # figure out header fields for each table based on quote keys
    headers = displayables[0].keys()
    header_row = StrikeRow(strike='strike', size_hint=(1, None))
    header_record = {key: key for key in headers}
    header_record['contract_type'] = 'put'
    header_row.append_sub_row(
        header_record,
        header_record,
        headers=headers,
        bidasks=bidasks,
        is_header=True,
        size_hint=(1, None),
    )
    header_record['contract_type'] = 'call'
    header_row.append_sub_row(
        header_record,
        header_record,
        headers=headers,
        bidasks=bidasks,
        is_header=True,
        size_hint=(1, None),
    )
    container.add_widget(header_row)
    table = TickerTable(
        sort_key='strike',
        cols=1,
        size_hint=(1, None),
    )
    header_row.table = table
    table.bind(minimum_height=table.setter('height'))
    pager = PagerView(
        container=container,
        contained=table,
        nursery=nursery
    )
    container.add_widget(pager)
    widgets.update({
        'root': container,
        'container': container,
        'table': table,
        'expiry_buttons': expiry_buttons,
        'pager': pager,
    })
    return chain


async def _async_main(
    symbol: str,
    portal: tractor._portal.Portal,
    brokermod: types.ModuleType,
    rate: int = 1,
    test: bool = False
) -> None:
    '''Launch kivy app + all other related tasks.

    This is started with cli cmd `piker options`.
    '''
    # retreive all contracts just because we need a default when the
    # UI starts up
    all_contracts = await contracts(brokermod, symbol)
    # start streaming soonest contract by default
    first_expiry = next(iter(all_contracts)).expiry

    async with trio.open_nursery() as nursery:
        # set up a pager view for large ticker lists
        chain = await new_chain_ui(
            portal,
            symbol,
            first_expiry,
            all_contracts,
            brokermod,
            nursery,
            rate=rate,
        )
        async with chain.open_scope():
            try:
                # Trio-kivy entry point.
                await async_runTouchApp(chain.widgets['root'])  # run kivy
            finally:
                # cancel GUI update task
                nursery.cancel_scope.cancel()
