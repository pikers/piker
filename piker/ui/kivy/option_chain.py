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
from kivy.uix.label import Label

from ...log import get_logger, get_console_log
from ...brokers.data import DataFeed
from ...brokers import get_brokermod
from .pager import PagerView
from .tabular import Row, HeaderCell, Cell, TickerTable
from .monitor import update_quotes


log = get_logger('option_chain')


async def modify_symbol(symbol):
    pass


class StrikeCell(HeaderCell):
    """Strike cell"""


_no_display = ['symbol', 'contract_type', 'strike', 'time', 'open']
_strike_row_cache = {}
_strike_cell_cache = {}
_no_contracts_msg = "No contracts available for symbol"


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

    def rowsitems(self):
        return self._sub_rows.items()


class ExpiryButton(Cell):
    # must be set to allow 'plain bg colors' since default texture is grey
    background_normal = ''

    def on_press(self, value=None):
        last = self.chain._last_expiry
        if last:
            last.click_toggle = False
        self.chain._last_expiry = self

        log.info(f"Clicked {self}")
        self.click_toggle = True
        self.chain.start_displaying(self.chain.symbol, self.key)


@asynccontextmanager
async def find_local_monitor():
    """Establish a portal to a local monitor for triggering
    symbol changes.
    """
    async with tractor.find_actor('monitor') as portal:
        if not portal:
            log.warn(
                "No monitor app could be found, no symbol link established..")
        else:
            log.info(f"Found {portal.channel.uid}")
        yield portal


class OptionChain(object):
    """A real-time options chain UI.
    """
    _title = "option chain: {symbol}\t(press ? for help)"

    def __init__(
        self,
        widgets: dict,
        bidasks: Dict[str, List[str]],
        feed: DataFeed,
        rate: int,
    ):
        self.symbol = None
        self.expiry = None
        self.rate = rate
        self.widgets = widgets
        self.bidasks = bidasks
        self._strikes2rows = {}
        self._nursery = None
        self._update_nursery = None
        self.feed = feed
        self._quote_gen = None
        # TODO: this should be moved down to the data feed layer
        # right now it's only needed for the UI update loop to cancel itself
        self._update_cs = None
        self._first_quotes = None
        self._last_expiry = None
        # flag to determine if one-time widgets have been generated
        self._static_widgets_initialized = False
        self._no_opts_label = None

    @property
    def no_opts_label(self):
        if self._no_opts_label is None:
            label = self._no_opts_label = Label(text=_no_contracts_msg)
            label.font_size = 30
        return self._no_opts_label

    async def _rx_symbols(self):
        async with find_local_monitor() as portal:
            if not portal:
                log.warn("No local monitor could be found")
                return
            async for symbol in await portal.run(
                'piker.ui.monitor',
                'stream_symbol_selection',
            ):
                log.info(f"Changing symbol subscriptions to {symbol}")
                self.start_displaying(symbol, self.expiry)

    @asynccontextmanager
    async def open_rt_display(self, nursery, symbol, expiry=None):
        """Open an internal update task scope required to allow
        for dynamic real-time operation.
        """
        n = self._nursery = nursery
        # fill out and start updating strike table
        n.start_soon(
            partial(self._start_displaying, symbol, expiry=expiry)
        )
        # listen for undlerlying symbol changes from a local monitor app
        n.start_soon(self._rx_symbols)
        yield self

        self._nursery = None
        # make sure we always tear down our existing data feed
        await self.feed.quote_gen.aclose()

    def clear_strikes(self):
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
                for contract_type, row in strike_row.rowsitems():
                    symbol = row._last_record['symbol']
                    table.symbols2rows[symbol] = row

                if strike not in self._strikes2rows:
                    # re-adding widgets is an error
                    self._strikes2rows[strike] = strike_row

        log.debug("Finished rendering rows!")

    def _init_static_widgets(self, displayables):
        assert self._static_widgets_initialized is False
        container = self.widgets['container']

        # calls / puts header
        type_header = BoxLayout(
            orientation='horizontal',
            size_hint=(1, 1/30.),
        )
        calls = Label(text='calls', font_size='20')
        puts = Label(text='puts', font_size='20')
        type_header.add_widget(calls)
        type_header.add_widget(puts)
        container.add_widget(type_header)

        # figure out header fields for each table based on quote keys
        headers = displayables[0].keys()
        header_row = StrikeRow(strike='strike', size_hint=(1, None))
        header_record = {key: key for key in headers}
        header_record['contract_type'] = 'put'
        header_row.append_sub_row(
            header_record,
            header_record,
            headers=headers,
            bidasks=self.bidasks,
            is_header=True,
            size_hint=(1, None),
        )
        header_record['contract_type'] = 'call'
        header_row.append_sub_row(
            header_record,
            header_record,
            headers=headers,
            bidasks=self.bidasks,
            is_header=True,
            size_hint=(1, None),
        )
        container.add_widget(header_row)

        # build out chain tables
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
            nursery=self._nursery
        )
        container.add_widget(pager)

        self.widgets.update({
            'table': table,
            'type_header': type_header,
            'table': table,
            'pager': pager,
        })

    async def _start_displaying(self, symbol, expiry=None):
        """Main routine to start displaying the real time updated strike
        table.

        Clear any existing data feed subscription that is no longer needed
        (eg. when clicking a new expiry button) spin up a new subscription,
        populate the table and start updating it.
        """
        table = self.widgets.get('table')
        if table:
            self.clear_strikes()

        if self._update_cs:
            log.warn("Cancelling existing update task")
            self._update_cs.cancel()
            await trio.sleep(0)

        # redraw any symbol specific UI components
        if self.symbol != symbol or expiry is None:
            # set window title
            self.widgets['window'].set_title(
                self._title.format(symbol=symbol)
            )

            # retreive all contracts to populate expiry row
            all_contracts = await self.feed.call_client(
                'get_all_contracts', symbols=[symbol])

            if not all_contracts:
                label = self.no_opts_label
                label.symbol = symbol
                if table:
                    table.add_widget(label)
                # always keep track of current subscription
                self.symbol, self.expiry = symbol, expiry
                return

            # XXX: Unfortunately we can't serialize named tuples over
            # msgpack... The expiry index is 2, see the ``ContractsKey`` named
            # tuple in the questrade broker mod. It would normally look
            # something like:
            # exp = next(iter(all_contracts)).expiry if not exp else exp
            ei = 2
            # start streaming soonest contract by default if not provided
            expiry = next(iter(all_contracts))[ei] if not expiry else expiry

            # TODO: figure out how to compact these buttons
            expiries = {
                key[ei]: key[ei][:key[ei].find('T')]
                for key in all_contracts
            }
            expiry_row = self.widgets['expiry_row']
            expiry_row.clear_widgets()

            for expiry, justdate in expiries.items():
                button = ExpiryButton(text=str(justdate), key=expiry)
                # assign us to each expiry button
                button.chain = self
                expiry_row.add_widget(button)

        if self._nursery is None:
            raise RuntimeError(
                "You must call open this chain's update scope first!")

        log.debug(f"Waiting on first_quotes for {symbol}:{expiry}")
        self._quote_gen, first_quotes = await self.feed.open_stream(
            [(symbol, expiry)],
            'option',
            rate=self.rate,
        )
        log.debug(f"Got first_quotes for {symbol}:{expiry}")
        records, displayables = self.feed.format_quotes(first_quotes)

        # draw static widgets only once
        if self._static_widgets_initialized is False:
            self._init_static_widgets(displayables)
            self._static_widgets_initialized = True

        self.render_rows(records, displayables)

        with trio.CancelScope() as cs:
            self._update_cs = cs
            await self._nursery.start(
                partial(
                    update_quotes,
                    self._nursery,
                    self.feed.brokermod.format_option_quote,
                    self.widgets,
                    self._quote_gen,
                    symbol_data={},
                    first_quotes=first_quotes,
                )
            )
        # always keep track of current subscription
        self.symbol, self.expiry = symbol, expiry

    def start_displaying(self, symbol, expiry):
        if self.symbol == symbol and self.expiry == expiry:
            log.info(f"Clicked {symbol}:{expiry} is already selected")
            return

        log.info(f"Subscribing for {symbol}:{expiry}")
        self._nursery.start_soon(
            partial(self._start_displaying, symbol, expiry=expiry)
        )


async def new_chain_ui(
    portal: tractor._portal.Portal,
    symbol: str,
    brokermod: types.ModuleType,
    rate: int = 1,
) -> None:
    """Create and return a new option chain UI.
    """
    # use `monitor` styling for now
    from .monitor import _kv
    Builder.load_string(_kv)

    # the master container
    container = BoxLayout(orientation='vertical', spacing=0)

    # expiry buttons row (populated later once contracts are retreived)
    expiry_row = BoxLayout(
        orientation='horizontal',
        size_hint=(1, None),
    )
    container.add_widget(expiry_row)

    widgets = {
        'window': Window,
        'root': container,
        'container': container,
        'expiry_row': expiry_row,
    }
    # define bid-ask "stacked" cells
    # (TODO: needs some rethinking and renaming for sure)
    bidasks = brokermod._option_bidasks

    feed = DataFeed(portal, brokermod)
    chain = OptionChain(
        widgets,
        bidasks,
        feed,
        rate=rate,
    )
    return chain


async def _async_main(
    symbol: str,
    brokername: str,
    rate: int = 1,
    loglevel: str = 'info',
    test: bool = False
) -> None:
    '''Launch kivy app + all other related tasks.

    This is started with cli cmd `piker options`.
    '''
    if loglevel is not None:
        get_console_log(loglevel)

    brokermod = get_brokermod(brokername)

    async with trio.open_nursery() as nursery:
        # get a portal to the data feed daemon
        async with tractor.wait_for_actor('brokerd') as portal:

            # set up a pager view for large ticker lists
            chain = await new_chain_ui(
                portal,
                symbol,
                brokermod,
                rate=rate,
            )
            async with chain.open_rt_display(nursery, symbol):
                try:
                    await async_runTouchApp(chain.widgets['root'])
                finally:
                    if chain._quote_gen:
                        await chain._quote_gen.aclose()
                    # cancel GUI update task
                    nursery.cancel_scope.cancel()
