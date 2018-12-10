"""
options: a real-time option chain.

Launch with ``piker options <symbol>``.
"""
import types
from functools import partial

import trio
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


class ExpiryButton(HeaderCell):
    def on_press(self, value=None):
        log.info(f"Clicked {self}")


class StrikeCell(Cell):
    """Strike cell"""


_no_display = ['symbol', 'contract_type', 'strike', 'time', 'open']


class StrikeRow(BoxLayout):
    """A 'row' composed of two ``Row``s sandwiching a
    ``StrikeCell`.
    """
    def __init__(self, strike, **kwargs):
        super().__init__(orientation='horizontal', **kwargs)
        self.strike = strike
        # store 2 rows: 1 for call, 1 for put
        self._sub_rows = {}
        self.table = None

    def append_sub_row(
        self,
        record: dict,
        bidasks=None,
        headers=(),
        table=None,
        **kwargs,
    ) -> None:
        if self.is_populated():
            raise TypeError(f"{self} can only append two sub-rows?")

        # the 'contract_type' determines whether this
        # is a put or call row
        contract_type = record['contract_type']

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
        # reassign widget for when rendered in the update loop
        row.widget = self
        self._sub_rows[contract_type] = row
        if self.is_populated():
            # calls on the left
            self.add_widget(self._sub_rows['call'])
            # strikes in the middle
            self.add_widget(
                StrikeCell(
                    key=self.strike,
                    text=str(self.strike),
                    is_header=True,
                    # make centre strike cell nice and small
                    size_hint=(1/8., 1),
                )
            )
            # puts on the right
            self.add_widget(self._sub_rows['put'])

    def is_populated(self):
        """Bool determing if both a put and call subrow have beed appended.
        """
        return len(self._sub_rows) == 2

    def update(self, record, displayable):
        self._sub_rows[record['contract_type']].update(
            record, displayable)


async def _async_main(
    symbol: str,
    portal: tractor._portal.Portal,
    brokermod: types.ModuleType,
    rate: int = 4,
    test: bool = False
) -> None:
    '''Launch kivy app + all other related tasks.

    This is started with cli cmd `piker options`.
    '''
    # retreive all contracts
    all_contracts = await contracts(brokermod, symbol)
    first_expiry = next(iter(all_contracts)).expiry

    if test:
        # stream from a local test file
        quote_gen = await portal.run(
            "piker.brokers.data", 'stream_from_file',
            filename=test
        )
    else:
        # start live streaming from broker daemon
        quote_gen = await portal.run(
            "piker.brokers.data",
            'start_quote_stream',
            broker=brokermod.name,
            symbols=[(symbol, first_expiry)],
            feed_type='option',
        )

    # get first quotes response
    log.debug("Waiting on first quote...")
    quotes = await quote_gen.__anext__()
    records, displayables = zip(*[
        brokermod.format_option_quote(quote, {})
        for quote in quotes.values()
    ])

    # define bid-ask "stacked" cells
    # (TODO: needs some rethinking and renaming for sure)
    bidasks = brokermod._option_bidasks

    # build out UI
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
        for key in all_contracts
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
        headers=headers,
        bidasks=bidasks,
        is_header=True,
        size_hint=(1, None),

    )
    header_record['contract_type'] = 'call'
    header_row.append_sub_row(
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

    strike_rows = {}
    for record, display in zip(sorted(
        records,
        key=lambda q: q['strike'],
    ), displayables):
        strike = record['strike']
        strike_row = strike_rows.setdefault(
            strike, StrikeRow(strike))
        strike_row.append_sub_row(
            record,
            bidasks=bidasks,
            table=table,
        )
        if strike_row.is_populated():
            # We must fill out the the table's symbol2rows manually
            # using each contracts "symbol" so that the quote updater
            # task can look up the right row to update easily
            # See update_quotes() and ``Row`` for details.
            for contract_type, row in strike_row._sub_rows.items():
                table.symbols2rows[row._last_record['symbol']] = row

            table.append_row(symbol, strike_row)

    async with trio.open_nursery() as nursery:
        # set up a pager view for large ticker lists
        table.bind(minimum_height=table.setter('height'))
        pager = PagerView(
            container=container,
            contained=table,
            nursery=nursery
        )
        container.add_widget(pager)
        widgets = {
            'root': container,
            'container': container,
            'table': table,
            'expiry_buttons': expiry_buttons,
            'pager': pager,
        }
        nursery.start_soon(
            partial(
                update_quotes,
                nursery,
                brokermod.format_option_quote,
                widgets,
                quote_gen,
                symbol_data={},
                first_quotes=quotes,
            )
        )
        try:
            # Trio-kivy entry point.
            await async_runTouchApp(widgets['root'])  # run kivy
        finally:
            await quote_gen.aclose()  # cancel aysnc gen call
            # un-subscribe from symbols stream (cancel if brokerd
            # was already torn down - say by SIGINT)
            with trio.move_on_after(0.2):
                await portal.run(
                    "piker.brokers.data", 'modify_quote_stream',
                    broker=brokermod.name,
                    feed_type='option',
                    symbols=[]
                )

            # cancel GUI update task
            nursery.cancel_scope.cancel()
