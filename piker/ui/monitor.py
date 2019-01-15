"""
monitor: a real-time, sorted watchlist.

Launch with ``piker monitor <watchlist name>``.

(Currently there's a bunch of questrade specific stuff in here)
"""
from types import ModuleType, AsyncGeneratorType
from typing import List, Callable

import trio
import tractor
from kivy.uix.boxlayout import BoxLayout
from kivy.lang import Builder
from kivy.app import async_runTouchApp
from kivy.core.window import Window

from ..brokers.data import DataFeed
from .tabular import (
    Row, TickerTable, _kv, _black_rgba, colorcode,
)
from ..log import get_logger
from .pager import PagerView


log = get_logger('monitor')


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
    feed = DataFeed(portal, brokermod)

    quote_gen, quotes = await feed.open_stream(
        tickers, 'stock', rate=rate)

    first_quotes, _ = feed.format_quotes(quotes)

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

    ss = tractor.current_actor().statespace
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
        ss['widgets'] = widgets
        nursery.start_soon(
            update_quotes,
            nursery,
            brokermod.format_stock_quote,
            widgets,
            quote_gen,
            feed._symbol_data_cache,
            quotes
        )
        try:
            # Trio-kivy entry point.
            await async_runTouchApp(widgets['root'])  # run kivy
        finally:
            # cancel remote data feed task
            await quote_gen.aclose()
            # cancel GUI update task
            nursery.cancel_scope.cancel()
