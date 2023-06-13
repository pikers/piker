# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

'''
Main app startup and run.

'''
from functools import partial
from types import ModuleType

from PyQt5.QtCore import QEvent
import trio

from ..service import maybe_spawn_brokerd
from . import _event
from ._exec import run_qtractor
from ..data.feed import install_brokerd_search
from ..accounting import unpack_fqme
from . import _search
from ._chart import GodWidget
from ..log import get_logger

log = get_logger(__name__)


async def load_provider_search(
    brokermod: str,
    loglevel: str,

) -> None:

    name = brokermod.name
    log.info(f'loading brokerd for {name}..')

    async with (

        maybe_spawn_brokerd(
            name,
            loglevel=loglevel
        ) as portal,

        install_brokerd_search(
            portal,
            brokermod,
        ),
    ):
        # keep search engine stream up until cancelled
        await trio.sleep_forever()


async def _async_main(

    # implicit required argument provided by ``qtractor_run()``
    main_widget: GodWidget,

    syms: list[str],
    brokers: dict[str, ModuleType],
    loglevel: str,

) -> None:
    """
    Main Qt-trio routine invoked by the Qt loop with the widgets ``dict``.

    Provision the "main" widget with initial symbol data and root nursery.

    """
    from . import _display
    from ._pg_overrides import _do_overrides
    _do_overrides()

    godwidget = main_widget

    # attempt to configure DPI aware font size
    screen = godwidget.window.current_screen()

    # configure graphics update throttling based on display refresh rate
    _display._quote_throttle_rate = min(
        round(screen.refreshRate()),
        _display._quote_throttle_rate,
    )
    log.info(f'Set graphics update rate to {_display._quote_throttle_rate} Hz')

    # TODO: do styling / themeing setup
    # _style.style_ze_sheets(godwidget)

    sbar = godwidget.window.status_bar
    starting_done = sbar.open_status('starting ze sexy chartz')

    needed_brokermods: dict[str, ModuleType] = {}
    for fqme in syms:
        brokername, *_ = unpack_fqme(fqme)
        needed_brokermods[brokername] = brokers[brokername]

    async with (
        trio.open_nursery() as root_n,
    ):
        # set root nursery and task stack for spawning other charts/feeds
        # that run cached in the bg
        godwidget._root_n = root_n

        # setup search widget and focus main chart view at startup
        # search widget is a singleton alongside the godwidget
        search = _search.SearchWidget(godwidget=godwidget)
        # search.bar.unfocus()
        # godwidget.hbox.addWidget(search)
        godwidget.search = search

        # this internally starts a ``display_symbol_data()`` task above
        order_mode_ready = await godwidget.load_symbols(
            fqmes=syms,
            loglevel=loglevel,
        )

        # spin up a search engine for the local cached symbol set
        async with _search.register_symbol_search(

            provider_name='cache',
            search_routine=partial(
                _search.search_simple_dict,
                source=godwidget._chart_cache,
            ),
            # cache is super fast so debounce on super short period
            pause_period=0.01,

        ):
            # load other providers into search **after**
            # the chart's select cache
            for brokername, mod in needed_brokermods.items():
                root_n.start_soon(
                    load_provider_search,
                    mod,
                    loglevel,
                )

            await order_mode_ready.wait()

            # start handling peripherals input for top level widgets
            async with (

                # search bar kb input handling
                _event.open_handlers(
                    [search.bar],
                    event_types={
                        QEvent.KeyPress,
                    },
                    async_handler=_search.handle_keyboard_input,
                    filter_auto_repeats=False,  # let repeats passthrough
                ),

                # completer view mouse click signal handling
                _event.open_signal_handler(
                    search.view.pressed,
                    search.view.on_pressed,
                ),
            ):
                # remove startup status text
                starting_done()
                await trio.sleep_forever()


def _main(
    syms: list[str],
    brokermods: list[ModuleType],
    piker_loglevel: str,
    tractor_kwargs,
) -> None:
    '''
    Sync entry point to start a chart: a ``tractor`` + Qt runtime.

    '''
    run_qtractor(
        func=_async_main,
        args=(
            syms,
            brokermods,
            piker_loglevel,
        ),
        main_widget_type=GodWidget,
        tractor_kwargs=tractor_kwargs,
    )
