# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for piker0)

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

"""
qompleterz: embeddable search and complete using trio, Qt and fuzzywuzzy.

"""

# link set for hackzin on this stuff:
# https://doc.qt.io/qt-5/qheaderview.html#moving-header-sections
# https://doc.qt.io/qt-5/model-view-programming.html
# https://doc.qt.io/qt-5/modelview.html
# https://doc.qt.io/qt-5/qtreeview.html#selectedIndexes
# https://doc.qt.io/qt-5/qmodelindex.html#siblingAtColumn
# https://doc.qt.io/qt-5/qitemselectionmodel.html#currentIndex
# https://www.programcreek.com/python/example/108109/PyQt5.QtWidgets.QTreeView
# https://doc.qt.io/qt-5/qsyntaxhighlighter.html
# https://github.com/qutebrowser/qutebrowser/blob/master/qutebrowser/completion/completiondelegate.py#L243
# https://forum.qt.io/topic/61343/highlight-matched-substrings-in-qstyleditemdelegate

from collections import defaultdict
from contextlib import asynccontextmanager
from functools import partial
from typing import (
    List, Optional, Callable,
    Awaitable, Sequence, Dict,
    Any, AsyncIterator, Tuple,
)
import time
# from pprint import pformat

from fuzzywuzzy import process as fuzzy
import trio
from trio_typing import TaskStatus
from PyQt5 import QtCore, QtGui
from PyQt5 import QtWidgets
from PyQt5.QtCore import (
    Qt,
    # QSize,
    QModelIndex,
    QItemSelectionModel,
)
from PyQt5.QtGui import (
    # QLayout,
    QStandardItem,
    QStandardItemModel,
)
from PyQt5.QtWidgets import (
    QWidget,
    QTreeView,
    # QListWidgetItem,
    # QAbstractScrollArea,
    QStyledItemDelegate,
)


from ..log import get_logger
from ._style import (
    _font,
    DpiAwareFont,
    # hcolor,
)


log = get_logger(__name__)


class SimpleDelegate(QStyledItemDelegate):
    """
    Super simple view delegate to render text in the same
    font size as the search widget.

    """

    def __init__(
        self,
        parent=None,
        font: DpiAwareFont = _font,
    ) -> None:
        super().__init__(parent)
        self.dpi_font = font


class CompleterView(QTreeView):

    mode_name: str = 'mode: search-nav'

    # XXX: relevant docs links:
    # - simple widget version of this:
    #   https://doc.qt.io/qt-5/qtreewidget.html#details
    # - MVC high level instructional:
    #   https://doc.qt.io/qt-5/model-view-programming.html
    # - MV tut:
    #   https://doc.qt.io/qt-5/modelview.html
    # - custome header view (for doing stuff like we have in  kivy?):
    #   https://doc.qt.io/qt-5/qheaderview.html#moving-header-sections

    # TODO: selection model stuff for eventual aggregate feeds
    # charting and mgmt;
    # https://doc.qt.io/qt-5/qabstractitemview.html#setSelectionModel
    # https://doc.qt.io/qt-5/qitemselectionmodel.html
    # https://doc.qt.io/qt-5/modelview.html#3-2-working-with-selections
    # https://doc.qt.io/qt-5/model-view-programming.html#handling-selections-of-items

    # TODO: mouse extended handling:
    # https://doc.qt.io/qt-5/qabstractitemview.html#entered

    def __init__(
        self,
        parent=None,
        labels: List[str] = [],
    ) -> None:

        super().__init__(parent)

        model = QStandardItemModel(self)
        self.labels = labels

        # a std "tabular" config
        self.setItemDelegate(SimpleDelegate())
        self.setModel(model)
        self.setAlternatingRowColors(True)
        # TODO: size this based on DPI font
        self.setIndentation(20)

        self.pressed.connect(self.on_pressed)

        # self.setUniformRowHeights(True)
        # self.setColumnWidth(0, 3)
        # self.setVerticalBarPolicy(Qt.ScrollBarAlwaysOff)
        # self.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)

        # ux settings
        self.setItemsExpandable(True)
        self.setExpandsOnDoubleClick(False)
        self.setAnimated(False)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # column headers
        model.setHorizontalHeaderLabels(labels)

        self._font_size: int = 0  # pixels

    def on_pressed(self, idx: QModelIndex) -> None:
        '''Mouse pressed on view handler.

        '''
        search = self.parent()
        search.chart_current_item(clear_to_cache=False)
        search.focus()

    def set_font_size(self, size: int = 18):
        # print(size)
        if size < 0:
            size = 16

        self._font_size = size

        self.setStyleSheet(f"font: {size}px")

    def resize(self):
        model = self.model()
        cols = model.columnCount()

        for i in range(cols):
            self.resizeColumnToContents(i)

        # inclusive of search bar and header "rows" in pixel terms
        rows = 100
        # max_rows = 8  # 6 + search and headers
        row_px = self.rowHeight(self.currentIndex())
        # print(f'font_h: {font_h}\n px_height: {px_height}')

        # TODO: probably make this more general / less hacky
        self.setMinimumSize(self.width(), rows * row_px)
        self.setMaximumSize(self.width() + 10, rows * row_px)
        self.setFixedWidth(333)

    def is_selecting_d1(self) -> bool:
        cidx = self.selectionModel().currentIndex()
        return cidx.parent() == QModelIndex()

    def previous_index(self) -> QModelIndex:

        cidx = self.selectionModel().currentIndex()
        one_above = self.indexAbove(cidx)

        if one_above.parent() == QModelIndex():
            # if the next node up's parent is the root we don't want to select
            # the next node up since it's a top level node and we only
            # select entries depth >= 2.

            # see if one more up is not the root and we can select it.
            two_above = self.indexAbove(one_above)
            if two_above != QModelIndex():
                return two_above
            else:
                return cidx

        return one_above  # just next up

    def next_index(self) -> QModelIndex:
        cidx = self.selectionModel().currentIndex()
        one_below = self.indexBelow(cidx)

        if one_below.parent() == QModelIndex():
            # if the next node up's parent is the root we don't want to select
            # the next node up since it's a top level node and we only
            # select entries depth >= 2.

            # see if one more up is not the root and we can select it.
            two_below = self.indexBelow(one_below)
            if two_below != QModelIndex():
                return two_below
            else:
                return cidx

        return one_below  # just next down

    def select_from_idx(

        self,
        idx: QModelIndex,

    ) -> QStandardItem:
        '''Select and return the item at index ``idx``.

        '''
        sel = self.selectionModel()
        model = self.model()

        sel.setCurrentIndex(
            idx,
            QItemSelectionModel.ClearAndSelect |
            QItemSelectionModel.Rows
        )

        return model.itemFromIndex(idx)

    def select_first(self) -> QStandardItem:
        '''Select the first depth >= 2 entry from the completer tree and
        return it's item.

        '''
        # ensure we're **not** selecting the first level parent node and
        # instead its child.
        model = self.model()
        for idx, item in self.iter_d1():
            if model.rowCount(idx) == 0:
                continue
            else:
                return self.select_from_idx(self.indexBelow(idx))

    def select_next(self) -> QStandardItem:
        idx = self.next_index()
        assert idx.isValid()
        return self.select_from_idx(idx)

    def select_previous(self) -> QStandardItem:
        idx = self.previous_index()
        assert idx.isValid()
        return self.select_from_idx(idx)

    def next_section(self, direction: str = 'down') -> QModelIndex:
        cidx = start_idx = self.selectionModel().currentIndex()

        # step up levels to depth == 1
        while cidx.parent() != QModelIndex():
            cidx = cidx.parent()

        # move to next section in `direction`
        op = {'up': -1, 'down': +1}[direction]
        next_row = cidx.row() + op
        nidx = self.model().index(next_row, cidx.column(), QModelIndex())

        # do nothing, if there is no valid "next" section
        if not nidx.isValid():
            return self.select_from_idx(start_idx)

        # go to next selectable child item
        self.select_from_idx(nidx)
        return self.select_next()

    def iter_d1(
        self,
    ) -> tuple[QModelIndex, QStandardItem]:

        model = self.model()
        isections = model.rowCount()

        # much thanks to following code to figure out breadth-first
        # traversing from the root node:
        # https://stackoverflow.com/a/33126689
        for i in range(isections):
            idx = model.index(i, 0, QModelIndex())
            item = model.itemFromIndex(idx)
            yield idx, item

    def find_section(
        self,
        section: str,

    ) -> Optional[QModelIndex]:
        '''Find the *first* depth = 1 section matching ``section`` in
        the tree and return its index.

        '''
        for idx, item in self.iter_d1():
            if item.text() == section:
                return idx
        else:
            # caller must expect his
            return None

    def clear_section(
        self,
        section: str,
        status_field: str = None,

    ) -> None:
        '''Clear all result-rows from under the depth = 1 section.

        '''
        idx = self.find_section(section)
        model = self.model()

        if idx is not None:
            if model.hasChildren(idx):
                rows = model.rowCount(idx)
                # print(f'removing {rows} from {section}')
                assert model.removeRows(0, rows, parent=idx)

            # remove section as well ?
            # model.removeRow(i, QModelIndex())

            if status_field is not None:
                model.setItem(idx.row(), 1, QStandardItem(status_field))

            else:
                model.setItem(idx.row(), 1, QStandardItem())

            self.resize()

            return idx
        else:
            return None

    def set_section_entries(
        self,
        section: str,
        values: Sequence[str],
        clear_all: bool = False,

    ) -> None:
        '''Set result-rows for depth = 1 tree section ``section``.

        '''
        model = self.model()
        if clear_all:
            # XXX: rewrite the model from scratch if caller requests it
            model.clear()

        model.setHorizontalHeaderLabels(self.labels)

        section_idx = self.clear_section(section)

        # if we can't find a section start adding to the root
        if section_idx is None:
            root = model.invisibleRootItem()
            section_item = QStandardItem(section)
            blank = QStandardItem('')
            root.appendRow([section_item, blank])

        else:
            section_item = model.itemFromIndex(section_idx)

        # values just needs to be sequence-like
        for i, s in enumerate(values):

            ix = QStandardItem(str(i))
            item = QStandardItem(s)

            # Add the item to the model
            section_item.appendRow([ix, item])

        self.expandAll()

        # TODO: figure out if we can avoid this line in a better way
        # such that "re-selection" doesn't happen tree-wise for each new
        # sub-search:
        # https://doc.qt.io/qt-5/model-view-programming.html#handling-selections-in-item-views

        # XXX: THE BELOW LINE MUST BE CALLED.
        # this stuff is super finicky and if not done right will cause
        # Qt crashes out our buttz. it's required in order to get the
        # view to show right after typing input.

        self.select_first()

        # TODO: the way we might be able to do this more sanely is,
        # 1. for the currently selected item, when start rewriting
        #    a section figure out the row, column, parent "abstract"
        #    position in the tree view and store it
        # 2. take that position and re-apply the selection to the new
        #    model/tree by looking up the new "equivalent element" and
        #    selecting

        self.show_matches()

    def show_matches(self) -> None:
        self.show()
        self.resize()


class SearchBar(QtWidgets.QLineEdit):

    mode_name: str = 'mode: search'

    def __init__(

        self,
        parent: QWidget,
        parent_chart: QWidget,  # noqa
        view: Optional[CompleterView] = None,
        font: DpiAwareFont = _font,

    ) -> None:

        super().__init__(parent)

        # self.setContextMenuPolicy(Qt.CustomContextMenu)
        # self.customContextMenuRequested.connect(self.show_menu)
        # self.setStyleSheet(f"font: 18px")

        self.view: CompleterView = view
        self.dpi_font = font
        self.godwidget = parent_chart

        # size it as we specify
        # https://doc.qt.io/qt-5/qsizepolicy.html#Policy-enum
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Expanding,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.setFont(font.font)

        # witty bit of margin
        self.setTextMargins(2, 2, 2, 2)

    def focus(self) -> None:
        self.selectAll()
        self.show()
        self.setFocus()

    def show(self) -> None:
        super().show()
        self.view.show_matches()

    def sizeHint(self) -> QtCore.QSize:
        """
        Scale edit box to size of dpi aware font.

        """
        psh = super().sizeHint()
        psh.setHeight(self.dpi_font.px_size + 2)
        return psh

    def unfocus(self) -> None:
        self.parent().hide()
        self.clearFocus()

        if self.view:
            self.view.hide()


class SearchWidget(QtGui.QWidget):
    '''Composed widget of ``SearchBar`` + ``CompleterView``.

    Includes helper methods for item management in the sub-widgets.

    '''
    mode_name: str = 'mode: search'

    def __init__(
        self,
        godwidget: 'GodWidget',  # type: ignore # noqa
        columns: List[str] = ['src', 'symbol'],
        parent=None,

    ) -> None:
        super().__init__(parent or godwidget)

        # size it as we specify
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )

        self.godwidget = godwidget

        self.vbox = QtGui.QVBoxLayout(self)
        self.vbox.setContentsMargins(0, 0, 0, 0)
        self.vbox.setSpacing(4)

        # split layout for the (label:| search bar entry)
        self.bar_hbox = QtGui.QHBoxLayout()
        self.bar_hbox.setContentsMargins(0, 0, 0, 0)
        self.bar_hbox.setSpacing(4)

        # add label to left of search bar
        self.label = label = QtGui.QLabel(parent=self)
        label.setTextFormat(3)  # markdown
        label.setFont(_font.font)
        label.setMargin(4)
        label.setText("`search`:")
        label.show()
        label.setAlignment(
            QtCore.Qt.AlignVCenter
            | QtCore.Qt.AlignLeft
        )

        self.bar_hbox.addWidget(label)

        self.view = CompleterView(
            parent=self,
            labels=columns,
        )
        self.bar = SearchBar(
            parent=self,
            parent_chart=godwidget,
            view=self.view,
        )
        self.bar_hbox.addWidget(self.bar)

        self.vbox.addLayout(self.bar_hbox)

        self.vbox.setAlignment(self.bar, Qt.AlignTop | Qt.AlignRight)
        self.vbox.addWidget(self.bar.view)
        self.vbox.setAlignment(self.view, Qt.AlignTop | Qt.AlignLeft)

    def focus(self) -> None:

        if self.view.model().rowCount(QModelIndex()) == 0:
            # fill cache list if nothing existing
            self.view.set_section_entries(
                'cache',
                list(reversed(self.godwidget._chart_cache)),
                clear_all=True,
            )

        self.bar.focus()
        self.show()

    def get_current_item(self) -> Optional[Tuple[str, str]]:
        '''Return the current completer tree selection as
        a tuple ``(parent: str, child: str)`` if valid, else ``None``.

        '''
        model = self.view.model()
        sel = self.view.selectionModel()
        cidx = sel.currentIndex()

        # TODO: get rid of this hard coded column -> 1
        # and use the ``CompleterView`` schema/settings
        # to figure out the desired field(s)
        # https://doc.qt.io/qt-5/qstandarditemmodel.html#itemFromIndex
        node = model.itemFromIndex(cidx.siblingAtColumn(1))

        if node:
            symbol = node.text()
            try:
                provider = node.parent().text()
            except AttributeError:
                # no text set
                return None

            # TODO: move this to somewhere non-search machinery specific?
            if provider == 'cache':
                symbol, _, provider = symbol.rpartition('.')

            return provider, symbol

        else:
            return None

    def chart_current_item(
        self,
        clear_to_cache: bool = True,
    ) -> Optional[str]:
        '''Attempt to load and switch the current selected
        completion result to the affiliated chart app.

        Return any loaded symbol

        '''
        value = self.get_current_item()
        if value is None:
            return None

        provider, symbol = value
        chart = self.godwidget

        log.info(f'Requesting symbol: {symbol}.{provider}')

        chart.load_symbol(
            provider,
            symbol,
            'info',
        )

        # fully qualified symbol name (SNS i guess is what we're
        # making?)
        fqsn = '.'.join([symbol, provider]).lower()

        if clear_to_cache:

            self.bar.clear()

            # Re-order the symbol cache on the chart to display in
            # LIFO order. this is normally only done internally by
            # the chart on new symbols being loaded into memory
            chart.set_chart_symbol(fqsn, chart.linkedsplits)

            self.view.set_section_entries(
                'cache',
                values=list(reversed(chart._chart_cache)),

                # remove all other completion results except for cache
                clear_all=True,
            )

        return fqsn


_search_active: trio.Event = trio.Event()
_search_enabled: bool = False


async def pack_matches(

    view: CompleterView,
    has_results: dict[str, set[str]],
    matches: dict[(str, str), [str]],
    provider: str,
    pattern: str,
    search: Callable[..., Awaitable[dict]],
    task_status: TaskStatus[
        trio.CancelScope] = trio.TASK_STATUS_IGNORED,

) -> None:

    log.info(f'Searching {provider} for "{pattern}"')

    if provider != 'cache':
        # insert provider entries with search status
        view.set_section_entries(
            section=provider,
            values=[],
        )
        view.clear_section(provider, status_field='-> searchin..')

    else:  # for the cache just clear it's entries and don't put a status
        view.clear_section(provider)

    with trio.CancelScope() as cs:
        task_status.started(cs)
        # ensure ^ status is updated
        results = await search(pattern)

    if provider != 'cache':  # XXX: don't cache the cache results xD
        matches[(provider, pattern)] = results

        # print(f'results from {provider}: {results}')
        has_results[pattern].add(provider)

    if results:
        # display completion results
        view.set_section_entries(
            section=provider,
            values=results,
        )
    else:
        view.clear_section(provider)


async def fill_results(

    search: SearchBar,
    recv_chan: trio.abc.ReceiveChannel,

    # kb debouncing pauses (bracket defaults)
    min_pause_time: float = 0.01,  # absolute min typing throttle

    # max pause required before slow relay
    max_pause_time: float = 6/16 + 0.001,

) -> None:
    """Task to search through providers and fill in possible
    completion results.

    """
    global _search_active, _search_enabled, _searcher_cache

    bar = search.bar
    view = bar.view
    view.select_from_idx(QModelIndex())

    last_text = bar.text()
    repeats = 0

    # cache of prior patterns to search results
    matches = defaultdict(list)
    has_results: defaultdict[str, set[str]] = defaultdict(set)

    while True:
        await _search_active.wait()
        period = None

        while True:

            last_text = bar.text()
            wait_start = time.time()

            with trio.move_on_after(max_pause_time):
                pattern = await recv_chan.receive()

            period = time.time() - wait_start
            print(f'{pattern} after {period}')

            # during fast multiple key inputs, wait until a pause
            # (in typing) to initiate search
            if period < min_pause_time:
                log.debug(f'Ignoring fast input for {pattern}')
                continue

            text = bar.text()
            # print(f'search: {text}')

            if not text or text.isspace():
                # print('idling')
                _search_active = trio.Event()
                break

            if text == last_text:
                repeats += 1

            if not _search_enabled:
                # print('search currently disabled')
                break

            already_has_results = has_results[text]
            log.debug(f'Search req for {text}')

            # issue multi-provider fan-out search request and place
            # "searching.." statuses on outstanding results providers
            async with trio.open_nursery() as n:

                for provider, (search, pause) in (
                    _searcher_cache.copy().items()
                ):
                    # XXX: only conduct search on this backend if it's
                    # registered for the corresponding pause period AND
                    # it hasn't already been searched with the current
                    # input pattern (in which case just look up the old
                    # results).
                    if (period >= pause) and (
                        provider not in already_has_results
                    ):

                        # TODO: it may make more sense TO NOT search the
                        # cache in a bg task since we know it's fully
                        # cpu-bound.
                        if provider != 'cache':
                            view.clear_section(
                                provider, status_field='-> searchin..')

                        await n.start(
                            pack_matches,
                            view,
                            has_results,
                            matches,
                            provider,
                            text,
                            search
                        )
                    else:  # already has results for this input text
                        results = matches[(provider, text)]

                        # TODO really for the cache we need an
                        # invalidation signal so that we only re-search
                        # the cache once it's been mutated by the chart
                        # switcher.. right now we're just always
                        # re-searching it's ``dict`` since it's easier
                        # but it also causes it to be slower then cached
                        # results from other providers on occasion.
                        if results and provider != 'cache':
                            view.set_section_entries(
                                section=provider,
                                values=results,
                            )
                        else:
                            view.clear_section(provider)

            if repeats > 2 and period > max_pause_time:
                _search_active = trio.Event()
                repeats = 0
                break

            bar.show()


async def handle_keyboard_input(

    searchbar: SearchBar,
    recv_chan: trio.abc.ReceiveChannel,

) -> None:

    global _search_active, _search_enabled

    # startup
    bar = searchbar
    search = searchbar.parent()
    chart = search.godwidget
    view = bar.view
    view.set_font_size(bar.dpi_font.px_size)

    send, recv = trio.open_memory_channel(16)

    async with trio.open_nursery() as n:

        # start a background multi-searcher task which receives
        # patterns relayed from this keyboard input handler and
        # async updates the completer view's results.
        n.start_soon(
            partial(
                fill_results,
                search,
                recv,
            )
        )

        async for event, etype, key, mods, txt in recv_chan:

            log.debug(f'key: {key}, mods: {mods}, txt: {txt}')

            ctl = False
            if mods == Qt.ControlModifier:
                ctl = True

            # # ctl + alt as combo
            # ctlalt = False
            # if (QtCore.Qt.AltModifier | QtCore.Qt.ControlModifier) == mods:
            #     ctlalt = True

            if key in (Qt.Key_Enter, Qt.Key_Return):

                search.chart_current_item(clear_to_cache=True)
                _search_enabled = False
                continue

            elif not ctl and not bar.text():
                # if nothing in search text show the cache
                view.set_section_entries(
                    'cache',
                    list(reversed(chart._chart_cache)),
                    clear_all=True,
                )
                continue

            # cancel and close
            if ctl and key in {
                Qt.Key_C,
                Qt.Key_Space,   # i feel like this is the "native" one
                Qt.Key_Alt,
            }:
                search.bar.unfocus()

                # kill the search and focus back on main chart
                if chart:
                    chart.linkedsplits.focus()

                continue

            if ctl and key in {
                Qt.Key_L,
            }:
                # like url (link) highlight in a web browser
                bar.focus()

            # selection navigation controls
            elif ctl and key in {
                Qt.Key_D,
            }:
                view.next_section(direction='down')
                _search_enabled = False

            elif ctl and key in {
                Qt.Key_U,
            }:
                view.next_section(direction='up')
                _search_enabled = False

            # selection navigation controls
            elif (ctl and key in {

                Qt.Key_K,
                Qt.Key_J,

            }) or key in {

                Qt.Key_Up,
                Qt.Key_Down,
            }:
                _search_enabled = False
                if key in {Qt.Key_K, Qt.Key_Up}:
                    item = view.select_previous()

                elif key in {Qt.Key_J, Qt.Key_Down}:
                    item = view.select_next()

                if item:
                    parent_item = item.parent()

                    if parent_item and parent_item.text() == 'cache':

                        # if it's a cache item, switch and show it immediately
                        search.chart_current_item(clear_to_cache=False)

            elif not ctl:
                # relay to completer task
                _search_enabled = True
                send.send_nowait(search.bar.text())
                _search_active.set()


async def search_simple_dict(
    text: str,
    source: dict,
) -> Dict[str, Any]:

    # search routine can be specified as a function such
    # as in the case of the current app's local symbol cache
    matches = fuzzy.extractBests(
        text,
        source.keys(),
        score_cutoff=90,
    )

    return [item[0] for item in matches]


# cache of provider names to async search routines
_searcher_cache: Dict[str, Callable[..., Awaitable]] = {}


@asynccontextmanager
async def register_symbol_search(

    provider_name: str,
    search_routine: Callable,
    pause_period: Optional[float] = None,

) -> AsyncIterator[dict]:

    global _searcher_cache

    pause_period = pause_period or 0.1

    # deliver search func to consumer
    try:
        _searcher_cache[provider_name] = (search_routine, pause_period)
        yield search_routine

    finally:
        _searcher_cache.pop(provider_name)
