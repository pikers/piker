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
    Optional,
    Callable,
    Awaitable,
    Sequence,
    Any,
    AsyncIterator,
    Iterator,
)
import time
# from pprint import pformat

from fuzzywuzzy import process as fuzzy
import trio
from trio_typing import TaskStatus
from PyQt5 import QtCore
from PyQt5 import QtWidgets
from PyQt5.QtCore import (
    Qt,
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
    # QStyledItemDelegate,
)


from ..log import get_logger
from ._style import (
    _font,
    hcolor,
)
from ._forms import Edit, FontScaledDelegate


log = get_logger(__name__)


class CompleterView(QTreeView):

    mode_name: str = 'search-nav'

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
        labels: list[str] = [],
    ) -> None:

        super().__init__(parent)

        model = QStandardItemModel(self)
        self.labels = labels

        # a std "tabular" config
        self.setItemDelegate(FontScaledDelegate(self))
        self.setModel(model)
        self.setAlternatingRowColors(True)
        # TODO: size this based on DPI font
        self.setIndentation(_font.px_size)

        self.setUniformRowHeights(True)
        # self.setColumnWidth(0, 3)
        # self.setVerticalBarPolicy(Qt.ScrollBarAlwaysOff)
        # self.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)

        # ux settings
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Expanding,
            QtWidgets.QSizePolicy.Expanding,
        )
        self.setItemsExpandable(True)
        self.setExpandsOnDoubleClick(False)
        self.setAnimated(False)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # column headers
        model.setHorizontalHeaderLabels(labels)

        self._font_size: int = 0  # pixels
        self._init: bool = False

    async def on_pressed(
        self,
        idx: QModelIndex,
    ) -> None:
        '''
        Mouse pressed on view handler.

        '''
        search = self.parent()

        await search.chart_current_item(
            clear_to_cache=True,
        )

        # XXX: this causes Qt to hang and segfault..lovely
        # self.show_cache_entries(
        #     only=True,
        #     keep_current_item_selected=True,
        # )

        search.focus()


    def set_font_size(self, size: int = 18):
        # print(size)
        if size < 0:
            size = 16

        self._font_size = size

        self.setStyleSheet(f"font: {size}px")

    def resize_to_results(
        self,
        w: Optional[float] = 0,
        h: Optional[float] = None,

    ) -> None:
        model = self.model()
        cols = model.columnCount()
        cidx = self.selectionModel().currentIndex()
        rows = model.rowCount()
        self.expandAll()

        # compute the approx height in pixels needed to include
        # all result rows in view.
        row_h = rows_h = self.rowHeight(cidx) * (rows + 1)
        for idx, item in self.iter_df_rows():
            row_h = self.rowHeight(idx)
            rows_h += row_h
            # print(f'row_h: {row_h}\nrows_h: {rows_h}')

            # TODO: could we just break early here on detection
            # of ``rows_h >= h``?

        col_w_tot = 0
        for i in range(cols):
            # only slap in a rows's height's worth
            # of padding once at startup.. no idea
            if (
                not self._init
                and row_h
            ):
                col_w_tot = row_h
                self._init = True

            self.resizeColumnToContents(i)
            col_w_tot += self.columnWidth(i)

        # NOTE: if the heigh `h` set here is **too large** then the
        # resize event will perpetually trigger as the window causes
        # some kind of recompute of callbacks.. so we have to ensure
        # it's limited.
        if h:
            h: int = round(h)
            abs_mx = round(0.91 * h)
            self.setMaximumHeight(abs_mx)

            if rows_h <= abs_mx:
                # self.setMinimumHeight(rows_h)
                self.setMinimumHeight(rows_h)
                # self.setFixedHeight(rows_h)

            else:
                self.setMinimumHeight(abs_mx)

        # dyncamically size to width of longest result seen
        curr_w = self.width()
        if curr_w < col_w_tot:
            self.setMinimumWidth(col_w_tot)

        self.update()

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
        '''
        Select and return the item at index ``idx``.

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
        '''
        Select the first depth >= 2 entry from the completer tree and
        return its item.

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

    def iter_df_rows(
        self,
        iparent: QModelIndex = QModelIndex(),

    ) -> Iterator[tuple[QModelIndex, QStandardItem]]:

        model = self.model()
        isections = model.rowCount(iparent)
        for i in range(isections):
            idx = model.index(i, 0, iparent)
            item = model.itemFromIndex(idx)
            yield idx, item

            if model.hasChildren(idx):
                # recursively yield child items depth-first
                yield from self.iter_df_rows(idx)

    def find_section(
        self,
        section: str,

    ) -> Optional[QModelIndex]:
        '''
        Find the *first* depth = 1 section matching ``section`` in
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
        '''
        Clear all result-rows from under the depth = 1 section.

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

            return idx
        else:
            return None

    def set_section_entries(
        self,
        section: str,
        values: Sequence[str],
        clear_all: bool = False,
        reverse: bool = False,

    ) -> None:
        '''
        Set result-rows for depth = 1 tree section ``section``.

        '''
        if (
            values
            and not isinstance(values[0], str)
        ):
            flattened: list[str] = []
            for val in values:
                flattened.extend(val)

            values = flattened

        if reverse:
            values = reversed(values)

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

    def show_matches(
        self,
        wh: Optional[tuple[float, float]] = None,

    ) -> None:

        if wh:
            self.resize_to_results(*wh)
        else:
            # case where it's just an update from results and *NOT*
            # a resize of some higher level parent-container widget.
            search = self.parent()
            w, h = search.space_dims()
            self.resize_to_results(w=w, h=h)

        self.show()


class SearchBar(Edit):

    mode_name: str = 'search'

    def __init__(

        self,
        parent: QWidget,
        godwidget: QWidget,
        view: Optional[CompleterView] = None,
        **kwargs,

    ) -> None:

        self.godwidget = godwidget
        super().__init__(parent, **kwargs)
        self.view: CompleterView = view

    def unfocus(self) -> None:
        self.parent().hide()
        self.clearFocus()

    def hide(self) -> None:
        if self.view:
            self.view.hide()
        super().hide()


class SearchWidget(QtWidgets.QWidget):
    '''
    Composed widget of ``SearchBar`` + ``CompleterView``.

    Includes helper methods for item management in the sub-widgets.

    '''
    mode_name: str = 'search'

    def __init__(
        self,
        godwidget: 'GodWidget',  # type: ignore # noqa
        columns: list[str] = ['src', 'symbol'],
        parent=None,

    ) -> None:
        super().__init__(parent)

        # size it as we specify
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )

        self.godwidget = godwidget
        godwidget.reg_for_resize(self)

        self.vbox = QtWidgets.QVBoxLayout(self)
        self.vbox.setContentsMargins(0, 4, 4, 0)
        self.vbox.setSpacing(4)

        # split layout for the (label:| search bar entry)
        self.bar_hbox = QtWidgets.QHBoxLayout()
        self.bar_hbox.setContentsMargins(0, 0, 0, 0)
        self.bar_hbox.setSpacing(4)

        # add label to left of search bar
        self.label = label = QtWidgets.QLabel(parent=self)
        label.setStyleSheet(
            f"""QLabel {{
                color : {hcolor('default_lightest')};
                font-size : {_font.px_size - 2}px;
            }}
            """
        )
        label.setTextFormat(3)  # markdown
        label.setFont(_font.font)
        label.setMargin(4)
        label.setText("search:")
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
            view=self.view,
            godwidget=godwidget,
        )
        self.bar_hbox.addWidget(self.bar)

        self.vbox.addLayout(self.bar_hbox)

        self.vbox.setAlignment(self.bar, Qt.AlignTop | Qt.AlignRight)
        self.vbox.addWidget(self.bar.view)
        self.vbox.setAlignment(self.view, Qt.AlignTop | Qt.AlignLeft)

    def focus(self) -> None:
        self.show()
        self.bar.focus()

    def show_cache_entries(
        self,
        only: bool = False,
        keep_current_item_selected: bool = False,

    ) -> None:
        '''
        Clear the search results view and show only cached (aka recently
        loaded with active data) feeds in the results section.

        '''
        godw = self.godwidget

        # first entry in the cache is the current symbol(s)
        fqsns = set()
        for multi_fqsns in list(godw._chart_cache):
            for fqsn in set(multi_fqsns):
                fqsns.add(fqsn)

        if keep_current_item_selected:
            sel = self.view.selectionModel()
            cidx = sel.currentIndex()

        self.view.set_section_entries(
            'cache',
            list(fqsns),
            # remove all other completion results except for cache
            clear_all=only,
            reverse=True,
        )

        if (
            keep_current_item_selected
            and cidx.isValid()
        ):
            # set current selection back to what it was before filling out
            # the view results.
            self.view.select_from_idx(cidx)
        else:
            self.view.select_first()

    def get_current_item(self) -> tuple[QModelIndex, str, str] | None:
        '''
        Return the current completer tree selection as
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

            return (
                cidx,
                provider,
                symbol,
            )

        else:
            return None

    async def chart_current_item(
        self,
        clear_to_cache: bool = True,

    ) -> Optional[str]:
        '''
        Attempt to load and switch the current selected
        completion result to the affiliated chart app.

        Return any loaded symbol.

        '''
        value = self.get_current_item()
        if value is None:
            return None

        cidx, provider, symbol = value
        godw = self.godwidget

        fqsn = f'{symbol}.{provider}'
        log.info(f'Requesting symbol: {fqsn}')

        # assert provider in symbol
        await godw.load_symbols(
            fqsns=[fqsn],
            loglevel='info',
        )

        # fully qualified symbol name (SNS i guess is what we're
        # making?)
        fqsn = '.'.join([symbol, provider]).lower()

        if clear_to_cache:

            self.bar.clear()

            # Re-order the symbol cache on the chart to display in
            # LIFO order. this is normally only done internally by
            # the chart on new symbols being loaded into memory
            godw.set_chart_symbols(
                (fqsn,), (
                    godw.hist_linked,
                    godw.rt_linked,
                )
            )
            self.show_cache_entries(
                only=True,
            )

        self.bar.focus()
        return fqsn

    def space_dims(self) -> tuple[float, float]:
        '''
        Compute and return the "available space dimentions" for this
        search widget in terms of px space for results by return the
        pair of width and height.

        '''
        # XXX: dun need dis rite?
        # win = self.window()
        # win_h = win.height()
        # sb_h = win.statusBar().height()
        godw = self.godwidget
        hl = godw.hist_linked
        edit_h = self.bar.height()
        h = hl.height() - edit_h
        w = hl.width()
        return w, h

    def on_resize(self) -> None:
        '''
        Resize relay event from god, resize all child widgets.

        Right now this is just view to contents and/or the fast chart
        height.

        '''
        w, h = self.space_dims()
        self.bar.view.show_matches(wh=(w, h))


_search_active: trio.Event = trio.Event()
_search_enabled: bool = False


async def pack_matches(

    view: CompleterView,
    has_results: dict[str, set[str]],
    matches: dict[(str, str), list[str]],
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
        results = list(await search(pattern))

    # XXX: don't cache the cache results xD
    if provider != 'cache':
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
    '''
    Task to search through providers and fill in possible
    completion results.

    '''
    global _search_active, _search_enabled, _searcher_cache

    bar = search.bar
    view = bar.view
    view.select_from_idx(QModelIndex())

    last_text = bar.text()
    repeats = 0

    # cache of prior patterns to search results
    matches = defaultdict(list)
    has_results: defaultdict[str, set[str]] = defaultdict(set)

    # show cached feed list at startup
    search.show_cache_entries()
    search.on_resize()

    while True:
        await _search_active.wait()
        period = None

        while True:

            last_text = bar.text()
            wait_start = time.time()

            with trio.move_on_after(max_pause_time):
                pattern = await recv_chan.receive()

            period = time.time() - wait_start
            log.debug(f'{pattern} after {period}')

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
                    if (
                        period >= pause
                        and provider not in already_has_results
                    ):

                        # TODO: it may make more sense TO NOT search the
                        # cache in a bg task since we know it's fully
                        # cpu-bound.
                        if provider != 'cache':
                            view.clear_section(
                                provider,
                                status_field='-> searchin..',
                            )

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
                        if (
                            results
                        ):
                            if provider != 'cache':
                                view.set_section_entries(
                                    section=provider,
                                    values=results,
                                )
                            else:
                                # if provider == 'cache':
                                # for the cache just show what we got
                                # that matches
                                search.show_cache_entries()

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
    searchw = searchbar.parent()
    godwidget = searchw.godwidget
    view = searchbar.view
    view.set_font_size(searchbar.dpi_font.px_size)
    send, recv = trio.open_memory_channel(616)

    async with trio.open_nursery() as n:

        # start a background multi-searcher task which receives
        # patterns relayed from this keyboard input handler and
        # async updates the completer view's results.
        n.start_soon(
            partial(
                fill_results,
                searchw,
                recv,
            )
        )

        searchbar.focus()
        searchw.show_cache_entries()
        await trio.sleep(0)

        async for kbmsg in recv_chan:
            event, etype, key, mods, txt = kbmsg.to_tuple()

            log.debug(f'key: {key}, mods: {mods}, txt: {txt}')

            ctl = False
            if mods == Qt.ControlModifier:
                ctl = True

            if key in (
                Qt.Key_Enter,
                Qt.Key_Return
            ):
                _search_enabled = False
                await searchw.chart_current_item(clear_to_cache=True)

                # XXX: causes hang and segfault..
                # searchw.show_cache_entries(
                #     only=True,
                #     keep_current_item_selected=True,
                # )

                view.show_matches()
                searchw.focus()

            elif (
                not ctl
                and not searchbar.text()
            ):
                # TODO: really should factor this somewhere..bc
                # we're doin it in another spot as well..
                searchw.show_cache_entries(only=True)
                continue

            # cancel and close
            if ctl and key in {
                Qt.Key_C,
                Qt.Key_Space,   # i feel like this is the "native" one
                Qt.Key_Alt,
            }:
                searchbar.unfocus()

                # kill the search and focus back on main chart
                if godwidget:
                    godwidget.focus()

                continue

            if (
                ctl
                and key in {Qt.Key_L}
            ):
                # like url (link) highlight in a web browser
                searchbar.focus()

            # selection navigation controls
            elif (
                ctl
                and key in {Qt.Key_D}
            ):
                view.next_section(direction='down')
                _search_enabled = False

            elif (
                ctl
                and key in {Qt.Key_U}
            ):
                view.next_section(direction='up')
                _search_enabled = False

            # selection navigation controls
            elif (
                ctl and (
                    key in {
                        Qt.Key_K,
                        Qt.Key_J,
                    }

                    or key in {
                        Qt.Key_Up,
                        Qt.Key_Down,
                    }
                )
            ):
                _search_enabled = False

                if key in {
                    Qt.Key_K,
                    Qt.Key_Up
                }:
                    item = view.select_previous()

                elif key in {
                    Qt.Key_J,
                    Qt.Key_Down,
                }:
                    item = view.select_next()

                if item:
                    parent_item = item.parent()

                    # if we're in the cache section and thus the next
                    # selection is a cache item, switch and show it
                    # immediately since it should be very fast.
                    if (
                        parent_item
                        and parent_item.text() == 'cache'
                    ):
                        await searchw.chart_current_item(clear_to_cache=False)

            # ACTUAL SEARCH BLOCK #
            # where we fuzzy complete and fill out sections.
            elif not ctl:
                # relay to completer task
                _search_enabled = True
                send.send_nowait(searchw.bar.text())
                _search_active.set()


async def search_simple_dict(
    text: str,
    source: dict,

) -> dict[str, Any]:

    tokens = []
    for key in source:
        if not isinstance(key, str):
            tokens.extend(key)
        else:
            tokens.append(key)

    # search routine can be specified as a function such
    # as in the case of the current app's local symbol cache
    matches = fuzzy.extractBests(
        text,
        tokens,
        score_cutoff=90,
    )

    return [item[0] for item in matches]


# cache of provider names to async search routines
_searcher_cache: dict[str, Callable[..., Awaitable]] = {}


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
