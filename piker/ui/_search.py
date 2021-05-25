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

    # def sizeHint(self, *args) -> QtCore.QSize:
    #     """
    #     Scale edit box to size of dpi aware font.

    #     """
    #     psh = super().sizeHint(*args)
    #     # psh.setHeight(self.dpi_font.px_size + 2)

    #     psh.setHeight(18)
    #     # psh.setHeight(18)
    #     return psh


class CompleterView(QTreeView):

    # XXX: relevant docs links:
    # - simple widget version of this:
    #   https://doc.qt.io/qt-5/qtreewidget.html#details
    # - MVC high level instructional:
    #   https://doc.qt.io/qt-5/model-view-programming.html
    # - MV tut:
    #   https://doc.qt.io/qt-5/modelview.html

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

        # self.setUniformRowHeights(True)
        # self.setColumnWidth(0, 3)

        # ux settings
        self.setItemsExpandable(True)
        self.setExpandsOnDoubleClick(False)
        self.setAnimated(False)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # self.setVerticalBarPolicy(Qt.ScrollBarAlwaysOff)
        # self.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)

        # column headers
        model.setHorizontalHeaderLabels(labels)

        self._font_size: int = 0  # pixels
        # self._cache: Dict[str, List[str]] = {}

    # def viewportSizeHint(self) -> QtCore.QSize:
    #     vps = super().viewportSizeHint()
    #     return QSize(vps.width(), _font.px_size * 6 * 2)

    # def sizeHint(self) -> QtCore.QSize:
    #     """Scale completion results up to 6/16 of window.
    #     """
    #     # height = self.window().height() * 1/6
    #     # psh.setHeight(self.dpi_font.px_size * 6)
    #     # print(_font.px_size)
    #     height = _font.px_size * 6 * 2
    #     # the default here is just the vp size without scroll bar
    #     # https://doc.qt.io/qt-5/qabstractscrollarea.html#viewportSizeHint
    #     vps = self.viewportSizeHint()
    #     # print(f'h: {height}\n{vps}')
    #     # psh.setHeight(12)
    #     return QSize(-1, height)

    def set_font_size(self, size: int = 18):
        # dpi_px_size = _font.px_size
        print(size)
        if size < 0:
            size = 16

        self._font_size = size

        self.setStyleSheet(f"font: {size}px")

    def show_matches(self) -> None:
        self.show()
        self.resize()

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
        return self.select_from_idx(
            self.indexBelow(self.model().index(0, 0, QModelIndex()))
        )

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

    def set_results(
        self,
        results: Dict[str, Sequence[str]],
    ) -> None:

        model = self.model()

        # XXX: currently we simply rewrite the model from scratch each call
        # since it seems to be super fast anyway.
        model.clear()

        model.setHorizontalHeaderLabels(self.labels)
        root = model.invisibleRootItem()

        for key, values in results.items():

            src = QStandardItem(key)
            root.appendRow(src)

            # values just needs to be sequence-like
            for i, s in enumerate(values):

                ix = QStandardItem(str(i))
                item = QStandardItem(s)

                # Add the item to the model
                src.appendRow([ix, item])

        self.expandAll()

        # XXX: these 2 lines MUST be in sequence in order
        # to get the view to show right after typing input.
        sel = self.selectionModel()

        # select row without selecting.. :eye_rollzz:
        # https://doc.qt.io/qt-5/qabstractitemview.html#setCurrentIndex
        sel.setCurrentIndex(
            model.index(0, 0, QModelIndex()),
            QItemSelectionModel.ClearAndSelect |
            QItemSelectionModel.Rows
        )

        self.select_first()
        self.show_matches()

    # def find_matches(
    #     self,
    #     field: str,
    #     txt: str,
    # ) -> List[QStandardItem]:
    #     model = self.model()
    #     items = model.findItems(
    #         txt,
    #         Qt.MatchContains,
    #         self.field_to_col(field),
    #     )


class SearchBar(QtWidgets.QLineEdit):

    def __init__(

        self,
        parent: QWidget,
        parent_chart: QWidget,  # noqa
        view: Optional[CompleterView] = None,
        font: DpiAwareFont = _font,

    ) -> None:

        super().__init__(parent)

        self.view: CompleterView = view
        self.dpi_font = font
        self.chart_app = parent_chart

        # size it as we specify
        # https://doc.qt.io/qt-5/qsizepolicy.html#Policy-enum
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Expanding,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.setFont(font.font)

        # witty bit of margin
        self.setTextMargins(2, 2, 2, 2)

        # self.setContextMenuPolicy(Qt.CustomContextMenu)
        # self.customContextMenuRequested.connect(self.show_menu)
        # self.setStyleSheet(f"font: 18px")

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
    def __init__(
        self,
        chart_space: 'ChartSpace',  # type: ignore # noqa
        columns: List[str] = ['src', 'symbol'],
        parent=None,
    ):
        super().__init__(parent or chart_space)

        # size it as we specify
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Expanding,
        )

        self.chart_app = chart_space

        self.vbox = QtGui.QVBoxLayout(self)
        self.vbox.setContentsMargins(0, 0, 0, 0)
        self.vbox.setSpacing(4)

        # split layout for the (label:| search bar entry)
        self.bar_hbox = QtGui.QHBoxLayout(self)
        self.bar_hbox.setContentsMargins(0, 0, 0, 0)
        self.bar_hbox.setSpacing(4)

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

        # https://doc.qt.io/qt-5/qlayout.html#SizeConstraint-enum
        # self.vbox.setSizeConstraint(QLayout.SetMaximumSize)

        self.view = CompleterView(
            parent=self,
            labels=columns,
        )
        self.bar = SearchBar(
            parent=self,
            parent_chart=chart_space,
            view=self.view,
        )
        self.bar_hbox.addWidget(self.bar)

        # self.vbox.addWidget(self.bar)
        # self.vbox.setAlignment(self.bar, Qt.AlignTop | Qt.AlignRight)
        self.vbox.addLayout(self.bar_hbox)

        self.vbox.setAlignment(self.bar, Qt.AlignTop | Qt.AlignRight)
        self.vbox.addWidget(self.bar.view)
        self.vbox.setAlignment(self.view, Qt.AlignTop | Qt.AlignLeft)

    def focus(self) -> None:

        if self.view.model().rowCount(QModelIndex()) == 0:
            # fill cache list if nothing existing
            self.view.set_results(
                {'cache': list(reversed(self.chart_app._chart_cache))})

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
            provider = node.parent().text()

            # TODO: move this to somewhere non-search machinery specific?
            if provider == 'cache':
                symbol, _, provider = symbol.rpartition('.')

            return provider, symbol

        else:
            return None


_search_active: trio.Event = trio.Event()
_search_enabled: bool = False


async def fill_results(

    search: SearchBar,
    symsearch: Callable[..., Awaitable],
    recv_chan: trio.abc.ReceiveChannel,

    # kb debouncing pauses
    min_pause_time: float = 0.0616,
    max_pause_time: float = 6/16,

) -> None:
    """Task to search through providers and fill in possible
    completion results.

    """
    global _search_active, _search_enabled

    bar = search.bar
    view = bar.view

    last_text = bar.text()
    repeats = 0

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
            print(f'search: {text}')

            if not text:
                print('idling')
                _search_active = trio.Event()
                break

            if repeats > 2 and period >= max_pause_time:
                _search_active = trio.Event()
                repeats = 0
                break

            if text == last_text:
                repeats += 1

            if not _search_enabled:
                print('search currently disabled')
                break

            log.debug(f'Search req for {text}')

            results = await symsearch(text, period=period)

            log.debug(f'Received search result {results}')

            if results and _search_enabled:

                # show the results in the completer view
                view.set_results(results)
                bar.show()


async def handle_keyboard_input(

    search: SearchWidget,
    recv_chan: trio.abc.ReceiveChannel,
    keyboard_pause_period: float = 0.0616,

) -> None:

    global _search_active, _search_enabled

    # startup
    chart = search.chart_app
    bar = search.bar
    view = bar.view
    view.set_font_size(bar.dpi_font.px_size)
    # nidx = view.currentIndex()

    symsearch = get_multi_search()
    send, recv = trio.open_memory_channel(16)

    async with trio.open_nursery() as n:
        n.start_soon(
            partial(
                fill_results,
                search,
                symsearch,
                recv,
            )
        )

        async for key, mods, txt in recv_chan:

            log.debug(f'key: {key}, mods: {mods}, txt: {txt}')

            ctl = False
            if mods == Qt.ControlModifier:
                ctl = True

            # alt = False
            # if mods == Qt.AltModifier:
            #     alt = True

            # # ctl + alt as combo
            # ctlalt = False
            # if (QtCore.Qt.AltModifier | QtCore.Qt.ControlModifier) == mods:
            #     ctlalt = True

            if key in (Qt.Key_Enter, Qt.Key_Return):

                value = search.get_current_item()
                if value is None:
                    continue

                provider, symbol = value

                log.info(f'Requesting symbol: {symbol}.{provider}')

                chart.load_symbol(
                    provider,
                    symbol,
                    'info',
                )

                # fully qualified symbol name (SNS i guess is what we're
                # making?)
                fqsn = '.'.join([symbol, provider]).lower()

                # Re-order the symbol cache on the chart to display in
                # LIFO order. this is normally only done internally by
                # the chart on new symbols being loaded into memory
                chart.set_chart_symbol(fqsn, chart.linkedcharts)

                search.bar.clear()
                view.set_results({
                    'cache': list(reversed(chart._chart_cache))
                })

                _search_enabled = False
                # release kb control of search bar
                # search.bar.unfocus()
                continue

            elif not ctl and not bar.text():
                # if nothing in search text show the cache
                view.set_results({
                    'cache': list(reversed(chart._chart_cache))
                })
                continue

            # selection tips:
            # - get parent node: search.index(row, 0)
            # - first node index: index = search.index(0, 0, parent)
            # - root node index: index = search.index(0, 0, QModelIndex())

            # cancel and close
            if ctl and key in {
                Qt.Key_C,
                Qt.Key_Space,   # i feel like this is the "native" one
                Qt.Key_Alt,
            }:
                search.bar.unfocus()

                # kill the search and focus back on main chart
                if chart:
                    chart.linkedcharts.focus()

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

            elif ctl and key in {
                Qt.Key_U,
            }:
                view.next_section(direction='up')

            # selection navigation controls
            elif ctl and key in {

                Qt.Key_K,
                Qt.Key_J,

            } or key in {

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

                        value = search.get_current_item()

                        if value is not None:
                            provider, symbol = value
                            chart.load_symbol(
                                provider,
                                symbol,
                                'info',
                            )

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


def get_multi_search() -> Callable[..., Awaitable]:

    global _searcher_cache

    async def multisearcher(
        pattern: str,
        period: str,

    ) -> dict:

        matches = {}

        async def pack_matches(
            provider: str,
            pattern: str,
            search: Callable[..., Awaitable[dict]],

        ) -> None:

            log.info(f'Searching {provider} for "{pattern}"')
            results = await search(pattern)
            if results:
                matches[provider] = results

        # TODO: make this an async stream?
        async with trio.open_nursery() as n:

            for provider, (search, min_pause) in _searcher_cache.items():

                # only conduct search on this backend if it's registered
                # for the corresponding pause period.
                if period >= min_pause:
                    # print(
                    #   f'searching {provider} after {period} > {min_pause}')
                    n.start_soon(pack_matches, provider, pattern, search)

        return matches

    return multisearcher


@asynccontextmanager
async def register_symbol_search(

    provider_name: str,
    search_routine: Callable,
    pause_period: Optional[float] = None,

) -> AsyncIterator[dict]:

    global _searcher_cache

    pause_period = pause_period or 0.061

    # deliver search func to consumer
    try:
        _searcher_cache[provider_name] = (search_routine, pause_period)
        yield search_routine

    finally:
        _searcher_cache.pop(provider_name)
