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

import sys
from functools import partial
from typing import (
    List, Optional, Callable,
    Awaitable, Sequence, Dict,
)
# from pprint import pformat

from PyQt5 import QtCore, QtGui
from PyQt5 import QtWidgets
import trio

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
from ..data import feed


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
        self.setIndentation(16)

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

    def set_results(
        self,
        results: Dict[str, Sequence[str]],
    ) -> None:

        model = self.model()
        model.clear()
        model.setHorizontalHeaderLabels(self.labels)

        # TODO: wtf.. this model shit
        # row_count = model.rowCount()
        # if row_count > 0:
        #     model.removeRows(
        #         0,
        #         row_count,

        #         # root index
        #         model.index(0, 0, QModelIndex()),
        #     )
        root = model.invisibleRootItem()

        for key, values in results.items():

            src = QStandardItem(key)
            root.appendRow(src)
            # self.expand(model.index(1, 0, QModelIndex()))

            # values just needs to be sequence-like
            for i, s in enumerate(values):

                # blank = QStandardItem('')
                ix = QStandardItem(str(i))
                item = QStandardItem(s)
                # item.setCheckable(False)

                # Add the item to the model
                src.appendRow([ix, item])

        self.expandAll()

    def show_matches(self) -> None:
        # print(f"SHOWING {self}")
        self.show()
        self.resize()

    def resize(self):
        model = self.model()
        cols = model.columnCount()

        for i in range(cols):
            self.resizeColumnToContents(i)

        # inclusive of search bar and header "rows" in pixel terms
        rows = 100
        # print(f'row count: {rows}')
        # max_rows = 8  # 6 + search and headers
        row_px = self.rowHeight(self.currentIndex())
        # print(f'font_h: {font_h}\n px_height: {px_height}')

        # TODO: probably make this more general / less hacky
        self.setMinimumSize(self.width(), rows * row_px)
        self.setMaximumSize(self.width(), rows * row_px)

    def select_previous(self) -> QModelIndex:
        cidx = self.currentIndex()
        nidx = self.indexAbove(cidx)
        if nidx.parent() is QModelIndex():
            nidx = self.indexAbove(cidx)
            breakpoint()

        return nidx

    def select_next(self) -> QModelIndex:
        cidx = self.currentIndex()
        nidx = self.indexBelow(cidx)
        if nidx.parent() is QModelIndex():
            nidx = self.indexBelow(cidx)
            breakpoint()

        return nidx

    def select_from_idx(
        self,
        idx: QModelIndex,
    ) -> None:
        sel = self.selectionModel()
        model = self.model()

        # select first indented entry
        if idx == model.index(0, 0):
            idx = self.select_next()

        sel.setCurrentIndex(
            idx,
            QItemSelectionModel.ClearAndSelect |
            QItemSelectionModel.Rows
        )

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
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.setFont(font.font)

        # witty bit of margin
        self.setTextMargins(2, 2, 2, 2)

        # self.setContextMenuPolicy(Qt.CustomContextMenu)
        # self.customContextMenuRequested.connect(self.show_menu)
        # self.setStyleSheet(f"font: 18px")

    def focus(self) -> None:
        self.clear()
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
        psh.setWidth(6*6*6)
        return psh

    def unfocus(self) -> None:
        self.hide()
        self.clearFocus()

        if self.view:
            self.view.hide()


_search_active: trio.Event = trio.Event()
_search_enabled: bool = False


async def fill_results(

    search: SearchBar,
    symsearch: Callable[..., Awaitable],
    recv_chan: trio.abc.ReceiveChannel,
    pause_time: float = 0.0616,

) -> None:
    """Task to search through providers and fill in possible
    completion results.

    """
    global _search_active, _search_enabled

    bar = search.bar
    view = bar.view
    sel = bar.view.selectionModel()
    model = bar.view.model()

    last_search_text = ''
    last_text = bar.text()
    repeats = 0

    while True:

        last_text = bar.text()
        await _search_active.wait()

        with trio.move_on_after(pause_time) as cs:
            # cs.shield = True
            pattern = await recv_chan.receive()
            print(pattern)

        # during fast multiple key inputs, wait until a pause
        # (in typing) to initiate search
        if not cs.cancelled_caught:
            log.debug(f'Ignoring fast input for {pattern}')
            continue

        text = bar.text()
        print(f'search: {text}')

        if not text:
            print('idling')
            _search_active = trio.Event()
            continue

        if text == last_text:
            repeats += 1

        if repeats > 1:
            _search_active = trio.Event()
            repeats = 0

        if not _search_enabled:
            print('search not ENABLED?')
            continue

        if last_search_text and last_search_text == text:
            continue

        log.debug(f'Search req for {text}')

        last_search_text = text
        results = await symsearch(text)
        log.debug(f'Received search result {results}')

        if results and _search_enabled:

            # TODO: indented branch results for each provider
            view.set_results(results)

            # XXX: these 2 lines MUST be in sequence in order
            # to get the view to show right after typing input.
            sel.setCurrentIndex(
                model.index(0, 0, QModelIndex()),
                QItemSelectionModel.ClearAndSelect |
                QItemSelectionModel.Rows
            )
            bar.show()

            # ensure we select first indented entry
            view.select_from_idx(sel.currentIndex())


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
        self.vbox.addWidget(self.bar)
        self.vbox.setAlignment(self.bar, Qt.AlignTop | Qt.AlignLeft)
        self.vbox.addWidget(self.bar.view)
        self.vbox.setAlignment(self.view, Qt.AlignTop | Qt.AlignLeft)


async def handle_keyboard_input(

    search: SearchWidget,
    recv_chan: trio.abc.ReceiveChannel,
    keyboard_pause_period: float = 0.0616,

) -> None:

    global _search_active, _search_enabled

    # startup
    bar = search.bar
    view = bar.view
    view.set_font_size(bar.dpi_font.px_size)
    model = view.model()
    nidx = cidx = view.currentIndex()
    sel = view.selectionModel()

    symsearch = feed.get_multi_search()
    send, recv = trio.open_memory_channel(16)

    async with trio.open_nursery() as n:
        # TODO: async debouncing?
        n.start_soon(
            partial(
                fill_results,
                search,
                symsearch,
                recv,
                pause_time=keyboard_pause_period,
            )
        )

        async for key, mods, txt in recv_chan:

            log.debug(f'key: {key}, mods: {mods}, txt: {txt}')
            # parent = view.currentIndex()
            cidx = sel.currentIndex()
            # view.select_from_idx(nidx)

            # if cidx == model.index(0, 0):
            #     print('uhh')
            #     cidx = view.select_next()
            #     sel.setCurrentIndex(
            #         cidx,
            #         QItemSelectionModel.ClearAndSelect |
            #         QItemSelectionModel.Rows
            #     )

            ctrl = False
            if mods == Qt.ControlModifier:
                ctrl = True

            if key in (Qt.Key_Enter, Qt.Key_Return):

                # TODO: get rid of this hard coded column -> 1
                # https://doc.qt.io/qt-5/qstandarditemmodel.html#itemFromIndex
                node = model.itemFromIndex(cidx.siblingAtColumn(1))
                if node:
                    value = node.text()
                    # print(f' value: {value}')
                else:
                    continue

                log.info(f'Requesting symbol: {value}')

                app = search.chart_app
                search.chart_app.load_symbol(
                    app.linkedcharts.symbol.brokers[0],
                    value,
                    'info',
                )

                _search_enabled = False
                # release kb control of search bar
                search.bar.unfocus()
                continue

            # selection tips:
            # - get parent node: search.index(row, 0)
            # - first node index: index = search.index(0, 0, parent)
            # - root node index: index = search.index(0, 0, QModelIndex())

            # we're in select mode or cancelling
            if ctrl:
                # cancel and close
                if key == Qt.Key_C:
                    search.bar.unfocus()

                    # kill the search and focus back on main chart
                    if search.chart_app:
                        search.chart_app.linkedcharts.focus()

                    continue

                # result selection nav
                if key in (Qt.Key_K, Qt.Key_J):
                    _search_enabled = False

                    if key == Qt.Key_K:
                        nidx = view.select_previous()

                    elif key == Qt.Key_J:
                        nidx = view.select_next()

                    # select row without selecting.. :eye_rollzz:
                    # https://doc.qt.io/qt-5/qabstractitemview.html#setCurrentIndex
                    if nidx.isValid():
                        view.select_from_idx(nidx)
                        # sel.setCurrentIndex(
                        #     nidx,
                        #     QItemSelectionModel.ClearAndSelect |
                        #     QItemSelectionModel.Rows
                        # )

                        # TODO: make this not hard coded to 2
                        # and use the ``CompleterView`` schema/settings
                        # to figure out the desired field(s)
                        # value = model.item(nidx.row(), 0).text()
            else:
                # relay to completer task
                _search_enabled = True
                send.send_nowait(search.bar.text())
                _search_active.set()


if __name__ == '__main__':

    # local testing of **just** the search UI
    app = QtWidgets.QApplication(sys.argv)

    syms = [
        'XMRUSD',
        'XBTUSD',
        'ETHUSD',
        'XMRXBT',
        'XDGUSD',
        'ADAUSD',
    ]

    # results.setFocusPolicy(Qt.NoFocus)
    view = CompleterView(['src', 'i', 'symbol'])
    search = SearchBar(None, view=view)
    search.view.set_results(syms)

    # make a root widget to tie shit together
    class W(QtGui.QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.vbox = QtGui.QVBoxLayout(self)
            self.vbox.setContentsMargins(0, 0, 0, 0)
            self.vbox.setSpacing(2)

    main = W()
    main.vbox.addWidget(search)
    main.vbox.addWidget(view)
    search.show()

    sys.exit(app.exec_())
