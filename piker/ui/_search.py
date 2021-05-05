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
from typing import List, Optional
import sys

from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import Qt, QEvent
from PyQt5 import QtWidgets
import trio

from PyQt5.QtCore import (
    Qt,
    QSize,
    QModelIndex,
    QItemSelectionModel,
    # QStringListModel
)
from PyQt5.QtGui import (
    QStandardItem,
    QStandardItemModel,
)
from PyQt5.QtWidgets import (
    QTreeView,
    # QListWidgetItem,
    QAbstractScrollArea,
    QStyledItemDelegate,
)
from fuzzywuzzy import process


from ..log import get_logger
from ._style import (
    _font,
    DpiAwareFont,
    hcolor,
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

    def __init__(
        self,
        parent=None,
    ) -> None:

        super().__init__(parent)

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

    def resize(self):
        model = self.model()
        cols = model.columnCount()

        for i in range(cols):
            self.resizeColumnToContents(i)

        # inclusive of search bar and header "rows" in pixel terms
        rows = model.rowCount() + 2
        # max_rows = 8  # 6 + search and headers
        row_px = self.rowHeight(self.currentIndex())
        # print(f'font_h: {font_h}\n px_height: {px_height}')

        # TODO: probably make this more general / less hacky
        self.setMinimumSize(self.width(), rows * row_px)
        self.setMaximumSize(self.width(), rows * row_px)

    def set_font_size(self, size: int = 18):
        # dpi_px_size = _font.px_size
        print(size)
        if size < 0:
            size = 16

        self._font_size = size

        self.setStyleSheet(f"font: {size}px")

    def set_results(
        self,
        results: List[str],
    ) -> None:

        model = self.model()

        for i, s in enumerate(results):

            ix = QStandardItem(str(i))
            item = QStandardItem(s)
            # item.setCheckable(False)

            src = QStandardItem('kraken')

            # Add the item to the model
            model.appendRow([src, ix, item])

    def find_matches(
        self,
        field: str,
        txt: str,
    ) -> List[QStandardItem]:
        model = self.model()
        items = model.findItems(
            txt,
            Qt.MatchContains,
            self.field_to_col(field),
        )


def mk_completer_view(

    labels: List[str],

) -> QTreeView:

    tree = CompleterView()
    model = QStandardItemModel(tree)

    # a std "tabular" config
    tree.setItemDelegate(SimpleDelegate())
    tree.setModel(model)
    tree.setAlternatingRowColors(True)
    tree.setIndentation(1)

    # tree.setUniformRowHeights(True)
    # tree.setColumnWidth(0, 3)

    # ux settings
    tree.setItemsExpandable(True)
    tree.setExpandsOnDoubleClick(False)
    tree.setAnimated(False)
    tree.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

    tree.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)

    # column headers
    model.setHorizontalHeaderLabels(labels)

    return tree


class FontSizedQLineEdit(QtWidgets.QLineEdit):

    def __init__(
        self,
        parent_chart: 'ChartSpace',  # noqa
        view: Optional[CompleterView] = None,
        font: DpiAwareFont = _font,
    ) -> None:
        super().__init__(parent_chart)

        # vbox = self.vbox = QtGui.QVBoxLayout(self)
        # vbox.addWidget(self)
        # self.vbox.setContentsMargins(0, 0, 0, 0)
        # self.vbox.setSpacing(2)

        self._view: CompleterView = view

        self.dpi_font = font
        self.chart_app = parent_chart

        # size it as we specify
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
        self.show_matches()
        # self.view.show()
        # self.view.resize()

    @property
    def view(self) -> CompleterView:

        if self._view is None:
            view = mk_completer_view(['src', 'i', 'symbol'])

            # print('yo')
            # self.chart_app.vbox.addWidget(view)
            # self.vbox.addWidget(view)

            self._view = view

        return self._view

    def show_matches(self):
        view = self.view
        view.set_font_size(self.dpi_font.px_size)
        view.show()
        # scale columns
        view.resize()

    def sizeHint(self) -> QtCore.QSize:
        """
        Scale edit box to size of dpi aware font.

        """
        psh = super().sizeHint()
        psh.setHeight(self.dpi_font.px_size + 2)
        # psh.setHeight(12)
        return psh

    def unfocus(self) -> None:
        self.hide()
        self.clearFocus()

        if self.view:
            self.view.hide()

    # def keyPressEvent(self, ev: QEvent) -> None:

    #     # XXX: we unpack here because apparently doing it
    #     # after pop from the mem chan isn't showing the same
    #     # event object? no clue wtf is going on there, likely
    #     # something to do with Qt internals and calling the
    #     # parent handler?
    #     key = ev.key()
    #     mods = ev.modifiers()
    #     txt = self.text()

    #     # run async processing
    #     self._send_chan.send_nowait((key, mods, txt))

    #     super().keyPressEvent(ev)

    #     # ev.accept()


async def handle_keyboard_input(
    self,
    recv_chan: trio.abc.ReceiveChannel,
) -> None:

    async for key, mods, txt in recv_chan:

        # by default we don't mart it as consumed?
        # ev.ignore()

        print(f'key: {key}, mods: {mods}, txt: {txt}')

        if key in (Qt.Key_Enter, Qt.Key_Return):

            print(f'Requesting symbol: {self.text()}')

            # TODO: ensure there is a matching completion or error and
            # do nothing
            symbol = self.text()

            app = self.chart_app
            self.chart_app.load_symbol(
                app.linkedcharts.symbol.brokers[0],
                symbol,
                'info',
            )

            # release kb control of search bar
            self.unfocus()
            continue
            # return

        ctrl = False
        if mods == Qt.ControlModifier:
            ctrl = True

        view = self.view
        model = view.model()
        nidx = cidx = view.currentIndex()
        sel = view.selectionModel()
        # sel.clear()

        # selection tips:
        # - get parent: self.index(row, 0)
        # - first item index: index = self.index(0, 0, parent)

        if ctrl:
            # we're in select mode or cancelling

            if key == Qt.Key_C:
                self.unfocus()

                # kill the search and focus back on main chart
                if self.chart_app:
                    self.chart_app.linkedcharts.focus()

                # return
                continue

            # result selection nav
            if key in (Qt.Key_K, Qt.Key_J):

                if key == Qt.Key_K:
                    # self.view.setFocus()
                    nidx = view.indexAbove(cidx)
                    print('move up')

                elif key == Qt.Key_J:
                    # self.view.setFocus()
                    nidx = view.indexBelow(cidx)
                    print('move down')

                # select row without selecting.. :eye_rollzz:
                # https://doc.qt.io/qt-5/qabstractitemview.html#setCurrentIndex
                if nidx.isValid():
                    sel.setCurrentIndex(
                        nidx,
                        QItemSelectionModel.ClearAndSelect |
                        QItemSelectionModel.Rows
                    )

                    # TODO: make this not hard coded to 2
                    # and use the ``CompleterView`` schema/settings
                    # to figure out the desired field(s)
                    value = model.item(nidx.row(), 2).text()
                    print(f'value: {value}')
                    self.setText(value)

        else:
            sel.setCurrentIndex(
                model.index(0, 0, QModelIndex()),
                QItemSelectionModel.ClearAndSelect |  # type: ignore[arg-type]
                QItemSelectionModel.Rows
            )

        self.show_matches()


if __name__ == '__main__':

    # local testing of **just** the search UI
    app = QtWidgets.QApplication(sys.argv)

    syms = [
        'XMRUSD',
        'XBTUSD',
        'XMRXBT',
        'XDGUSD',
        'ADAUSD',
    ]

    # results.setFocusPolicy(Qt.NoFocus)
    view = mk_completer_view(['src', 'i', 'symbol'])
    search = FontSizedQLineEdit(None, view=view)
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
    # main.show()
    search.show()

    sys.exit(app.exec_())
