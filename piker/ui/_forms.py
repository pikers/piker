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
Text entry "forms" widgets (mostly for configuration and UI user input).

'''
from typing import Optional

# import trio
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QSize, QModelIndex
from PyQt5.QtWidgets import (
    QWidget,
    QComboBox,
    QLineEdit,
    QProgressBar,
    QSizePolicy,
    QStyledItemDelegate,
    QStyleOptionViewItem,
)

from ._style import hcolor, _font, DpiAwareFont


class FontAndChartAwareLineEdit(QLineEdit):

    def __init__(

        self,
        parent: QWidget,
        # parent_chart: QWidget,  # noqa
        font: DpiAwareFont = _font,
        width_in_chars: int = None,

    ) -> None:

        # self.setContextMenuPolicy(Qt.CustomContextMenu)
        # self.customContextMenuRequested.connect(self.show_menu)
        # self.setStyleSheet(f"font: 18px")

        self.dpi_font = font
        # self.godwidget = parent_chart

        if width_in_chars:
            self._chars = int(width_in_chars)

        else:
            # chart count which will be used to calculate
            # width of input field.
            self._chars: int = 9

        super().__init__(parent)
        # size it as we specify
        # https://doc.qt.io/qt-5/qsizepolicy.html#Policy-enum
        self.setSizePolicy(
            QSizePolicy.Expanding,
            QSizePolicy.Fixed,
        )
        self.setFont(font.font)

        # witty bit of margin
        self.setTextMargins(2, 2, 2, 2)

    def sizeHint(self) -> QSize:
        """
        Scale edit box to size of dpi aware font.

        """
        psh = super().sizeHint()

        dpi_font = self.dpi_font
        psh.setHeight(dpi_font.px_size + 2)

        # space for ``._chars: int``
        char_w_pxs = dpi_font.boundingRect(self.text()).width()
        chars_w = char_w_pxs + 6  # * dpi_font.scale() * self._chars
        psh.setWidth(chars_w)

        return psh

    def set_width_in_chars(
        self,
        chars: int,

    ) -> None:
        self._chars = chars
        self.sizeHint()
        self.update()

    def focus(self) -> None:
        self.selectAll()
        self.show()
        self.setFocus()


class FontScaledDelegate(QStyledItemDelegate):
    '''
    Super simple view delegate to render text in the same
    font size as the search widget.

    '''
    def __init__(
        self,

        parent=None,
        font: DpiAwareFont = _font,

    ) -> None:

        super().__init__(parent)
        self.dpi_font = font

    def sizeHint(
        self,

        option: QStyleOptionViewItem,
        index: QModelIndex,

    ) -> QSize:

        # value = index.data()
        # br = self.dpi_font.boundingRect(value)
        # w, h = br.width(), br.height()
        parent = self.parent()

        if getattr(parent, '_max_item_size', None):
            return QSize(*self.parent()._max_item_size)

        else:
            return super().sizeHint(option, index)


class FieldsForm(QtGui.QWidget):

    def __init__(
        self,

        # godwidget: 'GodWidget',  # type: ignore # noqa
        parent=None,

    ) -> None:
        super().__init__(parent)

        # size it as we specify
        self.setSizePolicy(
            QSizePolicy.Fixed,
            QSizePolicy.Fixed,
        )

        # split layout for the (label:| text bar entry)
        self.hbox = QtGui.QHBoxLayout(self)
        self.hbox.setContentsMargins(16, 0, 16, 0)
        self.hbox.setSpacing(3)

    def add_field_label(
        self,
        name: str,

    ) -> QtGui.QLabel:

        # add label to left of search bar
        self.label = label = QtGui.QLabel(parent=self)
        label.setTextFormat(3)  # markdown
        label.setFont(_font.font)
        label.setStyleSheet(
            f"QLabel {{ color : {hcolor('papas_special')}; }}"
        )
        label.setMargin(4)

        label.setText(name)

        label.setAlignment(
            QtCore.Qt.AlignVCenter
            | QtCore.Qt.AlignLeft
        )
        label.show()

        self.hbox.addWidget(label)
        return label

    def add_edit_field(
        self,

        name: str,
        value: str,

        widget: Optional[QWidget] = None,

    ) -> None:

        # TODO: maybe a distint layout per "field" item?
        self.add_field_label(name)

        self.edit = FontAndChartAwareLineEdit(
            parent=self,
            # parent_chart=self.godwidget,
            # width_in_chars=6,
        )
        self.edit.setStyleSheet(
            f"QLineEdit {{ color : {hcolor('gunmetal')}; }}"
        )
        self.edit.setText(str(value))
        self.hbox.addWidget(self.edit)

    def add_select_field(
        self,

        name: str,
        values: list[str],

    ) -> QComboBox:

        # TODO: maybe a distint layout per "field" item?
        self.add_field_label(name)

        select = QComboBox(self)

        for i, value in enumerate(values):
            select.insertItem(i, str(value))

        select.setStyleSheet(
            f"QComboBox {{ color : {hcolor('gunmetal')}; }}"
        )
        select.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        select.setIconSize(QSize(0, 0))
        self.setSizePolicy(
            QSizePolicy.Fixed,
            QSizePolicy.Fixed,
        )
        view = select.view()
        view.setUniformItemSizes(True)
        view.setItemDelegate(FontScaledDelegate(view))

        # compute maximum item size so that the weird
        # "style item delegate" thing can then specify
        # that size on each item...
        values.sort()
        br = _font.boundingRect(str(values[-1]))
        w, h = br.width(), br.height()

        # TODO: something better then this monkey patch
        view._max_item_size = w, h

        # limit to 6 items?
        view.setMaximumHeight(6*h)
        select.show()

        self.hbox.addWidget(select)

        return select


# async def handle_form_input(

#     chart: 'ChartPlotWidget',  # noqa
#     form: FieldsForm,
#     recv_chan: trio.abc.ReceiveChannel,

# ) -> None:

#     async for event, etype, key, mods, txt in recv_chan:
#         print(f'key: {key}, mods: {mods}, txt: {txt}')

#         ctl = False
#         if mods == Qt.ControlModifier:
#             ctl = True

#         # cancel and close
#         if ctl and key in {
#             Qt.Key_C,
#             Qt.Key_Space,   # i feel like this is the "native" one
#             Qt.Key_Alt,
#         }:
#             # search.bar.unfocus()

#             # kill the search and focus back on main chart
#             if chart:
#                 chart.linkedsplits.focus()

#             continue


def mk_form(

    parent: QWidget,
    fields: dict,
    # orientation: str = 'horizontal',

) -> FieldsForm:

    form = FieldsForm(parent)

    for name, value in fields.items():
        form.add_edit_field(name, value)

    form.add_select_field('policy:', ['uniform', 'halfs'])

    form.add_field_label('fills:')
    fill_bar = QProgressBar(form)
    fill_bar.setMinimum(0)
    fill_bar.setMaximum(4)
    fill_bar.setValue(3)

    form.hbox.addWidget(fill_bar)

    return form
