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
from functools import partial
from typing import Optional
from contextlib import asynccontextmanager

import trio
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QSize, QModelIndex, Qt, QEvent
from PyQt5.QtWidgets import (
    QWidget,
    QLabel,
    QComboBox,
    QLineEdit,
    QProgressBar,
    QSizePolicy,
    QStyledItemDelegate,
    QStyleOptionViewItem,
)

from ._event import open_handlers
from ._style import hcolor, _font, _font_small, DpiAwareFont


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


class FieldsForm(QWidget):

    def __init__(
        self,

        godwidget: 'GodWidget',  # type: ignore # noqa
        parent=None,

    ) -> None:

        super().__init__(parent or godwidget)
        self.godwidget = godwidget

        # size it as we specify
        self.setSizePolicy(
            QSizePolicy.Fixed,
            QSizePolicy.Fixed,
        )
        # self.setMaximumHeight(30)
        self.setMaximumWidth(120)

        # split layout for the (label:| text bar entry)
        self.vbox = QtGui.QVBoxLayout(self)
        self.vbox.setAlignment(Qt.AlignBottom)
        self.vbox.setContentsMargins(0, 0, 4, 0)
        self.vbox.setSpacing(3)
        # self.vbox.addStretch()

        self.labels: dict[str, QLabel] = {}
        self.fields: dict[str, QWidget] = {}

        self._font_size = _font_small.px_size - 1
        self._max_item_width: (float, float) = 0, 0

    def add_field_label(
        self,
        name: str,
        font_size: Optional[int] = None,
        font_color: str = 'default_light',

    ) -> QtGui.QLabel:

        # add label to left of search bar
        self.label = label = QtGui.QLabel(parent=self)
        label.setTextFormat(Qt.MarkdownText)  # markdown
        label.setFont(_font.font)
        font_size = font_size or self._font_size - 3
        label.setStyleSheet(
            f"""QLabel {{
                color : {hcolor(font_color)};
                font-size : {font_size}px;
            }}
            """
        )
        label.setMargin(4)

        label.setText(name)

        label.setAlignment(
            QtCore.Qt.AlignVCenter
            | QtCore.Qt.AlignLeft
        )
        label.show()

        self.vbox.addWidget(label)
        self.labels[name] = label

        return label

    def add_edit_field(
        self,

        name: str,
        value: str,

        widget: Optional[QWidget] = None,

    ) -> FontAndChartAwareLineEdit:

        # TODO: maybe a distint layout per "field" item?
        self.add_field_label(name)

        edit = FontAndChartAwareLineEdit(
            parent=self,
            # parent_chart=self.godwidget,
            # width_in_chars=6,
        )
        edit.setStyleSheet(
            f"""QLineEdit {{
                color : {hcolor('gunmetal')};
                font-size : {self._font_size}px;
            }}
            """
        )
        edit.setText(str(value))
        self.vbox.addWidget(edit)

        self.fields[name] = edit

        return edit

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
            f"""QComboBox {{
                color : {hcolor('gunmetal')};
                font-size : {self._font_size}px;
            }}
            """
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
        # one entry in view
        select.setMinimumHeight(h)

        select.show()

        self.vbox.addWidget(select)

        return select


async def handle_field_input(

    # chart: 'ChartPlotWidget',  # noqa
    widget: QWidget,
    recv_chan: trio.abc.ReceiveChannel,
    form: FieldsForm,

) -> None:

    async for event, etype, key, mods, txt in recv_chan:
        print(f'key: {key}, mods: {mods}, txt: {txt}')

        ctl = False
        if mods == Qt.ControlModifier:
            ctl = True

        # cancel and close
        if ctl and key in {
            Qt.Key_C,
            Qt.Key_Space,   # i feel like this is the "native" one
            Qt.Key_Alt,
        }:
            # search.bar.unfocus()

            # kill the search and focus back on main chart
            widget.clearFocus()
            form.godwidget.linkedsplits.focus()

            continue


@asynccontextmanager
async def open_form(

    godwidget: QWidget,
    parent: QWidget,
    fields: dict,
    # orientation: str = 'horizontal',

) -> FieldsForm:

    form = FieldsForm(parent)

    # form.add_field_label(
    #     '### **pp conf**\n---',
    #     font_size=_font.px_size - 2,
    #     font_color='default_lightest',
    # )

    for name, config in fields.items():
        wtype = config['type']
        key = str(config['key'])

        # plain (line) edit field
        if wtype == 'edit':
            form.add_edit_field(key, config['default_value'])

        # drop-down selection
        elif wtype == 'select':
            values = list(config['default_value'])
            form.add_select_field(key, values)

    # form.add_field_label('fills:')
    fill_bar = QProgressBar(form)
    fill_bar.setMinimum(0)
    fill_bar.setMaximum(4)
    fill_bar.setValue(3)
    fill_bar.setOrientation(Qt.Vertical)
    fill_bar.setStyleSheet(
        f"""QProgressBar {{
            color : {hcolor('gunmetal')};
            font-size : {form._font_size}px;
            background-color: {hcolor('papas_special')};
            color: {hcolor('bracket')};
        }}
        """
    )

    form.vbox.addWidget(fill_bar)

    # form.vbox.addStretch()

    async with open_handlers(

        list(form.fields.values()),
        event_types={QEvent.KeyPress},

        async_handler=partial(
            handle_field_input,
            form=form,
        ),

        # block key repeats?
        filter_auto_repeats=True,
    ):
        yield form
