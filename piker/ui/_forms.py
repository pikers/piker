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
from __future__ import annotations
from contextlib import asynccontextmanager
from functools import partial
from textwrap import dedent
import math
from typing import Optional

import trio
from PyQt5 import QtCore, QtGui
from PyQt5.QtCore import QSize, QModelIndex, Qt, QEvent
from PyQt5.QtWidgets import (
    QWidget,
    QLabel,
    QComboBox,
    QLineEdit,
    QHBoxLayout,
    QVBoxLayout,
    QFormLayout,
    QProgressBar,
    QSizePolicy,
    QStyledItemDelegate,
    QStyleOptionViewItem,
)
import pydantic

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


# slew of resources which helped get this where it is:
# https://stackoverflow.com/questions/20648210/qcombobox-adjusttocontents-changing-height
# https://stackoverflow.com/questions/3151798/how-do-i-set-the-qcombobox-width-to-fit-the-largest-item
# https://stackoverflow.com/questions/6337589/qlistwidget-adjust-size-to-content#6370892
# https://stackoverflow.com/questions/25304267/qt-resize-of-qlistview
# https://stackoverflow.com/questions/28227406/how-to-set-qlistview-rows-height-permanently

class FieldsForm(QWidget):

    vbox: QVBoxLayout
    form: QFormLayout

    def __init__(
        self,
        parent=None,

    ) -> None:

        super().__init__(parent)

        # size it as we specify
        self.setSizePolicy(
            QSizePolicy.Expanding,
            QSizePolicy.Expanding,
        )

        # XXX: not sure why we have to create this here exactly
        # (instead of in the pane creation routine) but it's
        # here and is managed by downstream layout routines.
        # best guess is that you have to create layouts in order
        # of hierarchy in order for things to display correctly?
        # TODO: we may want to hand this *down* from some "pane manager"
        # thing eventually?
        self.vbox = QVBoxLayout(self)
        self.vbox.setAlignment(Qt.AlignVCenter)
        self.vbox.setContentsMargins(0, 4, 3, 6)
        self.vbox.setSpacing(0)

        # split layout for the (<label>: |<widget>|) parameters entry
        self.form = QFormLayout(self)
        self.form.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.form.setContentsMargins(0, 0, 0, 0)
        self.form.setSpacing(3)
        self.form.setHorizontalSpacing(0)

        self.vbox.addLayout(self.form, stretch=1/3)

        self.labels: dict[str, QLabel] = {}
        self.fields: dict[str, QWidget] = {}

        self._font_size = _font_small.px_size - 2
        self._max_item_width: (float, float) = 0, 0

    def add_field_label(
        self,

        name: str,

        font_size: Optional[int] = None,
        font_color: str = 'default_lightest',

    ) -> QtGui.QLabel:

        # add label to left of search bar
        self.label = label = QtGui.QLabel()
        font_size = font_size or self._font_size - 2
        label.setStyleSheet(
            f"""QLabel {{
                color : {hcolor(font_color)};
                font-size : {font_size}px;
            }}
            """
        )
        label.setFont(_font.font)
        label.setTextFormat(Qt.MarkdownText)  # markdown
        label.setMargin(0)

        label.setText(name)

        label.setAlignment(
            QtCore.Qt.AlignVCenter
            | QtCore.Qt.AlignLeft
        )

        # for later lookup
        self.labels[name] = label

        return label

    def add_edit_field(
        self,

        name: str,
        value: str,

    ) -> FontAndChartAwareLineEdit:

        # TODO: maybe a distint layout per "field" item?
        label = self.add_field_label(name)

        edit = FontAndChartAwareLineEdit(
            parent=self,
        )
        edit.setStyleSheet(
            f"""QLineEdit {{
                color : {hcolor('gunmetal')};
                font-size : {self._font_size}px;
            }}
            """
        )
        edit.setText(str(value))
        self.form.addRow(label, edit)

        self.fields[name] = edit

        return edit

    def add_select_field(
        self,

        name: str,
        values: list[str],

    ) -> QComboBox:

        # TODO: maybe a distint layout per "field" item?
        label = self.add_field_label(name)

        select = QComboBox(self)
        select._key = name

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

        self.form.addRow(label, select)

        return select


async def handle_field_input(

    widget: QWidget,
    recv_chan: trio.abc.ReceiveChannel,
    form: FieldsForm,
    model: pydantic.BaseModel,  # noqa
    focus_next: QWidget,

) -> None:

    async for kbmsg in recv_chan:

        if kbmsg.etype in {QEvent.KeyPress, QEvent.KeyRelease}:
            event, etype, key, mods, txt = kbmsg.to_tuple()
            print(f'key: {kbmsg.key}, mods: {kbmsg.mods}, txt: {kbmsg.txt}')

            # default controls set
            ctl = False
            if kbmsg.mods == Qt.ControlModifier:
                ctl = True

            if ctl and key in {  # cancel and refocus

                Qt.Key_C,
                Qt.Key_Space,  # i feel like this is the "native" one
                Qt.Key_Alt,
            }:

                widget.clearFocus()
                # normally the godwidget
                focus_next.focus()
                continue

            # process field input
            if key in (Qt.Key_Enter, Qt.Key_Return):
                value = widget.text()
                key = widget._key
                setattr(model, key, value)
                print(model.dict())


def mk_form(

    model: pydantic.BaseModel,
    parent: QWidget,
    fields_schema: dict,

) -> FieldsForm:

    form = FieldsForm(parent=parent)
    # TODO: generate components from model
    # instead of schema dict (aka use an ORM)
    form.model = model

    # generate sub-components from schema dict
    for name, config in fields_schema.items():
        wtype = config['type']
        label = str(config.get('label', name))

        # plain (line) edit field
        if wtype == 'edit':
            w = form.add_edit_field(
                label,
                config['default_value']
            )

        # drop-down selection
        elif wtype == 'select':
            values = list(config['default_value'])
            w = form.add_select_field(
                label,
                values
            )

            def write_model(text: str):
                print(f'{text}')
                setattr(form.model, name, text)

            w.currentTextChanged.connect(write_model)

        w._key = name

    return form


@asynccontextmanager
async def open_form_input_handling(

    form: FieldsForm,
    focus_next: QWidget,

) -> FieldsForm:

    assert form.model, f'{form} must define a `.model`'

    async with open_handlers(

        list(form.fields.values()),
        event_types={
            QEvent.KeyPress,
        },

        async_handler=partial(
            handle_field_input,
            form=form,
            model=form.model,
            focus_next=focus_next,
        ),

        # block key repeats?
        filter_auto_repeats=True,
    ):
        yield form


def mk_fill_status_bar(

    fields: FieldsForm,
    pane_vbox: QVBoxLayout,
    bar_h: int = 250,

) -> (QHBoxLayout, QProgressBar):

    w = fields.width()
    # indent = 18
    # bracket_val = 0.375 * 0.666 * w
    # indent = bracket_val / (1 + 5/8)

    # TODO: once things are sized to screen
    label_font = DpiAwareFont()
    label_font._set_qfont_px_size(_font.px_size - 6)
    br = label_font.boundingRect(f'{3.32:.1f}% port')
    w, h = br.width(), br.height()
    bar_h = 8/3 * w

    # PnL on lhs
    bar_labels_lhs = QVBoxLayout(fields)
    left_label = fields.add_field_label(
        dedent(f"""
        -{30}% PnL
        """),
        font_size=_font.px_size - 6,
        font_color='gunmetal',
    )

    bar_labels_lhs.addSpacing(5/8 * bar_h)
    bar_labels_lhs.addWidget(
        left_label,
        alignment=Qt.AlignLeft | Qt.AlignTop,
    )

    hbox = QHBoxLayout(fields)

    hbox.addLayout(bar_labels_lhs)
    # hbox.addSpacing(indent)  # push to right a bit

    # config
    # hbox.setSpacing(indent * 0.375)
    hbox.setSpacing(0)
    hbox.setAlignment(Qt.AlignTop | Qt.AlignLeft)
    hbox.setContentsMargins(0, 0, 0, 0)

    # TODO: use percentage str formatter:
    # https://docs.python.org/3/library/string.html#grammar-token-precision

    top_label = fields.add_field_label(
        dedent(f"""
        {3.32:.1f}% port
        """),
        font_size=_font.px_size - 6,
        font_color='gunmetal',
    )

    bottom_label = fields.add_field_label(
        dedent(f"""
        {5e3/4/1e3:.2f}k $fill\n
        """),
        font_size=_font.px_size - 6,
        font_color='gunmetal',
    )

    bar = QProgressBar(fields)

    hbox.addWidget(bar, alignment=Qt.AlignLeft | Qt.AlignTop)

    bar_labels_rhs_vbox = QVBoxLayout(fields)
    bar_labels_rhs_vbox.addWidget(
        top_label,
        alignment=Qt.AlignLeft | Qt.AlignTop
    )
    bar_labels_rhs_vbox.addWidget(
        bottom_label,
        alignment=Qt.AlignLeft | Qt.AlignBottom
    )

    hbox.addLayout(bar_labels_rhs_vbox)

    # compute "chunk" sizes for fill-status-bar based on some static height
    slots = 4
    border_size_px = 2
    slot_margin_px = 2

    # TODO: compute "used height" thus far and mostly fill the rest
    slot_height_px = math.floor(
        (bar_h - 2*border_size_px)/slots
    ) - slot_margin_px*1

    bar.setOrientation(Qt.Vertical)
    bar.setStyleSheet(
        f"""
        QProgressBar {{

            text-align: center;

            font-size : {fields._font_size - 2}px;

            background-color: {hcolor('papas_special')};
            color : {hcolor('papas_special')};

            border: {border_size_px}px solid {hcolor('default_light')};
            border-radius: 2px;
        }}

        QProgressBar::chunk {{

            background-color: {hcolor('default_spotlight')};
            color: {hcolor('papas_special')};

            border-radius: 2px;

            margin: {slot_margin_px}px;
            height: {slot_height_px}px;

        }}
        """
    )

    # margin-bottom: {slot_margin_px*2}px;
    # margin-top: {slot_margin_px*2}px;
    # color: #19232D;
    # width: 10px;

    bar.setRange(0, slots)
    bar.setValue(1)
    bar.setFormat('')
    bar.setMinimumHeight(bar_h)
    bar.setMaximumHeight(bar_h + slots*slot_margin_px)
    bar.setMinimumWidth(h * 1.375)
    bar.setMaximumWidth(h * 1.375)

    return hbox, bar


def mk_order_pane_layout(

    parent: QWidget,
    font_size: int = _font_small.px_size - 2

) -> FieldsForm:

    from ._position import mk_pp_alloc
    # TODO: some kinda pydantic sub-type
    # that enforces a composite widget attr er sumthin..
    # as part of our ORM thingers.
    allocator = mk_pp_alloc()

    # TODO: maybe just allocate the whole fields form here
    # and expect an async ctx entry?
    form = mk_form(
        parent=parent,
        model=allocator,
        fields_schema={
            'account': {
                'type': 'select',
                'default_value': [
                    'paper',
                    # 'ib.margin',
                    # 'ib.paper',
                ],
            },
            'size_unit': {
                'label': '**allocate**:',
                'type': 'select',
                'default_value': [
                    '$ size',
                    '% of port',
                    '# shares'
                ],
            },
            'disti_weight': {
                'label': '**weight**:',
                'type': 'select',
                'default_value': ['uniform'],
            },
            'size': {
                'label': '**size**:',
                'type': 'edit',
                'default_value': 5000,
            },
            'slots': {
                'type': 'edit',
                'default_value': 4,
            },
        },
    )
    form._font_size = font_size
    allocator._widget = form

    # top level pane layout
    # XXX: see ``FieldsForm.__init__()`` for why we can't do basic
    # config of the vbox here
    vbox = form.vbox

    # _, h = form.width(), form.height()
    # print(f'w, h: {w, h}')

    hbox, bar = mk_fill_status_bar(form, pane_vbox=vbox)

    # add pp fill bar + spacing
    vbox.addLayout(hbox, stretch=1/3)

    feed_label = form.add_field_label(
        dedent("""
        brokerd.ib\n
        |_@localhost:8509\n
        |_consumers: 4\n
        |_streams: 9\n
        """),
        font_size=_font.px_size - 5,
    )

    # add feed info label
    vbox.addWidget(
        feed_label,
        alignment=Qt.AlignBottom,
        stretch=1/3,
    )

    # TODO: handle resize events and appropriately scale this
    # to the sidepane height?
    # https://doc.qt.io/qt-5/layout.html#adding-widgets-to-a-layout
    vbox.setSpacing(36)

    form.show()

    return form
