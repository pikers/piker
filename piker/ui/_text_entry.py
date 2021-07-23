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
Text entry widgets (mostly for configuration).

'''
from PyQt5 import QtCore, QtGui
from PyQt5 import QtWidgets

from ._search import FontAndChartAwareLineEdit
from ._style import hcolor, _font


class LabeledTextInput(QtGui.QWidget):

    def __init__(
        self,
        godwidget: 'GodWidget',  # type: ignore # noqa
        parent=None,

    ) -> None:
        super().__init__(parent or godwidget)

        # size it as we specify
        self.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )

        self.godwidget = godwidget

        # split layout for the (label:| text bar entry)
        self.hbox = QtGui.QHBoxLayout(self)
        self.hbox.setContentsMargins(0, 0, 0, 0)
        self.hbox.setSpacing(4)

        # add label to left of search bar
        self.label = label = QtGui.QLabel(parent=self)
        label.setTextFormat(3)  # markdown
        label.setFont(_font.font)
        label.setStyleSheet(
            f"QLabel {{ color : {hcolor('gunmetal')}; }}"
        )
        label.setMargin(4)
        label.setText("`$cap:`")
        label.setAlignment(
            QtCore.Qt.AlignVCenter
            | QtCore.Qt.AlignLeft
        )
        label.show()

        self.hbox.addWidget(label)

        self.edit = FontAndChartAwareLineEdit(
            parent=self,
            parent_chart=godwidget,
        )
        self.edit.set_width_in_chars(6)
        self.edit.setText('5000')
        self.hbox.addWidget(self.edit)

    def sizeHint(self) -> QtCore.QSize:
        """
        Scale edit box to size of dpi aware font.

        """
        return self.edit.sizeHint()
