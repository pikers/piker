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
Customization of ``pyqtgraph`` core routines to speed up our use mostly
based on not requiring "scentific precision" for pixel perfect view
transforms.

"""
import pyqtgraph as pg


def invertQTransform(tr):
    """Return a QTransform that is the inverse of *tr*.
    Raises an exception if tr is not invertible.

    Note that this function is preferred over QTransform.inverted() due to
    bugs in that method. (specifically, Qt has floating-point precision issues
    when determining whether a matrix is invertible)

    """
    # see https://doc.qt.io/qt-5/qtransform.html#inverted

    # NOTE: if ``invertable == False``, ``qt_t`` is an identity
    qt_t, invertable = tr.inverted()

    return qt_t


def _do_overrides() -> None:
    """Dooo eeet.

    """
    # we don't care about potential fp issues inside Qt
    pg.functions.invertQTransform = invertQTransform
