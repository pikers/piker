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
Charting overlay helpers.

'''
from pyqtgraph.Qt.QtCore import (
    # QObject,
    # Signal,
    Qt,
    # QEvent,
)
from pyqtgraph.graphicsItems.AxisItem import AxisItem
# from pyqtgraph.graphicsItems.ViewBox import ViewBox
from pyqtgraph.graphicsItems.PlotItem.PlotItem import PlotItem
from pyqtgraph.Qt.QtWidgets import QGraphicsGridLayout, QGraphicsLinearLayout

# Define the layout "position" indices as to be passed
# to a ``QtWidgets.QGraphicsGridlayout.addItem()`` call:
# https://doc.qt.io/qt-5/qgraphicsgridlayout.html#addItem
# This was pulled from the internals of ``PlotItem.setAxisItem()``.
_axes_layout_indices: dict[str] = {
    # row incremented axes
    'top': (1, 1),
    'bottom': (3, 1),

    # view is @ (2, 1)

    # column incremented axes
    'left': (2, 0),
    'right': (2, 2),
}
# NOTE: To clarify this indexing, ``PlotItem.__init__()`` makes a grid
# with dimensions 4x3 and puts the ``ViewBox`` at postiion (2, 1) (aka
# row=2, col=1) in the grid layout since row (0, 1) is reserved for
# a title label and row 1 is for any potential "top" axis. Column 1
# is the "middle" (since 3 columns) and is where the plot/vb is placed.


class ComposedGridLayout:
    '''
    List-like interface to managing a sequence of overlayed
    ``PlotItem``s in the form:

    |    |     |    |    | top0     |    |    |     |    |
    |    |     |    |    | top1     |    |    |     |    |
    |    |     |    |    | ...      |    |    |     |    |
    |    |     |    |    | topN     |    |    |     |    |
    | lN | ... | l1 | l0 | ViewBox  | r0 | r1 | ... | rN |
    |    |     |    |    | bottom0  |    |    |     |    |
    |    |     |    |    | bottom1  |    |    |     |    |
    |    |     |    |    | ...      |    |    |     |    |
    |    |     |    |    | bottomN  |    |    |     |    |

    Where the index ``i`` in the sequence specifies the index
    ``<axis_name>i`` in the layout.

    The ``item: PlotItem`` passed to the constructor's grid layout is
    used verbatim as the "main plot" who's view box is give precedence
    for input handling. The main plot's axes are removed from it's
    layout and placed in the surrounding exterior layouts to allow for
    re-ordering if desired.

    '''
    def __init__(
        self,
        item: PlotItem,
        grid: QGraphicsGridLayout,
        reverse: bool = False,  # insert items to the "center"

    ) -> None:
        self.items: list[PlotItem] = []
        # self.grid = grid
        self.reverse = reverse

        # TODO: use a ``bidict`` here?
        self._pi2axes: dict[
            int,
            dict[str, AxisItem],
        ] = {}

        self._axes2pi: dict[
            AxisItem,
            dict[str, PlotItem],
        ] = {}

        # TODO: better name?
        # construct surrounding layouts for placing outer axes and
        # their legends and title labels.
        self.sides: dict[
            str,
            tuple[QGraphicsLinearLayout, list[AxisItem]]
        ] = {}

        for name, pos in _axes_layout_indices.items():
            layout = QGraphicsLinearLayout()
            self.sides[name] = (layout, [])

            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(0)

            if name in ('top', 'bottom'):
                orient = Qt.Vertical
            elif name in ('left', 'right'):
                orient = Qt.Horizontal

            layout.setOrientation(orient)

        self.insert(0, item)

        # insert surrounding linear layouts into the parent pi's layout
        # such that additional axes can be appended arbitrarily without
        # having to expand or resize the parent's grid layout.
        for name, (linlayout, axes) in self.sides.items():

            # TODO: do we need this?
            # axis should have been removed during insert above
            index = _axes_layout_indices[name]
            axis = item.layout.itemAt(*index)
            if axis and axis.isVisible():
                assert linlayout.itemAt(0) is axis

            # item.layout.removeItem(axis)
            item.layout.addItem(linlayout, *index)
            layout = item.layout.itemAt(*index)
            assert layout is linlayout

    def _register_item(
        self,
        index: int,
        plotitem: PlotItem,

    ) -> None:
        for name, axis_info in plotitem.axes.items():
            axis = axis_info['item']
            # register this plot's (maybe re-placed) axes for lookup.
            self._pi2axes.setdefault(index, {})[name] = axis
            self._axes2pi.setdefault(index, {})[name] = plotitem

        # enter plot into list for index tracking
        self.items.insert(index, plotitem)

    def insert(
        self,
        index: int,
        plotitem: PlotItem,

    ) -> (int, int):
        '''
        Place item at index by inserting all axes into the grid
        at list-order appropriate position.

        '''
        if index < 0:
            raise ValueError(f'`insert()` only supports an index >= 0')

        # add plot's axes in sequence to the embedded linear layouts
        # for each "side" thus avoiding graphics collisions.
        for name, axis_info in plotitem.axes.copy().items():
            linlayout, axes = self.sides[name]
            axis = axis_info['item']

            if axis in axes:
                # TODO: re-order using ``.pop()`` ?
                ValueError(f'{axis} is already in {name} layout!?')

            # linking sanity
            axis_view = axis.linkedView()
            assert axis_view is plotitem.vb

            if (
                not axis.isVisible()

                # XXX: we never skip moving the axes for the *first*
                # plotitem inserted (even if not shown) since we need to
                # move all the hidden axes into linear sub-layouts for
                # that "central" plot in the overlay. Also if we don't
                # do it there's weird geomoetry calc offsets that make
                # view coords slightly off somehow .. smh
                and not len(self.items) == 0
            ):
                continue

            # XXX: Remove old axis? No, turns out we don't need this?
            # DON'T unlink it since we the original ``ViewBox``
            # to still drive it B)
            # popped = plotitem.removeAxis(name, unlink=False)
            # assert axis is popped

            # invert insert index for layouts which are
            # not-left-to-right, top-to-bottom insert oriented
            if name in ('top', 'left'):
                index = min(len(axes) - index, 0)
                assert index >= 0

            linlayout.insertItem(index, axis)
            axes.insert(index, axis)

        self._register_item(index, plotitem)

        return index

    def append(
        self,
        item: PlotItem,

    ) -> (int, int):
        '''
        Append item's axes at indexes which put its axes "outside"
        previously overlayed entries.

        '''
        # for left and bottom axes we have to first remove
        # items and re-insert to maintain a list-order.
        return self.insert(len(self.items), item)

    def get_axis(
        self,
        plot: PlotItem,
        name: str,

    ) -> AxisItem:
        '''
        Retrieve the named axis for overlayed ``plot``.

        '''
        index = self.items.index(plot)
        return self._pi2axes[index][name]

    def pop(
        self,
        item: PlotItem,

    ) -> PlotItem:
        '''
        Remove item and restack all axes in list-order.

        '''
        raise NotImplemented
