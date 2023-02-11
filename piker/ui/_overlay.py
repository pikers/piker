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
from collections import defaultdict
from functools import partial
from typing import (
    Callable,
)

from pyqtgraph.graphicsItems.AxisItem import AxisItem
from pyqtgraph.graphicsItems.ViewBox import ViewBox
# from pyqtgraph.graphicsItems.GraphicsWidget import GraphicsWidget
from pyqtgraph.graphicsItems.PlotItem.PlotItem import PlotItem
from pyqtgraph.Qt.QtCore import (
    QObject,
    Signal,
    QEvent,
    Qt,
)
from pyqtgraph.Qt.QtWidgets import (
    # QGraphicsGridLayout,
    QGraphicsLinearLayout,
)

__all__ = ["PlotItemOverlay"]


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
    used verbatim as the "main plot" who's view box is given precedence
    for input handling. The main plot's axes are removed from its
    layout and placed in the surrounding exterior layouts to allow for
    re-ordering if desired.

    '''
    def __init__(
        self,
        pi: PlotItem,

    ) -> None:

        self.pitems: list[PlotItem] = []
        self._pi2axes: dict[  # TODO: use a ``bidict`` here?
            int,
            dict[str, AxisItem],
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
            layout.setMinimumWidth(0)

            if name in ('top', 'bottom'):
                orient = Qt.Vertical

            elif name in ('left', 'right'):
                orient = Qt.Horizontal

            layout.setOrientation(orient)

        self.insert_plotitem(
            0,
            pi,
            remove_axes=False,
        )

        # insert surrounding linear layouts into the parent pi's layout
        # such that additional axes can be appended arbitrarily without
        # having to expand or resize the parent's grid layout.
        for name, (linlayout, axes) in self.sides.items():

            # TODO: do we need this?
            # axis should have been removed during insert above
            index = _axes_layout_indices[name]
            axis = pi.layout.itemAt(*index)
            if axis and axis.isVisible():
                assert linlayout.itemAt(0) is axis

            # XXX: see comment in ``.insert_plotitem()``...
            # our `PlotItem.removeAxis()` does this internally.
            # pi.layout.removeItem(axis)

            pi.layout.addItem(linlayout, *index)
            layout = pi.layout.itemAt(*index)
            assert layout is linlayout

    def _register_item(
        self,
        index: int,
        plotitem: PlotItem,

    ) -> None:
        for name, axis_info in plotitem.axes.items():
            axis = axis_info['item']
            # register this plot's (maybe re-placed) axes for lookup.
            # print(f'inserting {name}:{axis} to index {index}')
            self._pi2axes.setdefault(name, {})[index] = axis

        # enter plot into list for index tracking
        self.pitems.insert(index, plotitem)

    def insert_plotitem(
        self,
        index: int,
        plotitem: PlotItem,

        remove_axes: bool = False,

    ) -> tuple[int, list[AxisItem]]:
        '''
        Place item at index by inserting all axes into the grid
        at list-order appropriate position.

        '''
        if index < 0:
            raise ValueError(
                '`.insert_plotitem()` only supports an index >= 0'
            )

        inserted_axes: list[AxisItem] = []

        # add plot's axes in sequence to the embedded linear layouts
        # for each "side" thus avoiding graphics collisions.
        for name, axis_info in plotitem.axes.copy().items():
            linlayout, axes = self.sides[name]
            axis = axis_info['item']
            inserted_axes.append(axis)

            if axis in axes:
                # TODO: re-order using ``.pop()`` ?
                ValueError(f'{axis} is already in {name} layout!?')

            # linking sanity
            axis_view = axis.linkedView()
            assert axis_view is plotitem.vb

            # if (
            #     not axis.isVisible()

            #     # XXX: we never skip moving the axes for the *root*
            #     # plotitem inserted (even if not shown) since we need to
            #     # move all the hidden axes into linear sub-layouts for
            #     # that "central" plot in the overlay. Also if we don't
            #     # do it there's weird geomoetry calc offsets that make
            #     # view coords slightly off somehow .. smh
            #     and not len(self.pitems) == 0
            # ):
            #     print(f'SKIPPING MOVE: {plotitem.name}:{name} -> {axis}')
            #     continue

            # invert insert index for layouts which are
            # not-left-to-right, top-to-bottom insert oriented
            insert_index = index
            if name in ('top', 'left'):
                insert_index = min(len(axes) - index, 0)
                assert insert_index >= 0

            linlayout.insertItem(insert_index, axis)
            axes.insert(index, axis)

        self._register_item(index, plotitem)

        if remove_axes:
            for name, axis_info in plotitem.axes.copy().items():
                axis = axis_info['item']
                # XXX: Remove old axis?
                # No, turns out we don't need this?
                # DON'T UNLINK IT since we need the original ``ViewBox`` to
                # still drive it with events/handlers B)
                popped = plotitem.removeAxis(name, unlink=False)
                assert axis is popped

        return (index, inserted_axes)

    def append_plotitem(
        self,
        item: PlotItem,

    ) -> (int, int):
        '''
        Append item's axes at indexes which put its axes "outside"
        previously overlayed entries.

        '''
        # for left and bottom axes we have to first remove
        # items and re-insert to maintain a list-order.
        return self.insert_plotitem(len(self.pitems), item)

    def get_axis(
        self,
        plot: PlotItem,
        name: str,

    ) -> AxisItem | None:
        '''
        Retrieve the named axis for overlayed ``plot`` or ``None``
        if axis for that name is not shown.

        '''
        index = self.pitems.index(plot)
        named = self._pi2axes[name]
        return named.get(index)

    # def pop(
    #     self,
    #     item: PlotItem,

    # ) -> PlotItem:
    #     '''
    #     Remove item and restack all axes in list-order.

    #     '''
    #     raise NotImplementedError


# Unimplemented features TODO:
# - 'A' (autobtn) should relay to all views
# - context menu single handler + relay?
# - layout unwind and re-pack for 'left' and 'top' axes
# - add labels to layout if detected in source ``PlotItem``

# UX nice-to-have TODO:
# - optional "focussed" view box support for view boxes
#   that have custom input handlers (eg. you might want to
#   scale the view to some "focussed" data view and have overlayed
#   viewboxes only respond to relayed events.)
# - figure out how to deal with menu raise events for multi-viewboxes.
#   (we might want to add a different menu which specs the name of the
#   view box currently being handled?
# - allow selection of a particular view box by interacting with its
#   axis?


class PlotItemOverlay:
    '''
    A composite for managing overlaid ``PlotItem`` instances such that
    you can make multiple graphics appear on the same graph with
    separate (non-colliding) axes apply ``ViewBox`` signal broadcasting
    such that all overlaid items respond to input simultaneously.

    '''
    def __init__(
        self,
        root_plotitem: PlotItem

    ) -> None:

        self.root_plotitem: PlotItem = root_plotitem
        self.relay_handlers: defaultdict[
            str,
            list[Callable],
        ] = defaultdict(list)

        # NOTE: required for scene layering/relaying; this guarantees
        # the "root" plot receives priority for interaction
        # events/signals.
        root_plotitem.vb.setZValue(10)

        self.layout = ComposedGridLayout(root_plotitem)
        self._relays: dict[str, Signal] = {}

    @property
    def overlays(self) -> list[PlotItem]:
        return self.layout.pitems

    def add_plotitem(
        self,
        plotitem: PlotItem,
        index: int | None = None,

        # event/signal names which will be broadcasted to all added
        # (relayee) ``PlotItem``s (eg. ``ViewBox.mouseDragEvent``).
        relay_events: list[str] = [],

        # (0,),  # link x
        # (1,),  # link y
        # (0, 1),  # link both
        link_axes: tuple[int] = (),

    ) -> tuple[int, list[AxisItem]]:

        root = self.root_plotitem
        vb: ViewBox = plotitem.vb

        # TODO: some sane way to allow menu event broadcast XD
        # vb.setMenuEnabled(False)

        # wire up any relay signal(s) from the source plot to added
        # "overlays". We use a plain loop instead of mucking with
        # re-connecting signal/slots which tends to be more invasive and
        # harder to implement and provides no measurable performance
        # gain.
        if relay_events:
            for ev_name in relay_events:
                relayee_handler: Callable[
                    [
                        ViewBox,
                        # lol, there isn't really a generic type thanks
                        # to the rewrite of Qt's event system XD
                        QEvent,

                        AxisItem | None,
                    ],
                    None,
                ] = getattr(vb, ev_name)

                sub_handlers: list[Callable] = self.relay_handlers[ev_name]

                # on the first registry of a relayed event we pop the
                # root's handler and override it to a custom broadcaster
                # routine.
                if not sub_handlers:

                    src_handler = getattr(
                       root.vb,
                       ev_name,
                    )

                    def broadcast(
                        ev: 'QEvent',

                        # TODO: drop this viewbox specific input and
                        # allow a predicate to be passed in by user.
                        axis: int | None = None,

                        *,

                        # these are bound in by the ``partial`` below
                        # and ensure a unique broadcaster per event.
                        ev_name: str = None,
                        src_handler: Callable = None,
                        relayed_from: 'ViewBox' = None,

                        # remaining inputs the source handler expects
                        **kwargs,

                    ) -> None:
                        '''
                        Broadcast signal or event: this is a source
                        event which will be relayed to attached
                        "relayee" plot item consumers.

                        The event is accepted halting any further
                        handlers from being triggered.

                        Sequence is,
                        - pre-relay to all consumers *first* - exactly
                          like how a ``Signal.emit()`` blocks until all
                          downstream relay handlers have run.
                        - run the event's source handler event

                        '''
                        ev.accept()

                        # broadcast first to relayees *first*. trigger
                        # relay of event to all consumers **before**
                        # processing/consumption in the source handler.
                        relayed_handlers = self.relay_handlers[ev_name]

                        assert getattr(vb, ev_name).__name__ == ev_name

                        # TODO: generalize as an input predicate
                        if axis is None:
                            for handler in relayed_handlers:
                                handler(
                                    ev,
                                    axis=axis,
                                    **kwargs,
                                )

                        # run "source" widget's handler last
                        src_handler(
                            ev,
                            axis=axis,
                        )

                    # dynamic handler override on the publisher plot
                    setattr(
                        root.vb,
                        ev_name,
                        partial(
                            broadcast,
                            ev_name=ev_name,
                            src_handler=src_handler
                        ),
                    )

                else:
                    assert getattr(root.vb, ev_name)
                    assert relayee_handler not in sub_handlers

                # append relayed-to widget's handler to relay table
                sub_handlers.append(relayee_handler)

        # link dim-axes to root if requested by user.
        for dim in link_axes:
            # link x and y axes to new view box such that the top level
            # viewbox propagates to the root (and whatever other
            # plotitem overlays that have been added).
            vb.linkView(dim, root.vb)

        # => NOTE: in order to prevent "more-then-linear" scaled
        # panning moves on (for eg. click-drag) certain range change
        # signals (i.e. ``.sigXRangeChanged``), the user needs to be
        # careful that any broadcasted ``relay_events`` are are short
        # circuited in sub-handlers (aka relayee's) implementations. As
        # an example if a ``ViewBox.mouseDragEvent`` is broadcasted, the
        # overlayed implementations need to be sure they either don't
        # also link the x-axes (by not providing ``link_axes=(0,)``
        # above) or that the relayee ``.mouseDragEvent()`` handlers are
        # ready to "``return`` early" in the case that
        # ``.sigXRangeChanged`` is emitted as part of linked axes.
        # For more details on such signalling mechanics peek in
        # ``ViewBox.linkView()``.

        # make overlaid viewbox impossible to focus since the top level
        # should handle all input and relay to overlays. Note that the
        # "root" plot item gettingn interaction priority is configured
        # with the ``.setZValue()`` during init.
        vb.setFlag(
            vb.GraphicsItemFlag.ItemIsFocusable,
            False
        )
        vb.setFocusPolicy(Qt.NoFocus)

        # => TODO: add a "focus" api for switching the "top level"
        # ``PlotItem`` dynamically.

        # append-compose into the layout all axes from this plot
        if index is None:
            insert_index, axes = self.layout.append_plotitem(plotitem)
        else:
            insert_index, axes = self.layout.insert_plotitem(index, plotitem)

        plotitem.vb.setGeometry(root.vb.sceneBoundingRect())

        def size_to_viewbox(vb: 'ViewBox'):
            plotitem.vb.setGeometry(root.vb.sceneBoundingRect())

        root.vb.sigResized.connect(size_to_viewbox)

        # ensure the overlayed view is redrawn on each cycle
        root.scene().sigPrepareForPaint.connect(vb.prepareForPaint)

        # focus state sanity
        vb.clearFocus()
        assert not vb.focusWidget()
        root.vb.setFocus()
        assert root.vb.focusWidget()

        vb.setZValue(100)

        return (
            index,
            axes,
        )

    def get_axis(
        self,
        plot: PlotItem,
        name: str,

    ) -> AxisItem:
        '''
        Retrieve the named axis for overlayed ``plot``.

        '''
        return self.layout.get_axis(plot, name)

    def get_axes(
        self,
        name: str,

    ) -> list[AxisItem]:
        '''
        Retrieve all axes for all plots with ``name: str``.

        If a particular overlay doesn't have a displayed named axis
        then it is not delivered in the returned ``list``.

        '''
        axes = []
        for plot in self.overlays:
            axis = self.layout.get_axis(plot, name)
            if axis:
                axes.append(axis)

        return axes

    # XXX: untested as of now.
    # TODO: need this as part of selecting a different root/source
    # plot to rewire interaction event broadcast dynamically.
    def _disconnect_all(
        self,
        plotitem: PlotItem,
    ) -> list[Signal]:
        '''
        Disconnects all signals related to this widget for the given chart.

        '''
        disconnected = []
        for pi, sig in self._relays.items():
            QObject.disconnect(sig)
            disconnected.append(sig)

        return disconnected

    # XXX: do we need this? Why would you build then destroy?
    # def remove_plotitem(self, plotItem: PlotItem) -> None:
    #     '''
    #     Remove this ``PlotItem`` from the overlayed set making not shown
    #     and unable to accept input.

    #     '''
    #     ...

    # TODO: i think this would be super hot B)
    # def focus_plotitem(self, plotitem: PlotItem) -> PlotItem:
    #     '''
    #     Apply focus to a contained PlotItem thus making it the "top level"
    #     item in the overlay able to accept peripheral's input from the user
    #     and responsible for zoom and panning control via its ``ViewBox``.

    #     '''
    #     ...
