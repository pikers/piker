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
from typing import Callable, Optional

from pyqtgraph.Qt.QtCore import (
    # QObject,
    # Signal,
    Qt,
    # QEvent,
)

from pyqtgraph.graphicsItems.AxisItem import AxisItem
from pyqtgraph.graphicsItems.ViewBox import ViewBox
from pyqtgraph.graphicsItems.GraphicsWidget import GraphicsWidget
from pyqtgraph.graphicsItems.PlotItem.PlotItem import PlotItem
from pyqtgraph.Qt.QtCore import QObject, Signal, QEvent
from pyqtgraph.Qt.QtWidgets import QGraphicsGridLayout, QGraphicsLinearLayout

from ._interaction import ChartView

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
            # print(f'inserting {name}:{axis} to index {index}')
            self._pi2axes.setdefault(name, {})[index] = axis

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
            raise ValueError('`insert()` only supports an index >= 0')

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
            insert_index = index
            if name in ('top', 'left'):
                insert_index = min(len(axes) - index, 0)
                assert insert_index >= 0

            linlayout.insertItem(insert_index, axis)
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

    ) -> Optional[AxisItem]:
        '''
        Retrieve the named axis for overlayed ``plot`` or ``None``
        if axis for that name is not shown.

        '''
        index = self.items.index(plot)
        named = self._pi2axes[name]
        return named.get(index)

    def pop(
        self,
        item: PlotItem,

    ) -> PlotItem:
        '''
        Remove item and restack all axes in list-order.

        '''
        raise NotImplementedError


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


# TODO: we might want to enabled some kind of manual flag to disable
# this method wrapping during type creation? As example a user could
# definitively decide **not** to enable broadcasting support by
# setting something like ``ViewBox.disable_relays = True``?
def mk_relay_method(

    signame: str,
    slot: Callable[
        [ViewBox,
         'QEvent',
         Optional[AxisItem]],
        None,
    ],

) -> Callable[
    [
        ViewBox,
        # lol, there isn't really a generic type thanks
        # to the rewrite of Qt's event system XD
        'QEvent',

        'Optional[AxisItem]',
        'Optional[ViewBox]',  # the ``relayed_from`` arg we provide
    ],
    None,
]:

    def maybe_broadcast(
        vb: 'ViewBox',
        ev: 'QEvent',
        axis: 'Optional[int]' = None,
        relayed_from: 'ViewBox' = None,

    ) -> None:
        '''
        (soon to be) Decorator which makes an event handler
        "broadcastable" to overlayed ``GraphicsWidget``s.

        Adds relay signals based on the decorated handler's name
        and conducts a signal broadcast of the relay signal if there
        are consumers registered.

        '''
        # When no relay source has been set just bypass all
        # the broadcast machinery.
        if vb.event_relay_source is None:
            ev.accept()
            return slot(
                vb,
                ev,
                axis=axis,
            )

        if relayed_from:
            assert axis is None

            # this is a relayed event and should be ignored (so it does not
            # halt/short circuit the graphicscene loop). Further the
            # surrounding handler for this signal must be allowed to execute
            # and get processed by **this consumer**.
            # print(f'{vb.name} rx relayed from {relayed_from.name}')
            ev.ignore()

            return slot(
                vb,
                ev,
                axis=axis,
            )

        if axis is not None:
            # print(f'{vb.name} handling axis event:\n{str(ev)}')
            ev.accept()
            return slot(
                vb,
                ev,
                axis=axis,
            )

        elif (
            relayed_from is None
            and vb.event_relay_source is vb  # we are the broadcaster
            and axis is None
        ):
            # Broadcast case: this is a source event which will be
            # relayed to attached consumers and accepted after all
            # consumers complete their own handling followed by this
            # routine's processing. Sequence is,
            # - pre-relay to all consumers *first* - ``.emit()`` blocks
            #   until all downstream relay handlers have run.
            # - run the source handler for **this** event and accept
            #   the event

            # Access the "bound signal" that is created
            # on the widget type as part of instantiation.
            signal = getattr(vb, signame)
            # print(f'{vb.name} emitting {signame}')

            # TODO/NOTE: we could also just bypass a "relay" signal
            # entirely and instead call the handlers manually in
            # a loop? This probably is a lot simpler and also doesn't
            # have any downside, and allows not touching target widget
            # internals.
            signal.emit(
                ev,
                axis,
                # passing this demarks a broadcasted/relayed event
                vb,
            )
            # accept event so no more relays are fired.
            ev.accept()

            # call underlying wrapped method with an extra
            # ``relayed_from`` value to denote that this is a relayed
            # event handling case.
            return slot(
                vb,
                ev,
                axis=axis,
            )

    return maybe_broadcast


# XXX: :( can't define signals **after** class compile time
# so this is not really useful.
# def mk_relay_signal(
#     func,
#     name: str = None,

# ) -> Signal:
#     (
#         args,
#         varargs,
#         varkw,
#         defaults,
#         kwonlyargs,
#         kwonlydefaults,
#         annotations
#     ) = inspect.getfullargspec(func)

#     # XXX: generate a relay signal with 1 extra
#     # argument for a ``relayed_from`` kwarg. Since
#     # ``'self'`` is already ignored by signals we just need
#     # to count the arguments since we're adding only 1 (and
#     # ``args`` will capture that).
#     numargs = len(args + list(defaults))
#     signal = Signal(*tuple(numargs * [object]))
#     signame = name or func.__name__ + 'Relay'
#     return signame, signal


def enable_relays(
    widget: GraphicsWidget,
    handler_names: list[str],

) -> list[Signal]:
    '''
    Method override helper which enables relay of a particular
    ``Signal`` from some chosen broadcaster widget to a set of
    consumer widgets which should operate their event handlers normally
    but instead of signals "relayed" from the broadcaster.

    Mostly useful for overlaying widgets that handle user input
    that you want to overlay graphically. The target ``widget`` type must
    define ``QtCore.Signal``s each with a `'Relay'` suffix for each
    name provided in ``handler_names: list[str]``.

    '''
    signals = []
    for name in handler_names:
        handler = getattr(widget, name)
        signame = name + 'Relay'
        # ensure the target widget defines a relay signal
        relay = getattr(widget, signame)
        widget.relays[signame] = name
        signals.append(relay)
        method = mk_relay_method(signame, handler)
        setattr(widget, name, method)

    return signals


enable_relays(
    ChartView,
    ['wheelEvent', 'mouseDragEvent']
)


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

        vb = root_plotitem.vb
        vb.event_relay_source = vb  # TODO: maybe change name?
        vb.setZValue(1000)  # XXX: critical for scene layering/relaying

        self.overlays: list[PlotItem] = []
        self.layout = ComposedGridLayout(
            root_plotitem,
            root_plotitem.layout,
        )
        self._relays: dict[str, Signal] = {}

    def add_plotitem(
        self,
        plotitem: PlotItem,
        index: Optional[int] = None,

        # TODO: we could also put the ``ViewBox.XAxis``
        # style enum here?
        # (0,),  # link x
        # (1,),  # link y
        # (0, 1),  # link both
        link_axes: tuple[int] = (),

    ) -> None:

        index = index or len(self.overlays)
        root = self.root_plotitem
        # layout: QGraphicsGridLayout = root.layout
        self.overlays.insert(index, plotitem)
        vb: ViewBox = plotitem.vb

        # mark this consumer overlay as ready to expect relayed events
        # from the root plotitem.
        vb.event_relay_source = root.vb

        # TODO: some sane way to allow menu event broadcast XD
        # vb.setMenuEnabled(False)

        # TODO: inside the `maybe_broadcast()` (soon to be) decorator
        # we need have checks that consumers have been attached to
        # these relay signals.
        if link_axes != (0, 1):

            # wire up relay signals
            for relay_signal_name, handler_name in vb.relays.items():
                # print(handler_name)
                # XXX: Signal class attrs are bound after instantiation
                # of the defining type, so we need to access that bound
                # version here.
                signal = getattr(root.vb, relay_signal_name)
                handler = getattr(vb, handler_name)
                signal.connect(handler)

        # link dim-axes to root if requested by user.
        # TODO: solve more-then-wanted scaled panning on click drag
        # which seems to be due to broadcast. So we probably need to
        # disable broadcast when axes are linked in a particular
        # dimension?
        for dim in link_axes:
            # link x and y axes to new view box such that the top level
            # viewbox propagates to the root (and whatever other
            # plotitem overlays that have been added).
            vb.linkView(dim, root.vb)

        # make overlaid viewbox impossible to focus since the top
        # level should handle all input and relay to overlays.
        # NOTE: this was solved with the `setZValue()` above!

        # TODO: we will probably want to add a "focus" api such that
        # a new "top level" ``PlotItem`` can be selected dynamically
        # (and presumably the axes dynamically sorted to match).
        vb.setFlag(
            vb.GraphicsItemFlag.ItemIsFocusable,
            False
        )
        vb.setFocusPolicy(Qt.NoFocus)

        # append-compose into the layout all axes from this plot
        self.layout.insert(index, plotitem)

        plotitem.setGeometry(root.vb.sceneBoundingRect())

        def size_to_viewbox(vb: 'ViewBox'):
            plotitem.setGeometry(vb.sceneBoundingRect())

        root.vb.sigResized.connect(size_to_viewbox)

        # ensure the overlayed view is redrawn on each cycle
        root.scene().sigPrepareForPaint.connect(vb.prepareForPaint)

        # focus state sanity
        vb.clearFocus()
        assert not vb.focusWidget()
        root.vb.setFocus()
        assert root.vb.focusWidget()

    # XXX: do we need this? Why would you build then destroy?
    def remove_plotitem(self, plotItem: PlotItem) -> None:
        '''
        Remove this ``PlotItem`` from the overlayed set making not shown
        and unable to accept input.

        '''
        ...

    # TODO: i think this would be super hot B)
    def focus_item(self, plotitem: PlotItem) -> PlotItem:
        '''
        Apply focus to a contained PlotItem thus making it the "top level"
        item in the overlay able to accept peripheral's input from the user
        and responsible for zoom and panning control via its ``ViewBox``.

        '''
        ...

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

    # TODO: i guess we need this if you want to detach existing plots
    # dynamically? XXX: untested as of now.
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
