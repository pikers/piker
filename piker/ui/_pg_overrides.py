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
Customization of ``pyqtgraph`` core routines and various types normally
for speedups.

Generally, our does not require "scentific precision" for pixel perfect
view transforms.

"""
from typing import Optional

import pyqtgraph as pg

from ._axes import Axis


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
    pg.PlotItem = PlotItem


# NOTE: the below customized type contains all our changes on a method
# by method basis as per the diff:
# https://github.com/pyqtgraph/pyqtgraph/commit/8e60bc14234b6bec1369ff4192dbfb82f8682920#diff-a2b5865955d2ba703dbc4c35ff01aa761aa28d2aeaac5e68d24e338bc82fb5b1R500

class PlotItem(pg.PlotItem):
    '''
    Overrides for the core plot object mostly pertaining to overlayed
    multi-view management as it relates to multi-axis managment.

    This object is the combination of a ``ViewBox`` and multiple
    ``AxisItem``s and so far we've added additional functionality and
    APIs for:
    - removal of axes

    ---

    From ``pyqtgraph`` super type docs:
    - Manage placement of ViewBox, AxisItems, and LabelItems
    - Create and manage a list of PlotDataItems displayed inside the
      ViewBox
    - Implement a context menu with commonly used display and analysis
      options

    '''
    def __init__(
        self,
        parent=None,
        name=None,
        labels=None,
        title=None,
        viewBox=None,
        axisItems=None,
        default_axes=['left', 'bottom'],
        enableMenu=True,
        **kargs
    ):
        super().__init__(
            parent=parent,
            name=name,
            labels=labels,
            title=title,
            viewBox=viewBox,
            axisItems=axisItems,
            # default_axes=default_axes,
            enableMenu=enableMenu,
            kargs=kargs,
        )
        self.name = name
        self.chart_widget = None
        # self.setAxisItems(
        #     axisItems,
        #     default_axes=default_axes,
        # )

    # NOTE: this is an entirely new method not in upstream.
    def removeAxis(
        self,
        name: str,
        unlink: bool = True,

    ) -> Optional[pg.AxisItem]:
        """
        Remove an axis from the contained axis items
        by ```name: str```.

        This means the axis graphics object will be removed
        from the ``.layout: QGraphicsGridLayout`` as well as unlinked
        from the underlying associated ``ViewBox``.

        If the ``unlink: bool`` is set to ``False`` then the axis will
        stay linked to its view and will only be removed from the
        layoutonly be removed from the layout.

        If no axis with ``name: str`` is found then this is a noop.

        Return the axis instance that was removed.

        """
        entry = self.axes.pop(name, None)

        if not entry:
            return

        axis = entry['item']
        self.layout.removeItem(axis)
        axis.scene().removeItem(axis)
        if unlink:
            axis.unlinkFromView()

        self.update()

        return axis

    # Why do we need to always have all axes created?
    #
    # I don't understand this at all.
    #
    # Everything seems to work if you just always apply the
    # set passed to this method **EXCEPT** for some super weird reason
    # the view box geometry still computes as though the space for the
    # `'bottom'` axis is always there **UNLESS** you always add that
    # axis but hide it?
    #
    # Why in tf would this be the case!?!?
    def setAxisItems(
        self,
        # XXX: yeah yeah, i know we can't use type annots like this yet.
        axisItems: Optional[dict[str, pg.AxisItem]] = None,
        add_to_layout: bool = True,
        default_axes: list[str] = ['left', 'bottom'],
    ):
        """
        Override axis item setting to only

        """
        axisItems = axisItems or {}

        # XXX: wth is is this even saying?!?
        # Array containing visible axis items
        # Also containing potentially hidden axes, but they are not
        # touched so it does not matter
        # visibleAxes = ['left', 'bottom']
        # Note that it does not matter that this adds
        # some values to visibleAxes a second time

        # XXX: uhhh wat^ ..?

        visibleAxes = list(default_axes) + list(axisItems.keys())

        # TODO: we should probably invert the loop here to not loop the
        # predefined "axis name set" and instead loop the `axisItems`
        # input and lookup indices from a predefined map.
        for name, pos in (
            ('top', (1, 1)),
            ('bottom', (3, 1)),
            ('left', (2, 0)),
            ('right', (2, 2))
        ):
            if (
                name in self.axes and
                name in axisItems
            ):
                # we already have an axis entry for this name
                # so remove the existing entry.
                self.removeAxis(name)

            # elif name not in axisItems:
            #     # this axis entry is not provided in this call
            #     # so remove any old/existing entry.
            #     self.removeAxis(name)

            # Create new axis
            if name in axisItems:
                axis = axisItems[name]
                if axis.scene() is not None:
                    if (
                        name not in self.axes
                        or axis != self.axes[name]["item"]
                    ):
                        raise RuntimeError(
                            "Can't add an axis to multiple plots. Shared axes"
                            " can be achieved with multiple AxisItem instances"
                            " and set[X/Y]Link.")

            else:
                # Set up new axis

                # XXX: ok but why do we want to add axes for all entries
                # if not desired by the user? The only reason I can see
                # adding this is without it there's some weird
                # ``ViewBox`` geometry bug.. where a gap for the
                # 'bottom' axis is somehow left in?
                # axis = pg.AxisItem(orientation=name, parent=self)
                axis = Axis(
                    self,
                    orientation=name,
                    parent=self,
                )

            axis.linkToView(self.vb)

            # XXX: shouldn't you already know the ``pos`` from the name?
            # Oh right instead of a global map that would let you
            # reasily look that up it's redefined over and over and over
            # again in methods..
            self.axes[name] = {'item': axis, 'pos': pos}

            # NOTE: in the overlay case the axis may be added to some
            # other layout and should not be added here.
            if add_to_layout:
                self.layout.addItem(axis, *pos)

            # place axis above images at z=0, items that want to draw
            # over the axes should be placed at z>=1:
            axis.setZValue(0.5)
            axis.setFlag(
                axis.GraphicsItemFlag.ItemNegativeZStacksBehindParent
            )
            if name in visibleAxes:
                self.showAxis(name, True)
            else:
                # why do we need to insert all axes to ``.axes`` and
                # only hide the ones the user doesn't specify? It all
                # seems to work fine without doing this except for this
                # weird gap for the 'bottom' axis that always shows up
                # in the view box geometry??
                self.hideAxis(name)

    def updateGrid(
        self,
        *args,
    ):
        alpha = self.ctrl.gridAlphaSlider.value()
        x = alpha if self.ctrl.xGridCheck.isChecked() else False
        y = alpha if self.ctrl.yGridCheck.isChecked() else False
        for name, dim in (
            ('top', x),
            ('bottom', x),
            ('left', y),
            ('right', y)
        ):
            if name in self.axes:
                self.getAxis(name).setGrid(dim)
        # self.getAxis('bottom').setGrid(x)
        # self.getAxis('left').setGrid(y)
        # self.getAxis('right').setGrid(y)
