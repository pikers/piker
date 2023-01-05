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
High level streaming graphics primitives.

This is an intermediate layer which associates real-time low latency
graphics primitives with underlying stream/flow related data structures
for fast incremental update.

'''
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)

import msgspec
import numpy as np
import pyqtgraph as pg
from PyQt5.QtGui import QPainterPath

from ..data._formatters import (
    IncrementalFormatter,
)
from ..data._pathops import (
    xy_downsample,
)
from ..log import get_logger
from .._profile import (
    Profiler,
)

if TYPE_CHECKING:
    from ._dataviz import Viz


log = get_logger(__name__)


class Renderer(msgspec.Struct):

    viz: Viz
    fmtr: IncrementalFormatter

    # output graphics rendering, the main object
    # processed in ``QGraphicsObject.paint()``
    path: QPainterPath | None = None
    fast_path: QPainterPath | None = None

    # downsampling state
    _last_uppx: float = 0
    _in_ds: bool = False

    def draw_path(
        self,
        x: np.ndarray,
        y: np.ndarray,
        connect: str | np.ndarray = 'all',
        path: QPainterPath | None = None,
        redraw: bool = False,

    ) -> QPainterPath:

        path_was_none = path is None

        if redraw and path:
            path.clear()

            # TODO: avoid this?
            if self.fast_path:
                self.fast_path.clear()

        path = pg.functions.arrayToQPath(
            x,
            y,
            connect=connect,
            finiteCheck=False,

            # reserve mem allocs see:
            # - https://doc.qt.io/qt-5/qpainterpath.html#reserve
            # - https://doc.qt.io/qt-5/qpainterpath.html#capacity
            # - https://doc.qt.io/qt-5/qpainterpath.html#clear
            # XXX: right now this is based on ad-hoc checks on a
            # hidpi 3840x2160 4k monitor but we should optimize for
            # the target display(s) on the sys.
            # if no_path_yet:
            #     graphics.path.reserve(int(500e3))
            # path=path,  # path re-use / reserving
        )

        # avoid mem allocs if possible
        if path_was_none:
            path.reserve(path.capacity())

        return path

    def render(
        self,

        new_read,
        array_key: str,
        profiler: Profiler,
        uppx: float = 1,

        # redraw and ds flags
        should_redraw: bool = False,
        new_sample_rate: bool = False,
        should_ds: bool = False,
        showing_src_data: bool = True,

        do_append: bool = True,
        use_fpath: bool = True,

        # only render datums "in view" of the ``ChartView``
        use_vr: bool = True,

    ) -> tuple[QPainterPath, bool]:
        '''
        Render the current graphics path(s)

        There are (at least) 3 stages from source data to graphics data:
        - a data transform (which can be stored in additional shm)
        - a graphics transform which converts discrete basis data to
          a `float`-basis view-coords graphics basis. (eg. ``ohlc_flatten()``,
          ``step_path_arrays_from_1d()``, etc.)

        - blah blah blah (from notes)

        '''
        # TODO: can the renderer just call ``Viz.read()`` directly?
        # unpack latest source data read
        fmtr = self.fmtr

        (
            _,
            _,
            array,
            ivl,
            ivr,
            in_view,
        ) = new_read

        # xy-path data transform: convert source data to a format
        # able to be passed to a `QPainterPath` rendering routine.
        fmt_out = fmtr.format_to_1d(
            new_read,
            array_key,
            profiler,

            slice_to_inview=use_vr,
        )

        # no history in view case
        if not fmt_out:
            # XXX: this might be why the profiler only has exits?
            return

        (
            x_1d,
            y_1d,
            connect,
            prepend_length,
            append_length,
            view_changed,
            # append_tres,

        ) = fmt_out

        # redraw conditions
        if (
            prepend_length > 0
            or new_sample_rate
            or view_changed

            # NOTE: comment this to try and make "append paths"
            # work below..
            or append_length > 0
        ):
            should_redraw = True

        path: QPainterPath = self.path
        fast_path: QPainterPath = self.fast_path
        reset: bool = False

        self.viz.yrange = None

        # redraw the entire source data if we have either of:
        # - no prior path graphic rendered or,
        # - we always intend to re-render the data only in view
        if (
            path is None
            or should_redraw
        ):
            # print(f"{self.viz.name} -> REDRAWING BRUH")
            if new_sample_rate and showing_src_data:
                log.info(f'DE-downsampling -> {array_key}')
                self._in_ds = False

            elif should_ds and uppx > 1:

                x_1d, y_1d, ymn, ymx = xy_downsample(
                    x_1d,
                    y_1d,
                    uppx,
                )
                self.viz.yrange = ymn, ymx
                # print(f'{self.viz.name} post ds: ymn, ymx: {ymn},{ymx}')

                reset = True
                profiler(f'FULL PATH downsample redraw={should_ds}')
                self._in_ds = True

            path = self.draw_path(
                x=x_1d,
                y=y_1d,
                connect=connect,
                path=path,
                redraw=True,
            )

            profiler(
                'generated fresh path. '
                f'(should_redraw: {should_redraw} '
                f'should_ds: {should_ds} new_sample_rate: {new_sample_rate})'
            )

        # TODO: get this piecewise prepend working - right now it's
        # giving heck on vwap...
        # elif prepend_length:

        #     prepend_path = pg.functions.arrayToQPath(
        #         x[0:prepend_length],
        #         y[0:prepend_length],
        #         connect='all'
        #     )

        #     # swap prepend path in "front"
        #     old_path = graphics.path
        #     graphics.path = prepend_path
        #     # graphics.path.moveTo(new_x[0], new_y[0])
        #     graphics.path.connectPath(old_path)

        elif (
            append_length > 0
            and do_append
        ):
            profiler(f'sliced append path {append_length}')
            # (
            #     x_1d,
            #     y_1d,
            #     connect,
            # ) = append_tres

            profiler(
                f'diffed array input, append_length={append_length}'
            )

            # if should_ds and uppx > 1:
            #     new_x, new_y = xy_downsample(
            #         new_x,
            #         new_y,
            #         uppx,
            #     )
            #     profiler(f'fast path downsample redraw={should_ds}')

            append_path = self.draw_path(
                x=x_1d,
                y=y_1d,
                connect=connect,
                path=fast_path,
            )
            profiler('generated append qpath')

            if use_fpath:
                # an attempt at trying to make append-updates faster..
                if fast_path is None:
                    fast_path = append_path
                    # fast_path.reserve(int(6e3))
                else:
                    # print(
                    #     f'{self.viz.name}: FAST PATH\n'
                    #     f"append_path br: {append_path.boundingRect()}\n"
                    #     f"path size: {size}\n"
                    #     f"append_path len: {append_path.length()}\n"
                    #     f"fast_path len: {fast_path.length()}\n"
                    # )

                    fast_path.connectPath(append_path)
                    size = fast_path.capacity()
                    profiler(f'connected fast path w size: {size}')

                    # graphics.path.moveTo(new_x[0], new_y[0])
                    # path.connectPath(append_path)

                    # XXX: lol this causes a hang..
                    # graphics.path = graphics.path.simplified()
            else:
                size = path.capacity()
                profiler(f'connected history path w size: {size}')
                path.connectPath(append_path)

        self.path = path
        self.fast_path = fast_path

        return self.path, reset
