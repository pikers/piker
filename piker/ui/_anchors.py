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

'''
Anchor funtions for UI placement of annotions.

'''


def marker_right_points(

    chart: 'ChartPlotWidget',  # noqa
    marker_size: int = 20,

) -> (float, float, float):

    # chart = self._chart
    l1_len = chart._max_l1_line_len
    ryaxis = chart.getAxis('right')

    r_axis_x = ryaxis.pos().x()
    up_to_l1_sc = r_axis_x - l1_len

    marker_right = up_to_l1_sc - (1.375 * 2 * marker_size)
    line_end = marker_right - (6/16 * marker_size)

    return line_end, marker_right, r_axis_x
