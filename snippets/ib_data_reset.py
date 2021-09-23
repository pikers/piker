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
IB api client data feed reset hack for i3.

'''
import subprocess

import i3ipc

i3 = i3ipc.Connection()
t = i3.get_tree()

# for tws
win_name = 'Interactive Brokers'  # what for gw tho?
con = t.find_named(win_name)[0]

win_id = con.window
win_width = con.rect.width

# TODO: seems to be a few libs for python but not sure
# if they support all the sub commands we need, order of
# most recent commit history:
# https://github.com/rr-/pyxdotool
# https://github.com/ShaneHutter/pyxdotool
# https://github.com/cphyc/pyxdotool

subprocess.call(
    f'xdotool windowactivate --sync {win_id} mousemove_relative '
    f'--sync {win_width} 1 click 1 key ctrl+alt+f'.split(' ')
)
