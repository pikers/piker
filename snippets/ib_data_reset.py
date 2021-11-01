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
win_names: list[str] = [
    'Interactive Brokers',  # tws running in i3
    'IB Gateway.',  # gw running in i3
]

for name in win_names:
    results = t.find_named(name)
    if results:
        con = results[0]
        print(f'Resetting data feed for {name}')
        win_id = str(con.window)
        w, h = con.rect.width, con.rect.height

        # TODO: seems to be a few libs for python but not sure
        # if they support all the sub commands we need, order of
        # most recent commit history:
        # https://github.com/rr-/pyxdotool
        # https://github.com/ShaneHutter/pyxdotool
        # https://github.com/cphyc/pyxdotool
        subprocess.call([
            'xdotool',
            'windowactivate', '--sync', win_id,

            # move mouse to bottom left of window (where there should
            # be nothing to click).
            'mousemove_relative', '--sync', str(w-3), str(h-3),

            # NOTE: we may need to stick a `--retry 3` in here..
            'click', '--window', win_id, '1',

            # hackzorzes
            'key', 'ctrl+alt+f',
        ])
