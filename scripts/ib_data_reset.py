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
IB api client data feed reset hack for i3.

'''
import subprocess

import i3ipc

i3 = i3ipc.Connection()
t = i3.get_tree()

orig_win_id = t.find_focused().window

# for tws
win_names: list[str] = [
    'Interactive Brokers',  # tws running in i3
    'IB Gateway',  # gw running in i3
    # 'IB',  # gw running in i3 (newer version?)
]

for name in win_names:
    results = t.find_titled(name)
    print(f'results for {name}: {results}')
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

        # TODO: only run the reconnect (2nd) kc on a detected
        # disconnect?
        for key_combo, timeout in [
            # only required if we need a connection reset.
            ('ctrl+alt+r', 12),
            # data feed reset.
            ('ctrl+alt+f', 6)
        ]:
            subprocess.call([
                'xdotool',
                'windowactivate', '--sync', win_id,

                # move mouse to bottom left of window (where there should
                # be nothing to click).
                'mousemove_relative', '--sync', str(w-4), str(h-4),

                # NOTE: we may need to stick a `--retry 3` in here..
                'click', '--window', win_id,
                '--repeat', '3', '1',

                # hackzorzes
                'key', key_combo,
                ],
                timeout=timeout,
            )

# re-activate and focus original window
subprocess.call([
    'xdotool',
    'windowactivate', '--sync', str(orig_win_id),
    'click', '--window', str(orig_win_id), '1',
])
