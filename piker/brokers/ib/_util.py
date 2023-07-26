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
``ib`` utilities and hacks suitable for use in the backend and/or as
runnable script-programs.

'''
from __future__ import annotations
from functools import partial
from typing import (
    Literal,
    TYPE_CHECKING,
)
import subprocess

import tractor

from .._util import get_logger

if TYPE_CHECKING:
    from .api import Client
    from ib_insync import IB

log = get_logger('piker.brokers.ib')

_reset_tech: Literal[
    'vnc',
    'i3ipc_xdotool',

    # TODO: in theory we can use a different linux DE API or
    # some other type of similar window scanning/mgmt client
    # (on other OSs) to do the same.

] = 'vnc'


async def data_reset_hack(
    # vnc_host: str,
    client: Client,
    reset_type: Literal['data', 'connection'],

) -> None:
    '''
    Run key combos for resetting data feeds and yield back to caller
    when complete.

    NOTE: this is a linux-only hack around!

    There are multiple "techs" you can use depending on your infra setup:

    - if running ib-gw in a container with a VNC server running the most
      performant method is the `'vnc'` option.

    - if running ib-gw/tws locally, and you are using `i3` you can use
      the ``i3ipc`` lib and ``xdotool`` to send the appropriate click
      and key-combos automatically to your local desktop's java X-apps.

    https://interactivebrokers.github.io/tws-api/historical_limitations.html#pacing_violations

    TODOs:
        - a return type that hopefully determines if the hack was
          successful.
        - other OS support?
        - integration with ``ib-gw`` run in docker + Xorg?
        - is it possible to offer a local server that can be accessed by
          a client? Would be sure be handy for running native java blobs
          that need to be wrangle.

    '''
    ib_client: IB = client.ib

    # look up any user defined vnc socket address mapped from
    # a particular API socket port.
    api_port: str = str(ib_client.client.port)
    vnc_host: str
    vnc_port: int
    vnc_host, vnc_port = client.conf['vnc_addrs'].get(
        api_port,
        ('localhost', 3003)
    )

    no_setup_msg:str = (
        f'No data reset hack test setup for {vnc_host}!\n'
        'See setup @\n'
        'https://github.com/pikers/piker/tree/master/piker/brokers/ib'
    )
    global _reset_tech

    match _reset_tech:
        case 'vnc':
            try:
                await tractor.to_asyncio.run_task(
                    partial(
                        vnc_click_hack,
                        host=vnc_host,
                        port=vnc_port,
                    )
                )
            except OSError:
                if vnc_host != 'localhost':
                    log.warning(no_setup_msg)
                    return False

                try:
                    import i3ipc  # noqa  (since a deps dynamic check)
                except ModuleNotFoundError:
                    log.warning(no_setup_msg)
                    return False

                try:
                    i3ipc_xdotool_manual_click_hack()
                    _reset_tech = 'i3ipc_xdotool'
                    return True
                except OSError:
                    log.exception(no_setup_msg)
                    return False

        case 'i3ipc_xdotool':
            i3ipc_xdotool_manual_click_hack()

        case _ as tech:
            raise RuntimeError(f'{tech} is not supported for reset tech!?')

    # we don't really need the ``xdotool`` approach any more B)
    return True


async def vnc_click_hack(
    host: str,
    port: int,
    reset_type: str = 'data'
) -> None:
    '''
    Reset the data or network connection for the VNC attached
    ib gateway using magic combos.

    '''
    try:
        import asyncvnc
    except ModuleNotFoundError:
        log.warning(
            "In order to leverage `piker`'s built-in data reset hacks, install "
            "the `asyncvnc` project: https://github.com/barneygale/asyncvnc"
        )
        return

    # two different hot keys which trigger diff types of reset
    # requests B)
    key = {
        'data': 'f',
        'connection': 'r'
    }[reset_type]

    async with asyncvnc.connect(
        host,
        port=port,

        # TODO: doesn't work see:
        # https://github.com/barneygale/asyncvnc/issues/7
        # password='ibcansmbz',

    ) as client:

        # move to middle of screen
        # 640x1800
        client.mouse.move(
            x=500,
            y=500,
        )
        client.mouse.click()
        client.keyboard.press('Ctrl', 'Alt', key)  # keys are stacked


def i3ipc_xdotool_manual_click_hack() -> None:
    '''
    Do the data reset hack but expecting a local X-window using `xdotool`.

    '''
    import i3ipc
    i3 = i3ipc.Connection()

    # TODO: might be worth offering some kinda api for grabbing
    # the window id from the pid?
    # https://stackoverflow.com/a/2250879
    t = i3.get_tree()

    orig_win_id = t.find_focused().window

    # for tws
    win_names: list[str] = [
        'Interactive Brokers',  # tws running in i3
        'IB Gateway',  # gw running in i3
        # 'IB',  # gw running in i3 (newer version?)
    ]

    try:
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
                    # ('ctrl+alt+r', 12),
                    # data feed reset.
                    ('ctrl+alt+f', 6)
                ]:
                    subprocess.call([
                        'xdotool',
                        'windowactivate', '--sync', win_id,

                        # move mouse to bottom left of window (where
                        # there should be nothing to click).
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
    except subprocess.TimeoutExpired:
        log.exception('xdotool timed out?')
