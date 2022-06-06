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
Feed status and controls widget(s) for embedding in a UI-pane.

"""

from __future__ import annotations
from textwrap import dedent
from typing import TYPE_CHECKING

# from PyQt5.QtCore import Qt

from ._style import _font, _font_small
# from ..calc import humanize
from ._label import FormatLabel

if TYPE_CHECKING:
    from ._chart import ChartPlotWidget
    from ..data.feed import Feed
    from ._forms import FieldsForm


def mk_feed_label(
    form: FieldsForm,
    feed: Feed,
    chart: ChartPlotWidget,

) -> FormatLabel:
    '''
    Generate a label from feed meta-data to be displayed
    in a UI sidepane.

    TODO: eventually buttons for changing settings over
    a feed control protocol.

    '''
    status = feed.status
    assert status

    msg = dedent("""
        actor: **{actor_name}**\n
        |_ @**{host}:{port}**\n
    """)

    for key, val in status.items():
        if key in ('host', 'port', 'actor_name'):
            continue
        msg += f'\n|_ {key}: **{{{key}}}**\n'

    feed_label = FormatLabel(
        fmt_str=msg,
        # |_ streams: **{symbols}**\n
        font=_font.font,
        font_size=_font_small.px_size,
        font_color='default_lightest',
    )

    # form.vbox.setAlignment(feed_label, Qt.AlignBottom)
    # form.vbox.setAlignment(Qt.AlignBottom)
    _ = chart.height() - (
        form.height() +
        form.fill_bar.height()
        # feed_label.height()
    )

    feed_label.format(**feed.status)

    return feed_label
