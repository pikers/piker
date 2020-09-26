"""
Stream format enforcement.
"""
from typing import AsyncIterator, Optional

import numpy as np


def iterticks(
    quote: dict,
    type: str = 'trade',
) -> AsyncIterator:
    """Iterate through ticks delivered per quote cycle.
    """
    # print(f"{quote}\n\n")
    ticks = quote.get('ticks', ())
    if ticks:
        for tick in ticks:
            # print(tick)
            if tick.get('type') == type:
                yield tick
