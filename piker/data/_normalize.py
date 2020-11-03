"""
Stream format enforcement.
"""
from typing import AsyncIterator, Optional, Tuple

import numpy as np


def iterticks(
    quote: dict,
    types: Tuple[str] = ('trade', 'utrade'),
) -> AsyncIterator:
    """Iterate through ticks delivered per quote cycle.
    """
    # print(f"{quote}\n\n")
    ticks = quote.get('ticks', ())
    if ticks:
        for tick in ticks:
            print(f"{quote['symbol']}: {tick}")
            if tick.get('type') in types:
                yield tick
