"""
Handy financial calculations.
"""
import math
import itertools


def humanize(number):
    """Convert large numbers to something with at most 3 digits and
    a letter suffix (eg. k: thousand, M: million, B: billion).
    """
    mag2suffix = {3: 'k', 6: 'M', 9: 'B'}
    mag = math.floor(math.log(number, 10))
    maxmag = max(itertools.takewhile(lambda key: mag >= key, mag2suffix))
    return "{:.3f}{}".format(number/10**maxmag, mag2suffix[maxmag])
