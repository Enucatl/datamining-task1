import logging
import itertools
import numpy as np

logger = logging.getLogger(__name__)

b = 42
r = 23
n = b * r
# n = 24
# t = 0.85

a_s = np.random.randint(1, 10000, size=n)
b_s = np.random.randint(0, 10000, size=n)

def mapper(key, value):
    # key: None
    # value: one line of input file
    key = int(value.split()[0].split("_")[1])
    shingles = np.array(
        sorted([int(x) for x in value.split()[1:]])
    )
    logger.debug(key)
    logger.debug(shingles)
    logger.debug(shingles.shape)
    outer = np.outer(shingles, a_s)
    logger.debug(outer.shape)
    hashes = np.mod((outer + b_s), 8192)
    logger.debug(hashes.shape)
    min_hash = np.min(hashes, axis=0)
    logger.debug(min_hash.shape)
    for band in np.split(min_hash, b):
        logger.debug(band.shape)
        yield hash(band.tostring()), key


def reducer(key, values):
    sorted_values = sorted(values)
    for value1, value2 in itertools.combinations(sorted_values, 2):
        yield value1, value2
