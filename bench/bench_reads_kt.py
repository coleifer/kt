#!/usr/bin/env python

"""
Benchmark script to measure time taken to read values using a variety of
different methods.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import contextlib
import time

from kt import *


# In-memory btree.
server = EmbeddedServer(database='%', quiet=True)
server.run()
db = server.client


def do_get(nrows, kprefix, klen):
    kfmt = '%s%%0%sd' % (kprefix, klen)
    for i in range(nrows):
        db.get(kfmt % i)

def do_get_bulk(nrows, chunksize, kprefix, klen):
    kfmt = '%s%%0%sd' % (kprefix, klen)
    for i in range(0, nrows, chunksize):
        keys = [kfmt % j for j in range(i, i + chunksize)]
        db.get_bulk(keys)

def do_match_prefix(nrows, chunksize, kprefix, klen):
    kfmt = '%s%%0%sd' % (kprefix, klen)
    for i in range(0, nrows, chunksize):
        prefix = (kfmt % i)[:-(len(str(chunksize)) - 1)]
        db.match_prefix(prefix, chunksize)

def do_match_regex(nrows, chunksize, kprefix, klen):
    kfmt = '%s%%0%sd' % (kprefix, klen)
    for i in range(0, nrows, chunksize):
        regex = (kfmt % i)[:-(len(str(chunksize)) - 1)]
        db.match_regex(regex + '*', chunksize)

def do_keys_nonlazy():
    for _ in db.keys_nonlazy():
        pass

def do_keys():
    for _ in db.keys():
        pass

def do_items():
    for _ in db.items():
        pass


@contextlib.contextmanager
def timed(msg, *params):
    pstr = ', '.join(map(str, params))
    s = time.time()
    yield
    print('%0.3fs - %s(%s)' % (time.time() - s, msg, pstr))


SETTINGS = (
    # (nrows, chunksiz, kprefix, ksiz, vsiz).
    (100000, 10000, 'a', 48, 512),
    (25000, 1250, 'b', 256, 1024 * 4),
    (1700, 100, 'c', 256, 1024 * 64),
)

# Setup database.

for nrows, chunksiz, kprefix, ksiz, vsiz in SETTINGS:
    for i in range(0, nrows, chunksiz):
        kfmt = '%s%%0%sd' % (kprefix, ksiz)
        vfmt = '%%0%sd' % (vsiz)
        accum = {kfmt % j: vfmt % j for j in range(i, i + chunksiz)}
        db.set_bulk(accum)

    mbsize = db.size / (1024. * 1024.)
    print('database initialized, size: %.fMB, %s records' % (mbsize, len(db)))

    with timed('get', nrows, kprefix, ksiz):
        do_get(nrows, kprefix, ksiz)

    with timed('get_bulk', nrows, chunksiz, kprefix, ksiz):
        do_get_bulk(nrows, chunksiz, kprefix, ksiz)

    with timed('match_prefix', nrows, chunksiz, kprefix, ksiz):
        do_match_prefix(nrows, chunksiz, kprefix, ksiz)

    with timed('match_regex', nrows, chunksiz, kprefix, ksiz):
        do_match_regex(nrows, chunksiz, kprefix, ksiz)

    with timed('keys (nonlazy)'):
        do_keys_nonlazy()

    #with timed('keys'):
    #    do_keys()

    #with timed('items'):
    #    do_items()

    print('\n')
    db.clear()

try:
    server.stop()
except OSError:
    pass
