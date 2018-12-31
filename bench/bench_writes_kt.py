#!/usr/bin/env python

"""
Benchmark script to measure time taken to set values using a variety of
different methods (set, set_bulk, set via http, set_bulk via http).
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import contextlib
import time

from kt import *


server = EmbeddedServer(quiet=True)
server.run()
db = server.client


def do_set(nrows, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(nrows):
        db.set(kfmt % i, vfmt % i)

def do_set_bulk(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(0, nrows, chunksize):
        accum = {kfmt % j: vfmt % j for j in range(i, i + chunksize)}
        db.set_bulk(accum)

def do_set_http(nrows, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(nrows):
        db._http.set(kfmt % i, vfmt % i)

def do_set_bulk_http(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(0, nrows, chunksize):
        accum = {kfmt % j: vfmt % j for j in range(i, i + chunksize)}
        db._http.set_bulk(accum)

@contextlib.contextmanager
def timed(msg, *params):
    pstr = ', '.join(map(str, params))
    s = time.time()
    yield
    print('%0.3fs - %s(%s)' % (time.time() - s, msg, pstr))


SETTINGS = (
    # (nrows, chunksiz, ksiz, vsiz).
    (200000, 10000, 48, 512),  # ~100MB of data, 20 batches.
    (25000, 1250, 256, 1024 * 4),  # ~100MB of data, 20 batches.
    (1700, 100, 256, 1024 * 64),  # ~100MB of data, 17 batches.
)
for nrows, chunksiz, ksiz, vsiz in SETTINGS:
    with timed('set', nrows, ksiz, vsiz):
        do_set(nrows, ksiz, vsiz)
    db.clear()

    # Lots of small requests is incredibly slow, so avoid pointless benchmark.
    if nrows < 25000:
        with timed('set_http', nrows, ksiz, vsiz):
            do_set_http(nrows, ksiz, vsiz)
        db.clear()

    with timed('set_bulk', nrows, chunksiz, ksiz, vsiz):
        do_set_bulk(nrows, chunksiz, ksiz, vsiz)
    db.clear()

    with timed('set_bulk_http', nrows, chunksiz, ksiz, vsiz):
        do_set_bulk_http(nrows, chunksiz, ksiz, vsiz)
    db.clear()
    print('\n')

try:
    server.stop()
except OSError:
    pass
