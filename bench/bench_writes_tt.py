#!/usr/bin/env python

"""
Benchmark script to measure time taken to set values using a variety of
different methods (set, set_bulk, setnr, setnr_bulk).
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import contextlib
import time

from kt import *


server = EmbeddedTokyoTyrantServer(quiet=True)
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

def do_setnr(nrows, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(nrows):
        db.setnr(kfmt % i, vfmt % i)

def do_setnr_bulk(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(0, nrows, chunksize):
        accum = {kfmt % j: vfmt % j for j in range(i, i + chunksize)}
        db.setnr_bulk(accum)

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

    with timed('setnr', nrows, ksiz, vsiz):
        do_setnr(nrows, ksiz, vsiz)
    db.clear()

    with timed('set_bulk', nrows, chunksiz, ksiz, vsiz):
        do_set_bulk(nrows, chunksiz, ksiz, vsiz)
    db.clear()

    with timed('setnr_bulk', nrows, chunksiz, ksiz, vsiz):
        do_setnr_bulk(nrows, chunksiz, ksiz, vsiz)
    db.clear()
    print('\n')

try:
    server.stop()
except OSError:
    pass
