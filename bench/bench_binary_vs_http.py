#!/usr/bin/env python

"""
Benchmark script to measure time taken to read, write and delete using the
binary protocol and HTTP protocol.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import contextlib
import time

from kt import *


server = EmbeddedServer(quiet=True)
server.run()
db = server.client


def do_set_bulk(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(0, nrows, chunksize):
        accum = {kfmt % j: vfmt % j for j in range(i, i + chunksize)}
        db.set_bulk(accum)

def do_set_bulk_http(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    vfmt = '%%0%sd' % vlen
    for i in range(0, nrows, chunksize):
        accum = {kfmt % j: vfmt % j for j in range(i, i + chunksize)}
        db._protocol_http.set_bulk(accum)

def do_get_bulk(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    for i in range(0, nrows, chunksize):
        accum = [kfmt % j for j in range(i, i + chunksize)]
        db.get_bulk(accum)

def do_get_bulk_http(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    for i in range(0, nrows, chunksize):
        accum = [kfmt % j for j in range(i, i + chunksize)]
        db._protocol_http.get_bulk(accum)

def do_remove_bulk(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    for i in range(0, nrows, chunksize):
        accum = [kfmt % j for j in range(i, i + chunksize)]
        db.remove_bulk(accum)

def do_remove_bulk_http(nrows, chunksize, klen, vlen):
    kfmt = '%%0%sd' % klen
    for i in range(0, nrows, chunksize):
        accum = [kfmt % j for j in range(i, i + chunksize)]
        db._protocol_http.remove_bulk(accum)

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
    with timed('set_bulk', nrows, chunksiz, ksiz, vsiz):
        do_set_bulk(nrows, chunksiz, ksiz, vsiz)
    with timed('get_bulk', nrows, chunksiz, ksiz, vsiz):
        do_get_bulk(nrows, chunksiz, ksiz, vsiz)
    with timed('remove_bulk', nrows, chunksiz, ksiz, vsiz):
        do_remove_bulk(nrows, chunksiz, ksiz, vsiz)

    db.clear()
    with timed('set_bulk_http', nrows, chunksiz, ksiz, vsiz):
        do_set_bulk_http(nrows, chunksiz, ksiz, vsiz)
    with timed('get_bulk_http', nrows, chunksiz, ksiz, vsiz):
        do_get_bulk_http(nrows, chunksiz, ksiz, vsiz)
    with timed('remove_bulk_http', nrows, chunksiz, ksiz, vsiz):
        do_remove_bulk_http(nrows, chunksiz, ksiz, vsiz)
    db.clear()
    print('\n')

try:
    server.stop()
except OSError:
    pass

