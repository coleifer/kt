#!/usr/bin/env python

"""
Benchmark script to ensure that *kt* plays nice with gevent. We spawn a number
of green threads, each of which calls a Lua script that sleeps -- effectively
blocking the socket. If gevent is working, then we should see the threads all
finishing at about the same time.
"""

from gevent import monkey; monkey.patch_all()
import gevent
import os
import sys
import time

from kt import *


nsec = 1
nthreads = 16
print('\x1b[1;33m%s green threads, sleeping for %s seconds' % (nthreads, nsec))
print('\x1b[0m')

curdir = os.path.dirname(__file__)
script = os.path.join(curdir, 'scripts/ttbench.lua')


# TokyoTyrant runs lua scripts in a dedicated thread, so we have only as much
# concurrency as worker threads.
server = EmbeddedTokyoTyrantServer(server_args=['-ext', script,
                                                '-thnum', str(nthreads)])
server.run()


def call_slow_script(nsec):
    tt = server._create_client()
    s = time.time()
    tt.script('sleep', key=str(nsec))

threads = []
start = time.time()
for i in range(nthreads):
    threads.append(gevent.spawn(call_slow_script, nsec))

for t in threads:
    t.join()

total = time.time() - start
if total >= (nsec * nthreads):
    sys.stdout.write(b'\x1b[1;31mFAIL! ')
else:
    sys.stdout.write(b'\x1b[1;32mOK! ')
print('TOTAL TIME: %0.3fs\x1b[0m' % total)
server.stop()
