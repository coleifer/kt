from gevent import monkey; monkey.patch_all()

import time

import gevent
from kt import KyotoTycoon


kt = KyotoTycoon()

def get_sleep_set(k, v, n=1):
    with kt:
        kt.set(k, v)
        time.sleep(n)
        kt.get(k)

n = 3
t = 256
start = time.time()

greenlets = []
for i in range(t):
    greenlets.append(
        gevent.spawn(get_sleep_set, 'k%d' % i, 'v%d' % i, n)
    )


for g in greenlets:
    g.join()

kt.clear()
kt.close()
stop = time.time()
print('done. slept=%s, took=%.2f for %s threads' % (n, stop - start, t))
