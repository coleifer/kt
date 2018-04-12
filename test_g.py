from gevent import monkey; monkey.patch_all()

import time

import gevent
from kt import KyotoTycoon


kt = KyotoTycoon()

def get_sleep_set(k, v, n=1):
    kt.set(k, v)
    time.sleep(n)
    kt.get(k)


greenlets = []
for i in range(100):
    greenlets.append(
        gevent.spawn(get_sleep_set, 'k%d' % i, 'v%d' % i, 3)
    )


for g in greenlets:
    g.join()

print('done')
