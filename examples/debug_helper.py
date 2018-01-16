#!/usr/bin/env python

import os
os.environ['RADICAL_DEBUG'] = 'TRUE'

import time
import radical.utils as ru

# ------------------------------------------------------------------------------
#
def worker_outer():
    worker_inner()

def worker_inner():
    cnt = 0
    print 'worker starts'
    time.sleep(3)
    while cnt < 100:
        print cnt
        cnt += 1
        time.sleep(1)

# ------------------------------------------------------------------------------
#
print os.getpid()
dh = ru.DebugHelper()
t  = ru.Thread(name='worker', target=worker_outer)
t.start()
time.sleep(50)
t.join()

