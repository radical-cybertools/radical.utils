#!/usr/bin/env python

import os
os.environ['RADICAL_DEBUG'] = 'TRUE'

import time
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# This example demonstrates the DebugHelper class.  Once started, use the
# printed pid to send a SIGUSR1 like this:
#
#   kill -USR1 <pid>
#
# and the DebugHelper will print stack traces for all threads to stdout.
#
# ------------------------------------------------------------------------------
#
def worker_outer():
    worker_inner()


def worker_inner():
    cnt = 0
    print('worker starts')
    time.sleep(3)
    while cnt < 100:
        print(cnt)
        cnt += 1
        time.sleep(1)


# ------------------------------------------------------------------------------
#
print(os.getpid())
dh = ru.DebugHelper()
t  = ru.Thread(name='worker', target=worker_outer)
t.start()
time.sleep(50)
t.join()

