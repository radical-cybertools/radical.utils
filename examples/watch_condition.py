#!/usr/bin/env python

import radical.utils as ru
import time

start = time.time()

def test():
    check = bool(time.time() - start > 10)
    print check
    return check

print ru.watch_condition(cond=test, target=True, timeout=2,    interval=1)
print ru.watch_condition(cond=test, target=True, timeout=None, interval=1)


