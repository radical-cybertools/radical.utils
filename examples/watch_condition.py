#!/usr/bin/env python

# ------------------------------------------------------------------------------
#
# watch a test for a specific condition to be met, and return the value of the
# condition (or None on timeout).
#
# ------------------------------------------------------------------------------

import radical.utils as ru
import time

start = time.time()

def test():
    check = bool(time.time() - start > 10)
    print check
    return check

print ru.watch_condition(cond=test, target=True, timeout=2,    interval=1)
print ru.watch_condition(cond=test, target=True, timeout=None, interval=1)


