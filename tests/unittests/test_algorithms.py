#!/usr/bin/env python

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lazy_bisect():

    scheduled = list()
    failed    = list()

    # --------------------------------------------------------------------------
    def schedule(n):

        if n in [61, 62, 63, 65]    or \
           n in list(range(22, 42)) or \
           n < 8:
            scheduled.append(n)
            return True

        else:
            failed.append(n)
            return False
    # --------------------------------------------------------------------------

    tasks = list(range(128 * 1024))
    tasks = ru.lazy_bisect(tasks, schedule)

    assert(len(failed) == 25), failed

    for task in tasks:
        assert(schedule(task) is False), task

    for task in scheduled:
        assert(task not in tasks), task


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_lazy_bisect()


# ------------------------------------------------------------------------------

