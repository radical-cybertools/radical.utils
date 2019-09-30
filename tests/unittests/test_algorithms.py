#!/usr/bin/env python

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lazy_bisect():

    failed = list()

    # --------------------------------------------------------------------------
    def schedule(n):

        if n in [61, 62, 63, 65]    or \
           n in list(range(22, 42)) or \
           n < 8:
            return True

        else:
            failed.append(n)
            return False
    # --------------------------------------------------------------------------

    good, bad = ru.lazy_bisect(list(), schedule)
    assert(not good)
    assert(not bad)

    tasks = list(range(128 * 1024))
    good, bad = ru.lazy_bisect(tasks, schedule)

    assert(len(failed) == 25), failed

    assert(len(tasks) == len(good) + len(bad))

    for task in good:
        assert(task not in bad), task
        assert(schedule(task) is True), task

    for task in bad:
        assert(task not in good), task
        assert(schedule(task) is False), task


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_lazy_bisect()


# ------------------------------------------------------------------------------

