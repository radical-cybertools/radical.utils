#!/usr/bin/env python

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lazy_bisect():

    # -------------------------------------
    class Thing(object):
        def __init__(self, uid):
            self.uid = uid

        def __lt__(self, other): return self.uid <  other.uid             # noqa
        def __gt__(self, other): return self.uid >  other.uid             # noqa
        def __le__(self, other): return self.uid <= other.uid             # noqa
        def __ge__(self, other): return self.uid >= other.uid             # noqa
        def __eq__(self, other): return self.uid == other.uid             # noqa
        def __ne__(self, other): return self.uid != other.uid             # noqa
    # -------------------------------------

    failed = list()

    # --------------------------------------------------------------------------
    def schedule(n):

        if n.uid in [64, 66, 67, 68]    or \
           n.uid in list(range(22, 42)) or \
           n.uid < 8:
            return True

        else:
            failed.append(n)
            return False
    # --------------------------------------------------------------------------

    good, bad = ru.lazy_bisect(list(), schedule)
    assert(not good)
    assert(not bad)

    things = [Thing(x) for x in range(128)]
    good, bad = ru.lazy_bisect(things, schedule)

    assert(len(failed) == 12), [len(failed), failed]
    assert(len(things) == len(good) + len(bad))

    for task in good:
        assert(task not in bad), (task, bad)
        assert(schedule(task) is True), task

    for task in bad:
        assert(task not in good), (task, good)
        assert(schedule(task) is False), task

    assert(len(things) == len(good) + len(bad)), \
          [len(things),   len(good),  len(bad)]


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_lazy_bisect()


  # import pprofile
  # profiler = pprofile.Profile()
  #
  # with profiler:
  #     try:
  #         test_lazy_bisect()
  #     except:
  #         pass
  #
  # profiler.print_stats()


# ------------------------------------------------------------------------------

