#!/usr/bin/env python3

import radical.utils as ru

from unittest import TestCase


class BisectTest(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_lazy_bisect(self):

        # -------------------------------------
        class Thing(object):
            def __init__(self, uid):
                self.uid = uid

            def __lt__(self, other): return self.uid <  other.uid         # noqa
            def __gt__(self, other): return self.uid >  other.uid         # noqa
            def __le__(self, other): return self.uid <= other.uid         # noqa
            def __ge__(self, other): return self.uid >= other.uid         # noqa
            def __eq__(self, other): return self.uid == other.uid         # noqa
            def __ne__(self, other): return self.uid != other.uid         # noqa
        # -------------------------------------

        failed = list()

        # ----------------------------------------------------------------------
        def schedule(n):

            if n.uid in [64, 66, 67, 68]    or \
               n.uid in list(range(22, 42)) or \
               n.uid < 8:
                return True

            else:
                failed.append(n)
                return False
        # ----------------------------------------------------------------------

        good, bad, fail = ru.lazy_bisect(list(), schedule)
        assert not good
        assert not bad
        assert not fail

        things = [Thing(x) for x in range(128)]
        good, bad, fail = ru.lazy_bisect(things, schedule)

        assert len(failed) == 12, [len(failed), failed]
        assert len(things) == len(good) + len(bad) + len(fail), \
              [len(things),   len(good),  len(bad),  len(fail)]

        for task in good:
            assert task not in bad, (task, bad)
            assert schedule(task) is True, task

        for task in bad:
            assert task not in good, (task, good)
            assert schedule(task) is False, task


    # --------------------------------------------------------------------------
    #
    def test_bisect_ratio(self):

        # ----------------------------------------------------------------------
        def schedule(n):
            self.call_count += 1
            return False
        # ----------------------------------------------------------------------
        #
        things = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]

        self.call_count = 0
        ru.lazy_bisect(things, schedule, ratio=0.5)
        assert self.call_count == 6

        self.call_count = 0
        ru.lazy_bisect(things, schedule, ratio=1.0)
        assert self.call_count == 10

        self.call_count = 0
        ru.lazy_bisect(things, schedule, ratio=0.0)
        assert self.call_count == 2


    # --------------------------------------------------------------------------
    #
    def test_collapse_ranges(self):

        tests = [
            # zero test
            [[],
             []],

            # basic test (unit)
            [[[1, 2], [3, 4]],
             [[1, 2], [3, 4]]],

            # range overlap -> combination
            [[[1, 2], [2, 3], [3, 4]],
             [[1, 4]]],

            # range repetition
            [[[1, 2], [2, 3], [1, 4]],
             [[1, 4]]],

            # complex results
            [[[1, 2], [2, 3], [4, 5], [7, 8]],
             [[1, 3], [4, 5], [7, 8]]],

            # range inversion
            [[[2, 1], [2, 3], [4, 5], [8, 7]],
             [[1, 3], [4, 5], [7, 8]]],

            # test zero range
            [[[1, 1], [7, 7], [7, 8]],
             [[1, 1], [7, 8]]],

            # range sorting
            [[[7, 8], [2, 1], [2, 3], [4, 5]],
             [[1, 3], [4, 5], [7, 8]]]
        ]

        for case, check in tests:
            assert check == ru.collapse_ranges(case), '\n%s\n%s' % (case, check)


    # --------------------------------------------------------------------------
    #
    def test_range_concurrency(self):

        tests = [
            # zero test
            [[],
             []],

            # basic test (unit)
            [[[1, 2]],
             [[1, 0], [1, 1], [2, 0]]],

            # basic test (unit)
            [[[1, 2], [3, 4]],
             [[1, 0], [1, 1], [2, 0], [3, 1], [4, 0]]],

            # range overlap -> combination
            [[[1, 2], [2, 3], [3, 4]],
             [[1, 0], [1, 1], [2, 1], [3, 1], [4, 0]]],

            # range repetition
            [[[1, 2], [2, 3], [1, 4]],
             [[1, 0], [1, 2], [2, 2], [3, 1], [4, 0]]],

            # complex results
            [[[1, 2], [2, 3], [4, 5], [7, 8]],
             [[1, 0], [1, 1], [2, 1], [3, 0], [4, 1], [5, 0], [7, 1], [8, 0]]],

            # range inversion
            [[[2, 1], [2, 3], [4, 5], [8, 7]],
             [[1, 0], [1, 1], [2, 1], [3, 0], [4, 1], [5, 0], [7, 1], [8, 0]]],

            # test zero range
            [[[1, 1], [7, 7], [7, 8]],
             [[1, 0], [1, 0], [7, 1], [8, 0]]],

            # range sorting
            [[[7, 8], [2, 1], [2, 3], [4, 5]],
             [[1, 0], [1, 1], [2, 1], [3, 0], [4, 1], [5, 0], [7, 1], [8, 0]]]
        ]

        for case, check in tests:
            print(case)
            assert check == ru.range_concurrency(case), \
                   '\n%s\n%s' % (case, check)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = BisectTest()

    tc.test_lazy_bisect()
    tc.test_bisect_ratio()
    tc.test_collapse_ranges()
    tc.test_range_concurrency()

    try:
        import pprofile
        profiler = pprofile.Profile()
    except ImportError:
        pass
    else:
        with profiler:
            try:
                tc.test_lazy_bisect()
            except:
                pass

        profiler.print_stats()


# ------------------------------------------------------------------------------

