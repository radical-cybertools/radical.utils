
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


''' 
Unit tests for ru.Process()
'''

import os
import sys
import time

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_process_basic():
    '''
    start a 'sleep 1', and expect this to finish within 1.x seconds
    '''

    class P(ru.Process):
        def work(self):
            time.sleep(0.1)
            sys.exit(0) # only run once!

    p = P()
    start = time.time()
    p.start()
    p.join()
    stop = time.time()
    assert(stop-start > 0.0)
    assert(stop-start < 0.2)


# ------------------------------------------------------------------------------
#
def test_process_init_fail():
    '''
    make sure the parent gets notified on failing init
    '''

    class P(ru.Process):
        def initialize(self):
            raise RuntimeError('oops')
        def work(self):
            time.sleep(0.1)

    try:
        p = P()
        p.start()
    except RuntimeError as e:
        assert('oops' in str(e)), str(e)

    assert(not p.is_alive())


# ------------------------------------------------------------------------------
#
def test_process_final_fail():
    '''
    make sure the parent gets notified on failing finalize
    '''

    class P(ru.Process):
        def finalize(self):
            raise RuntimeError('oops')
        def work(self):
            sys.exit()  # run only once

    try:
        p = P()
        p.start()
        p.stop()
        p.join()
    except RuntimeError as e:
        assert('oops' in str(e)), str(e)

    assert(not p.is_alive())


# ------------------------------------------------------------------------------
#
def test_process_parent_fail():
    '''
    make sure the child dies when the parent dies
    '''

    class Child(ru.Process):
        def __init__(self, c_pid):
            self._c_pid = c_pid
            ru.Process.__init__(self)
        def work(self):
            self._c_pid.value = os.getpid()
            time.sleep(0.1)  # run forever

    class Parent(ru.Process):
        def __init__(self, c_pid):
            self._c_pid = c_pid
            ru.Process.__init__(self)
        def initialize(self):
            self._c = Child(self._c_pid)
            self._c.start()
            assert(self._c.is_alive())
        def work(self):
            sys.exit()  # parent dies

    import multiprocessing as mp
    c_pid = mp.Value('i', 0)
    p = Parent(c_pid)
    p.start()
    os.kill(c_pid.value, 0)  # child is alive
    os.kill(p.pid, 9)
    p.join()
    # leave some time for child to die
    time.sleep(0.01)
    try:
        os.kill(c_pid.value, 0)
    except OSError as e:
        pass # child is gone
    except:
        assert(False)

    assert(not p.is_alive())


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    N = 10000

    print 1
    for i in range(N):
        test_process_parent_fail()
        print '.',

    print 2
    for i in range(N):
        test_process_final_fail()
        print '.',

    print 3
    for i in range(N):
        test_process_init_fail()
        print '.',

    print 4
    for i in range(N):
        test_process_basic()
        print '.',


# ------------------------------------------------------------------------------

