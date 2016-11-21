
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
    start a 'sleep 0.1', and expect this to finish within 0.x seconds
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
    assert(stop-start > 0.1)
    assert(stop-start < 1.0)


# ------------------------------------------------------------------------------
#
def test_process_init_fail():
    '''
    make sure the parent gets notified on failing init
    '''

    class P(ru.Process):
        def initialize_child(self):
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
        def finalize_child(self):
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

    class Parent(ru.Process):

        def __init__(self, c_pid):
            self._c_pid = c_pid
          # time.sleep(1)
            ru.Process.__init__(self)

        def initialize_child(self):
            self._c = Child(self._c_pid)
            self._c.start()
            assert(self._c.is_alive())
            self.to_watch(self._c)

        def work(self):
          # print 'parent.work'
            sys.exit()  # parent dies

        def finalize_child(self):
            self._c.terminate()
            self._c.join()
          # print ' ============= parent.final'


    class Child(ru.Process):
        def __init__(self, c_pid):
            self._c_pid = c_pid
          # time.sleep(2)
            ru.Process.__init__(self)

        def work(self):
          # print 'child.work'
            self._c_pid.value = os.getpid()
            time.sleep(0.1)  # run forever

        def finalize_child(self):
            pass
          # print ' ============= child.final'

    import multiprocessing as mp
    c_pid = mp.Value('i', 0)
    p = Parent(c_pid)
    p.start()
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

    N = 1000

    for i in range(N):
        test_process_final_fail()
        print '.',
   
    for i in range(N):
        test_process_init_fail()
        print '.',

    for i in range(N):
        test_process_parent_fail()
        print '.',
   
    for i in range(N):
        test_process_basic()
        print '.',
   
    sys.exit()


# ------------------------------------------------------------------------------

