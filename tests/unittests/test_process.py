
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


''' 
Unit tests for ru.Process()
'''

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
        def __init__(self, log):
            ru.Process.__init__(self, log)
        def work(self):
            time.sleep(1)
            sys.exit(0) # only run once!

    log = ru.get_logger('radical.util')
    p = P(log)
    start = time.time()
    p.start()
    p.join()
    stop = time.time()
    assert(stop-start > 1.0)
    assert(stop-start < 2.0)


# ------------------------------------------------------------------------------
#
def test_process_init_fail():
    '''
    make sure the parent gets notified on failing init
    '''

    class P(ru.Process):
        def __init__(self, log):
            ru.Process.__init__(self, log)
        def initialize(self):
            raise RuntimeError('oops')
        def work(self):
            time.sleep(0.1)

    log = ru.get_logger('radical.util')
    p = P(log)
    try:
        p.start()
    except RuntimeError as e:
        assert('oops' in str(e)), str(e)

    assert(not p.is_alive())

# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_process_init_fail()
    test_process_basic()


# ------------------------------------------------------------------------------

