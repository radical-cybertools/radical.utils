
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
            # only run once!
            sys.exit(0)

    log = ru.get_logger('radical.util')
    p = P(log)
    start = time.time()
    p.start()
    p.join()
    stop = time.time()
    assert(stop-start > 1.0)
    assert(stop-start < 2.0)

# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_process_basic()


# ------------------------------------------------------------------------------

